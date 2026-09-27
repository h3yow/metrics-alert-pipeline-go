package pipeline

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/notifier"
)

func base() time.Time      { return time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC) }
func at(sec int) time.Time { return base().Add(time.Duration(sec) * time.Second) }

func s1(metric, host, source string, v float64, sec int) model.Sample {
	return model.Sample{
		Metric: metric,
		Labels: model.Labels{{Name: "host", Value: host}},
		Value:  v, Timestamp: at(sec), Source: source,
	}
}

func rule() model.Rule {
	return model.Rule{
		ID: "high_cpu", Metric: "cpu.usage", Operator: model.OpGt,
		Threshold: 90, ForDuration: 15 * time.Second,
	}
}

// sampleSet mixes valid samples, a duplicate, illegal samples and a flap.
func sampleSet() []model.Sample {
	out := []model.Sample{
		s1("cpu.usage", "a", "node-a", 95, 5),
		s1("cpu.usage", "a", "node-a", 96, 10),
		s1("cpu.usage", "a", "node-a", 97, 25), // fires at 25 (5+15=20 <=25? sustained)
		s1("cpu.usage", "a", "node-a", 70, 40), // resolves
		s1("cpu.usage", "b", "node-b", 99, 5),
		s1("cpu.usage", "b", "node-b", 99, 25), // fires, silenced
		s1("cpu.usage", "c", "node-c", 98, 5),
		s1("cpu.usage", "c", "node-c", 50, 8), // flap resets pending
		s1("cpu.usage", "c", "node-c", 98, 12),
		s1("cpu.usage", "c", "node-c", 98, 30), // fires
		// illegal samples, distinguishable reasons
		{Metric: "", Labels: model.Labels{{Name: "host", Value: "x"}}, Value: 1, Timestamp: at(1), Source: "x"},
		{Metric: "m", Labels: model.Labels{{Name: "host", Value: "x"}}, Value: 1, Timestamp: time.Time{}, Source: "x"},
		{Metric: "m", Labels: model.Labels{{Name: "host", Value: "x"}}, Value: 1, Timestamp: at(1), Source: ""},
	}
	out = append(out,
		s1("cpu.usage", "a", "node-a", 96, 10), // exact duplicate
	)
	return out
}

type recSink struct{ names []string }

func (r *recSink) Notify(ev model.Event) {
	if ev.State != nil {
		host, _ := ev.State.Labels.Get("host")
		r.names = append(r.names, fmt.Sprintf("%s:%s", ev.Type, host))
	}
}

func newTestPipeline() *Pipeline {
	p := New(Config{WindowSize: 10 * time.Second,
		ExtraLabels: model.Labels{{Name: "env", Value: "local"}}},
		&recSink{})
	p.SetRules([]model.Rule{rule()})
	p.SetSilences([]model.Silence{{
		ID: "m-b", Metric: "cpu.usage", Match: map[string]string{"host": "b"},
		StartsAt: at(0), EndsAt: at(40),
	}})
	return p
}

func TestConcurrentIngestEqualsSerialViaReplay(t *testing.T) {
	// Two pipelines ingest the same set: one in timestamp order, one from
	// many goroutines in shuffled order. Aggregates and replay conclusions
	// must be identical.
	ordered := sampleSet()

	ser := newTestPipeline()
	for _, s := range ordered {
		t.Logf("[serial] input sample metric=%s host? source=%s v=%.0f ts=%s",
			s.Metric, s.Source, s.Value, s.Timestamp.Format("15:04:05"))
		evs := ser.Ingest(s)
		for _, ev := range evs {
			t.Logf("           basis/event: %s reason=%s", ev.Type, ev.Reason)
		}
	}

	shuffled := append([]model.Sample(nil), ordered...)
	// deterministic pseudo-shuffle
	for i := range shuffled {
		j := (i*7 + 3) % len(shuffled)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	conc := newTestPipeline()
	var wg sync.WaitGroup
	for _, s := range shuffled {
		wg.Add(1)
		go func(s model.Sample) { defer wg.Done(); conc.Ingest(s) }(s)
	}
	wg.Wait()

	// Aggregates are order-independent.
	if fmt.Sprint(ser.Aggregates()) != fmt.Sprint(conc.Aggregates()) {
		t.Fatalf("aggregates differ:\nserial=%v\nconc  =%v", ser.Aggregates(), conc.Aggregates())
	}

	// Live counters for accepted/rejected/duplicates are order-independent.
	st1, st2 := ser.Stats(), conc.Stats()
	if st1.Accepted != st2.Accepted || st1.Rejected != st2.Rejected || st1.Duplicates != st2.Duplicates {
		t.Fatalf("ingest counters differ: serial=%+v conc=%+v", st1, st2)
	}

	// Canonical judgments come from replay sorted by (ts, identity).
	e1, rs1, a1, states1 := ser.Replay()
	e2, rs2, a2, states2 := conc.Replay()
	if len(e1) != len(e2) || rs1 != rs2 {
		t.Fatalf("replay conclusions differ:\nserial events=%d stats=%+v\nconc events=%d stats=%+v",
			len(e1), rs1, len(e2), rs2)
	}
	if fmt.Sprint(a1) != fmt.Sprint(a2) || fmt.Sprint(states1) != fmt.Sprint(states2) {
		t.Fatal("replay aggregates or states differ")
	}
	t.Logf("basis: replay sorted by (timestamp, identity); notified=%d suppressed=%d finalStates=%d",
		rs1.Notified, rs1.Suppressed, len(states1))

	if st1.Accepted != 10 || st1.Rejected != 3 || st1.Duplicates != 1 {
		t.Fatalf("unexpected counters: %+v", st1)
	}
	// b's firing is silenced; a and c notify. Replay should also show b's
	// recovery outside the window as notified.
	if rs1.Notified < 2 || rs1.Suppressed != 1 {
		t.Fatalf("unexpected notify/suppress: %+v", rs1)
	}
}

func TestRejectedReasonsDistinguishable(t *testing.T) {
	p := New(Config{WindowSize: time.Minute}, notifier.RecorderFunc(func(model.Event) {}))
	seen := map[model.RejectReason]bool{}
	p.OnEvent(func(ev model.Event) {
		if ev.Type == model.EventRejected {
			seen[ev.Reason] = true
		}
	})
	for _, s := range sampleSet() {
		if s.Metric == "cpu.usage" {
			continue
		}
		evs := p.Ingest(s)
		t.Logf("input metric=%q source=%q zeroTS=%v; basis: reject reason=%q",
			s.Metric, s.Source, s.Timestamp.IsZero(), evs[0].Reason)
	}
	for _, want := range []model.RejectReason{
		model.ReasonEmptyName, model.ReasonBadTimestamp, model.ReasonEmptySource,
	} {
		if !seen[want] {
			t.Fatalf("missing distinguishable reject reason %q (got %v)", want, seen)
		}
	}
}

func TestReplayReproducesAfterSilenceChange(t *testing.T) {
	p := newTestPipeline()
	for _, s := range sampleSet() {
		p.Ingest(s)
	}
	_, before, _, _ := p.Replay()

	// Remove the silence window: the previously suppressed firing of host b
	// must now be notified, without mutating any alert state.
	liveStatesBefore := fmt.Sprint(p.States())
	p.SetSilences(nil)
	_, after, _, _ := p.Replay()
	if before.Suppressed != 1 || after.Suppressed != 0 {
		t.Fatalf("silence change not reflected: before=%+v after=%+v", before, after)
	}
	if after.Notified != before.Notified+1 {
		t.Fatalf("host-b firing should become notified: before=%d after=%d",
			before.Notified, after.Notified)
	}
	if liveStatesBefore != fmt.Sprint(p.States()) {
		t.Fatal("replay/config change must not mutate live states")
	}
	t.Logf("basis: replay with new silences -> suppressed %d -> %d, notified %d -> %d",
		before.Suppressed, after.Suppressed, before.Notified, after.Notified)
}
