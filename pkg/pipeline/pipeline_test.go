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

// evSink records full events for cycle-aware assertions.
type evSink struct {
	mu  sync.Mutex
	evs []model.Event
}

func (s *evSink) Notify(ev model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evs = append(s.evs, ev)
}

func (s *evSink) delivered() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.Event(nil), s.evs...)
}

func ruleWithResolve() model.Rule {
	r := rule()
	r.ResolveDuration = 10 * time.Second
	return r
}

// multiCycleSet drives host a through three full trigger/recovery rounds:
// round 1 notified, round 2 firing silenced (window [40,60)), round 3
// notified again. Round 1's recovery includes a glitch back to satisfied.
func multiCycleSet() []model.Sample {
	return []model.Sample{
		// round 1
		s1("cpu.usage", "a", "node-a", 95, 1),  // pending
		s1("cpu.usage", "a", "node-a", 96, 16), // firing c1 (held 15s)
		s1("cpu.usage", "a", "node-a", 50, 20), // recovery hold starts
		s1("cpu.usage", "a", "node-a", 99, 23), // glitch: hold cancelled, still firing
		s1("cpu.usage", "a", "node-a", 50, 25), // hold restarts
		s1("cpu.usage", "a", "node-a", 50, 35), // resolved c1 (held 10s)
		// round 2, firing falls into silence window [40,60)
		s1("cpu.usage", "a", "node-a", 95, 41), // pending
		s1("cpu.usage", "a", "node-a", 96, 56), // firing c2 (silenced)
		s1("cpu.usage", "a", "node-a", 40, 62), // hold starts (window over)
		s1("cpu.usage", "a", "node-a", 40, 72), // resolved c2
		// round 3
		s1("cpu.usage", "a", "node-a", 95, 80),  // pending
		s1("cpu.usage", "a", "node-a", 96, 95),  // firing c3
		s1("cpu.usage", "a", "node-a", 40, 100), // hold starts
		s1("cpu.usage", "a", "node-a", 40, 110), // resolved c3
	}
}

func newMultiCyclePipeline(sink *evSink) *Pipeline {
	p := New(Config{WindowSize: 10 * time.Second}, sink)
	p.SetRules([]model.Rule{ruleWithResolve()})
	p.SetSilences([]model.Silence{{
		ID: "maint", Metric: "cpu.usage", Match: map[string]string{"host": "a"},
		StartsAt: at(40), EndsAt: at(60),
	}})
	return p
}

func TestMultiCycleNotificationsExactlyOncePerRound(t *testing.T) {
	sink := &evSink{}
	p := newMultiCyclePipeline(sink)
	for _, s := range multiCycleSet() {
		evs := p.Ingest(s)
		for _, ev := range evs {
			cyc := 0
			if ev.State != nil {
				cyc = ev.State.Cycle
			}
			t.Logf("input v=%.0f ts=%s -> event=%s cycle=%d detail=%q",
				s.Value, s.Timestamp.Format("15:04:05"), ev.Type, cyc, ev.Detail)
		}
	}

	st := p.Stats()
	if st.Firing != 3 || st.Resolved != 3 {
		t.Fatalf("3 rounds must produce 3 firing + 3 resolved transitions, got %+v", st)
	}
	// Round 2's firing is silenced: 5 notified, 1 suppressed. Silence must
	// not eat round 3's notifications.
	if st.Notified != 5 || st.Suppressed != 1 {
		t.Fatalf("want notified=5 suppressed=1, got %+v", st)
	}
	// Every delivered notification carries its round number; each round
	// contributes exactly one firing and/or one recovery.
	type key struct {
		typ   model.EventType
		cycle int
	}
	counts := map[key]int{}
	for _, ev := range sink.delivered() {
		counts[key{ev.Type, ev.State.Cycle}]++
	}
	want := map[key]int{
		{model.EventFiring, 1}: 1, {model.EventResolved, 1}: 1,
		{model.EventResolved, 2}: 1, // round-2 firing was silenced
		{model.EventFiring, 3}:   1, {model.EventResolved, 3}: 1,
	}
	for k, w := range want {
		if counts[k] != w {
			t.Fatalf("delivered %v = %d, want %d (all=%v)", k, counts[k], w, counts)
		}
	}
	t.Logf("basis: 3 rounds x (firing+recovery) with round-2 firing silenced -> " +
		"notified=5 suppressed=1, per-round exactly-once holds")
}

func TestMultiCycleConcurrentEqualsSerial(t *testing.T) {
	set := multiCycleSet()

	ser := newMultiCyclePipeline(&evSink{})
	for _, s := range set {
		ser.Ingest(s)
	}

	conc := newMultiCyclePipeline(&evSink{})
	var wg sync.WaitGroup
	for _, s := range set {
		wg.Add(1)
		go func(s model.Sample) { defer wg.Done(); conc.Ingest(s) }(s)
	}
	wg.Wait()

	e1, st1, _, states1 := ser.Replay()
	e2, st2, _, states2 := conc.Replay()
	if st1 != st2 || len(e1) != len(e2) || fmt.Sprint(states1) != fmt.Sprint(states2) {
		t.Fatalf("concurrent and serial replay diverged:\nserial=%+v (%d events)\nconc=%+v (%d events)",
			st1, len(e1), st2, len(e2))
	}
	// Replay conclusions must also match the live serial run.
	live := ser.Stats()
	if st1.Firing != live.Firing || st1.Resolved != live.Resolved ||
		st1.Notified != live.Notified || st1.Suppressed != live.Suppressed {
		t.Fatalf("replay stats %+v must match live stats %+v", st1, live)
	}
	t.Logf("basis: replay sorted by (timestamp, identity); serial==concurrent==live: %+v", st1)
}

func TestMultiCycleDedupRetentionBounded(t *testing.T) {
	sink := &evSink{}
	p := New(Config{WindowSize: 10 * time.Second, DedupRetainCycles: 2}, sink)
	p.SetRules([]model.Rule{ruleWithResolve()})

	// 6 rapid rounds on the same series.
	for round := 0; round < 6; round++ {
		b := round * 100
		for _, s := range []model.Sample{
			s1("cpu.usage", "a", "node-a", 95, b+1),  // pending
			s1("cpu.usage", "a", "node-a", 96, b+16), // firing
			s1("cpu.usage", "a", "node-a", 40, b+20), // hold
			s1("cpu.usage", "a", "node-a", 40, b+30), // resolved
		} {
			p.Ingest(s)
		}
	}
	st := p.Stats()
	if st.Firing != 6 || st.Resolved != 6 || st.Notified != 12 {
		t.Fatalf("6 rounds must notify 12 times even with retain=2, got %+v", st)
	}
	t.Logf("basis: dedup retention=2 rounds bounds memory but every one of the "+
		"6 rounds still notified firing+recovery exactly once: %+v", st)
}
