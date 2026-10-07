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

// notifyRecord captures one delivered notification for assertions.
type notifyRecord struct {
	typ   model.EventType
	host  string
	cycle int
}

type roundSink struct {
	mu   sync.Mutex
	recs []notifyRecord
}

func (r *roundSink) Notify(ev model.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ev.State == nil {
		return
	}
	host, _ := ev.State.Labels.Get("host")
	r.recs = append(r.recs, notifyRecord{typ: ev.Type, host: host, cycle: ev.State.Cycle})
}

func (r *roundSink) count(typ model.EventType, host string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, x := range r.recs {
		if x.typ == typ && x.host == host {
			n++
		}
	}
	return n
}

func newRoundPipeline(sink *roundSink, forD, resolveD time.Duration, dedupRounds int) *Pipeline {
	p := New(Config{WindowSize: 10 * time.Second, DedupRounds: dedupRounds}, sink)
	p.SetRules([]model.Rule{{
		ID: "high_cpu", Metric: "cpu.usage", Operator: model.OpGt,
		Threshold: 90, ForDuration: forD, ResolveDuration: resolveD,
	}})
	return p
}

func ingestLogged(t *testing.T, p *Pipeline, s model.Sample) {
	t.Helper()
	evs := p.Ingest(s)
	for _, ev := range evs {
		cycle := 0
		if ev.State != nil {
			cycle = ev.State.Cycle
		}
		t.Logf("input metric=%s v=%.0f ts=%s -> event=%s cycle=%d basis=%s",
			s.Metric, s.Value, s.Timestamp.Format("15:04:05"), ev.Type, cycle, ev.Detail)
	}
}

func TestMultiRoundNotificationsExactlyOnce(t *testing.T) {
	sink := &roundSink{}
	p := newRoundPipeline(sink, 10*time.Second, 0, 0)

	// Two full trigger/recover rounds on host a.
	script := []model.Sample{
		s1("cpu.usage", "a", "node-a", 95, 0),   // round1 pending
		s1("cpu.usage", "a", "node-a", 96, 10),  // round1 fires (cycle 1)
		s1("cpu.usage", "a", "node-a", 97, 12),  // no-op while firing
		s1("cpu.usage", "a", "node-a", 50, 20),  // round1 resolves (cycle 1)
		s1("cpu.usage", "a", "node-a", 98, 30),  // round2 pending
		s1("cpu.usage", "a", "node-a", 99, 41),  // round2 fires (cycle 2)
		s1("cpu.usage", "a", "node-a", 40, 55),  // round2 resolves (cycle 2)
	}
	for _, s := range script {
		ingestLogged(t, p, s)
	}
	// Duplicate re-ingestion of the firing sample must not re-notify.
	ingestLogged(t, p, s1("cpu.usage", "a", "node-a", 99, 41))

	if n := sink.count(model.EventFiring, "a"); n != 2 {
		t.Fatalf("want exactly 2 firing notifications (one per round), got %d", n)
	}
	if n := sink.count(model.EventResolved, "a"); n != 2 {
		t.Fatalf("want exactly 2 recovery notifications (one per round), got %d", n)
	}
	// Each round is distinguishable by its cycle number.
	cycles := map[int]bool{}
	for _, r := range sink.recs {
		cycles[r.cycle] = true
	}
	if !cycles[1] || !cycles[2] {
		t.Fatalf("notifications must carry their round number, got %+v", sink.recs)
	}
	st := p.Stats()
	if st.Firing != 2 || st.Resolved != 2 {
		t.Fatalf("stats must count both rounds: %+v", st)
	}
	t.Logf("basis: per-round exactly-once; records=%+v stats=%+v", sink.recs, st)
}

func TestRecoveryFlapDoesNotNotify(t *testing.T) {
	sink := &roundSink{}
	p := newRoundPipeline(sink, 0, 10*time.Second, 0)

	script := []model.Sample{
		s1("cpu.usage", "a", "node-a", 95, 0),  // fires (for=0)
		s1("cpu.usage", "a", "node-a", 50, 5),  // spike: recovery wait starts
		s1("cpu.usage", "a", "node-a", 96, 8),  // satisfied again: cancel recovery
		s1("cpu.usage", "a", "node-a", 40, 12), // wait restarts
		s1("cpu.usage", "a", "node-a", 93, 15), // flap again: still firing
		s1("cpu.usage", "a", "node-a", 30, 20), // wait restarts
		s1("cpu.usage", "a", "node-a", 30, 30), // held 10s -> resolve once
	}
	for _, s := range script {
		ingestLogged(t, p, s)
	}
	if n := sink.count(model.EventFiring, "a"); n != 1 {
		t.Fatalf("flapping recovery must not re-fire, got %d firings", n)
	}
	if n := sink.count(model.EventResolved, "a"); n != 1 {
		t.Fatalf("want exactly 1 recovery after sustained contradiction, got %d", n)
	}
	st := p.Stats()
	if st.Firing != 1 || st.Resolved != 1 {
		t.Fatalf("stats: %+v", st)
	}
	t.Logf("basis: recovery flaps cancelled; notified firing=%d resolved=%d",
		sink.count(model.EventFiring, "a"), sink.count(model.EventResolved, "a"))
}

func TestSilenceAcrossRounds(t *testing.T) {
	sink := &roundSink{}
	p := newRoundPipeline(sink, 0, 0, 0)
	// Silence covers [0, 30): round 1 firing is suppressed, round 2 is not.
	p.SetSilences([]model.Silence{{
		ID: "maint", Metric: "cpu.usage", Match: map[string]string{"host": "a"},
		StartsAt: at(0), EndsAt: at(30),
	}})

	script := []model.Sample{
		s1("cpu.usage", "a", "node-a", 95, 5),  // round1 fires, silenced
		s1("cpu.usage", "a", "node-a", 50, 20), // round1 resolves, silenced
		s1("cpu.usage", "a", "node-a", 96, 35), // round2 fires, window over
		s1("cpu.usage", "a", "node-a", 40, 45), // round2 resolves
	}
	for _, s := range script {
		ingestLogged(t, p, s)
	}
	st := p.Stats()
	// Silence must not touch state/rounds/stats: both rounds counted.
	if st.Firing != 2 || st.Resolved != 2 {
		t.Fatalf("silence must not affect state/stats: %+v", st)
	}
	if st.Suppressed != 2 {
		t.Fatalf("round1 firing+recovery suppressed, want 2 got %d", st.Suppressed)
	}
	if n := sink.count(model.EventFiring, "a"); n != 1 {
		t.Fatalf("round2 firing must be delivered after silence ends, got %d", n)
	}
	if n := sink.count(model.EventResolved, "a"); n != 1 {
		t.Fatalf("round2 recovery must be delivered, got %d", n)
	}
	t.Logf("basis: silence only suppresses delivery inside window; stats=%+v", st)
}

func TestConcurrentMultiRoundEqualsSerial(t *testing.T) {
	// Multi-round script with recovery flapping and a silence window.
	build := func() *Pipeline {
		sink := &roundSink{}
		p := newRoundPipeline(sink, 10*time.Second, 5*time.Second, 4)
		p.SetSilences([]model.Silence{{
			ID: "m-b", Metric: "cpu.usage", Match: map[string]string{"host": "b"},
			StartsAt: at(0), EndsAt: at(100),
		}})
		return p
	}
	script := []model.Sample{
		s1("cpu.usage", "a", "node-a", 95, 0),
		s1("cpu.usage", "a", "node-a", 96, 10), // a round1 fires
		s1("cpu.usage", "a", "node-a", 50, 15), // recovery wait starts
		s1("cpu.usage", "a", "node-a", 97, 18), // flap: cancel recovery
		s1("cpu.usage", "a", "node-a", 40, 22), // wait restarts
		s1("cpu.usage", "a", "node-a", 40, 28), // a round1 resolves
		s1("cpu.usage", "a", "node-a", 98, 40), // a round2
		s1("cpu.usage", "a", "node-a", 99, 51), // a round2 fires
		s1("cpu.usage", "a", "node-a", 30, 60),
		s1("cpu.usage", "a", "node-a", 30, 66), // a round2 resolves
		s1("cpu.usage", "b", "node-b", 99, 0),
		s1("cpu.usage", "b", "node-b", 99, 10), // b fires, silenced
		s1("cpu.usage", "b", "node-b", 10, 20),
		s1("cpu.usage", "b", "node-b", 10, 26), // b resolves, silenced
		s1("cpu.usage", "b", "node-b", 99, 120), // b round2, silence over
		s1("cpu.usage", "b", "node-b", 99, 131), // b round2 fires
	}

	ser := build()
	for _, s := range script {
		ser.Ingest(s)
	}

	conc := build()
	var wg sync.WaitGroup
	for _, s := range script {
		wg.Add(1)
		go func(s model.Sample) { defer wg.Done(); conc.Ingest(s) }(s)
	}
	wg.Wait()

	e1, st1, _, states1 := ser.Replay()
	e2, st2, _, states2 := conc.Replay()
	if st1 != st2 || len(e1) != len(e2) || fmt.Sprint(states1) != fmt.Sprint(states2) {
		t.Fatalf("concurrent vs serial replay differ:\nserial=%+v (%d events)\nconc=%+v (%d events)",
			st1, len(e1), st2, len(e2))
	}
	// Conclusions: a fires twice + resolves twice; b round1 silenced,
	// round2 firing delivered after the window.
	if st1.Notified != 5 || st1.Suppressed != 2 {
		t.Fatalf("unexpected replay stats: %+v", st1)
	}
	for _, ev := range e1 {
		if ev.Type == model.EventNotified || ev.Type == model.EventSuppressed {
			cycle := 0
			if ev.State != nil {
				cycle = ev.State.Cycle
			}
			t.Logf("replay event=%s cycle=%d basis=%s", ev.Type, cycle, ev.Detail)
		}
	}
	t.Logf("basis: concurrent==serial via replay; stats=%+v", st1)
}
