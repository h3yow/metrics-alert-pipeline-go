package engine

import (
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

func tsec(s int) time.Time { return time.Unix(int64(s), 0) }

func cpuSample(host string, v float64, sec int) model.Sample {
	return model.Sample{
		Metric:    "cpu.usage",
		Labels:    model.Labels{{Name: "host", Value: host}},
		Value:     v,
		Timestamp: tsec(sec),
		Source:    "node",
	}
}

func highCPURule(forD time.Duration) model.Rule {
	return model.Rule{
		ID: "high_cpu", Metric: "cpu.usage", Operator: model.OpGt,
		Threshold: 90, ForDuration: forD,
	}
}

// typesOf returns the ordered event types, joining Observe+Tick semantics.
func eventTypes(evs []model.Event) []model.EventType {
	out := make([]model.EventType, 0, len(evs))
	for _, ev := range evs {
		out = append(out, ev.Type)
	}
	return out
}

func hasType(evs []model.Event, t model.EventType) bool {
	for _, ev := range evs {
		if ev.Type == t {
			return true
		}
	}
	return false
}

func TestPendingFlapNeverFires(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(15 * time.Second)})

	// True, then a contradicting sample before 15s: the wait timer resets.
	t.Logf("input: 95@1s -> 50@8s -> 98@12s; basis: flap resets pending timer")
	e.Observe(cpuSample("a", 95, 1), tsec(1))
	e.Observe(cpuSample("a", 50, 8), tsec(8))
	e.Observe(cpuSample("a", 98, 12), tsec(12))
	if evs := e.Tick(tsec(20)); hasType(evs, model.EventFiring) {
		t.Fatalf("flapping condition must not fire at 20s: %v", eventTypes(evs))
	}
	if len(e.States()) != 1 || e.States()[0].Status != model.StatusPending {
		t.Fatalf("state should be pending after flap: %+v", e.States())
	}
	// New 15s stretch elapsed (12 -> 30).
	if evs := e.Tick(tsec(30)); !hasType(evs, model.EventFiring) {
		t.Fatalf("should fire after sustained stretch: %v", eventTypes(evs))
	}
}

func TestFiresAndResolvesExactlyOnce(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(10 * time.Second)})

	t.Logf("input: 95@1 95@5 95@11; basis: pending at 1, firing once at 11")
	if evs := e.Observe(cpuSample("a", 95, 1), tsec(1)); !hasType(evs, model.EventPending) {
		t.Fatalf("expected pending: %v", eventTypes(evs))
	}
	e.Observe(cpuSample("a", 96, 5), tsec(5))
	evs := e.Observe(cpuSample("a", 97, 11), tsec(11))
	if !hasType(evs, model.EventFiring) {
		t.Fatalf("expected firing at 11s: %v", eventTypes(evs))
	}

	// Repeated high reports while firing: no duplicate alerts.
	for _, sec := range []int{12, 13, 14} {
		if evs := e.Observe(cpuSample("a", 99, sec), tsec(sec)); len(evs) != 0 {
			t.Fatalf("repeated firing reports must be no-op, got %v", eventTypes(evs))
		}
	}
	t.Logf("input: 99@12..14; basis: repeated reports while firing produce no events")

	// Recovery happens exactly once.
	evs = e.Observe(cpuSample("a", 70, 20), tsec(20))
	if len(evs) != 1 || evs[0].Type != model.EventResolved {
		t.Fatalf("expected exactly one resolution, got %v", eventTypes(evs))
	}
	t.Logf("input: 70@20; basis: one and only one resolution")
	if len(e.States()) != 0 {
		t.Fatalf("state removed after resolution: %+v", e.States())
	}
}

func TestForZeroFiresImmediately(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(0)})
	evs := e.Observe(cpuSample("a", 95, 1), tsec(1))
	if !hasType(evs, model.EventPending) || !hasType(evs, model.EventFiring) {
		t.Fatalf("for=0 must go pending then firing immediately: %v", eventTypes(evs))
	}
}

func TestRuleChangeIsolatesState(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(0)})
	e.Observe(cpuSample("a", 95, 1), tsec(1)) // firing v1

	// Change threshold -> version bump. SetRules resolves the old firing
	// exactly once; the first observation under v2 evaluates the new rule.
	resolved := e.SetRules([]model.Rule{{
		ID: "high_cpu", Metric: "cpu.usage", Operator: model.OpGt,
		Threshold: 50, ForDuration: 0,
	}})
	if len(resolved) != 1 || resolved[0].Status != model.StatusInactive || resolved[0].RuleVersion != 1 {
		t.Fatalf("expected one v1 resolution at rule change, got %+v", resolved)
	}
	evs := e.Observe(cpuSample("a", 60, 2), tsec(2))
	if !hasType(evs, model.EventPending) || !hasType(evs, model.EventFiring) {
		t.Fatalf("expected new pending+firing under v2: %v", eventTypes(evs))
	}
	st := e.States()
	if len(st) != 1 || st[0].RuleVersion != 2 {
		t.Fatalf("new state must carry v2: %+v", st)
	}
	t.Logf("basis: rule revision produces isolated state, got version=%d", st[0].RuleVersion)
}

func TestRemovedRuleDropsStates(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(0)})
	e.Observe(cpuSample("a", 95, 1), tsec(1))
	e.SetRules(nil)
	if len(e.States()) != 0 {
		t.Fatalf("removed rule states must be dropped: %+v", e.States())
	}
}

func TestTickPromotesPending(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(10 * time.Second)})
	e.Observe(cpuSample("a", 95, 0), tsec(0))
	if evs := e.Tick(tsec(9)); hasType(evs, model.EventFiring) {
		t.Fatal("must not fire before for-duration")
	}
	if evs := e.Tick(tsec(10)); !hasType(evs, model.EventFiring) {
		t.Fatalf("tick at exactly for-duration must promote: %v", eventTypes(evs))
	}
}

func countType(evs []model.Event, t model.EventType) int {
	n := 0
	for _, ev := range evs {
		if ev.Type == t {
			n++
		}
	}
	return n
}

func logEvents(t *testing.T, tag string, evs []model.Event) {
	t.Helper()
	for _, ev := range evs {
		cycle := 0
		if ev.State != nil {
			cycle = ev.State.Cycle
		}
		t.Logf("  [%s] event=%s cycle=%d basis=%s", tag, ev.Type, cycle, ev.Detail)
	}
}

func TestMultiCycleTriggerResolve(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(10 * time.Second)})

	// Round 1: fire then recover.
	t.Logf("round1 input: 95@1 95@12 (fire) 70@20 (recover)")
	evs := e.Observe(cpuSample("a", 95, 1), tsec(1))
	logEvents(t, "r1", evs)
	evs = e.Observe(cpuSample("a", 95, 12), tsec(12))
	logEvents(t, "r1", evs)
	if countType(evs, model.EventFiring) != 1 || evs[0].State.Cycle != 1 {
		t.Fatalf("round1 must fire exactly once with cycle=1: %v", eventTypes(evs))
	}
	evs = e.Observe(cpuSample("a", 70, 20), tsec(20))
	logEvents(t, "r1", evs)
	if countType(evs, model.EventResolved) != 1 || evs[0].State.Cycle != 1 {
		t.Fatalf("round1 must resolve exactly once with cycle=1: %v", eventTypes(evs))
	}

	// Round 2: the same series triggers again and must go through the full
	// flow like the first time, with a new cycle number.
	t.Logf("round2 input: 96@30 96@41 (fire) 60@50 (recover)")
	e.Observe(cpuSample("a", 96, 30), tsec(30))
	evs = e.Observe(cpuSample("a", 96, 41), tsec(41))
	logEvents(t, "r2", evs)
	if countType(evs, model.EventFiring) != 1 || evs[len(evs)-1].State.Cycle != 2 {
		t.Fatalf("round2 must fire with cycle=2, got %v", eventTypes(evs))
	}
	evs = e.Observe(cpuSample("a", 60, 50), tsec(50))
	logEvents(t, "r2", evs)
	if countType(evs, model.EventResolved) != 1 || evs[0].State.Cycle != 2 {
		t.Fatalf("round2 must resolve with cycle=2, got %v", eventTypes(evs))
	}

	// Round 3 via Tick-driven promotion still increments the cycle.
	t.Logf("round3 input: 99@60 then Tick@75 (fire by tick)")
	e.Observe(cpuSample("a", 99, 60), tsec(60))
	evs = e.Tick(tsec(75))
	logEvents(t, "r3", evs)
	if countType(evs, model.EventFiring) != 1 || evs[0].State.Cycle != 3 {
		t.Fatalf("round3 must fire with cycle=3, got %v", eventTypes(evs))
	}
}

func TestResolveFlapDoesNotRecover(t *testing.T) {
	r := highCPURule(0)
	r.ResolveDuration = 10 * time.Second
	e := New()
	e.SetRules([]model.Rule{r})

	t.Logf("input: 95@1 (fire, for=0)")
	evs := e.Observe(cpuSample("a", 95, 1), tsec(1))
	logEvents(t, "fire", evs)
	if countType(evs, model.EventFiring) != 1 {
		t.Fatalf("must fire immediately: %v", eventTypes(evs))
	}

	// One contradicting spike only starts the recovery wait.
	t.Logf("input: 70@5 (contradiction starts recovery wait, no resolve)")
	evs = e.Observe(cpuSample("a", 70, 5), tsec(5))
	logEvents(t, "spike", evs)
	if countType(evs, model.EventResolved) != 0 || countType(evs, model.EventResolving) != 1 {
		t.Fatalf("single spike must not resolve, got %v", eventTypes(evs))
	}
	if st := e.States(); len(st) != 1 || st[0].Status != model.StatusResolving {
		t.Fatalf("state should be resolving: %+v", st)
	}

	// Condition satisfied again before resolve_for elapses: back to firing,
	// as if the recovery never started, with no notification.
	t.Logf("input: 96@8 (satisfied again -> back to firing, no events)")
	evs = e.Observe(cpuSample("a", 96, 8), tsec(8))
	logEvents(t, "rearm", evs)
	if len(evs) != 0 {
		t.Fatalf("flap back to firing must produce no events, got %v", eventTypes(evs))
	}
	if st := e.States(); len(st) != 1 || st[0].Status != model.StatusFiring || st[0].Cycle != 1 {
		t.Fatalf("state should be firing cycle=1: %+v", st)
	}

	// A new contradiction restarts the recovery wait from scratch.
	t.Logf("input: 60@12 60@15 (wait restarts) then 60@22 (held 10s -> resolve once)")
	e.Observe(cpuSample("a", 60, 12), tsec(12))
	evs = e.Observe(cpuSample("a", 60, 15), tsec(15))
	if countType(evs, model.EventResolved) != 0 {
		t.Fatalf("must not resolve before resolve_for: %v", eventTypes(evs))
	}
	evs = e.Observe(cpuSample("a", 60, 22), tsec(22))
	logEvents(t, "resolve", evs)
	if countType(evs, model.EventResolved) != 1 || evs[len(evs)-1].State.Cycle != 1 {
		t.Fatalf("must resolve exactly once after sustained contradiction: %v", eventTypes(evs))
	}
	if len(e.States()) != 0 {
		t.Fatalf("state removed after resolution: %+v", e.States())
	}
}

func TestResolveDurationIndependentOfFor(t *testing.T) {
	r := highCPURule(30 * time.Second)
	r.ResolveDuration = 5 * time.Second
	e := New()
	e.SetRules([]model.Rule{r})

	t.Logf("input: 95@0 95@30 (fire after for=30s); 50@35 50@40 (resolve after resolve_for=5s)")
	e.Observe(cpuSample("a", 95, 0), tsec(0))
	if evs := e.Tick(tsec(29)); countType(evs, model.EventFiring) != 0 {
		t.Fatal("must not fire before for=30s")
	}
	if evs := e.Tick(tsec(30)); countType(evs, model.EventFiring) != 1 {
		t.Fatal("must fire at for=30s")
	}
	evs := e.Observe(cpuSample("a", 50, 35), tsec(35))
	logEvents(t, "wait", evs)
	if countType(evs, model.EventResolved) != 0 {
		t.Fatalf("must not resolve on first contradiction: %v", eventTypes(evs))
	}
	evs = e.Observe(cpuSample("a", 50, 40), tsec(40))
	logEvents(t, "resolve", evs)
	if countType(evs, model.EventResolved) != 1 {
		t.Fatalf("resolve_for=5s must resolve at +5s, independent of for=30s: %v", eventTypes(evs))
	}
}

func TestTickDrivenResolution(t *testing.T) {
	r := highCPURule(0)
	r.ResolveDuration = 10 * time.Second
	e := New()
	e.SetRules([]model.Rule{r})

	t.Logf("input: 95@1 (fire) 60@5 (recovery wait starts); Tick@14 no, Tick@15 yes")
	e.Observe(cpuSample("a", 95, 1), tsec(1))
	e.Observe(cpuSample("a", 60, 5), tsec(5))
	if evs := e.Tick(tsec(14)); countType(evs, model.EventResolved) != 0 {
		t.Fatalf("must not resolve before resolve_for elapses: %v", eventTypes(evs))
	}
	evs := e.Tick(tsec(15))
	logEvents(t, "tick", evs)
	if countType(evs, model.EventResolved) != 1 {
		t.Fatalf("tick at resolve_for must resolve: %v", eventTypes(evs))
	}
}

func TestResolveZeroKeepsImmediateRecovery(t *testing.T) {
	// Compatibility: ResolveDuration=0 keeps the historical behavior of
	// resolving on the first contradicting sample.
	e := New()
	e.SetRules([]model.Rule{highCPURule(0)})
	e.Observe(cpuSample("a", 95, 1), tsec(1))
	evs := e.Observe(cpuSample("a", 10, 2), tsec(2))
	logEvents(t, "compat", evs)
	if countType(evs, model.EventResolved) != 1 || countType(evs, model.EventResolving) != 0 {
		t.Fatalf("resolve_for=0 must resolve immediately without resolving state: %v", eventTypes(evs))
	}
}
