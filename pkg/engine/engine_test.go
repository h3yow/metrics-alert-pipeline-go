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

func highCPURuleWithResolve(forD, resolveD time.Duration) model.Rule {
	r := highCPURule(forD)
	r.ResolveDuration = resolveD
	return r
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

func TestMultiCycleReTrigger(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(10 * time.Second)})

	var all []model.Event
	feed := func(v float64, sec int) {
		evs := e.Observe(cpuSample("a", v, sec), tsec(sec))
		all = append(all, evs...)
		for _, ev := range evs {
			cyc := 0
			if ev.State != nil {
				cyc = ev.State.Cycle
			}
			t.Logf("input v=%.0f @%ds -> event=%s cycle=%d basis=%s",
				v, sec, ev.Type, cyc,
				map[model.EventType]string{
					model.EventPending:  "condition newly satisfied, wait starts",
					model.EventFiring:   "condition held >= for duration",
					model.EventResolved: "contradicting sample while firing",
				}[ev.Type])
		}
	}

	// Three full trigger/recovery rounds on the same series.
	for round := 0; round < 3; round++ {
		base := round * 100
		feed(95, base+1)  // pending
		feed(96, base+11) // firing (held 10s)
		feed(99, base+12) // no-op while firing
		feed(50, base+20) // resolves
	}

	if got := countType(all, model.EventFiring); got != 3 {
		t.Fatalf("want 3 firing events across 3 rounds, got %d", got)
	}
	if got := countType(all, model.EventResolved); got != 3 {
		t.Fatalf("want 3 resolved events across 3 rounds, got %d", got)
	}
	// Each round must be distinguishable: cycles 1, 2, 3 exactly once each.
	fireCycles, resolveCycles := map[int]int{}, map[int]int{}
	for _, ev := range all {
		switch ev.Type {
		case model.EventFiring:
			fireCycles[ev.State.Cycle]++
		case model.EventResolved:
			resolveCycles[ev.State.Cycle]++
		}
	}
	for c := 1; c <= 3; c++ {
		if fireCycles[c] != 1 || resolveCycles[c] != 1 {
			t.Fatalf("cycle %d must fire/resolve exactly once: fire=%d resolve=%d",
				c, fireCycles[c], resolveCycles[c])
		}
	}
	t.Logf("basis: rounds are numbered 1..3, each with exactly one firing + one recovery")
}

func TestResolveFlapDoesNotResolve(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURuleWithResolve(10*time.Second, 10*time.Second)})

	feed := func(v float64, sec int) []model.Event {
		evs := e.Observe(cpuSample("a", v, sec), tsec(sec))
		t.Logf("input v=%.0f @%ds -> events=%v", v, sec, eventTypes(evs))
		return evs
	}

	feed(95, 1)  // pending
	feed(96, 11) // firing
	feed(50, 15) // first contradiction: recovery hold starts, NOT resolved
	if evs := feed(50, 18); len(evs) != 0 {
		t.Fatalf("contradiction inside resolve hold must not resolve: %v", eventTypes(evs))
	}
	// Glitch back to satisfied at 20s: recovery hold cancelled silently.
	if evs := feed(97, 20); len(evs) != 0 {
		t.Fatalf("re-satisfied condition must cancel recovery hold silently: %v", eventTypes(evs))
	}
	if st := e.States(); len(st) != 1 || st[0].Status != model.StatusFiring || !st[0].ResolveSince.IsZero() {
		t.Fatalf("should be plain firing after cancelled hold: %+v", e.States())
	}
	// A new contradiction restarts the hold from scratch (20s+? no: 25s).
	feed(50, 25)
	if evs := feed(50, 34); len(evs) != 0 {
		t.Fatalf("hold restarted at 25s, 34s is too early: %v", eventTypes(evs))
	}
	evs := feed(50, 35) // 25+10: continuous contradiction for 10s
	if len(evs) != 1 || evs[0].Type != model.EventResolved {
		t.Fatalf("sustained contradiction must resolve exactly once: %v", eventTypes(evs))
	}
	t.Logf("basis: recovery needs continuous contradiction >= resolve duration; " +
		"a satisfied sample in between cancels the hold without any notification")
}

func TestResolveDurationViaTick(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURuleWithResolve(10*time.Second, 30*time.Second)})

	e.Observe(cpuSample("a", 95, 1), tsec(1))
	e.Observe(cpuSample("a", 96, 11), tsec(11)) // firing
	e.Observe(cpuSample("a", 40, 20), tsec(20)) // hold starts at 20
	t.Logf("input: 95@1 96@11 -> firing; 40@20 -> recovery hold starts (resolve=30s)")

	if evs := e.Tick(tsec(49)); hasType(evs, model.EventResolved) {
		t.Fatal("tick before resolve duration must not resolve")
	}
	evs := e.Tick(tsec(50)) // 20+30, last value still contradicting
	if !hasType(evs, model.EventResolved) {
		t.Fatalf("tick at resolve duration with contradicting last value must resolve: %v", eventTypes(evs))
	}
	if evs[0].State.Cycle != 1 {
		t.Fatalf("resolution must carry cycle 1, got %d", evs[0].State.Cycle)
	}
	t.Logf("basis: time-driven tick resolves only after contradiction held >= resolve duration")
}

func TestResolveDurationIndependentOfFor(t *testing.T) {
	e := New()
	// for=0 (immediate trigger) but resolve=20s: the two durations must not
	// influence each other.
	e.SetRules([]model.Rule{highCPURuleWithResolve(0, 20*time.Second)})

	if evs := e.Observe(cpuSample("a", 95, 1), tsec(1)); !hasType(evs, model.EventFiring) {
		t.Fatal("for=0 must fire immediately")
	}
	e.Observe(cpuSample("a", 10, 5), tsec(5)) // hold starts
	if evs := e.Observe(cpuSample("a", 10, 24), tsec(24)); hasType(evs, model.EventResolved) {
		t.Fatal("resolve duration (20s from 5s) not yet elapsed at 24s")
	}
	if evs := e.Observe(cpuSample("a", 10, 25), tsec(25)); !hasType(evs, model.EventResolved) {
		t.Fatal("resolve duration elapsed at 25s, must resolve")
	}
	t.Logf("basis: for=0 fires instantly while resolve=20s still guards recovery")
}

func TestPendingFlapDoesNotBurnCycle(t *testing.T) {
	e := New()
	e.SetRules([]model.Rule{highCPURule(10 * time.Second)})

	// Two pending episodes reset by flaps, then a real firing: it must be
	// cycle 1, because a round only exists once the alert actually fires.
	e.Observe(cpuSample("a", 95, 1), tsec(1))
	e.Observe(cpuSample("a", 50, 3), tsec(3)) // flap resets pending
	e.Observe(cpuSample("a", 95, 5), tsec(5))
	e.Observe(cpuSample("a", 50, 7), tsec(7)) // flap again
	e.Observe(cpuSample("a", 95, 10), tsec(10))
	evs := e.Observe(cpuSample("a", 96, 20), tsec(20)) // held 10s -> firing
	if !hasType(evs, model.EventFiring) {
		t.Fatalf("expected firing: %v", eventTypes(evs))
	}
	if got := evs[0].State.Cycle; got != 1 {
		t.Fatalf("pending flaps must not consume round numbers, firing cycle=%d want 1", got)
	}
	t.Logf("input: 95@1 50@3 95@5 50@7 95@10 96@20; basis: cycle numbers count "+
		"actual firing rounds, first firing is cycle 1 (got %d)", evs[0].State.Cycle)
}
