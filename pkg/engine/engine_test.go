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
