package notifier

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

type fakeSilencer struct{ hit *model.Silence }

func (f fakeSilencer) Match(metric string, ls model.Labels, t time.Time) *model.Silence {
	return f.hit
}

type sink struct {
	mu  sync.Mutex
	got []model.Event
}

func (s *sink) Notify(ev model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, ev)
}

func firingEvent(host string, t int) model.Event {
	return model.Event{
		Type: model.EventFiring,
		Time: time.Unix(int64(t), 0),
		State: &model.AlertState{
			AlertKey: "rule:high_cpu#v1", SeriesKey: "cpu|" + host,
			Metric: "cpu.usage", Labels: model.Labels{{Name: "host", Value: host}},
		},
	}
}

func TestDedupStableKey(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil)

	e1 := n.Dispatch(firingEvent("a", 1))
	e2 := n.Dispatch(firingEvent("a", 2))
	if e1.Type != model.EventNotified || e2.Type != model.EventSuppressed {
		t.Fatalf("first should notify, duplicate should suppress: %s then %s", e1.Type, e2.Type)
	}
	t.Logf("basis: same stable key fired twice -> 1 notify + 1 dedup-suppress")
	if len(s.got) != 1 {
		t.Fatalf("sink received %d notifications, want 1", len(s.got))
	}

	// A different cycle type (resolution) for the same key is independent.
	rev := firingEvent("a", 3)
	rev.Type = model.EventResolved
	if e := n.Dispatch(rev); e.Type != model.EventNotified {
		t.Fatalf("resolution must be delivered once: %s", e.Type)
	}
	fired, resolved := n.Delivered()
	if len(fired) != 1 || len(resolved) != 1 {
		t.Fatalf("delivered fired=%d resolved=%d", len(fired), len(resolved))
	}
}

func TestSilenceSuppressesOnly(t *testing.T) {
	s := &sink{}
	sil := fakeSilencer{hit: &model.Silence{ID: "maintenance"}}
	n := New(RecorderFunc(s.Notify), sil)

	ev := firingEvent("b", 5)
	out := n.Dispatch(ev)
	if out.Type != model.EventSuppressed || out.SilenceID != "maintenance" {
		t.Fatalf("silenced firing should be suppressed, got %s/%s", out.Type, out.SilenceID)
	}
	// Alert state itself is untouched by the notifier.
	if ev.State.Status != "" {
		t.Fatal("notifier must not mutate alert state")
	}
	if len(s.got) != 0 {
		t.Fatal("nothing should be delivered while silenced")
	}
	t.Logf("basis: silence consumed firing slot but state unchanged")

	// Recovery after the silence window ends must still be delivered once.
	n2 := New(RecorderFunc(s.Notify), fakeSilencer{}) // no active silence now
	rev := firingEvent("b", 50)
	rev.Type = model.EventResolved
	if out := n2.Dispatch(rev); out.Type != model.EventNotified {
		t.Fatalf("recovery outside silence must be delivered, got %s", out.Type)
	}
}

func TestConcurrentDispatchAtMostOnce(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.Dispatch(firingEvent("a", 1)) }()
	}
	wg.Wait()
	if len(s.got) != 1 {
		t.Fatalf("concurrent duplicate dispatches delivered %d times, want 1", len(s.got))
	}
	t.Logf("input: 32 concurrent identical firing events; basis: exactly 1 delivery")
}

func cycleEvent(host string, cycle, t int) model.Event {
	ev := firingEvent(host, t)
	ev.State.Cycle = cycle
	return ev
}

func TestDedupPerCycle(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil)

	// Round 1: firing + resolved; round 2 of the same series must not be
	// swallowed by round 1's dedup memory.
	for cycle := 1; cycle <= 2; cycle++ {
		if e := n.Dispatch(cycleEvent("a", cycle, cycle*10)); e.Type != model.EventNotified {
			t.Fatalf("cycle %d firing must notify, got %s", cycle, e.Type)
		}
		rev := cycleEvent("a", cycle, cycle*10+5)
		rev.Type = model.EventResolved
		if e := n.Dispatch(rev); e.Type != model.EventNotified {
			t.Fatalf("cycle %d recovery must notify, got %s", cycle, e.Type)
		}
	}
	// Re-dispatch of an old round is still a duplicate.
	if e := n.Dispatch(cycleEvent("a", 1, 99)); e.Type != model.EventSuppressed {
		t.Fatalf("re-dispatched cycle-1 firing must stay duplicate, got %s", e.Type)
	}
	if len(s.got) != 4 {
		t.Fatalf("want 4 deliveries (2 rounds x firing+recovery), got %d", len(s.got))
	}
	t.Logf("basis: dedup identity includes the round number; rounds 1 and 2 each " +
		"delivered firing+recovery exactly once, old-round re-dispatch suppressed")
}

type windowSilencer struct{ sil model.Silence }

func (w windowSilencer) Match(metric string, ls model.Labels, t time.Time) *model.Silence {
	if w.sil.Contains(t) && w.sil.Matches(metric, ls) {
		return &w.sil
	}
	return nil
}

func TestSilenceDoesNotConsumeCycleSlot(t *testing.T) {
	s := &sink{}
	sil := windowSilencer{sil: model.Silence{
		ID: "night", Metric: "cpu.usage",
		Match:    map[string]string{"host": "a"},
		StartsAt: time.Unix(0, 0), EndsAt: time.Unix(100, 0),
	}}
	n := New(RecorderFunc(s.Notify), sil)

	// Round 1 fires inside the silence window: suppressed, no slot consumed.
	if e := n.Dispatch(cycleEvent("a", 1, 10)); e.Type != model.EventSuppressed || e.SilenceID != "night" {
		t.Fatalf("silenced firing must be suppressed, got %s/%s", e.Type, e.SilenceID)
	}
	// Round 2 fires after the window: delivered normally.
	if e := n.Dispatch(cycleEvent("a", 2, 200)); e.Type != model.EventNotified {
		t.Fatalf("post-silence round must notify, got %s", e.Type)
	}
	// And because silence consumed nothing, even a late re-dispatch of
	// round 1 (after the window) is delivered rather than deduped.
	if e := n.Dispatch(cycleEvent("a", 1, 300)); e.Type != model.EventNotified {
		t.Fatalf("silence must not consume the round-1 slot, got %s", e.Type)
	}
	if len(s.got) != 2 {
		t.Fatalf("want 2 deliveries, got %d", len(s.got))
	}
	t.Logf("basis: silence only drops the notification; round numbering, state and " +
		"later rounds are unaffected, and no dedup slot is consumed")
}

func TestDedupMemoryBounded(t *testing.T) {
	s := &sink{}
	n := NewWithOptions(RecorderFunc(s.Notify), nil, Options{RetainCycles: 3})

	// Rounds 1..10 fire and resolve; each round must deliver exactly once.
	for cycle := 1; cycle <= 10; cycle++ {
		if e := n.Dispatch(cycleEvent("a", cycle, cycle*10)); e.Type != model.EventNotified {
			t.Fatalf("cycle %d must notify, got %s", cycle, e.Type)
		}
	}
	if len(s.got) != 10 {
		t.Fatalf("want 10 deliveries, got %d", len(s.got))
	}
	// Memory is bounded to the 3 most recent rounds.
	fired, _ := n.Delivered()
	if len(fired) != 3 {
		t.Fatalf("dedup memory must keep only 3 rounds, keeps %d: %v", len(fired), fired)
	}
	// A round inside the window is still deduped exactly.
	if e := n.Dispatch(cycleEvent("a", 10, 999)); e.Type != model.EventSuppressed {
		t.Fatalf("recent round re-dispatch must be duplicate, got %s", e.Type)
	}
	// An evicted old round is not mistaken for a new one (no re-delivery).
	if e := n.Dispatch(cycleEvent("a", 1, 1000)); e.Type != model.EventSuppressed {
		t.Fatalf("evicted round must not be re-delivered, got %s", e.Type)
	}
	if len(s.got) != 10 {
		t.Fatalf("evicted/duplicate rounds must not add deliveries, got %d", len(s.got))
	}
	t.Logf("basis: retain=3 keeps rounds 8..10; evicted rounds suppress as old, " +
		"never suppress newer rounds, memory stays bounded")
}

func TestConcurrentDispatchPerCycleAtMostOnce(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil)
	var wg sync.WaitGroup
	// 16 goroutines x the same 5 rounds: each round delivered exactly once.
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cycle := 1; cycle <= 5; cycle++ {
				n.Dispatch(cycleEvent("a", cycle, cycle))
			}
		}()
	}
	wg.Wait()
	if len(s.got) != 5 {
		t.Fatalf("concurrent dispatch of 5 rounds delivered %d, want 5", len(s.got))
	}
	t.Logf("input: 16 goroutines x 5 identical rounds; basis: exactly 1 delivery per round")
}
