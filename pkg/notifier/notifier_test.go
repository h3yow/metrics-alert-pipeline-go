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

func firingCycle(host string, cycle, t int) model.Event {
	ev := firingEvent(host, t)
	ev.State.Cycle = cycle
	return ev
}

func TestMultiCycleDedup(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil)

	// Three trigger rounds of the same series: each round must deliver its
	// own firing + resolved exactly once; duplicates inside a round are
	// swallowed, later rounds are not.
	for cycle := 1; cycle <= 3; cycle++ {
		f1 := n.Dispatch(firingCycle("a", cycle, cycle*10))
		f2 := n.Dispatch(firingCycle("a", cycle, cycle*10+1)) // duplicate
		rev := firingCycle("a", cycle, cycle*10+2)
		rev.Type = model.EventResolved
		r1 := n.Dispatch(rev)
		r2 := n.Dispatch(rev) // duplicate
		t.Logf("cycle=%d basis: firing=%s dup=%s resolved=%s dup=%s",
			cycle, f1.Type, f2.Type, r1.Type, r2.Type)
		if f1.Type != model.EventNotified || r1.Type != model.EventNotified {
			t.Fatalf("cycle %d must notify firing+resolved, got %s/%s", cycle, f1.Type, r1.Type)
		}
		if f2.Type != model.EventSuppressed || r2.Type != model.EventSuppressed {
			t.Fatalf("cycle %d duplicates must be suppressed, got %s/%s", cycle, f2.Type, r2.Type)
		}
	}
	if len(s.got) != 6 {
		t.Fatalf("3 cycles x (1 firing + 1 resolved) = 6 deliveries, got %d", len(s.got))
	}
	fired, resolved := n.Delivered()
	if len(fired) != 3 || len(resolved) != 3 {
		t.Fatalf("delivered fired=%v resolved=%v", fired, resolved)
	}
	t.Logf("basis: per-cycle slots; delivered keys=%v", fired)
}

func TestDedupMemoryBounded(t *testing.T) {
	s := &sink{}
	n := New(RecorderFunc(s.Notify), nil, WithMaxRounds(3))

	// 10 rounds on one key plus rounds on other keys: memory stays bounded.
	for cycle := 1; cycle <= 10; cycle++ {
		n.Dispatch(firingCycle("a", cycle, cycle*10))
	}
	for cycle := 1; cycle <= 5; cycle++ {
		n.Dispatch(firingCycle("b", cycle, cycle*10))
	}
	if got := n.MemoryEntries(); got > 3+3 {
		t.Fatalf("memory must be bounded to maxRounds per key, got %d entries", got)
	}
	t.Logf("basis: maxRounds=3 -> retained entries=%d (a:3 + b:3)", n.MemoryEntries())

	// Within the retained window a duplicate is still swallowed.
	if e := n.Dispatch(firingCycle("a", 10, 999)); e.Type != model.EventSuppressed {
		t.Fatalf("recent cycle duplicate must be suppressed, got %s", e.Type)
	}
	// An evicted old cycle is treated as new (documented trade-off) and
	// crucially does not disturb newer rounds.
	before := len(s.got)
	if e := n.Dispatch(firingCycle("a", 1, 1000)); e.Type != model.EventNotified {
		t.Fatalf("evicted old cycle should be re-delivered, got %s", e.Type)
	}
	t.Logf("basis: evicted cycle=1 re-delivered (bounded memory trade-off), deliveries %d->%d", before, len(s.got))
	if e := n.Dispatch(firingCycle("a", 9, 1001)); e.Type != model.EventSuppressed {
		t.Fatalf("newer retained cycle must still be deduped, got %s", e.Type)
	}
}

func TestSilenceDoesNotBlockNextCycle(t *testing.T) {
	s := &sink{}
	sil := &fakeSilencer{hit: &model.Silence{ID: "maint"}}
	n := New(RecorderFunc(s.Notify), sil)

	// Cycle 1 firing lands inside the silence window.
	if e := n.Dispatch(firingCycle("a", 1, 5)); e.Type != model.EventSuppressed {
		t.Fatalf("silenced firing must be suppressed, got %s", e.Type)
	}
	// Silence window ends: cycle 2 of the same series must notify normally.
	*sil = fakeSilencer{}
	if e := n.Dispatch(firingCycle("a", 2, 50)); e.Type != model.EventNotified {
		t.Fatalf("next cycle after silence must notify, got %s", e.Type)
	}
	// And cycle 1's recovery (outside the window) is still delivered.
	rev := firingCycle("a", 1, 60)
	rev.Type = model.EventResolved
	if e := n.Dispatch(rev); e.Type != model.EventNotified {
		t.Fatalf("recovery after silence must be delivered, got %s", e.Type)
	}
	if len(s.got) != 2 {
		t.Fatalf("want 2 deliveries (cycle2 firing + cycle1 recovery), got %d", len(s.got))
	}
	t.Logf("basis: silence consumes only its own cycle's slot; later cycles unaffected")
}
