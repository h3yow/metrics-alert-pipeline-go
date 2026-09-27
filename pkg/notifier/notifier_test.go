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
