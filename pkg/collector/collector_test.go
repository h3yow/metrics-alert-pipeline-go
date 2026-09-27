package collector

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

func sample(value float64, ts time.Time, source string) model.Sample {
	return model.Sample{
		Metric:    "cpu.usage",
		Labels:    model.Labels{{Name: "host", Value: "a"}},
		Value:     value,
		Timestamp: ts,
		Source:    source,
	}
}

func TestSubmitRejectsWithDistinctReasons(t *testing.T) {
	c := New(model.Labels{{Name: "env", Value: "local"}})
	ts := time.Unix(1000, 0)
	cases := []struct {
		name   string
		s      model.Sample
		reason model.RejectReason
	}{
		{"empty source", model.Sample{Metric: "m", Timestamp: ts}, model.ReasonEmptySource},
		{"empty metric", model.Sample{Source: "x", Timestamp: ts}, model.ReasonEmptyName},
		{"zero timestamp", model.Sample{Metric: "m", Source: "x"}, model.ReasonBadTimestamp},
		{"source label mismatch", func() model.Sample {
			s := sample(1, ts, "x")
			s.Labels = append(s.Labels, model.Label{Name: model.SourceLabel, Value: "y"})
			return s
		}(), model.ReasonSourceMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("input sample=%+v; basis: expected reject reason=%q", tc.s, tc.reason)
			r := c.Submit(tc.s)
			if r.Accepted || r.Reason != tc.reason {
				t.Fatalf("got accepted=%v reason=%q, want reason=%q", r.Accepted, r.Reason, tc.reason)
			}
		})
	}
}

func TestSubmitMergesLabelsAndDedups(t *testing.T) {
	c := New(model.Labels{{Name: "env", Value: "local"}})
	ts := time.Unix(1000, 0)
	s := sample(95, ts, "node-a")

	r1 := c.Submit(s)
	if !r1.Accepted {
		t.Fatalf("first submit should be accepted: %+v", r1)
	}
	if v, ok := r1.Sample.Labels.Get("env"); !ok || v != "local" {
		t.Fatalf("extra label not merged: %v", r1.Sample.Labels)
	}
	if v, _ := r1.Sample.Labels.Get(model.SourceLabel); v != "node-a" {
		t.Fatalf("source label = %q, want node-a", v)
	}
	t.Logf("accepted normalized sample labels=%v; identity=%s", r1.Sample.Labels, r1.Sample.Identity())

	r2 := c.Submit(s)
	if !r2.Duplicate || r2.Accepted {
		t.Fatalf("identical resubmit must be duplicate, got %+v", r2)
	}
	if c.SeenCount() != 1 {
		t.Fatalf("seen = %d, want 1", c.SeenCount())
	}
}

func TestConcurrentSubmitNoLossNoDoubleCount(t *testing.T) {
	c := New(nil)
	ts := time.Unix(1000, 0)

	const goroutines = 16
	const perG = 100
	var wg sync.WaitGroup
	var acceptMu sync.Mutex
	var accepted, duplicates int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Every goroutine submits the same 100 identities.
				s := sample(float64(i), ts.Add(time.Duration(i)*time.Millisecond), "node-a")
				r := c.Submit(s)
				acceptMu.Lock()
				if r.Accepted {
					accepted++
				}
				if r.Duplicate {
					duplicates++
				}
				acceptMu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	t.Logf("input: %d goroutines x %d identical samples; basis: exactly 1 acceptance per identity",
		goroutines, perG)
	if accepted != perG {
		t.Fatalf("accepted = %d, want %d (lost or double-accumulated samples)", accepted, perG)
	}
	if duplicates != perG*(goroutines-1) {
		t.Fatalf("duplicates = %d, want %d", duplicates, perG*(goroutines-1))
	}
	if c.SeenCount() != perG {
		t.Fatalf("seen = %d, want %d", c.SeenCount(), perG)
	}
}
