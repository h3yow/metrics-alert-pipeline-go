package silence

import (
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

func TestMatchWindowAndLabels(t *testing.T) {
	s := New()
	s.Set([]model.Silence{
		{ID: "b-window", Metric: "cpu.usage", Match: map[string]string{"host": "b"},
			StartsAt: time.Unix(0, 0), EndsAt: time.Unix(40, 0)},
	})
	ls := model.Labels{{Name: "host", Value: "b"}}
	other := model.Labels{{Name: "host", Value: "a"}}

	if got := s.Match("cpu.usage", ls, time.Unix(10, 0)); got == nil || got.ID != "b-window" {
		t.Fatal("should match inside window and labels")
	}
	for _, tc := range []struct {
		name string
		t    time.Time
		l    model.Labels
		m    string
	}{
		{"before window", time.Unix(-1, 0), ls, "cpu.usage"},
		{"at end is excluded (half-open)", time.Unix(40, 0), ls, "cpu.usage"},
		{"other host", time.Unix(10, 0), other, "cpu.usage"},
		{"other metric", time.Unix(10, 0), ls, "mem.usage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("basis: time=%s labels=%v metric=%s must not match", tc.t, tc.l, tc.m)
			if got := s.Match(tc.m, tc.l, tc.t); got != nil {
				t.Fatalf("unexpected silence match: %s", got.ID)
			}
		})
	}
}

func TestInvalidSilencePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("invalid silence must panic on Set")
		}
	}()
	New().Set([]model.Silence{{ID: "x", StartsAt: time.Unix(10, 0), EndsAt: time.Unix(5, 0)}})
}
