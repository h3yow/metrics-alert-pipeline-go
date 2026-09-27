package silence

import (
	"reflect"
	"testing"
	"time"
)

func TestMatchAt(t *testing.T) {
	t0 := time.Unix(1000, 0).UTC()
	sils := []Silence{
		{ID: "sil-host1", Matchers: map[string]string{"host": "h1"}, StartsAt: t0, EndsAt: t0.Add(10 * time.Minute)},
		{ID: "sil-global", Matchers: map[string]string{}, StartsAt: t0, EndsAt: t0.Add(time.Minute)},
		{ID: "sil-other", Matchers: map[string]string{"host": "h2"}, StartsAt: t0, EndsAt: t0.Add(10 * time.Minute)},
	}
	st := NewStore(sils)

	cases := []struct {
		name   string
		at     time.Time
		labels map[string]string
		want   []string
	}{
		{"start inclusive", t0, map[string]string{"host": "h1"}, []string{"sil-global", "sil-host1"}},
		{"host match mid", t0.Add(5 * time.Minute), map[string]string{"host": "h1"}, []string{"sil-host1"}},
		{"other host", t0.Add(5 * time.Minute), map[string]string{"host": "h2"}, []string{"sil-other"}},
		{"no label match", t0.Add(5 * time.Minute), map[string]string{"host": "h9"}, nil},
		{"end exclusive", t0.Add(10 * time.Minute), map[string]string{"host": "h1"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := st.MatchAt(c.labels, c.at)
			t.Logf("at=%s labels=%v -> silences=%v", c.at.Format("15:04:05"), c.labels, got)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("want %v, got %v", c.want, got)
			}
		})
	}
}
