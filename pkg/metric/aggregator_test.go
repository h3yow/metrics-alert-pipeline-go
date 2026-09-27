package metric

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func TestValidateReasons(t *testing.T) {
	cases := []struct {
		name   string
		s      Sample
		expect RejectReason
	}{
		{"empty source", Sample{Name: "a"}, ReasonEmptySource},
		{"empty name", Sample{Source: "s1"}, ReasonEmptyName},
		{"bad name", Sample{Source: "s1", Name: "1bad"}, ReasonBadName},
		{"bad label", Sample{Source: "s1", Name: "m", Labels: map[string]string{"a-b": "v"}}, ReasonBadLabelKey},
		{"reserved label", Sample{Source: "s1", Name: "m", Labels: map[string]string{"__x": "v"}}, ReasonReservedLabel},
		{"negative counter", Sample{Source: "s1", Name: "m", Kind: Counter, Value: -1, Timestamp: ts(1)}, ReasonNegativeCounter},
		{"old ts", Sample{Source: "s1", Name: "m", Kind: Gauge, Timestamp: ts(-1)}, ReasonBadTimestamp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Validate(c.s)
			if r == nil {
				t.Fatalf("expected rejection %s, got nil; sample=%s", c.expect, c.s)
			}
			t.Logf("input: %s -> reason=%s detail=%s", c.s, r.Reason, r.Detail)
			if r.Reason != c.expect {
				t.Fatalf("expected %s, got %s", c.expect, r.Reason)
			}
		})
	}
}

func TestOutOfOrderMerge(t *testing.T) {
	a := NewAggregator(time.Minute, nil)
	// 乱序到达：10:00:30, 10:00:00, 10:00:50 全部落在同一窗口
	base := time.Unix(600, 0).UTC()
	samples := []Sample{
		{Source: "s1", Name: "cpu", Kind: Gauge, Value: 30, Timestamp: base.Add(30 * time.Second)},
		{Source: "s1", Name: "cpu", Kind: Gauge, Value: 0, Timestamp: base},
		{Source: "s1", Name: "cpu", Kind: Gauge, Value: 50, Timestamp: base.Add(50 * time.Second)},
	}
	for _, s := range samples {
		if _, r := a.Ingest(s); r != nil {
			t.Fatalf("unexpected reject: %s", r)
		}
	}
	snap := a.Snapshot()
	if len(snap.Windows) != 1 {
		t.Fatalf("want 1 window, got %d", len(snap.Windows))
	}
	var k0 SeriesKey
	var ag0 *Aggregate
	for _, b := range snap.Windows {
		for k, ag := range b {
			k0, ag0 = k, ag
		}
	}
	k, ag := k0, ag0
	{
		t.Logf("series %s: count=%d sum=%v min=%v max=%v first=%v@%s last=%v@%s",
			k, ag.Count, ag.Sum, ag.Min, ag.Max, ag.First, ag.FirstTS.Format("15:04:05"), ag.Last, ag.LastTS.Format("15:04:05"))
		if ag.Count != 3 || ag.Sum != 80 || ag.Min != 0 || ag.Max != 50 || ag.First != 0 || ag.Last != 50 {
			t.Fatalf("unexpected aggregate: %+v", ag)
		}
	}
}

func TestDuplicateID(t *testing.T) {
	a := NewAggregator(time.Minute, nil)
	base := time.Unix(600, 0).UTC()
	s := func(v float64) Sample {
		return Sample{ID: "id-1", Source: "s1", Name: "req", Kind: Counter, Value: v, Timestamp: base}
	}
	a.Ingest(s(5))
	a.Ingest(s(5)) // 同 ID 重试，必须被忽略
	a.Ingest(s(5))
	snap := a.Snapshot()
	for _, b := range snap.Windows {
		for _, ag := range b {
			if ag.Count != 1 || ag.Sum != 5 || ag.Duplicate != 2 {
				t.Fatalf("want count=1 sum=5 dup=2, got count=%d sum=%v dup=%d", ag.Count, ag.Sum, ag.Duplicate)
			}
		}
	}
	if snap.TotalDup != 2 || snap.TotalOK != 1 {
		t.Fatalf("want dup=2 ok=1, got dup=%d ok=%d", snap.TotalDup, snap.TotalOK)
	}
}

func TestConcurrentNoLoss(t *testing.T) {
	a := NewAggregator(time.Minute, nil)
	base := time.Unix(600, 0).UTC()
	const goroutines = 50
	const per = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				s := Sample{
					Source:    fmt.Sprintf("s%d", g%5),
					Name:      "req_total",
					Kind:      Counter,
					Value:     1,
					Timestamp: base.Add(time.Duration(i) * time.Second),
					Labels:    map[string]string{"region": "cn"},
				}
				if _, r := a.Ingest(s); r != nil {
					t.Errorf("unexpected reject: %s", r)
				}
			}
		}(g)
	}
	wg.Wait()
	snap := a.Snapshot()
	total := int64(0)
	for _, b := range snap.Windows {
		for _, ag := range b {
			total += ag.Count
		}
	}
	if total != goroutines*per {
		t.Fatalf("lost counts: want %d got %d (in=%d ok=%d rej=%d)",
			goroutines*per, total, snap.TotalIn, snap.TotalOK, snap.TotalRej)
	}
	t.Logf("concurrent ingest ok: samples=%d series=%d sum-lanes=%d", snap.TotalOK, func() int {
		n := 0
		for _, b := range snap.Windows {
			n += len(b)
		}
		return n
	}(), total)
}

func TestSourceLabelsMerged(t *testing.T) {
	sl := map[string]map[string]string{"s1": {"env": "prod", "host": "h1"}}
	a := NewAggregator(time.Minute, sl)
	base := time.Unix(600, 0).UTC()
	a.Ingest(Sample{Source: "s1", Name: "m", Kind: Gauge, Value: 1, Timestamp: base, Labels: map[string]string{"host": "override"}})
	snap := a.Snapshot()
	for _, b := range snap.Windows {
		for k, ag := range b {
			if ag.Labels["env"] != "prod" || ag.Labels["host"] != "override" || ag.Labels[SourceLabelKey] != "s1" {
				t.Fatalf("label merge wrong: %s", k)
			}
			t.Logf("merged labels for %s: %v", k, ag.Labels)
		}
	}
}
