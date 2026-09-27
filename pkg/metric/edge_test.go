package metric

import (
	"math"
	"testing"
	"time"
)

// TestCrossWindowOutOfOrder 样本即使跨窗口乱序到达，也必须并入各自正确的窗口。
func TestCrossWindowOutOfOrder(t *testing.T) {
	a := NewAggregator(time.Minute, nil)
	base := time.Unix(600, 0).UTC()
	// 先到 w3，再到 w1，最后到 w2
	order := []Sample{
		{Source: "s", Name: "m", Kind: Gauge, Value: 3, Timestamp: base.Add(2*time.Minute + 5*time.Second)},
		{Source: "s", Name: "m", Kind: Gauge, Value: 1, Timestamp: base.Add(5 * time.Second)},
		{Source: "s", Name: "m", Kind: Gauge, Value: 2, Timestamp: base.Add(time.Minute + 5*time.Second)},
	}
	for _, s := range order {
		if _, rj := a.Ingest(s); rj != nil {
			t.Fatalf("reject: %s", rj)
		}
	}
	snap := a.Snapshot()
	if len(snap.Windows) != 3 {
		t.Fatalf("want 3 windows, got %d", len(snap.Windows))
	}
	for i, w := range snap.SortedWindows() {
		b := snap.Windows[w]
		want := float64(i + 1)
		for k, ag := range b {
			if ag.Count != 1 || ag.Last != want {
				t.Fatalf("window %s series %s want last=%v got count=%d last=%v",
					w.Format("15:04:05"), k, want, ag.Count, ag.Last)
			}
			t.Logf("window %s -> last=%v (arrived out of order)", w.Format("15:04:05"), ag.Last)
		}
	}
}

func TestMoreRejectReasons(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name   string
		s      Sample
		expect RejectReason
	}{
		{"nan", Sample{Source: "s", Name: "m", Kind: Gauge, Value: math.NaN(), Timestamp: ts(600)}, ReasonNaN},
		{"+inf", Sample{Source: "s", Name: "m", Kind: Gauge, Value: math.Inf(1), Timestamp: ts(600)}, ReasonInf},
		{"-inf", Sample{Source: "s", Name: "m", Kind: Gauge, Value: math.Inf(-1), Timestamp: ts(600)}, ReasonInf},
		{"future", Sample{Source: "s", Name: "m", Kind: Gauge, Value: 1, Timestamp: now.Add(time.Hour)}, ReasonFutureTimestamp},
		{"empty name", Sample{Source: "s", Name: "", Kind: Gauge, Timestamp: ts(600)}, ReasonEmptyName},
		{"empty source", Sample{Source: "", Name: "m", Kind: Gauge, Timestamp: ts(600)}, ReasonEmptySource},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Validate(c.s)
			if r == nil || r.Reason != c.expect {
				got := RejectReason("nil")
				if r != nil {
					got = r.Reason
				}
				t.Fatalf("want %s got %s; input=%s", c.expect, got, c.s)
			}
			t.Logf("input: %s -> %s", c.s, r)
		})
	}
}

// TestGaugeFirstLastBySampleTime 同窗口 First/Last 严格按采样时间而非到达顺序。
func TestGaugeFirstLastBySampleTime(t *testing.T) {
	a := NewAggregator(time.Minute, nil)
	base := time.Unix(600, 0).UTC()
	// 倒序到达：30s -> 0s -> 50s
	a.Ingest(Sample{Source: "s", Name: "m", Kind: Gauge, Value: 100, Timestamp: base.Add(30 * time.Second)})
	a.Ingest(Sample{Source: "s", Name: "m", Kind: Gauge, Value: 200, Timestamp: base})
	a.Ingest(Sample{Source: "s", Name: "m", Kind: Gauge, Value: 300, Timestamp: base.Add(50 * time.Second)})
	snap := a.Snapshot()
	for _, b := range snap.Windows {
		for _, ag := range b {
			if ag.First != 200 || ag.Last != 300 || ag.Sum != 600 {
				t.Fatalf("first/last must follow sample time: first=%v last=%v sum=%v", ag.First, ag.Last, ag.Sum)
			}
		}
	}
}
