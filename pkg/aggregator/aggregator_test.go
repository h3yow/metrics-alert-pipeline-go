package aggregator

import (
	"sync"
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

func mkSample(v float64, sec int) model.Sample {
	return model.Sample{
		Metric:    "cpu.usage",
		Labels:    model.Labels{{Name: "host", Value: "a"}},
		Value:     v,
		Timestamp: time.Unix(int64(sec), 0),
		Source:    "node-a",
	}
}

func TestOutOfOrderMergeUniform(t *testing.T) {
	// Same set merged in three different arrival orders must yield the same
	// first/last/count/sum/min/max.
	ordered := []model.Sample{mkSample(95, 1), mkSample(50, 2), mkSample(98, 3)}
	shuffled := []model.Sample{ordered[2], ordered[0], ordered[1]}
	reverse := []model.Sample{ordered[2], ordered[1], ordered[0]}

	merge := func(in []model.Sample) model.Aggregate {
		a := New(time.Minute)
		var got *model.Aggregate
		for _, s := range in {
			t.Logf("input sample ts=%ds value=%.0f; merge basis: first=min(ts,id) last=max(ts,id)",
				s.Timestamp.Unix(), s.Value)
			got = a.Add(s)
		}
		return *got
	}

	want := merge(ordered)
	for _, in := range [][]model.Sample{shuffled, reverse} {
		got := merge(in)
		if got.Count != want.Count || got.Sum != want.Sum || got.Min != want.Min ||
			got.Max != want.Max || got.First != want.First || got.Last != want.Last {
			t.Fatalf("aggregate mismatch:\n got =%+v\nwant=%+v", got, want)
		}
	}
	if want.Count != 3 || want.Sum != 243 || want.Min != 50 || want.Max != 98 ||
		want.First != 95 || want.Last != 98 {
		t.Fatalf("unexpected merged values: %+v", want)
	}
}

func TestAddIsIdempotent(t *testing.T) {
	a := New(time.Minute)
	s := mkSample(95, 1)
	a.Add(s)
	a.Add(s)
	ag := a.Get(s.SeriesKey(), s.Timestamp)
	if ag.Count != 1 || ag.Sum != 95 {
		t.Fatalf("duplicate sample accumulated twice: %+v", ag)
	}
}

func TestWindowAlignment(t *testing.T) {
	a := New(10 * time.Second)
	s1 := mkSample(1, 5)
	s2 := mkSample(2, 9)
	s3 := mkSample(3, 10) // next bucket
	a.Add(s1)
	a.Add(s2)
	a.Add(s3)
	snaps := a.Snapshot()
	if len(snaps) != 2 {
		t.Fatalf("windows = %d, want 2", len(snaps))
	}
	if snaps[0].Count != 2 || snaps[1].Count != 1 {
		t.Fatalf("window bucket counts wrong: %d,%d", snaps[0].Count, snaps[1].Count)
	}
	if got := a.WindowStart(time.Unix(19, 0)); got.Unix() != 10 {
		t.Fatalf("window start = %d, want 10", got.Unix())
	}
}

func TestConcurrentEqualsSerial(t *testing.T) {
	// Deterministic sample set: one identity per (value, second).
	const n = 500
	all := make([]model.Sample, n)
	for i := 0; i < n; i++ {
		all[i] = mkSample(float64(i%7), i)
	}

	// Serial baseline.
	serial := New(10 * time.Second)
	for _, s := range all {
		serial.Add(s)
	}
	serialSnap := serial.Snapshot()

	// Concurrent: shuffle across goroutines plus some duplicates.
	conc := New(10 * time.Second)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < n; i += 8 {
				conc.Add(all[i])
				if i%50 == 0 {
					conc.Add(all[i]) // duplicate must be ignored
				}
			}
		}(g)
	}
	wg.Wait()
	concSnap := conc.Snapshot()

	if len(concSnap) != len(serialSnap) {
		t.Fatalf("window count: concurrent=%d serial=%d", len(concSnap), len(serialSnap))
	}
	var totalCount int64
	for i := range serialSnap {
		c, s := concSnap[i], serialSnap[i]
		if c.Count != s.Count || c.Sum != s.Sum || c.Min != s.Min || c.Max != s.Max ||
			c.First != s.First || c.Last != s.Last {
			t.Fatalf("bucket %d mismatch:\n conc=%+v\nser =%+v", i, c, s)
		}
		totalCount += s.Count
	}
	t.Logf("input: %d samples (8 goroutines, some duplicates); basis: aggregate equality; totalCount=%d",
		n, totalCount)
	if totalCount != n {
		t.Fatalf("total count = %d, want %d", totalCount, n)
	}
}
