// Package aggregator accumulates validated samples into per-series window
// aggregates. It is safe for concurrent use and merges out-of-order samples
// by a single deterministic rule set, so arrival order never changes the
// final numbers.
package aggregator

import (
	"sort"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// bucket is the internal mutable aggregate for one series/window.
type bucket struct {
	model.Aggregate
	firstTS, lastTS time.Time
	firstID, lastID string
	ids             map[string]struct{}
}

// Aggregator stores window aggregates keyed by series and window start.
type Aggregator struct {
	mu         sync.RWMutex
	windowSize time.Duration
	windows    map[string]map[time.Time]*bucket
}

// New creates an Aggregator with fixed-size windows.
func New(windowSize time.Duration) *Aggregator {
	if windowSize <= 0 {
		panic("aggregator: window size must be positive")
	}
	return &Aggregator{
		windowSize: windowSize,
		windows:    make(map[string]map[time.Time]*bucket),
	}
}

// WindowStart returns the deterministic bucket start for a timestamp:
// windows are aligned to multiples of the window size since the Unix epoch.
func (a *Aggregator) WindowStart(t time.Time) time.Time {
	return t.Truncate(a.windowSize)
}

// Add merges one validated sample into its window. The same sample identity
// is accumulated at most once, even under concurrent calls. Returns the
// resulting aggregate (a copy is unnecessary under the caller's lock; use
// Get/Snapshot for reads).
//
// Out-of-order merge rules (uniform regardless of arrival order):
//   - Count/Sum/Min/Max are commutative, so order cannot change them.
//   - First is the value at the smallest (timestamp, identity).
//   - Last  is the value at the largest  (timestamp, identity).
//
// The identity tie-break makes equal-timestamp/distinct-value samples merge
// deterministically.
func (a *Aggregator) Add(s model.Sample) *model.Aggregate {
	ws := a.WindowStart(s.Timestamp)
	we := ws.Add(a.windowSize)
	key := s.SeriesKey()
	id := s.Identity()

	a.mu.Lock()
	defer a.mu.Unlock()

	byWindow := a.windows[key]
	if byWindow == nil {
		byWindow = make(map[time.Time]*bucket)
		a.windows[key] = byWindow
	}
	b := byWindow[ws]
	if b == nil {
		b = &bucket{
			Aggregate: model.Aggregate{
				Metric:      s.Metric,
				Labels:      append(model.Labels(nil), s.Labels...),
				WindowStart: ws,
				WindowEnd:   we,
			},
			ids: make(map[string]struct{}),
		}
		byWindow[ws] = b
	}
	if _, dup := b.ids[id]; dup {
		// Idempotent: never double-accumulate an identical sample.
		ag := b.Aggregate
		return &ag
	}
	b.ids[id] = struct{}{}

	b.Count++
	b.Sum += s.Value
	if b.Count == 1 {
		b.Min, b.Max = s.Value, s.Value
		b.First, b.Last = s.Value, s.Value
		b.firstTS, b.lastTS = s.Timestamp, s.Timestamp
		b.firstID, b.lastID = id, id
	} else {
		if s.Value < b.Min {
			b.Min = s.Value
		}
		if s.Value > b.Max {
			b.Max = s.Value
		}
		if older(s.Timestamp, id, b.firstTS, b.firstID) {
			b.First = s.Value
			b.firstTS = s.Timestamp
			b.firstID = id
		}
		if newer(s.Timestamp, id, b.lastTS, b.lastID) {
			b.Last = s.Value
			b.lastTS = s.Timestamp
			b.lastID = id
		}
	}
	ag := b.Aggregate
	return &ag
}

// older reports whether (t1, id1) sorts strictly before (t2, id2).
func older(t1 time.Time, id1 string, t2 time.Time, id2 string) bool {
	if t1 != t2 {
		return t1.Before(t2)
	}
	return id1 < id2
}

// newer reports whether (t1, id1) sorts strictly after (t2, id2).
func newer(t1 time.Time, id1 string, t2 time.Time, id2 string) bool {
	if t1 != t2 {
		return t1.After(t2)
	}
	return id1 > id2
}

// Get returns a copy of the aggregate for one series/window, or nil.
func (a *Aggregator) Get(seriesKey string, w time.Time) *model.Aggregate {
	a.mu.RLock()
	defer a.mu.RUnlock()
	byWindow := a.windows[seriesKey]
	if byWindow == nil {
		return nil
	}
	b := byWindow[a.WindowStart(w)]
	if b == nil {
		return nil
	}
	ag := b.Aggregate
	ag.Labels = append(model.Labels(nil), b.Labels...)
	return &ag
}

// Snapshot returns deep copies of all aggregates ordered by series key and
// window start, making serial and concurrent results comparable.
func (a *Aggregator) Snapshot() []model.Aggregate {
	a.mu.RLock()
	defer a.mu.RUnlock()

	keys := make([]string, 0, len(a.windows))
	for k := range a.windows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []model.Aggregate
	for _, k := range keys {
		byWindow := a.windows[k]
		starts := make([]time.Time, 0, len(byWindow))
		for st := range byWindow {
			starts = append(starts, st)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
		for _, st := range starts {
			b := byWindow[st]
			ag := b.Aggregate
			ag.Labels = append(model.Labels(nil), b.Labels...)
			out = append(out, ag)
		}
	}
	return out
}

// Reset removes all accumulated state.
func (a *Aggregator) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.windows = make(map[string]map[time.Time]*bucket)
}
