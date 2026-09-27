// Package silence matches alerts against time-bounded silence windows.
package silence

import (
	"sort"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// Store holds the current silence definitions.
type Store struct {
	mu       sync.Mutex
	silences map[string]model.Silence
}

// New creates an empty silence store.
func New() *Store {
	return &Store{silences: make(map[string]model.Silence)}
}

// Set replaces all silence definitions after validating each one.
func (s *Store) Set(defs []model.Silence) {
	for _, d := range defs {
		if err := d.Validate(); err != nil {
			panic(err)
		}
	}
	next := make(map[string]model.Silence, len(defs))
	for _, d := range defs {
		next[d.Key()] = d
	}
	s.mu.Lock()
	s.silences = next
	s.mu.Unlock()
}

// All returns all configured silences ordered by ID for deterministic
// iteration.
func (s *Store) All() []model.Silence {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Silence, 0, len(s.silences))
	for _, d := range s.silences {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Match returns the first (lowest-ID) silence containing time t and matching
// the given metric/labels, or nil when nothing matches.
func (s *Store) Match(metric string, ls model.Labels, t time.Time) *model.Silence {
	for _, d := range s.All() {
		if d.Contains(t) && d.Matches(metric, ls) {
			d := d
			return &d
		}
	}
	return nil
}
