// Package collector validates inbound samples, merges source tags and
// rejects illegal samples with distinguishable reasons.
package collector

import (
	"sync"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// Result describes what happened when a sample was submitted.
type Result struct {
	Accepted  bool
	Duplicate bool
	// Sample is the normalized sample (source label merged) when accepted
	// or duplicate; it is the original sample when rejected.
	Sample model.Sample
	Reason model.RejectReason
	Detail string
}

// Collector receives samples from many sources concurrently. Reported
// identities are tracked so the exact same sample is never accumulated twice.
type Collector struct {
	mu          sync.Mutex
	seen        map[string]struct{}
	extraLabels model.Labels
}

// New creates a Collector. extraLabels are merged into every accepted
// sample; they must not contain the reserved source label.
func New(extraLabels model.Labels) *Collector {
	if _, ok := extraLabels.Get(model.SourceLabel); ok {
		panic("collector: extra labels must not contain the reserved " + model.SourceLabel)
	}
	return &Collector{
		seen:        make(map[string]struct{}),
		extraLabels: append(model.Labels(nil), extraLabels...),
	}
}

// Submit validates a sample, merges source/environment tags and checks for
// duplicates. It is safe for concurrent use.
//
// Ordering of rejections:
//  1. empty source
//  2. invalid sample (name, timestamp, value, labels, source mismatch)
//
// and only accepted-looking samples reach the identity de-duplication check.
func (c *Collector) Submit(s model.Sample) Result {
	if s.Source == "" {
		return Result{Sample: s, Reason: model.ReasonEmptySource}
	}
	if err := s.Validate(); err != nil {
		r := Result{Sample: s}
		if re, ok := err.(*model.RejectError); ok {
			r.Reason, r.Detail = re.Reason, re.Detail
		}
		return r
	}

	// Normalize: merge collector-wide labels, then stamp the source label
	// last so it always equals the declared source.
	merged := s.Labels.Merge(c.extraLabels)
	merged = merged.Merge(model.Labels{{Name: model.SourceLabel, Value: s.Source}})
	s.Labels = merged
	if err := s.Validate(); err != nil {
		r := Result{Sample: s}
		if re, ok := err.(*model.RejectError); ok {
			r.Reason, r.Detail = re.Reason, re.Detail
		}
		return r
	}

	id := s.Identity()
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[id]; ok {
		return Result{Duplicate: true, Sample: s}
	}
	c.seen[id] = struct{}{}
	return Result{Accepted: true, Sample: s}
}

// SeenCount returns how many distinct sample identities have been accepted.
func (c *Collector) SeenCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// Reset clears the duplicate-tracking table.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = make(map[string]struct{})
}
