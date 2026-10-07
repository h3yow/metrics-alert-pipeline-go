// Package pipeline wires collector, aggregator, engine and notifier into one
// locally runnable pipeline, and supports deterministic replay of the full
// accepted sample history for reproducing judgments after config changes.
package pipeline

import (
	"sort"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/aggregator"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/collector"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/engine"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/notifier"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

// Stats summarizes pipeline operation counters.
type Stats struct {
	Accepted   int64
	Rejected   int64
	Duplicates int64
	Pending    int64
	Firing     int64
	Resolved   int64
	Notified   int64
	Suppressed int64
}

// Config bundles pipeline configuration.
type Config struct {
	WindowSize  time.Duration
	ExtraLabels model.Labels
	// DedupRetainCycles bounds the notifier's de-dup memory to the most
	// recent N trigger/recovery rounds per alert key. Values <= 0 select
	// notifier.DefaultRetainCycles.
	DedupRetainCycles int
}

// Pipeline is the full local metrics/alerting component.
type Pipeline struct {
	mu         sync.Mutex
	cfg        Config
	collector  *collector.Collector
	aggregator *aggregator.Aggregator
	engine     *engine.Engine
	silences   *silence.Store
	notifier   *notifier.Notifier
	history    []model.Sample
	stats      Stats
	listeners  []func(model.Event)
}

// New creates a Pipeline.
func New(cfg Config, rec notifier.Recorder) *Pipeline {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = time.Minute
	}
	p := &Pipeline{
		cfg:      cfg,
		silences: silence.New(),
		engine:   engine.New(),
	}
	p.collector = collector.New(cfg.ExtraLabels)
	p.aggregator = aggregator.New(cfg.WindowSize)
	p.notifier = notifier.NewWithOptions(rec, p.silences,
		notifier.Options{RetainCycles: cfg.DedupRetainCycles})
	return p
}

// SetRules replaces the rule set (engine handles version bumping). An alert
// firing under an old revision is resolved exactly once and its recovery is
// dispatched (silence/de-dup rules still apply).
func (p *Pipeline) SetRules(rules []model.Rule) []model.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	resolved := p.engine.SetRules(rules)
	evs := make([]model.Event, 0, len(resolved))
	for _, st := range resolved {
		st := st
		ev := model.Event{Type: model.EventResolved, Time: st.LastEvalTime, State: &st,
			Detail: "rule definition changed"}
		p.stats.Resolved++
		out := p.notifier.Dispatch(ev)
		switch out.Type {
		case model.EventNotified:
			p.stats.Notified++
		case model.EventSuppressed:
			p.stats.Suppressed++
		}
		evs = append(evs, out)
		p.emit(out)
	}
	return evs
}

// SetSilences replaces the silence windows. Replaying history with the new
// set reproduces what notifications would have been suppressed.
func (p *Pipeline) SetSilences(defs []model.Silence) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.silences.Set(defs)
}

// OnEvent registers a listener invoked for every emitted event.
func (p *Pipeline) OnEvent(fn func(model.Event)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners = append(p.listeners, fn)
}

func (p *Pipeline) emit(ev model.Event) {
	for _, fn := range p.listeners {
		fn(ev)
	}
}

// dispatch runs state events through the notifier (silence + dedup) and
// updates counters. Caller holds p.mu.
func (p *Pipeline) dispatchLocked(evs []model.Event) []model.Event {
	out := make([]model.Event, 0, len(evs))
	for _, ev := range evs {
		p.countTransitionLocked(ev.Type)
		res := p.notifier.Dispatch(ev)
		switch res.Type {
		case model.EventNotified:
			p.stats.Notified++
		case model.EventSuppressed:
			p.stats.Suppressed++
		}
		out = append(out, res)
		p.emit(res)
	}
	return out
}

func (p *Pipeline) countTransitionLocked(t model.EventType) {
	switch t {
	case model.EventPending:
		p.stats.Pending++
	case model.EventFiring:
		p.stats.Firing++
	case model.EventResolved:
		p.stats.Resolved++
	}
}

// Ingest validates, aggregates and evaluates one sample. The evaluation time
// is the sample timestamp, so judgments are independent of wall-clock
// arrival time and reproducible by replay.
func (p *Pipeline) Ingest(s model.Sample) []model.Event {
	p.mu.Lock()
	defer p.mu.Unlock()

	res := p.collector.Submit(s)
	switch {
	case res.Duplicate:
		p.stats.Duplicates++
		ev := model.Event{
			Type:   model.EventDuplicate,
			Time:   res.Sample.Timestamp,
			Sample: samplePtr(res.Sample),
		}
		p.emit(ev)
		return []model.Event{ev}
	case !res.Accepted:
		p.stats.Rejected++
		ev := model.Event{
			Type:   model.EventRejected,
			Time:   s.Timestamp,
			Sample: samplePtr(s),
			Reason: res.Reason,
			Detail: res.Detail,
		}
		p.emit(ev)
		return []model.Event{ev}
	}

	norm := res.Sample
	p.stats.Accepted++
	p.history = append(p.history, norm)
	p.aggregator.Add(norm)
	p.emit(model.Event{Type: model.EventAccepted, Time: norm.Timestamp, Sample: samplePtr(norm)})

	trans := p.engine.Observe(norm, norm.Timestamp)
	return p.dispatchLocked(trans)
}

// Tick runs a time-driven evaluation using the supplied clock time.
func (p *Pipeline) Tick(now time.Time) []model.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	trans := p.engine.Tick(now)
	return p.dispatchLocked(trans)
}

// History returns a copy of every accepted sample in arrival order.
func (p *Pipeline) History() []model.Sample {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]model.Sample(nil), p.history...)
}

// Aggregates returns a snapshot of all aggregates.
func (p *Pipeline) Aggregates() []model.Aggregate { return p.aggregator.Snapshot() }

// States returns a snapshot of all alert states.
func (p *Pipeline) States() []model.AlertState { return p.engine.States() }

// Stats returns a copy of the current counters.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Replay rebuilds aggregates and alert judgments from accepted history using
// the current rules/silences. Samples are sorted by (timestamp, identity),
// so out-of-order arrivals and concurrent submission orders produce
// identical conclusions. Live pipeline state is never mutated.
func (p *Pipeline) Replay() ([]model.Event, Stats, []model.Aggregate, []model.AlertState) {
	p.mu.Lock()
	rules := p.engine.Rules()
	silences := silence.New()
	silences.Set(p.silences.All())
	history := append([]model.Sample(nil), p.history...)
	windowSize := p.cfg.WindowSize
	recorder := &recordingSink{}
	p.mu.Unlock()

	sort.SliceStable(history, func(i, j int) bool {
		if !history[i].Timestamp.Equal(history[j].Timestamp) {
			return history[i].Timestamp.Before(history[j].Timestamp)
		}
		return history[i].Identity() < history[j].Identity()
	})

	agg := aggregator.New(windowSize)
	eng := engine.New()
	eng.SetRules(rules)
	ntf := notifier.NewWithOptions(recorder, silences,
		notifier.Options{RetainCycles: p.cfg.DedupRetainCycles})

	var events []model.Event
	var st Stats
	var lastTS time.Time
	for _, s := range history {
		// Deterministic evaluation order at each distinct timestamp:
		// promote pending alerts using "latest value with ts <= now", then
		// observe the new sample.
		for _, ev := range eng.Tick(s.Timestamp) {
			events = append(events, p.deliverReplay(ntf, &st, ev))
		}
		agg.Add(s)
		st.Accepted++
		events = append(events, model.Event{
			Type:   model.EventAccepted,
			Time:   s.Timestamp,
			Sample: samplePtr(s),
		})
		for _, ev := range eng.Observe(s, s.Timestamp) {
			events = append(events, p.deliverReplay(ntf, &st, ev))
		}
		lastTS = s.Timestamp
	}
	// Promotions earned exactly at the final timestamp.
	if !lastTS.IsZero() {
		for _, ev := range eng.Tick(lastTS) {
			events = append(events, p.deliverReplay(ntf, &st, ev))
		}
	}
	return events, st, agg.Snapshot(), eng.States()
}

// deliverReplay routes one transition through the replayed notifier and
// accounts it, returning the delivered/suppressed event.
func (p *Pipeline) deliverReplay(
	ntf *notifier.Notifier, st *Stats, ev model.Event,
) model.Event {
	stCountTransition(st, ev.Type)
	res := ntf.Dispatch(ev)
	if res.Type == model.EventNotified {
		st.Notified++
	}
	if res.Type == model.EventSuppressed {
		st.Suppressed++
	}
	return res
}

// recordingSink collects delivered notifications for replay accounting.
type recordingSink struct{ delivered []model.Event }

func (r *recordingSink) Notify(ev model.Event) { r.delivered = append(r.delivered, ev) }

func stCountTransition(st *Stats, t model.EventType) {
	switch t {
	case model.EventPending:
		st.Pending++
	case model.EventFiring:
		st.Firing++
	case model.EventResolved:
		st.Resolved++
	}
}

func samplePtr(s model.Sample) *model.Sample {
	cp := s
	cp.Labels = append(model.Labels(nil), s.Labels...)
	return &cp
}
