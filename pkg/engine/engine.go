// Package engine evaluates alert rules against samples and maintains the
// inactive -> pending -> firing state machine with duration ("for") and
// flapping protection.
//
// State rules:
//
//   - inactive: condition is false / never observed true.
//   - pending:  condition is true but has not held continuously for the
//     rule's ForDuration. Any contradicting sample resets the
//     waiting timer (short flapping never alerts).
//   - firing:   condition held continuously for ForDuration. While firing,
//     further matching samples change nothing (no duplicate
//     alerts); the first contradicting sample moves the alert to
//     inactive and emits exactly one resolution.
//
// Rule revisions carry a Version: states from an outdated version are
// isolated from the new definition.
package engine

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// stateKey identifies one rule revision + one series instance.
func stateKey(r model.Rule, series string) string {
	return r.VersionedKey() + "|" + series
}

// Engine keeps rule definitions and per rule/series alert states.
type Engine struct {
	mu     sync.Mutex
	rules  map[string]model.Rule
	states map[string]*model.AlertState
}

// New creates an empty engine.
func New() *Engine {
	return &Engine{
		rules:  make(map[string]model.Rule),
		states: make(map[string]*model.AlertState),
	}
}

// contentKey is the canonical definition of a rule, used to detect changes.
func contentKey(r model.Rule) string {
	keys := make([]string, 0, len(r.Match))
	for k := range r.Match {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := r.Metric + "|" + string(r.Operator) + "|" +
		strconv.FormatFloat(r.Threshold, 'g', -1, 64) + "|" +
		strconv.FormatInt(int64(r.ForDuration), 10) + "|"
	for _, k := range keys {
		out += k + "=" + r.Match[k] + ","
	}
	return out
}

// SetRules replaces all rules. Rules whose definition is unchanged keep
// their version; changed or new rules receive a fresh version so historical
// judgments are never merged with the new definition.
//
// States of removed rules are dropped. States of changed rules are dropped
// as well; a firing old revision is finalized to inactive exactly once and
// surfaced by PendingChanges so the pipeline can emit one recovery.
func (e *Engine) SetRules(rules []model.Rule) (resolved []model.AlertState) {
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			panic(err)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	next := make(map[string]model.Rule, len(rules))
	for _, r := range rules {
		prev, existed := e.rules[r.Key()]
		switch {
		case !existed:
			if r.Version == 0 {
				r.Version = 1
			}
		case contentKey(prev) == contentKey(r):
			r.Version = prev.Version // identical definition
		default:
			r.Version = prev.Version + 1 // definition changed
		}
		next[r.Key()] = r
	}

	for sk, st := range e.states {
		r, ok := next["rule:"+st.RuleID]
		if !ok || r.Version != st.RuleVersion {
			// Rule removed or revised: an old firing is finalized exactly
			// once before its state disappears.
			if st.Status == model.StatusFiring {
				final := *cloneState(st)
				final.Status = model.StatusInactive
				resolved = append(resolved, final)
			}
			delete(e.states, sk)
		}
	}
	sort.Slice(resolved, func(i, j int) bool {
		if resolved[i].RuleID != resolved[j].RuleID {
			return resolved[i].RuleID < resolved[j].RuleID
		}
		return resolved[i].SeriesKey < resolved[j].SeriesKey
	})
	e.rules = next
	return resolved
}

// Rules returns the current rule set ordered by ID.
func (e *Engine) Rules() []model.Rule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]model.Rule, 0, len(e.rules))
	for _, r := range e.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Observe feeds one validated sample at time now and returns state-change
// events (pending/firing/resolved). Repeated matching samples while firing
// produce no extra events; resolution is emitted exactly once.
func (e *Engine) Observe(s model.Sample, now time.Time) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.Event
	for _, r := range e.sortedRulesLocked() {
		if !r.MatchesSeries(s.Metric, s.Labels) {
			continue
		}
		events = append(events, e.evaluateLocked(r, s.SeriesKey(), s.Metric, s.Labels, s.Value, now)...)
	}
	return events
}

// Tick performs time-driven evaluation at now: pending alerts whose
// condition is still satisfied by the last observed value and whose
// ForDuration has elapsed are promoted to firing.
func (e *Engine) Tick(now time.Time) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.Event
	keys := make([]string, 0, len(e.states))
	for k := range e.states {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		st := e.states[k]
		r, ok := e.rules["rule:"+st.RuleID]
		if !ok || r.Version != st.RuleVersion {
			continue
		}
		if st.Status != model.StatusPending {
			st.LastEvalTime = now
			continue
		}
		if !r.Evaluate(st.LastValue) {
			continue
		}
		if now.Sub(st.ActiveSince) >= r.ForDuration {
			st.Status = model.StatusFiring
			st.LastChangeAt = now
			st.LastEvalTime = now
			st.FiredNotified = false
			events = append(events, model.Event{
				Type:  model.EventFiring,
				Time:  now,
				State: cloneState(st),
			})
		}
	}
	return events
}

// evaluateLocked runs one transition rule. Caller holds e.mu.
func (e *Engine) evaluateLocked(
	r model.Rule, seriesKey, metric string, ls model.Labels,
	value float64, now time.Time,
) []model.Event {
	var events []model.Event
	k := stateKey(r, seriesKey)
	st := e.states[k]
	cond := r.Evaluate(value)

	switch {
	case st == nil && cond:
		st = &model.AlertState{
			AlertKey:     r.VersionedKey(),
			RuleID:       r.ID,
			RuleVersion:  r.Version,
			SeriesKey:    seriesKey,
			Metric:       metric,
			Labels:       append(model.Labels(nil), ls...),
			Status:       model.StatusPending,
			ActiveSince:  now,
			LastValue:    value,
			LastEvalTime: now,
			LastChangeAt: now,
		}
		e.states[k] = st
		events = append(events, model.Event{Type: model.EventPending, Time: now, State: cloneState(st)})
		if r.ForDuration <= 0 {
			st.Status = model.StatusFiring
			st.LastChangeAt = now
			events = append(events, model.Event{Type: model.EventFiring, Time: now, State: cloneState(st)})
		}

	case st != nil && st.Status == model.StatusPending:
		st.LastValue = value
		st.LastEvalTime = now
		if cond && now.Sub(st.ActiveSince) >= r.ForDuration {
			st.Status = model.StatusFiring
			st.LastChangeAt = now
			events = append(events, model.Event{Type: model.EventFiring, Time: now, State: cloneState(st)})
		} else if !cond {
			// Flapping protection: contradicting evidence restarts the wait.
			delete(e.states, k)
		}

	case st != nil && st.Status == model.StatusFiring:
		st.LastValue = value
		st.LastEvalTime = now
		if !cond {
			st.Status = model.StatusInactive
			st.LastChangeAt = now
			events = append(events, model.Event{Type: model.EventResolved, Time: now, State: cloneState(st)})
			delete(e.states, k)
		}
		// cond == true: repeated reports while firing are a no-op.
	}
	return events
}

func (e *Engine) sortedRulesLocked() []model.Rule {
	out := make([]model.Rule, 0, len(e.rules))
	for _, r := range e.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// States returns deep copies of all current alert states, ordered
// deterministically.
func (e *Engine) States() []model.AlertState {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([]string, 0, len(e.states))
	for k := range e.states {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]model.AlertState, 0, len(keys))
	for _, k := range keys {
		out = append(out, *cloneState(e.states[k]))
	}
	return out
}

// Reset clears all alert states.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.states = make(map[string]*model.AlertState)
}

// Compare compares a value against a threshold; exported for reuse/tests.
func Compare(op model.Operator, value, threshold float64) bool {
	return model.Compare(op, value, threshold)
}

func cloneState(st *model.AlertState) *model.AlertState {
	cp := *st
	cp.Labels = append(model.Labels(nil), st.Labels...)
	return &cp
}
