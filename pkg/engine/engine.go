// Package engine evaluates alert rules against samples and maintains the
// inactive -> pending -> firing -> resolving state machine with duration
// guards and flapping protection on both sides.
//
// State rules:
//
//   - inactive: condition is false / never observed true.
//   - pending:  condition is true but has not held continuously for the
//     rule's ForDuration. Any contradicting sample resets the
//     waiting timer (short flapping never alerts).
//   - firing:   condition held continuously for ForDuration. While firing,
//     further matching samples change nothing (no duplicate
//     alerts). Recovery is guarded by ResolveDuration: a
//     contradicting sample only starts the recovery wait.
//   - resolving: contradicting evidence is accumulating. If the condition
//     becomes true again the alert returns to firing as if the
//     recovery never started (no notification). Only when the
//     contradiction holds continuously for ResolveDuration is
//     exactly one resolution emitted. ResolveDuration == 0
//     resolves on the first contradicting sample.
//
// Each firing episode is a numbered cycle: the cycle number is assigned at
// promotion to firing and carried by the firing and resolved events, so
// repeated trigger/recover rounds of the same series are distinguishable
// and de-duplicated per round instead of swallowing later rounds.
//
// Rule revisions carry a Version: states from an outdated version are
// isolated from the new definition.
package engine

import (
	"fmt"
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
	cycles map[string]int // last assigned cycle per stateKey
}

// New creates an empty engine.
func New() *Engine {
	return &Engine{
		rules:  make(map[string]model.Rule),
		states: make(map[string]*model.AlertState),
		cycles: make(map[string]int),
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
		strconv.FormatInt(int64(r.ForDuration), 10) + "|" +
		strconv.FormatInt(int64(r.ResolveDuration), 10) + "|"
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
			if st.Status == model.StatusFiring || st.Status == model.StatusResolving {
				final := *cloneState(st)
				final.Status = model.StatusInactive
				resolved = append(resolved, final)
			}
			delete(e.states, sk)
			delete(e.cycles, sk)
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
// ForDuration has elapsed are promoted to firing; resolving alerts whose
// contradiction has held for ResolveDuration are resolved.
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
		switch st.Status {
		case model.StatusPending:
			st.LastEvalTime = now
			if !r.Evaluate(st.LastValue) {
				continue
			}
			if now.Sub(st.ActiveSince) >= r.ForDuration {
				events = append(events, e.promoteLocked(k, st, r, now))
			}
		case model.StatusResolving:
			st.LastEvalTime = now
			if r.Evaluate(st.LastValue) {
				// Last known value satisfies again: the pending recovery
				// is cancelled as if it never started.
				st.Status = model.StatusFiring
				st.ResolveSince = time.Time{}
				st.LastChangeAt = now
				continue
			}
			if now.Sub(st.ResolveSince) >= r.ResolveDuration {
				events = append(events, e.resolveLocked(k, st, r, now))
			}
		default:
			st.LastEvalTime = now
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
		events = append(events, model.Event{
			Type: model.EventPending, Time: now, State: cloneState(st),
			Detail: fmt.Sprintf("condition true (%.4g %s %.4g), waiting for %s",
				value, r.Operator, r.Threshold, r.ForDuration),
		})
		if r.ForDuration <= 0 {
			events = append(events, e.promoteLocked(k, st, r, now))
		}

	case st != nil && st.Status == model.StatusPending:
		st.LastValue = value
		st.LastEvalTime = now
		if cond && now.Sub(st.ActiveSince) >= r.ForDuration {
			events = append(events, e.promoteLocked(k, st, r, now))
		} else if !cond {
			// Flapping protection: contradicting evidence restarts the wait.
			delete(e.states, k)
		}

	case st != nil && st.Status == model.StatusFiring:
		st.LastValue = value
		st.LastEvalTime = now
		if !cond {
			if r.ResolveDuration <= 0 {
				events = append(events, e.resolveLocked(k, st, r, now))
			} else {
				// Recovery needs sustained contradicting evidence too:
				// a single counter-example only starts the wait.
				st.Status = model.StatusResolving
				st.ResolveSince = now
				st.LastChangeAt = now
				events = append(events, model.Event{
					Type: model.EventResolving, Time: now, State: cloneState(st),
					Detail: fmt.Sprintf("cycle %d: value %.4g violates %s %.4g, recovery waits %s",
						st.Cycle, value, r.Operator, r.Threshold, r.ResolveDuration),
				})
			}
		}
		// cond == true: repeated reports while firing are a no-op.

	case st != nil && st.Status == model.StatusResolving:
		st.LastValue = value
		st.LastEvalTime = now
		if cond {
			// Flap back to satisfied: the pending recovery is cancelled,
			// no resolution is or will be emitted for this episode.
			st.Status = model.StatusFiring
			st.ResolveSince = time.Time{}
			st.LastChangeAt = now
		} else if now.Sub(st.ResolveSince) >= r.ResolveDuration {
			events = append(events, e.resolveLocked(k, st, r, now))
		}
	}
	return events
}

// promoteLocked assigns the next cycle number and moves the alert to
// firing. Caller holds e.mu.
func (e *Engine) promoteLocked(k string, st *model.AlertState, r model.Rule, now time.Time) model.Event {
	e.cycles[k]++
	st.Cycle = e.cycles[k]
	st.Status = model.StatusFiring
	st.LastChangeAt = now
	st.LastEvalTime = now
	st.FiredNotified = false
	return model.Event{
		Type: model.EventFiring, Time: now, State: cloneState(st),
		Detail: fmt.Sprintf("cycle %d: condition held %s >= for %s",
			st.Cycle, now.Sub(st.ActiveSince).Round(time.Millisecond), r.ForDuration),
	}
}

// resolveLocked emits the single resolution of the current cycle and drops
// the state; the next satisfied sample starts a fresh cycle. Caller holds
// e.mu.
func (e *Engine) resolveLocked(k string, st *model.AlertState, r model.Rule, now time.Time) model.Event {
	held := "first contradicting sample"
	if !st.ResolveSince.IsZero() {
		held = fmt.Sprintf("condition false held %s >= resolve_for %s",
			now.Sub(st.ResolveSince).Round(time.Millisecond), r.ResolveDuration)
	}
	st.Status = model.StatusInactive
	st.LastChangeAt = now
	st.LastEvalTime = now
	ev := model.Event{
		Type: model.EventResolved, Time: now, State: cloneState(st),
		Detail: fmt.Sprintf("cycle %d: %s", st.Cycle, held),
	}
	delete(e.states, k)
	return ev
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

// Reset clears all alert states and cycle counters.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.states = make(map[string]*model.AlertState)
	e.cycles = make(map[string]int)
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
