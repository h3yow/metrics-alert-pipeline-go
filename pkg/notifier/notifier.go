// Package notifier turns firing/resolved events into notifications, applying
// silence suppression and stable-key de-duplication.
//
// Semantics:
//   - Silence suppresses delivery only; the incoming alert state is left
//     untouched, so statistics in the pipeline still count the transition.
//   - De-duplication uses a stable identity (rule revision + series + state
//     cycle). At most one firing and one recovery notification are delivered
//     per cycle; repeated reports while firing never re-notify.
//   - A firing that was suppressed still "consumes" the firing slot, and a
//     recovery is still delivered if it is no longer silenced, so recoveries
//     are never lost.
package notifier

import (
	"sort"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// Recorder is the sink for delivered notifications (e.g. logs).
type Recorder interface {
	Notify(ev model.Event)
}

// RecorderFunc adapts a function to Recorder.
type RecorderFunc func(ev model.Event)

func (f RecorderFunc) Notify(ev model.Event) { f(ev) }

// Silencer reports whether an alert is silenced at the event time.
type Silencer interface {
	Match(metric string, ls model.Labels, t time.Time) *model.Silence
}

// Notifier guarantees each stable alert key delivers at most one firing
// notification and one recovery notification per state cycle.
type Notifier struct {
	mu           sync.Mutex
	recorder     Recorder
	silencer     Silencer
	firedSeen    map[string]bool
	resolvedSeen map[string]bool
}

// New creates a Notifier. A nil silencer means nothing is ever silenced.
func New(r Recorder, s Silencer) *Notifier {
	if r == nil {
		panic("notifier: recorder must not be nil")
	}
	return &Notifier{
		recorder:     r,
		silencer:     s,
		firedSeen:    make(map[string]bool),
		resolvedSeen: make(map[string]bool),
	}
}

// stableKey is the de-duplication identity of one alert cycle.
func stableKey(ev model.Event) string {
	if ev.State == nil {
		return ""
	}
	return ev.State.AlertKey + "|" + ev.State.SeriesKey
}

// Dispatch applies silence and de-duplication. The returned event is the
// original firing/resolved event when delivered, an EventSuppressed copy
// when a silence matched, or the input unchanged for unrelated event types.
func (n *Notifier) Dispatch(ev model.Event) model.Event {
	switch ev.Type {
	case model.EventFiring, model.EventResolved:
	default:
		return ev
	}
	key := stableKey(ev)

	n.mu.Lock()
	defer n.mu.Unlock()

	var seen map[string]bool
	if ev.Type == model.EventFiring {
		seen = n.firedSeen
	} else {
		seen = n.resolvedSeen
	}
	if seen[key] {
		// Duplicate transition: never deliver twice.
		return model.Event{
			Type:   model.EventSuppressed,
			Time:   ev.Time,
			State:  ev.State,
			Sample: ev.Sample,
			Detail: "duplicate notification",
		}
	}

	// Silence is checked after the dedup slot logic; we mark the slot only
	// after knowing the outcome below.
	if n.silencer != nil && ev.State != nil {
		if sil := n.silencer.Match(ev.State.Metric, ev.State.Labels, ev.Time); sil != nil {
			// Suppression consumes the slot: the same firing is not
			// re-delivered later, but a future recovery still is.
			seen[key] = true
			return model.Event{
				Type:      model.EventSuppressed,
				Time:      ev.Time,
				State:     ev.State,
				Sample:    ev.Sample,
				SilenceID: sil.ID,
				Detail:    "matched silence " + sil.ID,
			}
		}
	}

	seen[key] = true
	n.recorder.Notify(ev)
	out := ev
	out.Type = model.EventNotified
	return out
}

// Delivered returns keys that have been delivered or suppressed by type,
// ordered for deterministic inspection.
func (n *Notifier) Delivered() (fired []string, resolved []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, ok := range n.firedSeen {
		if ok {
			fired = append(fired, k)
		}
	}
	for k, ok := range n.resolvedSeen {
		if ok {
			resolved = append(resolved, k)
		}
	}
	sort.Strings(fired)
	sort.Strings(resolved)
	return fired, resolved
}

// Reset clears de-duplication memory.
func (n *Notifier) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.firedSeen = make(map[string]bool)
	n.resolvedSeen = make(map[string]bool)
}
