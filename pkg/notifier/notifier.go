// Package notifier turns firing/resolved events into notifications, applying
// silence suppression and stable-key de-duplication.
//
// Semantics:
//   - Silence suppresses delivery only; the incoming alert state is left
//     untouched, so statistics in the pipeline still count the transition.
//   - De-duplication uses a stable identity (rule revision + series + cycle).
//     At most one firing and one recovery notification are delivered per
//     cycle; repeated reports while firing never re-notify, and a series
//     that triggers again after a recovery starts a new cycle with its own
//     exactly-once guarantee instead of being swallowed as a duplicate.
//   - A firing that was suppressed still "consumes" the firing slot of its
//     own cycle (the old alert is not re-delivered after the window ends),
//     but later cycles are unaffected: once the silence expires, the next
//     round notifies normally.
//   - De-dup memory is bounded: only the most recent MaxRounds cycles per
//     alert key are remembered. Cycles evicted by the bound never block new
//     (higher) cycles; a re-report of an evicted old cycle is treated as
//     new, which is the documented trade-off of bounded memory.
package notifier

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// DefaultMaxRounds is the de-dup retention when no explicit bound is set:
// the most recent 64 cycles per alert key are remembered.
const DefaultMaxRounds = 64

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

// Option customizes a Notifier.
type Option func(*Notifier)

// WithMaxRounds bounds the de-duplication memory to the most recent n
// cycles per alert key. Values <= 0 select DefaultMaxRounds.
func WithMaxRounds(n int) Option {
	return func(nt *Notifier) {
		if n > 0 {
			nt.maxRounds = n
		}
	}
}

// cycleSlots records the delivered/suppressed slots of one cycle.
type cycleSlots struct {
	fired    bool
	resolved bool
}

// keyMem is the bounded de-dup memory of one alert key.
type keyMem struct {
	cycles   map[int]*cycleSlots
	maxCycle int
}

// Notifier guarantees each stable alert key delivers at most one firing
// notification and one recovery notification per cycle.
type Notifier struct {
	mu        sync.Mutex
	recorder  Recorder
	silencer  Silencer
	maxRounds int
	mem       map[string]*keyMem
}

// New creates a Notifier. A nil silencer means nothing is ever silenced.
func New(r Recorder, s Silencer, opts ...Option) *Notifier {
	if r == nil {
		panic("notifier: recorder must not be nil")
	}
	n := &Notifier{
		recorder:  r,
		silencer:  s,
		maxRounds: DefaultMaxRounds,
		mem:       make(map[string]*keyMem),
	}
	for _, o := range opts {
		o(n)
	}
	return n
}

// stableKey is the de-duplication identity of one alert instance.
func stableKey(ev model.Event) string {
	if ev.State == nil {
		return ""
	}
	return ev.State.AlertKey + "|" + ev.State.SeriesKey
}

// cycleOf extracts the round number carried by the event's state.
func cycleOf(ev model.Event) int {
	if ev.State == nil {
		return 0
	}
	return ev.State.Cycle
}

// slotLocked returns the slots for (key, cycle) and whether the cycle is
// still within the retained window. Cycles outside the window are not
// remembered. Caller holds n.mu.
func (n *Notifier) slotLocked(key string, cycle int) (slots *cycleSlots, retained bool) {
	km := n.mem[key]
	if km == nil {
		km = &keyMem{cycles: make(map[int]*cycleSlots)}
		n.mem[key] = km
	}
	if cycle > km.maxCycle {
		km.maxCycle = cycle
	}
	// Evict cycles beyond the retention window. Eviction only removes
	// strictly older rounds, so a new round is never blocked by an old one.
	watermark := km.maxCycle - n.maxRounds
	for c := range km.cycles {
		if c <= watermark {
			delete(km.cycles, c)
		}
	}
	if cycle <= watermark {
		return nil, false
	}
	slots = km.cycles[cycle]
	if slots == nil {
		slots = &cycleSlots{}
		km.cycles[cycle] = slots
	}
	return slots, true
}

// Dispatch applies silence and de-duplication. The returned event is the
// original firing/resolved event when delivered, an EventSuppressed copy
// when a silence matched or the same cycle+type was already handled, or the
// input unchanged for unrelated event types.
func (n *Notifier) Dispatch(ev model.Event) model.Event {
	switch ev.Type {
	case model.EventFiring, model.EventResolved:
	default:
		return ev
	}
	key := stableKey(ev)
	cycle := cycleOf(ev)

	n.mu.Lock()
	defer n.mu.Unlock()

	slots, retained := n.slotLocked(key, cycle)
	var seen *bool
	if retained {
		if ev.Type == model.EventFiring {
			seen = &slots.fired
		} else {
			seen = &slots.resolved
		}
		if *seen {
			// Duplicate transition within the same cycle: never deliver twice.
			return model.Event{
				Type:   model.EventSuppressed,
				Time:   ev.Time,
				State:  ev.State,
				Sample: ev.Sample,
				Detail: fmt.Sprintf("duplicate notification (cycle %d)", cycle),
			}
		}
	}

	// Silence is checked before marking the slot; suppression consumes the
	// slot of this cycle only, so later cycles notify normally.
	if n.silencer != nil && ev.State != nil {
		if sil := n.silencer.Match(ev.State.Metric, ev.State.Labels, ev.Time); sil != nil {
			if seen != nil {
				*seen = true
			}
			return model.Event{
				Type:      model.EventSuppressed,
				Time:      ev.Time,
				State:     ev.State,
				Sample:    ev.Sample,
				SilenceID: sil.ID,
				Detail:    fmt.Sprintf("matched silence %s (cycle %d)", sil.ID, cycle),
			}
		}
	}

	if seen != nil {
		*seen = true
	}
	n.recorder.Notify(ev)
	out := ev
	out.Type = model.EventNotified
	return out
}

// Delivered returns keys that have been delivered or suppressed by type,
// ordered for deterministic inspection. Each entry carries its cycle:
// "rule:...#vN|series|cycle=C".
func (n *Notifier) Delivered() (fired []string, resolved []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, km := range n.mem {
		for c, slots := range km.cycles {
			if slots.fired {
				fired = append(fired, k+"|cycle="+strconv.Itoa(c))
			}
			if slots.resolved {
				resolved = append(resolved, k+"|cycle="+strconv.Itoa(c))
			}
		}
	}
	sort.Strings(fired)
	sort.Strings(resolved)
	return fired, resolved
}

// MemoryEntries reports the number of retained (key, cycle) slot pairs;
// it is bounded by the number of alert keys times MaxRounds.
func (n *Notifier) MemoryEntries() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	total := 0
	for _, km := range n.mem {
		total += len(km.cycles)
	}
	return total
}

// String summarizes the retention configuration for logs.
func (n *Notifier) String() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "notifier{maxRounds=%d keys=%d}", n.maxRounds, len(n.mem))
	return b.String()
}

// Reset clears de-duplication memory.
func (n *Notifier) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mem = make(map[string]*keyMem)
}
