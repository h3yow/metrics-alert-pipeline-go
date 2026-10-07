// Package notifier turns firing/resolved events into notifications, applying
// silence suppression and stable-key de-duplication.
//
// Semantics:
//   - Silence suppresses delivery only; the incoming alert state, its cycle
//     numbering and the pipeline statistics are left untouched, and no
//     de-dup slot is consumed: once the silence window ends, later rounds
//     (or a re-dispatch of the same transition) are delivered normally.
//   - De-duplication uses a stable identity (rule revision + series +
//     cycle). Every trigger/recovery round gets its own slot, so at most
//     one firing and one recovery notification are delivered per round;
//     repeated reports of the same round never re-notify, while a new
//     round of the same series is never swallowed by an old one.
//   - De-dup memory is bounded: per alert key only the most recent
//     RetainCycles rounds are remembered. Evicted old rounds can never
//     suppress a newer round (round numbers are monotonic); a re-delivery
//     of an evicted round is treated as a duplicate, never as new.
package notifier

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
)

// DefaultRetainCycles bounds de-dup memory when no explicit option is set:
// per alert key the most recent 1024 rounds are remembered.
const DefaultRetainCycles = 1024

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

// Options tunes the notifier.
type Options struct {
	// RetainCycles is how many of the most recent rounds are remembered
	// per alert key for de-duplication. Values <= 0 select
	// DefaultRetainCycles.
	RetainCycles int
}

// cycleMem is the bounded de-dup memory of one alert key: the highest
// round seen plus the exact rounds still inside the retention window.
type cycleMem struct {
	max  int
	seen map[int]bool
}

// Notifier guarantees each stable alert key delivers at most one firing
// notification and one recovery notification per cycle (round).
type Notifier struct {
	mu           sync.Mutex
	recorder     Recorder
	silencer     Silencer
	retainCycles int
	firedSeen    map[string]*cycleMem
	resolvedSeen map[string]*cycleMem
}

// New creates a Notifier with default options. A nil silencer means
// nothing is ever silenced.
func New(r Recorder, s Silencer) *Notifier {
	return NewWithOptions(r, s, Options{})
}

// NewWithOptions creates a Notifier with explicit options.
func NewWithOptions(r Recorder, s Silencer, opts Options) *Notifier {
	if r == nil {
		panic("notifier: recorder must not be nil")
	}
	if opts.RetainCycles <= 0 {
		opts.RetainCycles = DefaultRetainCycles
	}
	return &Notifier{
		recorder:     r,
		silencer:     s,
		retainCycles: opts.RetainCycles,
		firedSeen:    make(map[string]*cycleMem),
		resolvedSeen: make(map[string]*cycleMem),
	}
}

// stableKey is the de-duplication identity of one alert round.
func stableKey(ev model.Event) string {
	if ev.State == nil {
		return ""
	}
	return ev.State.AlertKey + "|" + ev.State.SeriesKey
}

// Dispatch applies silence and de-duplication. The returned event is the
// original firing/resolved event when delivered, an EventSuppressed copy
// when a silence matched or the round was already handled, or the input
// unchanged for unrelated event types.
func (n *Notifier) Dispatch(ev model.Event) model.Event {
	switch ev.Type {
	case model.EventFiring, model.EventResolved:
	default:
		return ev
	}
	key := stableKey(ev)
	cycle := 0
	if ev.State != nil {
		cycle = ev.State.Cycle
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	table := n.firedSeen
	if ev.Type == model.EventResolved {
		table = n.resolvedSeen
	}
	mem := table[key]
	if mem == nil {
		mem = &cycleMem{seen: make(map[int]bool)}
		table[key] = mem
	}

	suppressed := func(detail string) model.Event {
		return model.Event{
			Type:   model.EventSuppressed,
			Time:   ev.Time,
			State:  ev.State,
			Sample: ev.Sample,
			Detail: detail,
		}
	}

	if mem.seen[cycle] {
		// Duplicate transition of the same round: never deliver twice.
		return suppressed(fmt.Sprintf("duplicate notification (cycle %d)", cycle))
	}
	if cycle < mem.max && cycle <= mem.max-n.retainCycles {
		// The round fell out of the retention window; it can only be a
		// late re-delivery of an old round, never a new one (round numbers
		// are monotonic per key).
		return suppressed(fmt.Sprintf("cycle %d evicted from dedup memory", cycle))
	}

	// Silence is checked before any slot is recorded and never consumes
	// one: suppression only drops this single notification.
	if n.silencer != nil && ev.State != nil {
		if sil := n.silencer.Match(ev.State.Metric, ev.State.Labels, ev.Time); sil != nil {
			out := suppressed("matched silence " + sil.ID)
			out.SilenceID = sil.ID
			return out
		}
	}

	mem.seen[cycle] = true
	if cycle > mem.max {
		mem.max = cycle
	}
	// Evict rounds outside the retention window so memory stays bounded.
	low := mem.max - n.retainCycles + 1
	for c := range mem.seen {
		if c < low {
			delete(mem.seen, c)
		}
	}

	n.recorder.Notify(ev)
	out := ev
	out.Type = model.EventNotified
	return out
}

// Delivered returns the rounds that have been delivered (or are reserved)
// per type, as "key|cycle=N" strings ordered for deterministic inspection.
func (n *Notifier) Delivered() (fired []string, resolved []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	collect := func(table map[string]*cycleMem) []string {
		var out []string
		for k, mem := range table {
			for c := range mem.seen {
				out = append(out, k+"|cycle="+strconv.Itoa(c))
			}
		}
		sort.Strings(out)
		return out
	}
	return collect(n.firedSeen), collect(n.resolvedSeen)
}

// Reset clears de-duplication memory.
func (n *Notifier) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.firedSeen = make(map[string]*cycleMem)
	n.resolvedSeen = make(map[string]*cycleMem)
}
