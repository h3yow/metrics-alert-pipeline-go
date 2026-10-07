// Package model defines the core data types shared across the metrics
// collection, aggregation and alerting pipeline.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SourceLabel is the reserved label carrying the reporting source. Clients
// must not set it directly; the collector fills it from Sample.Source.
const SourceLabel = "__source__"

var labelNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// RejectReason explains why a sample was rejected.
type RejectReason string

const (
	ReasonEmptyName      RejectReason = "empty_metric_name"
	ReasonBadTimestamp   RejectReason = "bad_timestamp"
	ReasonNaN            RejectReason = "nan_value"
	ReasonInf            RejectReason = "infinite_value"
	ReasonInvalidLabel   RejectReason = "invalid_label"
	ReasonSourceMismatch RejectReason = "source_mismatch"
	ReasonEmptySource    RejectReason = "empty_source"
)

// RejectError is returned for samples that fail validation. The reason is
// machine-readable and distinguishes failure categories.
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Detail
}

// Labels is an ordered set of key/value pairs. Fingerprints are computed
// over the sorted key set so construction order does not affect identity.
type Labels []Label

// Label is a single key/value tag.
type Label struct {
	Name  string
	Value string
}

func (ls Labels) Validate() error {
	seen := make(map[string]struct{}, len(ls))
	for _, l := range ls {
		if !labelNameRE.MatchString(l.Name) {
			return &RejectError{Reason: ReasonInvalidLabel, Detail: "invalid label name: " + l.Name}
		}
		if _, dup := seen[l.Name]; dup {
			return &RejectError{Reason: ReasonInvalidLabel, Detail: "duplicate label: " + l.Name}
		}
		seen[l.Name] = struct{}{}
	}
	return nil
}

// encoded produces a collision-free, order-independent representation.
func (ls Labels) encoded() string {
	cp := append(Labels(nil), ls...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })
	var b strings.Builder
	for _, l := range cp {
		b.WriteString(strconv.Itoa(len(l.Name)))
		b.WriteByte(':')
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(len(l.Value)))
		b.WriteByte(':')
		b.WriteString(l.Value)
		b.WriteByte(',')
	}
	return b.String()
}

// Fingerprint returns a stable, fixed-width identity for the label set.
func (ls Labels) Fingerprint() string {
	sum := sha256.Sum256([]byte(ls.encoded()))
	return hex.EncodeToString(sum[:16])
}

func (ls Labels) Get(name string) (string, bool) {
	for _, l := range ls {
		if l.Name == name {
			return l.Value, true
		}
	}
	return "", false
}

// Merge returns receiver labels plus extras; receiver values win on key
// collision. Construction order of the receiver is preserved.
func (ls Labels) Merge(extra Labels) Labels {
	out := append(Labels(nil), ls...)
	have := make(map[string]struct{}, len(ls))
	for _, l := range ls {
		have[l.Name] = struct{}{}
	}
	for _, l := range extra {
		if _, ok := have[l.Name]; ok {
			continue
		}
		out = append(out, l)
		have[l.Name] = struct{}{}
	}
	return out
}

// Match reports whether every selector entry exists with the same value.
func (ls Labels) Match(sel map[string]string) bool {
	for k, v := range sel {
		gv, ok := ls.Get(k)
		if !ok || gv != v {
			return false
		}
	}
	return true
}

// Sample is a single metric observation reported by a source.
type Sample struct {
	Metric    string
	Labels    Labels
	Value     float64
	Timestamp time.Time
	Source    string
}

// SeriesKey identifies a metric/label-set series, independent of time.
func (s Sample) SeriesKey() string {
	return s.Metric + "|" + s.Labels.Fingerprint()
}

// Identity is the de-duplication identity: the exact same observation
// (same series, timestamp and value) is reported at most once.
func (s Sample) Identity() string {
	return s.SeriesKey() + "|" +
		strconv.FormatInt(s.Timestamp.UnixNano(), 10) + "|" +
		strconv.FormatUint(math.Float64bits(s.Value), 16)
}

// Validate returns a *RejectError with a distinguishable reason.
func (s Sample) Validate() error {
	if strings.TrimSpace(s.Metric) == "" {
		return &RejectError{Reason: ReasonEmptyName}
	}
	if s.Timestamp.IsZero() {
		return &RejectError{Reason: ReasonBadTimestamp, Detail: "timestamp is zero"}
	}
	if math.IsNaN(s.Value) {
		return &RejectError{Reason: ReasonNaN}
	}
	if math.IsInf(s.Value, 0) {
		return &RejectError{Reason: ReasonInf}
	}
	if err := s.Labels.Validate(); err != nil {
		return err
	}
	if v, ok := s.Labels.Get(SourceLabel); ok && v != s.Source {
		return &RejectError{
			Reason: ReasonSourceMismatch,
			Detail: fmt.Sprintf("label %s=%q but source=%q", SourceLabel, v, s.Source),
		}
	}
	return nil
}

// Aggregate is the merged result for one series over one window.
type Aggregate struct {
	Metric      string
	Labels      Labels
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Last        float64
	First       float64
}

// AlertStatus is the state of an alert rule instance.
type AlertStatus string

const (
	StatusInactive  AlertStatus = "inactive"
	StatusPending   AlertStatus = "pending"
	StatusFiring    AlertStatus = "firing"
	StatusResolving AlertStatus = "resolving"
)

// Operator is a threshold comparison operator.
type Operator string

const (
	OpGt  Operator = ">"
	OpGte Operator = ">="
	OpLt  Operator = "<"
	OpLte Operator = "<="
	OpEq  Operator = "=="
	OpNeq Operator = "!="
)

// Rule is an alerting rule. Version marks rule revisions; judgments made
// under an old version never silently merge into a new one.
//
// ForDuration guards triggering: the condition must hold continuously for
// that long before firing. ResolveDuration independently guards recovery:
// contradicting evidence must hold continuously for that long before the
// alert resolves; a single satisfied sample in between cancels the pending
// recovery. ResolveDuration == 0 resolves on the first contradicting sample
// (the historical single-cycle behavior).
type Rule struct {
	ID              string
	Metric          string
	Match           map[string]string
	Operator        Operator
	Threshold       float64
	ForDuration     time.Duration
	ResolveDuration time.Duration
	Version         int
}

// Key is the stable identity of the rule definition (without version).
func (r Rule) Key() string { return "rule:" + r.ID }

// VersionedKey identifies one specific revision of a rule.
func (r Rule) VersionedKey() string {
	return fmt.Sprintf("rule:%s#v%d", r.ID, r.Version)
}

func (r Rule) MatchesSeries(metric string, ls Labels) bool {
	return r.Metric == metric && ls.Match(r.Match)
}

func (r Rule) Evaluate(v float64) bool { return Compare(r.Operator, v, r.Threshold) }

func (r Rule) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("rule id is empty")
	}
	if strings.TrimSpace(r.Metric) == "" {
		return fmt.Errorf("rule %s metric is empty", r.ID)
	}
	switch r.Operator {
	case OpGt, OpGte, OpLt, OpLte, OpEq, OpNeq:
	default:
		return fmt.Errorf("rule %s has unknown operator %q", r.ID, r.Operator)
	}
	if r.ForDuration < 0 {
		return fmt.Errorf("rule %s has negative for duration", r.ID)
	}
	if r.ResolveDuration < 0 {
		return fmt.Errorf("rule %s has negative resolve duration", r.ID)
	}
	return nil
}

// Compare compares value with threshold using op.
func Compare(op Operator, value, threshold float64) bool {
	switch op {
	case OpGt:
		return value > threshold
	case OpGte:
		return value >= threshold
	case OpLt:
		return value < threshold
	case OpLte:
		return value <= threshold
	case OpEq:
		return value == threshold
	case OpNeq:
		return value != threshold
	default:
		return false
	}
}

// Silence suppresses notifications (never alert state or statistics) for
// matching alerts during the half-open window [StartsAt, EndsAt).
type Silence struct {
	ID       string
	Metric   string
	Match    map[string]string
	StartsAt time.Time
	EndsAt   time.Time
}

func (s Silence) Key() string { return "silence:" + s.ID }

func (s Silence) Contains(t time.Time) bool {
	return !t.Before(s.StartsAt) && t.Before(s.EndsAt)
}

func (s Silence) Matches(metric string, ls Labels) bool {
	if s.Metric != "" && s.Metric != metric {
		return false
	}
	return ls.Match(s.Match)
}

func (s Silence) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("silence id is empty")
	}
	if !s.EndsAt.After(s.StartsAt) {
		return fmt.Errorf("silence %s ends before it starts", s.ID)
	}
	return nil
}

// AlertState is the current state of one rule/series pair.
//
// Cycle identifies the trigger round: it is assigned when the alert enters
// firing and stays attached through resolving/resolved, so every firing and
// recovery notification carries the round it belongs to. The same series
// firing again after a recovery starts a new cycle with the next number.
type AlertState struct {
	AlertKey         string
	RuleID           string
	RuleVersion      int
	SeriesKey        string
	Metric           string
	Labels           Labels
	Status           AlertStatus
	Cycle            int
	ActiveSince      time.Time
	ResolveSince     time.Time
	LastValue        float64
	LastEvalTime     time.Time
	LastChangeAt     time.Time
	FiredNotified    bool
	ResolvedNotified bool
}

// EventType classifies pipeline events.
type EventType string

const (
	EventAccepted   EventType = "sample_accepted"
	EventRejected   EventType = "sample_rejected"
	EventDuplicate  EventType = "sample_duplicate"
	EventPending    EventType = "alert_pending"
	EventFiring     EventType = "alert_firing"
	EventResolving  EventType = "alert_resolving"
	EventResolved   EventType = "alert_resolved"
	EventSuppressed EventType = "notification_suppressed"
	EventNotified   EventType = "notification_sent"
)

// Event is an observable fact produced while processing a sample or a tick.
type Event struct {
	Type      EventType
	Time      time.Time
	Sample    *Sample
	Reason    RejectReason
	Detail    string
	State     *AlertState
	SilenceID string
}
