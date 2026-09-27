// Package engine 把指标采集聚合、规则状态机评估、静默与通知去重串成一条本地链路。
// 所有入口均为并发安全；评估必须按窗口时间顺序推进（乱序安全由聚合层保证）。
package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/alert"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

// Notification 是对外的一条告警通知（触发或恢复）。
type Notification struct {
	ID         string // 稳定通知标识：rule|series|kind#occurrence
	RuleID     string
	SeriesKey  metric.SeriesKey
	Kind       alert.TransitionKind // to_firing 或 to_inactive
	At         time.Time
	Labels     map[string]string
	Value      float64
	SilencedBy []string // 命中的静默 ID
	Sent       bool     // false 表示被静默抑制：记录但不发送
	Reason     string
}

func (n Notification) String() string {
	state := "SENT"
	if !n.Sent {
		state = "SILENCED"
	}
	return fmt.Sprintf("[%s] notify id=%s rule=%s series=%s kind=%s at=%s value=%v silencedBy=%v",
		state, n.ID, n.RuleID, n.SeriesKey, n.Kind,
		n.At.UTC().Format(time.RFC3339Nano), n.Value, n.SilencedBy)
}

// Stats 汇总统计。静默只影响 Notified/Silenced，不影响 Firing/Resolved。
type Stats struct {
	Received   int64
	Accepted   int64
	Duplicated int64
	Rejected   int64
	Firing     int64
	Resolved   int64
	Notified   int64
	Silenced   int64
}

// Engine 串起采集聚合、规则评估、静默匹配与去重通知。
type Engine struct {
	mu     sync.Mutex
	window time.Duration
	agg    *metric.Aggregator
	ev     *alert.Evaluator
	sil    *silence.Store

	// 已产生通知的稳定 ID 集合，兜底去重。
	delivered map[string]struct{}
	// 每个 rule|series 的 firing/resolve 轮次计数，构成稳定通知 ID。
	occurrence map[string]map[alert.TransitionKind]int64

	// 已评估过的窗口，防止重复评估同一窗口导致重复通知。
	evaluatedWindows map[time.Time]struct{}

	stats Stats
	// 最近一轮产生的通知（含被静默的），便于调用方审计。
	lastNotifications []Notification
}

// New 创建引擎。
func New(window time.Duration, sourceLabels map[string]map[string]string,
	rules []alert.Rule, sils []silence.Silence) *Engine {
	return &Engine{
		window:           window,
		agg:              metric.NewAggregator(window, sourceLabels),
		ev:               alert.NewEvaluator(rules),
		sil:              silence.NewStore(sils),
		delivered:        map[string]struct{}{},
		occurrence:       map[string]map[alert.TransitionKind]int64{},
		evaluatedWindows: map[time.Time]struct{}{},
	}
}

// Report 上报一条样本（并发安全）。非法样本返回可区分的拒绝原因。
// 计数统一在引擎锁内维护，与并发写入严格一致，无锁间缝隙。
func (e *Engine) Report(s metric.Sample) *metric.RejectedSample {
	// 先做无锁的纯校验，非法样本不触碰聚合状态；合法样本再进入临界区。
	if rj := metric.Validate(s); rj != nil {
		e.mu.Lock()
		e.stats.Received++
		e.stats.Rejected++
		e.mu.Unlock()
		return rj
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	// 注意：聚合器内部自加锁，这里在引擎锁内调用，锁顺序恒为 engine -> aggregator，
	// 不会形成环路；去重判定与计数在同一引擎临界区内，对外口径一致。
	_, dup, _ := e.agg.IngestTracked(s)
	e.stats.Received++
	if dup {
		e.stats.Duplicated++
	} else {
		e.stats.Accepted++
	}
	return nil
}

// RunWindow 对窗口 [at-window, at) 做一次评估，按顺序必须用递增的 at 调用。
// 返回本轮产生的通知（含被静默抑制的通知，Sent=false）。
// 同一窗口重复评估会被拒绝（返回 nil），防止重复通知。
func (e *Engine) RunWindow(at time.Time) []Notification {
	e.mu.Lock()
	defer e.mu.Unlock()

	wStart := at.Add(-e.window)
	if _, ok := e.evaluatedWindows[wStart]; ok {
		return nil
	}
	e.evaluatedWindows[wStart] = struct{}{}

	bucket, _ := e.agg.Window(wStart)
	events := e.ev.Evaluate(at, e.window, bucket)
	notifs := e.emitNotifications(events, bucket)
	e.lastNotifications = notifs
	return notifs
}

// emitNotifications 把状态机事件转成对外通知：过滤内部流转、生成稳定 ID、
// 兜底去重、匹配静默、更新统计。调用方必须持有 e.mu。
func (e *Engine) emitNotifications(events []alert.Event,
	bucket map[metric.SeriesKey]*metric.Aggregate) []Notification {
	var notifs []Notification
	for _, ev := range events {
		// pending 进出属于内部状态流转，不产生对外通知。
		if ev.Kind != alert.TrToFiring && ev.Kind != alert.TrToInactive {
			continue
		}
		n := e.buildNotification(ev, bucket)
		// 兜底去重：理论上状态机不会重复，这里保证 ID 全局唯一。
		if _, ok := e.delivered[n.ID]; ok {
			continue
		}
		e.delivered[n.ID] = struct{}{}

		if ev.Kind == alert.TrToFiring {
			e.stats.Firing++
		} else {
			e.stats.Resolved++
		}
		if n.Sent {
			e.stats.Notified++
		} else {
			e.stats.Silenced++
		}
		notifs = append(notifs, n)
	}
	return notifs
}

// buildNotification 根据事件构造通知，完成静默匹配与标签解析。
func (e *Engine) buildNotification(ev alert.Event,
	bucket map[metric.SeriesKey]*metric.Aggregate) Notification {

	group := string(ev.RuleID) + "|" + string(ev.SeriesKey)
	occ, ok := e.occurrence[group]
	if !ok {
		occ = map[alert.TransitionKind]int64{}
		e.occurrence[group] = occ
	}
	occ[ev.Kind]++
	id := fmt.Sprintf("%s|%s#%d", group, ev.Kind, occ[ev.Kind])

	var labels map[string]string
	if ag := bucket[ev.SeriesKey]; ag != nil {
		labels = cloneLabels(ag.Labels)
	} else {
		labels = map[string]string{}
	}
	silencedBy := e.sil.MatchAt(labels, ev.At)

	return Notification{
		ID:         id,
		RuleID:     ev.RuleID,
		SeriesKey:  ev.SeriesKey,
		Kind:       ev.Kind,
		At:         ev.At,
		Labels:     labels,
		Value:      ev.Value,
		SilencedBy: silencedBy,
		Sent:       len(silencedBy) == 0,
		Reason:     ev.Reason,
	}
}

// Stats 返回统计快照（值拷贝）。
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// Snapshot 返回聚合器只读快照。
func (e *Engine) Snapshot() metric.Snapshot { return e.agg.Snapshot() }

// Aggregator 暴露底层聚合器，供重放工具直接读取窗口。
func (e *Engine) Aggregator() *metric.Aggregator { return e.agg }

// Evaluator 暴露底层评估器，供重放/状态检查。
func (e *Engine) Evaluator() *alert.Evaluator { return e.ev }

// LastNotifications 返回最近一次窗口评估产生的通知。
func (e *Engine) LastNotifications() []Notification {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Notification, len(e.lastNotifications))
	copy(out, e.lastNotifications)
	return out
}

// UpdateRules 更新规则集；受影响规则的状态机会被重置。
func (e *Engine) UpdateRules(rules []alert.Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ev.UpdateRules(rules)
}

// UpdateSilences 更新静默配置。只影响之后窗口的通知，状态与历史通知不变。
func (e *Engine) UpdateSilences(sils []silence.Silence) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sil.Update(sils)
}

func cloneLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
