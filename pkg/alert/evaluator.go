package alert

import (
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
)

// SeriesState 是某条规则×序列维度的状态机状态。
type SeriesState struct {
	RuleID       string
	SeriesKey    metric.SeriesKey
	State        State
	Since        time.Time // 进入当前状态的时刻（窗口结束时间）
	BreachStart  time.Time // 当前连续越界区间的首个窗口结束时刻
	RecoverStart time.Time // firing 后当前连续恢复区间的首个窗口结束时刻
	LastBreachTS time.Time
	LastOKTS     time.Time
	LastValue    float64
	RuleVersion  string
	LastWindow   time.Time
}

// Evaluator 对聚合结果做规则判定，维护规则×序列维度的状态机。
// 评估过程是确定性的：按窗口时间顺序喂入同样的输入，必得同样的事件序列。
type Evaluator struct {
	rules  map[string]Rule
	states map[string]*SeriesState // key: ruleID + "\x00" + seriesKey
}

func stateKey(ruleID string, sk metric.SeriesKey) string {
	return ruleID + "\x00" + string(sk)
}

// NewEvaluator 创建评估器。
func NewEvaluator(rules []Rule) *Evaluator {
	e := &Evaluator{
		rules:  make(map[string]Rule),
		states: make(map[string]*SeriesState),
	}
	for _, r := range rules {
		e.rules[r.ID] = r
	}
	return e
}

// UpdateRules 替换规则集，返回新增、内容变更、移除的规则 ID。
// 被变更或移除规则对应的序列状态会被清除，避免旧配置的状态污染新结论；
// 对历史样本重新跑一遍评估即可按新配置复现判定（见 Replay）。
func (e *Evaluator) UpdateRules(rules []Rule) (added, changed, removed []string) {
	next := make(map[string]Rule, len(rules))
	for _, r := range rules {
		next[r.ID] = r
	}

	for id, r := range next {
		old, ok := e.rules[id]
		switch {
		case !ok:
			added = append(added, id)
		case old.Fingerprint() != r.Fingerprint():
			changed = append(changed, id)
		}
	}
	for id := range e.rules {
		if _, ok := next[id]; !ok {
			removed = append(removed, id)
		}
	}
	drop := map[string]bool{}
	for _, id := range changed {
		drop[id] = true
	}
	for _, id := range removed {
		drop[id] = true
	}
	if len(drop) > 0 {
		for k, st := range e.states {
			if drop[st.RuleID] {
				delete(e.states, k)
			}
		}
	}
	e.rules = next
	sort.Strings(added)
	sort.Strings(changed)
	sort.Strings(removed)
	return added, changed, removed
}

// matchRule 判断一个聚合序列是否被规则选中：指标名相等且所有选择器标签相等。
func matchRule(r Rule, ag *metric.Aggregate) bool {
	if ag.Name != r.Metric {
		return false
	}
	for k, v := range r.MatchLabels {
		if ag.Labels[k] != v {
			return false
		}
	}
	return true
}

// Evaluate 对一个窗口的聚合结果做状态推进，windowEnd 为该窗口结束时刻，
// windowLen 为窗口长度（用于把离散窗口数换算成持续时长）。
// 已跟踪但本窗口无数据的序列视为"未越界"参与推进，保证恢复判定不被缺数据卡住。
func (e *Evaluator) Evaluate(windowEnd time.Time, windowLen time.Duration,
	aggs map[metric.SeriesKey]*metric.Aggregate) []Event {

	ruleIDs := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ruleIDs = append(ruleIDs, id)
	}
	sort.Strings(ruleIDs)

	type hit struct {
		rule Rule
		key  metric.SeriesKey
		ag   *metric.Aggregate // nil 表示本窗口无数据
	}
	var hits []hit
	for _, id := range ruleIDs {
		r := e.rules[id]
		keys := make([]metric.SeriesKey, 0, len(aggs))
		for k := range aggs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		m := map[metric.SeriesKey]*metric.Aggregate{}
		for _, k := range keys {
			if ag := aggs[k]; matchRule(r, ag) {
				m[k] = ag
			}
		}
		// 已跟踪但本窗口无数据的序列也要推进（视为未越界）。
		for _, st := range e.states {
			if st.RuleID != id {
				continue
			}
			if _, ok := m[st.SeriesKey]; !ok {
				m[st.SeriesKey] = nil
			}
		}
		for k, ag := range m {
			hits = append(hits, hit{rule: r, key: k, ag: ag})
		}
	}

	var events []Event
	for _, h := range hits {
		if ev := e.advance(h.rule, h.key, h.ag, windowEnd, windowLen); ev != nil {
			events = append(events, *ev)
		}
	}
	return events
}

// breachDuration 把"从首个窗口到当前窗口（含两端）"换算成持续时长。
// 离散窗口数 = (at-start)/windowLen + 1，乘以窗口长度。
func breachDuration(start, at time.Time, windowLen time.Duration) time.Duration {
	if start.IsZero() {
		return 0
	}
	windows := int64(1)
	if windowLen > 0 {
		windows += int64(at.Sub(start) / windowLen)
	}
	return time.Duration(windows) * windowLen
}

// advance 推进单个规则×序列的状态机。
func (e *Evaluator) advance(r Rule, key metric.SeriesKey, ag *metric.Aggregate,
	at time.Time, windowLen time.Duration) *Event {

	sk := stateKey(r.ID, key)
	st, ok := e.states[sk]
	if !ok {
		st = &SeriesState{
			RuleID:      r.ID,
			SeriesKey:   key,
			State:       StateInactive,
			Since:       at,
			RuleVersion: r.Fingerprint(),
		}
		e.states[sk] = st
	}

	breach := false
	value := 0.0
	if ag != nil && ag.Count > 0 {
		value = ag.Last
		breach = r.Comparator.Compare(ag.Last, r.Threshold)
	}
	if breach {
		st.LastBreachTS = at
	} else {
		st.LastOKTS = at
	}
	st.LastValue = value
	st.LastWindow = at

	prev := st.State

	switch st.State {
	case StateInactive:
		if !breach {
			return nil
		}
		st.State = StatePending
		st.Since = at
		st.BreachStart = at
		d := breachDuration(st.BreachStart, at, windowLen)
		// ForDuration 为 0 时同一窗口直接触发。
		if d >= r.ForDuration {
			st.State = StateFiring
			st.Since = at
			return e.mkEvent(st, prev, TrToFiring, at, value, breach,
				fmt.Sprintf("value %v %s threshold %v at %s; for=%s satisfied at once -> firing",
					value, r.Comparator, r.Threshold, at.UTC().Format("15:04:05"), r.ForDuration))
		}
		return e.mkEvent(st, prev, TrToPending, at, value, breach,
			fmt.Sprintf("value %v %s threshold %v at %s; -> pending, breach lasted %s, need %s",
				value, r.Comparator, r.Threshold, at.UTC().Format("15:04:05"), d, r.ForDuration))

	case StatePending:
		if !breach {
			// 等待期内恢复：立即回 inactive。短抖动被吞掉，不产生对外通知。
			st.State = StateInactive
			st.Since = at
			st.BreachStart = time.Time{}
			return e.mkEvent(st, prev, TrNone, at, value, breach,
				fmt.Sprintf("value %v recovered during pending at %s before %s; -> inactive, flap suppressed, no notification",
					value, at.UTC().Format("15:04:05"), r.ForDuration))
		}
		d := breachDuration(st.BreachStart, at, windowLen)
		if d >= r.ForDuration {
			st.State = StateFiring
			st.Since = at
			return e.mkEvent(st, prev, TrToFiring, at, value, breach,
				fmt.Sprintf("breach sustained %s >= for=%s (value %v %s %v); -> firing",
					d, r.ForDuration, value, r.Comparator, r.Threshold))
		}
		return e.mkEvent(st, prev, TrNone, at, value, breach,
			fmt.Sprintf("still pending: breach lasted %s, need %s (value %v %s %v)",
				d, r.ForDuration, value, r.Comparator, r.Threshold))

	case StateFiring:
		if breach {
			// 持续越界或抖动后重新越界：保持 firing，不重复告警；取消恢复计时。
			if st.BreachStart.IsZero() {
				st.BreachStart = at
			}
			st.RecoverStart = time.Time{}
			return e.mkEvent(st, prev, TrNone, at, value, breach,
				fmt.Sprintf("firing continues (value %v %s %v); no duplicate alert",
					value, r.Comparator, r.Threshold))
		}
		// 未越界：开始/继续恢复观察；连续恢复达 KeepDuration 才真正恢复。
		if st.RecoverStart.IsZero() {
			st.RecoverStart = at
		}
		d := breachDuration(st.RecoverStart, at, windowLen)
		if d >= r.KeepDuration {
			st.State = StateInactive
			st.Since = at
			st.BreachStart = time.Time{}
			st.RecoverStart = time.Time{}
			return e.mkEvent(st, prev, TrToInactive, at, value, breach,
				fmt.Sprintf("recovered sustained %s >= keep=%s (last value %v); -> inactive, resolved",
					d, r.KeepDuration, value))
		}
		return e.mkEvent(st, prev, TrNone, at, value, breach,
			fmt.Sprintf("firing, recovering for %s, need %s (value %v); hold firing, exit flap suppressed",
				d, r.KeepDuration, value))
	}
	return nil
}

func (e *Evaluator) mkEvent(st *SeriesState, from State, kind TransitionKind, at time.Time,
	value float64, breach bool, reason string) *Event {
	return &Event{
		RuleID:    st.RuleID,
		SeriesKey: st.SeriesKey,
		State:     st.State,
		From:      from,
		Kind:      kind,
		At:        at,
		Value:     value,
		Breach:    breach,
		Reason:    reason,
	}
}

// States 返回当前所有序列状态的副本（按 ruleID/seriesKey 排序）。
func (e *Evaluator) States() []SeriesState {
	keys := make([]string, 0, len(e.states))
	for k := range e.states {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]SeriesState, 0, len(keys))
	for _, k := range keys {
		out = append(out, *e.states[k])
	}
	return out
}
