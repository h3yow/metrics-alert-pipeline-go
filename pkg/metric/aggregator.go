package metric

import (
	"sync"
	"time"
)

// SourceLabelKey 是聚合器注入的来源标签，保留前缀，禁止样本自带。
const SourceLabelKey = "__source"

// Aggregator 是并发安全的多来源指标聚合器。
// 同一指标并发上报不允许丢失或重复累加；乱序样本按统一口径合并。
type Aggregator struct {
	mu           sync.Mutex
	window       time.Duration
	sourceLabels map[string]map[string]string
	// buckets[windowStart][seriesKey] 聚合槽
	buckets map[time.Time]map[SeriesKey]*Aggregate
	// seen 记录已处理过的样本 ID，保证同一 ID 只累加一次
	seen map[string]struct{}
	// idWindow 记录每个 ID 首次出现的窗口，便于审计（不影响判定）
	idWindow map[string]time.Time

	rejected []RejectedSample
	totalIn  int64
	totalDup int64
	totalOK  int64
	totalRej int64
	lastWin  time.Time
}

// NewAggregator 创建聚合器。window 为聚合窗口长度（按 epoch 对齐），
// sourceLabels 为每个来源附加的默认标签（可空）。
func NewAggregator(window time.Duration, sourceLabels map[string]map[string]string) *Aggregator {
	if window <= 0 {
		window = time.Minute
	}
	sl := make(map[string]map[string]string, len(sourceLabels))
	for src, l := range sourceLabels {
		sl[src] = cloneLabels(l)
	}
	return &Aggregator{
		window:       window,
		sourceLabels: sl,
		buckets:      make(map[time.Time]map[SeriesKey]*Aggregate),
		seen:         make(map[string]struct{}),
		idWindow:     make(map[string]time.Time),
	}
}

// alignWindow 将任意时刻对齐到窗口起点（Unix 0 起整窗对齐，保证口径统一）。
func (a *Aggregator) alignWindow(t time.Time) time.Time {
	u := t.UnixNano()
	w := int64(a.window)
	start := (u / w) * w
	return time.Unix(0, start).In(t.Location())
}

// mergedLabels 归并来源默认标签、样本标签，并注入保留来源标签。
// 样本标签优先级高于来源默认标签。
func (a *Aggregator) mergedLabels(s Sample) map[string]string {
	base := cloneLabels(a.sourceLabels[s.Source])
	for k, v := range s.Labels {
		base[k] = v
	}
	base[SourceLabelKey] = s.Source
	return base
}

// Ingest 接收一条样本（并发安全）。
// 返回该样本归属窗口的起点；非法样本返回原因（不计入任何聚合）。
// 带相同 ID 的重复样本只首次生效，后续记为 Duplicate，不累加。
func (a *Aggregator) Ingest(s Sample) (time.Time, *RejectedSample) {
	w, _, rj := a.ingest(s)
	return w, rj
}

// IngestTracked 与 Ingest 相同，额外返回该样本是否因重复 ID 被忽略
// （true 表示本次未参与聚合）。供需要在同一临界区同步计数的上层使用。
func (a *Aggregator) IngestTracked(s Sample) (time.Time, bool, *RejectedSample) {
	return a.ingest(s)
}

func (a *Aggregator) ingest(s Sample) (time.Time, bool, *RejectedSample) {
	if rj := Validate(s); rj != nil {
		a.mu.Lock()
		a.totalIn++
		a.totalRej++
		a.rejected = append(a.rejected, *rj)
		a.mu.Unlock()
		return time.Time{}, false, rj
	}
	if s.Kind == "" {
		s.Kind = Gauge
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalIn++

	// 基于稳定上报 ID 的去重：与窗口无关，同一 ID 全局只生效一次。
	if s.ID != "" {
		if _, ok := a.seen[s.ID]; ok {
			a.totalDup++
			// 重复样本仍需定位窗口，以便累加到该窗口的 Duplicate 计数。
			w := a.alignWindow(s.Timestamp)
			labels := a.mergedLabels(s)
			key := SeriesKeyOf(s.Name, labels)
			ag := a.slot(w, key, s, labels)
			ag.Duplicate++
			a.lastWin = w
			return w, true, nil
		}
		a.seen[s.ID] = struct{}{}
		a.idWindow[s.ID] = a.alignWindow(s.Timestamp)
	}

	w := a.alignWindow(s.Timestamp)
	labels := a.mergedLabels(s)
	key := SeriesKeyOf(s.Name, labels)
	ag := a.slot(w, key, s, labels)
	mergeSample(ag, s)
	a.totalOK++
	a.lastWin = w
	return w, false, nil
}

// slot 获取（或创建）某窗口某序列的聚合槽，调用方需持锁。
func (a *Aggregator) slot(w time.Time, key SeriesKey, s Sample, labels map[string]string) *Aggregate {
	b, ok := a.buckets[w]
	if !ok {
		b = make(map[SeriesKey]*Aggregate)
		a.buckets[w] = b
	}
	ag, ok := b[key]
	if !ok {
		ag = &Aggregate{
			Name:   s.Name,
			Labels: cloneLabels(labels),
			Window: w,
			Kind:   s.Kind,
		}
		b[key] = ag
	}
	return ag
}

// mergeSample 按统一口径把样本并入聚合结果。
// 关键：First/Last 依据采样时间戳判定，而非到达顺序，因此乱序到达结果一致。
func mergeSample(ag *Aggregate, s Sample) {
	if ag.Count == 0 {
		ag.Count = 1
		ag.Sum = s.Value
		ag.Min = s.Value
		ag.Max = s.Value
		ag.First = s.Value
		ag.Last = s.Value
		ag.FirstTS = s.Timestamp
		ag.LastTS = s.Timestamp
		return
	}
	ag.Count++
	ag.Sum += s.Value
	if s.Value < ag.Min {
		ag.Min = s.Value
	}
	if s.Value > ag.Max {
		ag.Max = s.Value
	}
	if s.Timestamp.Before(ag.FirstTS) {
		ag.First = s.Value
		ag.FirstTS = s.Timestamp
	}
	if s.Timestamp.After(ag.LastTS) {
		ag.Last = s.Value
		ag.LastTS = s.Timestamp
	}
}

// Window 返回一个窗口内全部序列的聚合副本（窗口不存在返回 nil,false）。
func (a *Aggregator) Window(w time.Time) (map[SeriesKey]*Aggregate, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.buckets[w]
	if !ok {
		return nil, false
	}
	out := make(map[SeriesKey]*Aggregate, len(b))
	for k, v := range b {
		out[k] = v.Clone()
	}
	return out, true
}

// Snapshot 返回全部窗口聚合结果的只读深拷贝快照。
func (a *Aggregator) Snapshot() Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make(map[time.Time]map[SeriesKey]*Aggregate, len(a.buckets))
	for w, b := range a.buckets {
		cp := make(map[SeriesKey]*Aggregate, len(b))
		for k, v := range b {
			cp[k] = v.Clone()
		}
		out[w] = cp
	}
	rj := make([]RejectedSample, len(a.rejected))
	copy(rj, a.rejected)

	return Snapshot{
		Window:   a.lastWin,
		Windows:  out,
		Rejected: rj,
		TotalIn:  a.totalIn,
		TotalDup: a.totalDup,
		TotalOK:  a.totalOK,
		TotalRej: a.totalRej,
	}
}

// Stats 仅返回计数部分，供引擎汇总。
func (a *Aggregator) Totals() (in, ok, dup, rej int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.totalIn, a.totalOK, a.totalDup, a.totalRej
}
