package metric

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RejectReason 非法样本被拒绝的可区分原因。
type RejectReason string

const (
	ReasonEmptyName       RejectReason = "empty_metric_name"
	ReasonBadName         RejectReason = "invalid_metric_name"
	ReasonBadLabelKey     RejectReason = "invalid_label_key"
	ReasonReservedLabel   RejectReason = "reserved_label_key"
	ReasonNaN             RejectReason = "value_is_nan"
	ReasonInf             RejectReason = "value_is_inf"
	ReasonNegativeCounter RejectReason = "counter_value_negative"
	ReasonEmptySource     RejectReason = "empty_source"
	ReasonBadTimestamp    RejectReason = "timestamp_out_of_range"
	ReasonFutureTimestamp RejectReason = "timestamp_in_future"
	ReasonDuplicate       RejectReason = "duplicate_sample_id"
)

// Kind 指标类型，决定聚合口径。
type Kind string

const (
	Counter Kind = "counter"
	Gauge   Kind = "gauge"
)

func (k Kind) Valid() bool { return k == Counter || k == Gauge }

// Sample 是一次指标上报样本。
type Sample struct {
	ID        string            // 可选的稳定上报 ID，非空时用于重复上报去重
	Source    string            // 来源标识
	Name      string            // 指标名
	Kind      Kind              // 指标类型
	Value     float64           // 指标值
	Timestamp time.Time         // 采样时间
	Labels    map[string]string // 样本自带标签
}

// String 打印样本摘要，便于测试日志展示输入。
func (s Sample) String() string {
	return fmt.Sprintf("Sample{id=%q source=%q name=%q kind=%s value=%v ts=%s labels=%s}",
		s.ID, s.Source, s.Name, s.Kind, s.Value,
		s.Timestamp.UTC().Format(time.RFC3339Nano), formatLabels(s.Labels))
}

// SeriesKey 是一条时间序列的稳定标识（指标名 + 排序后标签）。
type SeriesKey string

// RejectedSample 记录被拒绝的样本及原因。
type RejectedSample struct {
	Sample Sample
	Reason RejectReason
	Detail string
}

func (r RejectedSample) Error() string {
	return fmt.Sprintf("sample rejected: reason=%s detail=%s sample=%s", r.Reason, r.Detail, r.Sample)
}

func (r RejectedSample) String() string { return r.Error() }

// Aggregate 是某个时间窗口、某条序列上的聚合结果。
// 所有字段的合并口径与样本到达顺序无关（乱序安全）。
type Aggregate struct {
	Name      string
	Labels    map[string]string
	Window    time.Time
	Kind      Kind
	Count     int64   // 参与聚合的样本数（重复 ID 不计入）
	Sum       float64 // 所有样本值之和
	Min       float64
	Max       float64
	Last      float64 // 采样时间最大的样本值（乱序安全）
	First     float64 // 采样时间最小的样本值
	LastTS    time.Time
	FirstTS   time.Time
	Duplicate int64 // 命中重复 ID 被忽略的样本数
}

// Clone 返回深拷贝，供快照使用。
func (a *Aggregate) Clone() *Aggregate {
	if a == nil {
		return nil
	}
	cp := *a
	cp.Labels = cloneLabels(a.Labels)
	return &cp
}

// Snapshot 是聚合器某一时刻的只读快照。
// Windows 内按窗口起点 -> 序列键两层组织，同一序列跨窗口互不覆盖。
type Snapshot struct {
	Window   time.Time // 最近一次写入所在窗口
	Windows  map[time.Time]map[SeriesKey]*Aggregate
	Rejected []RejectedSample
	TotalIn  int64
	TotalDup int64
	TotalOK  int64
	TotalRej int64
}

func formatLabels(l map[string]string) string {
	if len(l) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, l[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func cloneLabels(l map[string]string) map[string]string {
	if len(l) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}

// MergeLabels 合并来源默认标签与样本标签；样本标签优先。
// 保留前缀标签（__source）由聚合器在合并后注入，样本侧携带会被校验拒绝。
func MergeLabels(base, extra map[string]string) map[string]string {
	out := cloneLabels(base)
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// SortedWindows 返回快照中按时间升序排列的窗口起点。
func (s Snapshot) SortedWindows() []time.Time {
	out := make([]time.Time, 0, len(s.Windows))
	for w := range s.Windows {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}
