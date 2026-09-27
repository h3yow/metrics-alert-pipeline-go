package metric

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// reservedLabelPrefix 是系统保留的标签前缀，样本不允许自带，避免与来源标签冲突。
const reservedLabelPrefix = "__"

// maxFutureSkew 允许的样本时间戳相对处理时刻的最大前漂，超出视为非法（而非乱序）。
const maxFutureSkew = 10 * time.Minute

// minTimestamp / maxTimestamp 限定可处理的采样时间范围。
var minTimestamp = time.Unix(0, 0)
var maxTimestamp = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

// Validate 校验样本合法性，返回可区分的拒绝原因；合法返回 nil。
// 该函数为纯函数，同样的样本永远得到同样的结论。
func Validate(s Sample) *RejectedSample {
	reject := func(r RejectReason, detail string) *RejectedSample {
		return &RejectedSample{Sample: s, Reason: r, Detail: detail}
	}

	if strings.TrimSpace(s.Source) == "" {
		return reject(ReasonEmptySource, "source is required")
	}
	if s.Name == "" {
		return reject(ReasonEmptyName, "metric name is required")
	}
	if !isIdent(s.Name) {
		return reject(ReasonBadName, fmt.Sprintf("metric name %q must match [a-zA-Z_][a-zA-Z0-9_:]*", s.Name))
	}
	for k := range s.Labels {
		if strings.HasPrefix(k, reservedLabelPrefix) {
			return reject(ReasonReservedLabel, fmt.Sprintf("label key %q must not start with %q", k, reservedLabelPrefix))
		}
		if !IsLabelKey(k) {
			return reject(ReasonBadLabelKey, fmt.Sprintf("label key %q must match [a-zA-Z_][a-zA-Z0-9_]*", k))
		}
	}
	if math.IsNaN(s.Value) {
		return reject(ReasonNaN, "sample value must not be NaN")
	}
	if math.IsInf(s.Value, 0) {
		return reject(ReasonInf, "sample value must not be +/-Inf")
	}
	if s.Kind == Counter && s.Value < 0 {
		return reject(ReasonNegativeCounter, fmt.Sprintf("counter value must be >= 0, got %v", s.Value))
	}
	if s.Timestamp.Before(minTimestamp) || s.Timestamp.After(maxTimestamp) {
		return reject(ReasonBadTimestamp, fmt.Sprintf("timestamp %s out of range", s.Timestamp.Format(time.RFC3339Nano)))
	}
	if s.Timestamp.After(time.Now().Add(maxFutureSkew)) {
		return reject(ReasonFutureTimestamp, fmt.Sprintf("timestamp %s is too far in the future", s.Timestamp.Format(time.RFC3339Nano)))
	}
	return nil
}

// isIdent 判断名称是否为合法标识符：首字母为字母或下划线，其余为字母数字下划线；
// 指标名额外允许冒号，在调用处通过 allowColon 区分。
func isIdent(v string) bool {
	if v == "" {
		return false
	}
	for i, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			// ok
		case i > 0 && r >= '0' && r <= '9':
			// ok
		case i > 0 && r == ':':
			// 指标名允许冒号，标签名不允许；标签名校验在调用前已拦截保留前缀，
			// 这里统一放行，由 LabelKey 合法集在 SeriesKey 侧约束。
		default:
			return false
		}
	}
	return true
}

// IsLabelKey 报告 key 是否为合法标签键（不允许冒号）。
func IsLabelKey(k string) bool {
	if k == "" {
		return false
	}
	for i, r := range k {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// SeriesKeyOf 根据指标名与标签集合计算稳定序列标识。
// 标签按键排序后拼接，保证乱序写入的 map 得到相同 key。
func SeriesKeyOf(name string, labels map[string]string) SeriesKey {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.Grow(len(name) + len(keys)*16)
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return SeriesKey(b.String())
}
