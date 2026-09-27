package alert

import (
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"

	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// State 告警状态。
type State string

const (
	StateInactive State = "inactive"
	StatePending  State = "pending"
	StateFiring   State = "firing"
)

func (s State) String() string { return string(s) }

// Comparator 比较算子。
type Comparator string

const (
	OpGT Comparator = "gt"
	OpGE Comparator = "ge"
	OpLT Comparator = "lt"
	OpLE Comparator = "le"
	OpEQ Comparator = "eq"
)

// Compare 按算子比较 value 与 threshold。
func (c Comparator) Compare(value, threshold float64) bool {
	switch c {
	case OpGT:
		return value > threshold
	case OpGE:
		return value >= threshold
	case OpLT:
		return value < threshold
	case OpLE:
		return value <= threshold
	case OpEQ:
		return value == threshold
	default:
		return false
	}
}

func (c Comparator) Valid() bool {
	switch c {
	case OpGT, OpGE, OpLT, OpLE, OpEQ:
		return true
	default:
		return false
	}
}

// Rule 告警规则定义。
type Rule struct {
	ID           string
	Metric       string
	MatchLabels  map[string]string // 标签选择器（相等匹配）
	Comparator   Comparator
	Threshold    float64
	ForDuration  time.Duration // 条件需持续多久才从 pending 进入 firing
	KeepDuration time.Duration // firing 后条件恢复正常需持续多久才回到 inactive（退出抖动保护）
}

// ruleWire 是规则指纹的确定性序列化形态。
type ruleWire struct {
	ID           string            `json:"id"`
	Metric       string            `json:"metric"`
	MatchLabels  map[string]string `json:"match_labels"`
	Comparator   string            `json:"comparator"`
	Threshold    float64           `json:"threshold"`
	ForDuration  int64             `json:"for_duration_nanos"`
	KeepDuration int64             `json:"keep_duration_nanos"`
}

// Fingerprint 为规则配置生成稳定指纹：内容相同则指纹相同，
// 与 map 迭代顺序无关，用于检测配置变更与历史重放。
func (r Rule) Fingerprint() string {
	w := ruleWire{
		ID:           r.ID,
		Metric:       r.Metric,
		MatchLabels:  r.MatchLabels,
		Comparator:   string(r.Comparator),
		Threshold:    r.Threshold,
		ForDuration:  int64(r.ForDuration),
		KeepDuration: int64(r.KeepDuration),
	}
	b, _ := json.Marshal(w) // map 键由 encoding/json 按字典序输出
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// TransitionKind 状态流转类型。
type TransitionKind string

const (
	TrNone       TransitionKind = "none"
	TrToPending  TransitionKind = "to_pending"
	TrToFiring   TransitionKind = "to_firing"
	TrToInactive TransitionKind = "to_inactive" // firing -> inactive，恢复
)

// Event 是一次评估产生的状态流转事件。
// 对用户有通知意义的只有 to_firing / to_inactive；pending 的进出仅记录依据。
type Event struct {
	RuleID    string
	SeriesKey metric.SeriesKey
	State     State
	From      State
	Kind      TransitionKind
	At        time.Time // 该事件对应的窗口结束时刻
	Value     float64
	Breach    bool
	Reason    string // 判定依据的人类可读描述
}
