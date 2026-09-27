package silence

import (
	"sort"
	"time"
)

// Silence 是一条静默配置：在 [StartsAt, EndsAt) 内，命中标签选择器的告警不发送通知。
type Silence struct {
	ID       string
	Matchers map[string]string // 相等匹配标签；空 map 视为匹配任意标签
	StartsAt time.Time
	EndsAt   time.Time
}

// ActiveAt 报告静默在 at 时刻是否生效（左闭右开窗口）。
func (s Silence) ActiveAt(at time.Time) bool {
	return !at.Before(s.StartsAt) && at.Before(s.EndsAt)
}

// Match 报告标签集合是否满足静默选择器（全部 matcher 相等；空选择器匹配任意）。
func (s Silence) Match(labels map[string]string) bool {
	for k, v := range s.Matchers {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// Store 管理静默配置，支持按时刻查询命中情况。
// 静默只抑制通知，不改变告警状态与统计（firing/resolved 计数照常）。
type Store struct {
	items []Silence
}

// NewStore 创建静默存储（内部拷贝一份）。
func NewStore(sils []Silence) *Store {
	cp := cloneAll(sils)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	return &Store{items: cp}
}

// Update 替换全部静默配置。历史窗口的判定复现应使用当时的配置
// （引擎按配置版本记录通知，Store 只提供"某时刻+某配置"的纯查询）。
func (s *Store) Update(sils []Silence) {
	cp := cloneAll(sils)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	s.items = cp
}

// MatchAt 返回在 at 时刻同时满足"窗口生效"与"标签命中"的静默 ID 列表，
// 按 ID 排序保证结果确定；未命中返回 nil。
func (s *Store) MatchAt(labels map[string]string, at time.Time) []string {
	var ids []string
	for _, si := range s.items {
		if si.ActiveAt(at) && si.Match(labels) {
			ids = append(ids, si.ID)
		}
	}
	return ids
}

// All 返回全部静默配置副本（按 ID 排序）。
func (s *Store) All() []Silence {
	return cloneAll(s.items)
}

func cloneAll(in []Silence) []Silence {
	out := make([]Silence, len(in))
	for i, si := range in {
		out[i] = Silence{
			ID:       si.ID,
			Matchers: cloneMap(si.Matchers),
			StartsAt: si.StartsAt,
			EndsAt:   si.EndsAt,
		}
	}
	return out
}

func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
