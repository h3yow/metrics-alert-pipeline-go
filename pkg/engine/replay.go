package engine

import (
	"sort"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/alert"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

// ReplayConfig 是一次历史重放使用的完整配置快照。
type ReplayConfig struct {
	Window       time.Duration
	SourceLabels map[string]map[string]string
	Rules        []alert.Rule
	Silences     []silence.Silence
}

// ReplayRecord 是重放中每个窗口的输出。
type ReplayRecord struct {
	WindowEnd time.Time
	Events    []alert.Event           // 含内部 pending 流转，便于审计判定依据
	Notifs    []Notification          // 仅对外通知（含被静默的）
	Rejected  []metric.RejectedSample // 该窗口内被拒绝的样本
}

// ReplayResult 是一次重放的完整结果。
type ReplayResult struct {
	Records []ReplayRecord
	Stats   Stats
}

// Replay 用给定配置对历史样本从零开始重放。
// 样本按采样时间排序后依次喂入（无论传入顺序），保证结论与采集时的到达顺序无关；
// 重复样本 ID 仍按去重口径只生效一次。返回逐窗口的事件与通知。
//
// 用途：规则或静默配置更新后，把同一批历史样本用新配置重跑一遍，
// 即可复现"如果当时是这套配置会得到什么结论"。
func Replay(samples []metric.Sample, cfg ReplayConfig) ReplayResult {
	window := cfg.Window
	if window <= 0 {
		window = time.Minute
	}
	eng := New(window, cfg.SourceLabels, cfg.Rules, cfg.Silences)

	// 稳定排序：按采样时间；同时间戳按 ID、来源、指标名排序，保证完全确定。
	ordered := make([]metric.Sample, len(samples))
	copy(ordered, samples)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.Name < b.Name
	})

	// 先把样本全部喂入引擎（与在线上报同一路径，计数口径一致）；
	// 非法样本不参与窗口评估，单独汇总。
	var globalRejected []metric.RejectedSample
	var minT, maxT time.Time
	for _, s := range ordered {
		if rj := eng.Report(s); rj != nil {
			globalRejected = append(globalRejected, *rj)
			continue
		}
		if minT.IsZero() || s.Timestamp.Before(minT) {
			minT = s.Timestamp
		}
		if maxT.IsZero() || s.Timestamp.After(maxT) {
			maxT = s.Timestamp
		}
	}

	// 计算窗口序列：从首个样本所在窗口到末个样本所在窗口，逐窗口评估。
	res := ReplayResult{}
	if len(ordered) == 0 {
		res.Stats = eng.Stats()
		return res
	}
	align := func(t time.Time) time.Time {
		u := t.UnixNano()
		d := int64(window)
		return time.Unix(0, (u/d)*d).UTC()
	}
	firstW := align(minT)
	lastW := align(maxT)

	for ws := firstW; !ws.After(lastW); ws = ws.Add(window) {
		at := ws.Add(window)
		bucket, _ := eng.agg.Window(ws)
		events := eng.ev.Evaluate(at, window, bucket)
		// 复用引擎同一套通知构造、静默匹配与去重统计逻辑。
		eng.mu.Lock()
		notifs := eng.emitNotifications(events, bucket)
		eng.mu.Unlock()

		if len(events) == 0 && len(notifs) == 0 {
			continue
		}
		res.Records = append(res.Records, ReplayRecord{
			WindowEnd: at,
			Events:    events,
			Notifs:    notifs,
		})
	}

	// 非法样本单独成一条记录（WindowEnd 为零值），错误原因可区分。
	if len(globalRejected) > 0 {
		rec := ReplayRecord{Rejected: globalRejected}
		res.Records = append([]ReplayRecord{rec}, res.Records...)
	}

	// 计数（接收/接受/重复/拒绝/通知/静默）全程由 eng.Report 与
	// emitNotifications 维护，这里直接读取最终快照。
	res.Stats = eng.Stats()
	return res
}
