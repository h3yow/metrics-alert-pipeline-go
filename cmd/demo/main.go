// Command demo 在单进程内模拟完整的指标聚合与告警评估链路：
// 多来源并发上报（含重复 ID、乱序样本、非法样本）-> 窗口聚合 ->
// 状态机判定（pending/firing/恢复抖动保护）-> 静默与去重通知 ->
// 配置变更后的历史重放。
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/alert"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/engine"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

const window = time.Minute

func main() {
	seed := flag.Int64("seed", 42, "random seed for concurrent/ordering jitter")
	flag.Parse()

	base := time.Unix(3600, 0).UTC()
	rng := rand.New(rand.NewSource(*seed))

	rule := alert.Rule{
		ID: "cpu-high", Metric: "cpu",
		MatchLabels:  map[string]string{},
		Comparator:   alert.OpGT,
		Threshold:    80,
		ForDuration:  2 * window, // 连续 2 个窗口越界才触发
		KeepDuration: window,     // 恢复保护 1 个窗口
	}
	sils := []silence.Silence{{
		ID:       "maintenance-h2",
		Matchers: map[string]string{"host": "h2"},
		StartsAt: base.Add(2 * window),
		EndsAt:   base.Add(5 * window),
	}}
	sourceLabels := map[string]map[string]string{
		"agent-a": {"env": "prod"},
		"agent-b": {"env": "staging"},
	}

	eng := engine.New(window, sourceLabels, []alert.Rule{rule}, sils)

	// 场景：8 个窗口，h1 与 h2 两台主机的 cpu；其中包含短时抖动、持续越界、恢复。
	// 值序列（每窗口一条）：
	plan := map[string][]float64{
		"h1": {50, 85, 90, 92, 60, 91, 55, 40},
		"h2": {70, 70, 95, 96, 97, 40, 40, 40}, // h2 持续越界窗口落在静默期
	}

	type job struct {
		s metric.Sample
	}
	var jobs []job
	idc := 0
	for host, vals := range plan {
		source := "agent-a"
		if host == "h2" {
			source = "agent-b"
		}
		for i, v := range vals {
			idc++
			s := metric.Sample{
				ID:        fmt.Sprintf("evt-%03d", idc),
				Source:    source,
				Name:      "cpu",
				Kind:      metric.Gauge,
				Value:     v,
				Timestamp: base.Add(time.Duration(i) * window).Add(30 * time.Second),
				Labels:    map[string]string{"host": host},
			}
			jobs = append(jobs, job{s: s})
		}
	}
	// 加入两条重复上报（同一 ID 重试）与两条非法样本。
	jobs = append(jobs,
		job{metric.Sample{ID: "evt-001", Source: "agent-a", Name: "cpu", Kind: metric.Gauge, Value: 50,
			Timestamp: base.Add(30 * time.Second), Labels: map[string]string{"host": "h1"}}},
		job{metric.Sample{ID: "evt-002", Source: "agent-a", Name: "cpu", Kind: metric.Gauge, Value: 85,
			Timestamp: base.Add(window + 30*time.Second), Labels: map[string]string{"host": "h1"}}},
		job{metric.Sample{Source: "agent-a", Name: "1bad-name", Kind: metric.Gauge, Value: 1,
			Timestamp: base.Add(30 * time.Second)}},
		job{metric.Sample{Source: "agent-a", Name: "cpu", Kind: metric.Counter, Value: -3,
			Timestamp: base.Add(30 * time.Second), Labels: map[string]string{"host": "h1"}}},
	)

	// 打乱后并发上报，模拟真实采集：乱序到达 + 并发 + 重试。
	rng.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })
	fmt.Println("== 阶段1：并发乱序上报 ==")
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			if rj := eng.Report(j.s); rj != nil {
				fmt.Printf("  REJECT %s\n", rj)
			}
		}(j)
	}
	wg.Wait()
	st := eng.Stats()
	fmt.Printf("上报统计: 接收=%d 接受=%d 重复忽略=%d 拒绝=%d\n\n",
		st.Received, st.Accepted, st.Duplicated, st.Rejected)

	fmt.Println("== 阶段2：逐窗口聚合结果（与到达顺序无关）==")
	snap := eng.Snapshot()
	wins := append([]time.Time(nil), snap.SortedWindows()...)
	sort.Slice(wins, func(i, j int) bool { return wins[i].Before(wins[j]) })
	for _, wt := range wins {
		b := snap.Windows[wt]
		keys := make([]metric.SeriesKey, 0, len(b))
		for k := range b {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, k := range keys {
			ag := b[k]
			fmt.Printf("  window=%s series=%s count=%d last=%v min=%v max=%v dup=%d\n",
				wt.Format("15:04:05"), k, ag.Count, ag.Last, ag.Min, ag.Max, ag.Duplicate)
		}
	}
	fmt.Println()

	fmt.Println("== 阶段3：状态机判定与通知（去重 + 静默）==")
	for i := 1; i <= 8; i++ {
		at := base.Add(time.Duration(i) * window)
		notifs := eng.RunWindow(at)
		for _, n := range notifs {
			tag := "发送"
			if !n.Sent {
				tag = "静默抑制"
			}
			fmt.Printf("  window 结束 %s: [%s] %s id=%s\n",
				at.Format("15:04:05"), tag, n.Kind, n.ID)
			fmt.Printf("      依据: %s\n", n.Reason)
		}
	}
	fmt.Println()

	fmt.Println("== 阶段4：最终统计 ==")
	st = eng.Stats()
	fmt.Printf("  firing 次数=%d（含被静默，状态统计不受静默影响）\n", st.Firing)
	fmt.Printf("  resolve 次数=%d\n", st.Resolved)
	fmt.Printf("  实际发送通知=%d，被静默抑制=%d\n\n", st.Notified, st.Silenced)

	// 配置变更后重放：阈值提高到 90（等于不越界口径改为 >90），演示历史结论可复现。
	fmt.Println("== 阶段5：配置变更（阈值 80 -> 90）后对同一批历史样本重放 ==")
	allSamples := make([]metric.Sample, 0, len(jobs))
	for _, j := range jobs {
		allSamples = append(allSamples, j.s)
	}
	newRule := rule
	newRule.Threshold = 90
	res := engine.Replay(allSamples, engine.ReplayConfig{
		Window:       window,
		SourceLabels: sourceLabels,
		Rules:        []alert.Rule{newRule},
		Silences:     sils,
	})
	fmt.Printf("  重放结果: firing=%d resolve=%d 通知发送=%d 静默=%d 拒绝=%d\n",
		res.Stats.Firing, res.Stats.Resolved, res.Stats.Notified, res.Stats.Silenced, res.Stats.Rejected)
	fmt.Println("  (结论仅由历史样本 + 新配置确定性推出，与在线引擎当前状态无关)")
}
