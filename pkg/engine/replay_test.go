package engine

import (
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/alert"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

// sampleSet 构造一批乱序、含重复 ID 与非法样本的历史样本。
func sampleSet() []metric.Sample {
	base := baseTime()
	mk := func(v float64, win int, id string) metric.Sample {
		return cpuSample("agent-a", "h1", v,
			base.Add(time.Duration(win-1)*w+30*time.Second), id)
	}
	good := []metric.Sample{
		mk(90, 1, "e1"),
		mk(95, 3, "e3"), // 故意乱序：先给 w3
		mk(10, 2, "e2"),
		mk(10, 4, "e4"),
		mk(90, 3, "e3"), // 与 e3 同 ID 的重复重试
	}
	bad := metric.Sample{
		Source: "agent-a", Name: "cpu", Kind: metric.Gauge,
		Value: 1, Timestamp: base.Add(30 * time.Second),
		Labels: map[string]string{"host": "h1", "__bad": "x"},
	}
	return append(good, bad)
}

func TestReplayDeterministicAndReproducesOnline(t *testing.T) {
	cfg := ReplayConfig{
		Window: w,
		Rules:  []alert.Rule{highCPURule(w, w)},
	}

	r1 := Replay(sampleSet(), cfg)
	r2 := Replay(sampleSet(), cfg) // 同样输入乱序传入，两次结果必须一致
	if len(r1.Records) != len(r2.Records) {
		t.Fatalf("replay not deterministic: %d vs %d records", len(r1.Records), len(r2.Records))
	}
	var notifIDs1, notifIDs2 []string
	for i := range r1.Records {
		a, b := r1.Records[i], r2.Records[i]
		if !a.WindowEnd.Equal(b.WindowEnd) || len(a.Notifs) != len(b.Notifs) {
			t.Fatalf("record %d differs", i)
		}
		for j := range a.Notifs {
			notifIDs1 = append(notifIDs1, a.Notifs[j].ID)
			notifIDs2 = append(notifIDs2, b.Notifs[j].ID)
		}
	}
	for _, rec := range r1.Records {
		for _, n := range rec.Notifs {
			t.Logf("replay notify: %s", n)
		}
		for _, rj := range rec.Rejected {
			t.Logf("replay reject: %s", rj)
		}
	}

	if r1.Stats.Rejected != 1 || r1.Stats.Accepted != 4 || r1.Stats.Duplicated != 1 {
		t.Fatalf("unexpected replay stats: %+v", r1.Stats)
	}
	// w1 firing；w2 resolve(keep=1m 单窗满足)；w3 再 firing；w4 再 resolve。
	if r1.Stats.Firing != 2 || r1.Stats.Resolved != 2 {
		t.Fatalf("want firing=2 resolved=2, got %+v", r1.Stats)
	}

	// 与在线引擎按时间顺序推进的结论对齐（通知 ID 序列一致）。
	eng := New(w, nil, cfg.Rules, nil)
	valid := sampleSet()[:5]
	for _, s := range valid {
		if rj := eng.Report(s); rj != nil {
			t.Fatalf("online reject: %s", rj)
		}
	}
	var onlineIDs []string
	for i := 1; i <= 4; i++ {
		for _, n := range eng.RunWindow(baseTime().Add(time.Duration(i) * w)) {
			onlineIDs = append(onlineIDs, n.ID)
		}
	}
	var replayIDs []string
	for _, rec := range r1.Records {
		for _, n := range rec.Notifs {
			replayIDs = append(replayIDs, n.ID)
		}
	}
	if len(onlineIDs) != len(replayIDs) {
		t.Fatalf("online vs replay notifications differ: %v vs %v", onlineIDs, replayIDs)
	}
	for i := range onlineIDs {
		if onlineIDs[i] != replayIDs[i] {
			t.Fatalf("notify id[%d] online=%s replay=%s", i, onlineIDs[i], replayIDs[i])
		}
	}
}

func TestReplayWithChangedConfig(t *testing.T) {
	samples := sampleSet()
	base := baseTime()

	// 旧配置：阈值 80，无静默 -> firing 通知会发出
	oldCfg := ReplayConfig{Window: w, Rules: []alert.Rule{highCPURule(w, w)}}
	old := Replay(samples, oldCfg)

	// 新配置：阈值提高到 99（w1=90/w3=95 都不再越界），并加静默
	newRules := []alert.Rule{{
		ID: "cpu-high", Metric: "cpu", Comparator: alert.OpGT, Threshold: 99,
		ForDuration: w, KeepDuration: w,
	}}
	newCfg := ReplayConfig{
		Window:   w,
		Rules:    newRules,
		Silences: []silence.Silence{{ID: "mute", Matchers: map[string]string{"host": "h1"}, StartsAt: base, EndsAt: base.Add(4 * w)}},
	}
	new := Replay(samples, newCfg)

	if old.Stats.Firing != 2 {
		t.Fatalf("old config expected 2 firings, got %d", old.Stats.Firing)
	}
	if new.Stats.Firing != 0 {
		t.Fatalf("new threshold 99 must yield 0 firings, got %d", new.Stats.Firing)
	}
	t.Logf("old config: firing=%d notified=%d | new config: firing=%d notified=%d",
		old.Stats.Firing, old.Stats.Notified, new.Stats.Firing, new.Stats.Notified)

	// 再验证静默配置的变化：阈值恢复 80，静默命中 -> firing 仍计数但通知被抑制。
	mutedCfg := ReplayConfig{Window: w, Rules: oldCfg.Rules, Silences: newCfg.Silences}
	muted := Replay(samples, mutedCfg)
	// 静默窗 [w0, w0+4m)：w1..w3 结束时刻在窗内（3 条被抑制），w4 结束时刻已出窗（1 条发送）。
	if muted.Stats.Firing != 2 || muted.Stats.Resolved != 2 || muted.Stats.Notified != 1 || muted.Stats.Silenced != 3 {
		t.Fatalf("muted replay expected firing=2 resolved=2 notified=1 silenced=3, got %+v", muted.Stats)
	}
}
