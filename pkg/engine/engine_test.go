package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/alert"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/silence"
)

const w = time.Minute

func baseTime() time.Time { return time.Unix(3600, 0).UTC() }

func cpuSample(source, host string, v float64, ts time.Time, id string) metric.Sample {
	return metric.Sample{
		ID:        id,
		Source:    source,
		Name:      "cpu",
		Kind:      metric.Gauge,
		Value:     v,
		Timestamp: ts,
		Labels:    map[string]string{"host": host},
	}
}

func highCPURule(forDur, keepDur time.Duration) alert.Rule {
	return alert.Rule{
		ID: "cpu-high", Metric: "cpu",
		Comparator:   alert.OpGT,
		Threshold:    80,
		ForDuration:  forDur,
		KeepDuration: keepDur,
	}
}

// runScenario 串行喂入一个完整场景，返回各窗口通知与最终统计。
func runScenario(t *testing.T, r alert.Rule, sils []silence.Silence, concurrent bool) ([]Notification, Stats) {
	t.Helper()
	eng := New(w, nil, []alert.Rule{r}, sils)
	base := baseTime()

	// 每个窗口的样本：(窗口序号 1-based, value)；value<=0 表示无样本
	// 序列 host=h1：w1..w3 越界，w4 短时恢复，w5 又越界，w6..w7 恢复
	plan := []struct {
		win int
		v   float64
	}{
		{1, 90}, {2, 95}, {3, 99}, {4, 10}, {5, 90}, {6, 10}, {7, 10},
	}

	ingest := func(s metric.Sample) {
		if rj := eng.Report(s); rj != nil {
			t.Fatalf("unexpected reject: %s", rj)
		}
	}

	for _, p := range plan {
		winStart := base.Add(time.Duration(p.win-1) * w)
		ts := winStart.Add(30 * time.Second)
		s := cpuSample("agent-a", "h1", p.v, ts,
			fmt.Sprintf("evt-w%d", p.win))
		if concurrent && p.win == 2 {
			// 在窗口 2 额外并发上报同一序列的不同样本与重复 ID，验证并发安全。
			var wg sync.WaitGroup
			extra := []metric.Sample{
				cpuSample("agent-a", "h1", 96, winStart.Add(20*time.Second), "evt-w2-c1"),
				cpuSample("agent-a", "h1", 97, winStart.Add(25*time.Second), "evt-w2-c2"),
				cpuSample("agent-a", "h1", 96, winStart.Add(20*time.Second), "evt-w2-c1"), // 重复 ID
			}
			for _, es := range extra {
				wg.Add(1)
				go func(es metric.Sample) {
					defer wg.Done()
					ingest(es)
				}(es)
			}
			wg.Wait()
		}
		ingest(s)
	}

	var all []Notification
	for i := 1; i <= len(plan); i++ {
		at := base.Add(time.Duration(i) * w)
		ns := eng.RunWindow(at)
		for _, n := range ns {
			t.Logf("window %d -> %s reason=%s", i, n, n.Reason)
			all = append(all, n)
		}
	}
	return all, eng.Stats()
}

func TestScenarioSerialBaseline(t *testing.T) {
	r := highCPURule(2*w, w)
	notifs, st := runScenario(t, r, nil, false)
	// w2 进入 firing（w1 pending, w2 持续2窗口）；
	// w4 恢复1窗口未达keep=1m? keep=1m 单窗口即满足 -> w4 恢复；
	// w5 再越界 pending，w6 恢复（pending 中恢复，无通知），所以只有一轮 firing/resolve。
	firing, resolved := 0, 0
	for _, n := range notifs {
		if n.Kind == alert.TrToFiring {
			firing++
		}
		if n.Kind == alert.TrToInactive {
			resolved++
		}
		if !n.Sent {
			t.Fatalf("no silences configured, notification must be sent: %s", n)
		}
	}
	if firing != 1 || resolved != 1 {
		t.Fatalf("want firing=1 resolved=1, got %d/%d", firing, resolved)
	}
	t.Logf("stats: %+v", st)
	if st.Received != 7 || st.Accepted != 7 || st.Rejected != 0 || st.Duplicated != 0 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestConcurrentEqualsSerial(t *testing.T) {
	r := highCPURule(2*w, w)

	// 串行基准
	serEng := New(w, nil, []alert.Rule{r}, nil)
	// 构造与并发场景相同的样本集合（含重复 ID），顺序固定
	base := baseTime()
	var serialSamples []metric.Sample
	mk := func(v float64, win int, id string, off time.Duration) metric.Sample {
		return cpuSample("agent-a", "h1", v,
			base.Add(time.Duration(win-1)*w).Add(off), id)
	}
	for _, p := range []struct {
		win int
		v   float64
	}{{1, 90}, {2, 95}, {3, 99}, {4, 10}, {5, 90}, {6, 10}, {7, 10}} {
		serialSamples = append(serialSamples, mk(p.v, p.win, fmt.Sprintf("evt-w%d", p.win), 30*time.Second))
	}
	serialSamples = append(serialSamples,
		mk(96, 2, "evt-w2-c1", 20*time.Second),
		mk(97, 2, "evt-w2-c2", 25*time.Second),
		mk(96, 2, "evt-w2-c1", 20*time.Second), // 重复 ID
	)

	for _, s := range serialSamples {
		if rj := serEng.Report(s); rj != nil {
			t.Fatalf("serial reject: %s", rj)
		}
	}

	// 并发执行同样集合
	conEng := New(w, nil, []alert.Rule{r}, nil)
	var wg sync.WaitGroup
	for _, s := range serialSamples {
		wg.Add(1)
		go func(s metric.Sample) {
			defer wg.Done()
			if rj := conEng.Report(s); rj != nil {
				t.Errorf("concurrent reject: %s", rj)
			}
		}(s)
	}
	wg.Wait()

	ss := serEng.Stats()
	cs := conEng.Stats()
	if ss.Accepted != cs.Accepted || ss.Duplicated != cs.Duplicated || ss.Rejected != cs.Rejected {
		t.Fatalf("stats differ: serial=%+v concurrent=%+v", ss, cs)
	}
	if ss.Accepted != 9 || ss.Duplicated != 1 {
		t.Fatalf("want accepted=9 duplicated=1, got %+v", ss)
	}

	// 两端按相同窗口顺序评估，通知序列必须完全一致（类型、值、ID、发送与否）。
	evalAll := func(eng *Engine) []Notification {
		var out []Notification
		for i := 1; i <= 7; i++ {
			out = append(out, eng.RunWindow(base.Add(time.Duration(i)*w))...)
		}
		return out
	}
	gotS := evalAll(serEng)
	gotC := evalAll(conEng)
	if len(gotS) != len(gotC) {
		t.Fatalf("notification count differ: serial=%d concurrent=%d", len(gotS), len(gotC))
	}
	for i := range gotS {
		a, b := gotS[i], gotC[i]
		if a.ID != b.ID || a.Kind != b.Kind || a.Value != b.Value || a.Sent != b.Sent {
			t.Fatalf("notification[%d] differ:\nserial=%s\nconcur=%s", i, a, b)
		}
	}
	t.Logf("concurrent == serial: %d notifications, accepted=%d dup=%d", len(gotC), cs.Accepted, cs.Duplicated)
}

func TestSilenceSuppressesOnly(t *testing.T) {
	r := highCPURule(w, w) // for=1m：w1 即 firing
	base := baseTime()
	// 静默覆盖 w1 结束时刻，精确命中 host=h1
	sils := []silence.Silence{{
		ID:       "maintenance",
		Matchers: map[string]string{"host": "h1"},
		StartsAt: base,
		EndsAt:   base.Add(2 * w),
	}}
	eng := New(w, nil, []alert.Rule{r}, sils)

	// w1 越界（firing 被静默），w2 恢复
	eng.Report(cpuSample("a", "h1", 90, base.Add(30*time.Second), "e1"))
	n1 := eng.RunWindow(base.Add(w))
	eng.Report(cpuSample("a", "h1", 10, base.Add(w+30*time.Second), "e2"))
	n2 := eng.RunWindow(base.Add(2 * w))

	if len(n1) != 1 || n1[0].Sent || len(n1[0].SilencedBy) != 1 || n1[0].SilencedBy[0] != "maintenance" {
		t.Fatalf("w1 firing should be silenced, got %+v", n1)
	}
	t.Logf("w1: %s", n1[0])
	if len(n2) != 1 || !n2[0].Sent {
		t.Fatalf("w2 resolve should be sent (silence ended at w2 boundary), got %+v", n2)
	}
	t.Logf("w2: %s", n2[0])

	st := eng.Stats()
	// 状态/统计不被静默改变：firing=1 resolved=1；通知仅发出 1 条，静默 1 条。
	if st.Firing != 1 || st.Resolved != 1 || st.Notified != 1 || st.Silenced != 1 {
		t.Fatalf("silence must not change state stats: %+v", st)
	}
}
