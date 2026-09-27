package alert

import (
	"testing"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/metric"
)

const win = time.Minute

// buildAggs 构造一个窗口内若干序列的聚合（每个序列单个样本）。
func buildAggs(windowStart time.Time, vals map[metric.SeriesKey]float64) map[metric.SeriesKey]*metric.Aggregate {
	out := make(map[metric.SeriesKey]*metric.Aggregate, len(vals))
	for k, v := range vals {
		out[k] = &metric.Aggregate{
			Name:    "cpu",
			Labels:  map[string]string{"host": "h1"},
			Window:  windowStart,
			Kind:    metric.Gauge,
			Count:   1,
			Sum:     v,
			Min:     v,
			Max:     v,
			First:   v,
			Last:    v,
			FirstTS: windowStart,
			LastTS:  windowStart,
		}
	}
	return out
}

func key(name string, l map[string]string) metric.SeriesKey {
	return metric.SeriesKeyOf(name, l)
}

func kinds(evs []Event) []TransitionKind {
	out := make([]TransitionKind, 0, len(evs))
	for _, ev := range evs {
		t := ev.Kind
		if t == TrNone {
			continue // 只关心有状态意义的流转
		}
		out = append(out, t)
	}
	return out
}

func eqKinds(t *testing.T, got []TransitionKind, want []TransitionKind) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("transitions want %v, got %v", want, got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("transition[%d] want %s, got %s", i, want[i], got[i])
		}
	}
}

// TestPendingThenFire 连续越界持续达到 ForDuration 后触发。
func TestPendingThenFire(t *testing.T) {
	labels := map[string]string{"host": "h1"}
	sk := key("cpu", labels)
	r := Rule{ID: "r1", Metric: "cpu", Comparator: OpGT, Threshold: 80,
		ForDuration: 2 * win, KeepDuration: win}
	e := NewEvaluator([]Rule{r})

	base := time.Unix(3600, 0).UTC() // 01:00
	var all []TransitionKind
	// 窗口 1：越界 -> pending
	evs := e.Evaluate(base.Add(win), win, buildAggs(base, map[metric.SeriesKey]float64{sk: 90}))
	for _, ev := range evs {
		t.Logf("w1 input cpu=90 -> state=%s kind=%s reason=%s", ev.State, ev.Kind, ev.Reason)
	}
	all = append(all, kinds(evs)...)
	eqKinds(t, all, []TransitionKind{TrToPending})

	// 窗口 2：持续越界 -> firing
	evs = e.Evaluate(base.Add(2*win), win, buildAggs(base.Add(win), map[metric.SeriesKey]float64{sk: 95}))
	for _, ev := range evs {
		t.Logf("w2 input cpu=95 -> state=%s kind=%s reason=%s", ev.State, ev.Kind, ev.Reason)
	}
	all = append(all, kinds(evs)...)
	eqKinds(t, all, []TransitionKind{TrToPending, TrToFiring})
}

// TestShortFlapNoFire pending 期间短时恢复不得误触发。
func TestShortFlapNoFire(t *testing.T) {
	labels := map[string]string{"host": "h1"}
	sk := key("cpu", labels)
	r := Rule{ID: "r1", Metric: "cpu", Comparator: OpGT, Threshold: 80,
		ForDuration: 3 * win, KeepDuration: win}
	e := NewEvaluator([]Rule{r})
	base := time.Unix(3600, 0).UTC()

	push := func(i int, v float64) []Event {
		evs := e.Evaluate(base.Add(time.Duration(i)*win), win,
			buildAggs(base.Add(time.Duration(i-1)*win), map[metric.SeriesKey]float64{sk: v}))
		for _, ev := range evs {
			t.Logf("w%d input cpu=%v -> state=%s kind=%s reason=%s", i, v, ev.State, ev.Kind, ev.Reason)
		}
		return evs
	}

	var fired int
	for i, v := range []float64{90, 10, 90, 90, 90} {
		evs := push(i+1, v)
		for _, ev := range evs {
			if ev.Kind == TrToFiring {
				fired++
			}
		}
	}
	// w1 pending; w2 恢复回 inactive（无通知）; w3 重新 pending; w4,w5 持续 -> w5 firing
	if fired != 1 {
		t.Fatalf("short flap must not fire early, expected exactly 1 firing, got %d", fired)
	}
	st := e.States()
	if len(st) != 1 || st[0].State != StateFiring {
		t.Fatalf("expected single firing state, got %+v", st)
	}
}

// TestNoDuplicateAlertAndResolve firing 期间重复越界不得重复告警，恢复必须恰好一次通知。
func TestNoDuplicateAlertAndResolve(t *testing.T) {
	labels := map[string]string{"host": "h1"}
	sk := key("cpu", labels)
	r := Rule{ID: "r1", Metric: "cpu", Comparator: OpGT, Threshold: 80,
		ForDuration: win, KeepDuration: 2 * win}
	e := NewEvaluator([]Rule{r})
	base := time.Unix(3600, 0).UTC()

	push := func(i int, v float64) []Event {
		evs := e.Evaluate(base.Add(time.Duration(i)*win), win,
			buildAggs(base.Add(time.Duration(i-1)*win), map[metric.SeriesKey]float64{sk: v}))
		for _, ev := range evs {
			t.Logf("w%d input cpu=%v -> state=%s kind=%s reason=%s", i, v, ev.State, ev.Kind, ev.Reason)
		}
		return evs
	}

	count := func(evs []Event, kd TransitionKind) int {
		n := 0
		for _, ev := range evs {
			if ev.Kind == kd {
				n++
			}
		}
		return n
	}

	var firing, resolved int
	// w1 breach -> firing (for=1m)
	firing += count(push(1, 90), TrToFiring)
	// w2 w3 持续越界：不得重复告警
	firing += count(push(2, 99), TrToFiring)
	firing += count(push(3, 91), TrToFiring)
	// w4 恢复一个窗口，未达 keep=2m，仍 firing（无恢复通知）
	resolved += count(push(4, 10), TrToInactive)
	// w5 又越界：取消恢复，仍 firing，不重复触发
	firing += count(push(5, 90), TrToFiring)
	// w6 w7 连续恢复
	resolved += count(push(6, 10), TrToInactive)
	resolved += count(push(7, 10), TrToInactive)

	if firing != 1 {
		t.Fatalf("expected exactly 1 firing notification, got %d", firing)
	}
	if resolved != 1 {
		t.Fatalf("expected exactly 1 resolve notification, got %d", resolved)
	}
	st := e.States()
	if st[0].State != StateInactive {
		t.Fatalf("expected inactive after resolve, got %s", st[0].State)
	}
}

// TestRuleFingerprint 配置变更可检测。
func TestRuleFingerprint(t *testing.T) {
	r1 := Rule{ID: "r1", Metric: "cpu", Comparator: OpGT, Threshold: 80, ForDuration: win}
	r2 := r1
	r2.Threshold = 90
	if r1.Fingerprint() == r2.Fingerprint() {
		t.Fatal("threshold change must change fingerprint")
	}
	r3 := Rule{ID: "r1", Metric: "cpu", Comparator: OpGT, Threshold: 80, ForDuration: win,
		MatchLabels: map[string]string{"b": "2", "a": "1"}}
	r4 := r3
	r4.MatchLabels = map[string]string{"a": "1", "b": "2"}
	if r3.Fingerprint() != r4.Fingerprint() {
		t.Fatal("label map order must not affect fingerprint")
	}
	e := NewEvaluator([]Rule{r1})
	added, changed, removed := e.UpdateRules([]Rule{r2, {ID: "r2", Metric: "m", Comparator: OpGT}})
	if len(added) != 1 || added[0] != "r2" || len(changed) != 1 || changed[0] != "r1" || len(removed) != 0 {
		t.Fatalf("unexpected diff: added=%v changed=%v removed=%v", added, changed, removed)
	}
	_, _, removed = e.UpdateRules(nil)
	if len(removed) != 2 {
		t.Fatalf("want 2 removed, got %v", removed)
	}
}
