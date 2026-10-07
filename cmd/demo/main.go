// Command demo runs the metrics aggregation and alerting pipeline entirely
// inside one local process: concurrent producers, out-of-order samples,
// duplicates, illegal samples, alert flapping, a silence window and a rule
// change with deterministic replay are all simulated and printed.
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/metrics-alert-pipeline-go/pkg/model"
	"github.com/h3yow/metrics-alert-pipeline-go/pkg/pipeline"
)

func main() {
	runDemo()
}

func runDemo() {
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

	cpu := func(host string) model.Labels {
		return model.Labels{{Name: "host", Value: host}}
	}

	// High CPU for 30s triggers after a 15s "for" guard; recovery needs the
	// contradiction to hold for 5s so single spikes don't end the alert.
	highCPU := model.Rule{
		ID:              "high_cpu",
		Metric:          "cpu.usage",
		Match:           map[string]string{},
		Operator:        model.OpGt,
		Threshold:       90,
		ForDuration:     15 * time.Second,
		ResolveDuration: 5 * time.Second,
	}

	p := pipeline.New(pipeline.Config{
		WindowSize:  10 * time.Second,
		ExtraLabels: model.Labels{{Name: "env", Value: "local"}},
	}, pipelinePrinter{})
	p.SetRules([]model.Rule{highCPU})

	// Silence host-b for the first 40 seconds.
	p.SetSilences([]model.Silence{{
		ID:       "maintenance-host-b",
		Metric:   "cpu.usage",
		Match:    map[string]string{"host": "b"},
		StartsAt: at(0),
		EndsAt:   at(40),
	}})

	// Phase 1: a concurrent junk batch (duplicates, illegal samples, plain
	// aggregate data). The interleaving is nondeterministic; aggregates and
	// Replay conclusions are not.
	junk := []model.Sample{
		{Metric: "cpu.usage", Labels: cpu("e"), Value: 20, Timestamp: at(3), Source: "node-e"},
		{Metric: "cpu.usage", Labels: cpu("e"), Value: 30, Timestamp: at(7), Source: "node-e"},
		{Metric: "cpu.usage", Labels: cpu("e"), Value: 30, Timestamp: at(7), Source: "node-e"}, // exact duplicate
		{Metric: "", Labels: cpu("x"), Value: 1, Timestamp: at(1), Source: "node-x"},
		{Metric: "cpu.usage", Labels: cpu("x"), Value: 1, Timestamp: time.Time{}, Source: "node-x"},
		{Metric: "cpu.usage", Labels: cpu("x"), Value: 1, Timestamp: at(1), Source: ""},
		{Metric: "cpu.usage", Labels: model.Labels{{Name: "bad-name", Value: "x"}}, Value: 1, Timestamp: at(1), Source: "node-x"},
	}
	var wg sync.WaitGroup
	for _, s := range junk {
		wg.Add(1)
		go func(s model.Sample) {
			defer wg.Done()
			p.Ingest(s)
		}(s)
	}
	wg.Wait()

	// Phase 2: the alert narrative, ingested in timestamp order so the live
	// notification log reads like the canonical replay.
	narrative := []model.Sample{
		// host-a round 1: fires after a sustained 15s stretch, recovers only
		// after the low value holds for the 5s resolve guard.
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 95, Timestamp: at(5), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(10), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 97, Timestamp: at(25), Source: "node-a"}, // fires (cycle 1)
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 70, Timestamp: at(40), Source: "node-a"}, // recovery wait starts
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 70, Timestamp: at(46), Source: "node-a"}, // held 6s >= 5s: resolves (cycle 1)
		// host-b: fires inside the silence window, recovers outside it.
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 99, Timestamp: at(5), Source: "node-b"},
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 99, Timestamp: at(25), Source: "node-b"}, // fires but silenced
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 80, Timestamp: at(45), Source: "node-b"}, // recovery wait (window over)
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 80, Timestamp: at(51), Source: "node-b"}, // resolves -> delivered
		// host-c: a short flap on the trigger side resets the pending wait.
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(5), Source: "node-c"},
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 50, Timestamp: at(8), Source: "node-c"}, // flap: resets pending
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(12), Source: "node-c"},
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(30), Source: "node-c"}, // fires
		// host-d: recovery flapping. Low spikes interrupted by a high sample
		// never resolve; the wait restarts instead.
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 95, Timestamp: at(5), Source: "node-d"},
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 95, Timestamp: at(25), Source: "node-d"}, // fires
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 40, Timestamp: at(30), Source: "node-d"}, // recovery wait starts
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 96, Timestamp: at(33), Source: "node-d"}, // flap: cancel recovery
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 40, Timestamp: at(36), Source: "node-d"}, // wait restarts
		{Metric: "cpu.usage", Labels: cpu("d"), Value: 40, Timestamp: at(42), Source: "node-d"}, // held 6s >= 5s: resolves
		// host-a round 2: the same series triggers again and goes through
		// the full flow as cycle 2, with its own exactly-once notifications.
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(52), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(68), Source: "node-a"}, // fires (cycle 2)
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 50, Timestamp: at(72), Source: "node-a"}, // recovery wait
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 50, Timestamp: at(78), Source: "node-a"}, // resolves (cycle 2)
	}
	for _, s := range narrative {
		p.Ingest(s)
	}

	printHeader("AGGREGATES (deterministic despite concurrency / reordering)")
	for _, a := range p.Aggregates() {
		host, _ := a.Labels.Get("host")
		fmt.Printf("window %s host=%s count=%d sum=%.0f min=%.0f max=%.0f first=%.0f last=%.0f\n",
			a.WindowStart.Format("15:04:05"), host, a.Count, a.Sum, a.Min, a.Max, a.First, a.Last)
	}

	printHeader("STATS")
	st := p.Stats()
	fmt.Printf("accepted=%d rejected=%d duplicates=%d pending=%d firing=%d resolved=%d notified=%d suppressed=%d\n",
		st.Accepted, st.Rejected, st.Duplicates, st.Pending, st.Firing, st.Resolved, st.Notified, st.Suppressed)

	printHeader("CONFIG CHANGE: threshold lowered to 80, then replay history")
	p.SetRules([]model.Rule{{
		ID:          "high_cpu",
		Metric:      "cpu.usage",
		Operator:    model.OpGt,
		Threshold:   80,
		ForDuration: 15 * time.Second,
	}})
	evs, rst, aggs, states := p.Replay()
	fmt.Printf("replay events=%d notified=%d suppressed=%d aggregates=%d liveStates=%d finalStates=%d\n",
		len(evs), rst.Notified, rst.Suppressed, len(aggs), len(p.States()), len(states))

	printHeader("REPLAY FINAL ALERT STATES")
	for _, s := range states {
		host, _ := s.Labels.Get("host")
		fmt.Printf("rule=%s#v%d host=%s status=%s cycle=%d lastValue=%.0f\n",
			s.RuleID, s.RuleVersion, host, s.Status, s.Cycle, s.LastValue)
	}
}

type pipelinePrinter struct{}

func (pipelinePrinter) Notify(ev model.Event) {
	host := ""
	cycle := 0
	if ev.State != nil {
		host, _ = ev.State.Labels.Get("host")
		cycle = ev.State.Cycle
	}
	switch ev.Type {
	case model.EventFiring, model.EventResolved, model.EventSuppressed:
		fmt.Printf("[notify %s] %s host=%s cycle=%d silence=%s\n",
			ev.Time.Format("15:04:05"), ev.Type, host, cycle, ev.SilenceID)
	}
}

func printHeader(title string) {
	fmt.Println()
	fmt.Println("== " + title + " ==")
}
