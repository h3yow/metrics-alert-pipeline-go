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

	// High CPU for 15s triggers; recovery needs 10s of continuous
	// contradiction so single glitches don't flap the alert.
	highCPU := model.Rule{
		ID:              "high_cpu",
		Metric:          "cpu.usage",
		Match:           map[string]string{},
		Operator:        model.OpGt,
		Threshold:       90,
		ForDuration:     15 * time.Second,
		ResolveDuration: 10 * time.Second,
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

	// Build a deliberately shuffled batch: includes out-of-order samples,
	// duplicates, illegal samples and a short flap on host-c.
	batch := []model.Sample{
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 95, Timestamp: at(5), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(10), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 97, Timestamp: at(25), Source: "node-a"}, // fires (cycle 1)
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 70, Timestamp: at(40), Source: "node-a"}, // recovery hold starts
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 99, Timestamp: at(45), Source: "node-a"}, // glitch: hold cancelled
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 60, Timestamp: at(50), Source: "node-a"}, // hold restarts
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 60, Timestamp: at(60), Source: "node-a"}, // resolves (cycle 1)
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 95, Timestamp: at(70), Source: "node-a"}, // round 2 pending
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(85), Source: "node-a"}, // fires again (cycle 2)
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 60, Timestamp: at(95), Source: "node-a"},
		{Metric: "cpu.usage", Labels: cpu("a"), Value: 60, Timestamp: at(105), Source: "node-a"}, // resolves (cycle 2)
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 99, Timestamp: at(5), Source: "node-b"},
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 99, Timestamp: at(25), Source: "node-b"}, // fires but silenced
		{Metric: "cpu.usage", Labels: cpu("b"), Value: 80, Timestamp: at(45), Source: "node-b"}, // not silenced -> recovery delivered
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(5), Source: "node-c"},
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 50, Timestamp: at(8), Source: "node-c"}, // flap: resets pending
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(12), Source: "node-c"},
		{Metric: "cpu.usage", Labels: cpu("c"), Value: 98, Timestamp: at(30), Source: "node-c"}, // fires
	}
	// Exact duplicate and various illegal samples.
	batch = append(batch,
		model.Sample{Metric: "cpu.usage", Labels: cpu("a"), Value: 96, Timestamp: at(10), Source: "node-a"},
		model.Sample{Metric: "", Labels: cpu("x"), Value: 1, Timestamp: at(1), Source: "node-x"},
		model.Sample{Metric: "cpu.usage", Labels: cpu("x"), Value: 1, Timestamp: time.Time{}, Source: "node-x"},
		model.Sample{Metric: "cpu.usage", Labels: cpu("x"), Value: 1, Timestamp: at(1), Source: ""},
		model.Sample{Metric: "cpu.usage", Labels: model.Labels{{Name: "bad-name", Value: "x"}}, Value: 1, Timestamp: at(1), Source: "node-x"},
	)

	// Concurrent submission across goroutines. The actual interleaving is
	// nondeterministic; aggregates and Replay conclusions are not.
	var wg sync.WaitGroup
	for _, s := range batch {
		wg.Add(1)
		go func(s model.Sample) {
			defer wg.Done()
			p.Ingest(s)
		}(s)
	}
	wg.Wait()

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
		fmt.Printf("rule=%s#v%d host=%s status=%s lastValue=%.0f\n",
			s.RuleID, s.RuleVersion, host, s.Status, s.LastValue)
	}
}

type pipelinePrinter struct{}

func (pipelinePrinter) Notify(ev model.Event) {
	host := ""
	if ev.State != nil {
		host, _ = ev.State.Labels.Get("host")
	}
	switch ev.Type {
	case model.EventFiring, model.EventResolved, model.EventSuppressed:
		cycle := 0
		if ev.State != nil {
			cycle = ev.State.Cycle
		}
		fmt.Printf("[notify %s] %s host=%s cycle=%d silence=%s\n",
			ev.Time.Format("15:04:05"), ev.Type, host, cycle, ev.SilenceID)
	}
}

func printHeader(title string) {
	fmt.Println()
	fmt.Println("== " + title + " ==")
}
