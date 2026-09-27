package model

import (
	"math"
	"testing"
	"time"
)

func TestLabelsFingerprintOrderIndependent(t *testing.T) {
	a := Labels{{Name: "host", Value: "a"}, {Name: "zone", Value: "z1"}}
	b := Labels{{Name: "zone", Value: "z1"}, {Name: "host", Value: "a"}}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("fingerprints differ for reordered labels: %s vs %s", a.Fingerprint(), b.Fingerprint())
	}
	c := Labels{{Name: "host", Value: "b"}, {Name: "zone", Value: "z1"}}
	if a.Fingerprint() == c.Fingerprint() {
		t.Fatalf("different label values must produce different fingerprints")
	}
}

func TestLabelsMerge(t *testing.T) {
	base := Labels{{Name: "host", Value: "a"}}
	extra := Labels{{Name: "env", Value: "local"}, {Name: "host", Value: "b"}}
	m := base.Merge(extra)
	if v, _ := m.Get("host"); v != "a" {
		t.Fatalf("receiver label must win on collision, got %q", v)
	}
	if v, _ := m.Get("env"); v != "local" {
		t.Fatalf("extra label missing")
	}
	if len(base) != 1 {
		t.Fatalf("merge must not mutate receiver")
	}
}

func TestSampleValidateReasons(t *testing.T) {
	ts := time.Unix(100, 0)
	cases := []struct {
		name   string
		sample Sample
		want   RejectReason
	}{
		{"empty name", Sample{Metric: "", Source: "s", Timestamp: ts, Value: 1}, ReasonEmptyName},
		{"zero time", Sample{Metric: "m", Source: "s", Value: 1}, ReasonBadTimestamp},
		{"nan", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: math.NaN()}, ReasonNaN},
		{"inf", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: math.Inf(1)}, ReasonInf},
		{"bad label", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: 1,
			Labels: Labels{{Name: "bad-name", Value: "x"}}}, ReasonInvalidLabel},
		{"dup label", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: 1,
			Labels: Labels{{Name: "k", Value: "1"}, {Name: "k", Value: "2"}}}, ReasonInvalidLabel},
		{"source mismatch", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: 1,
			Labels: Labels{{Name: SourceLabel, Value: "other"}}}, ReasonSourceMismatch},
		{"valid", Sample{Metric: "m", Source: "s", Timestamp: ts, Value: 1}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("input sample metric=%q source=%q value=%v labels=%v; validation basis: reason=%q",
				tc.sample.Metric, tc.sample.Source, tc.sample.Value, tc.sample.Labels, tc.want)
			err := tc.sample.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			re, ok := err.(*RejectError)
			if !ok {
				t.Fatalf("expected *RejectError, got %T: %v", err, err)
			}
			if re.Reason != tc.want {
				t.Fatalf("reason = %q, want %q", re.Reason, tc.want)
			}
		})
	}
}

func TestIdentityDistinguishesObservations(t *testing.T) {
	ts := time.Unix(100, 0)
	s1 := Sample{Metric: "m", Labels: Labels{{Name: "host", Value: "a"}}, Value: 1, Timestamp: ts, Source: "x"}
	s2 := s1
	if s1.Identity() != s2.Identity() {
		t.Fatal("identical samples must share identity")
	}
	s3 := s1
	s3.Value = 2
	if s1.Identity() == s3.Identity() {
		t.Fatal("different values must have different identities")
	}
	s4 := s1
	s4.Timestamp = ts.Add(time.Second)
	if s1.Identity() == s4.Identity() {
		t.Fatal("different timestamps must have different identities")
	}
}

func TestRuleEvaluateAndSilence(t *testing.T) {
	r := Rule{ID: "r", Metric: "m", Operator: OpGt, Threshold: 10, ForDuration: time.Second}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if !r.Evaluate(11) || r.Evaluate(10) {
		t.Fatal("threshold comparison wrong")
	}
	if !r.MatchesSeries("m", Labels{{Name: "host", Value: "a"}}) {
		t.Fatal("rule should match series")
	}
	if r.MatchesSeries("other", nil) {
		t.Fatal("rule must not match other metric")
	}

	sil := Silence{ID: "s", Match: map[string]string{"host": "a"},
		StartsAt: time.Unix(100, 0), EndsAt: time.Unix(160, 0)}
	if err := sil.Validate(); err != nil {
		t.Fatal(err)
	}
	if !sil.Contains(time.Unix(101, 0)) || sil.Contains(time.Unix(160, 0)) || sil.Contains(time.Unix(99, 0)) {
		t.Fatal("half-open window [start,end) wrong")
	}
	if !sil.Matches("m", Labels{{Name: "host", Value: "a"}}) {
		t.Fatal("silence should match label")
	}
}
