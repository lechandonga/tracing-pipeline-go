package sampling

import (
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

func sp(service string, attrs map[string]string) model.Span {
	return model.Span{
		TraceID: "0102030405060708090a0b0c0d0e0f10",
		SpanID:  "1111111111111111", Service: service, Attributes: attrs,
	}
}

func TestDeterministicAcrossServicesAndInstances(t *testing.T) {
	cfg := Config{Rate: 0.5}
	s1, s2 := New(cfg), New(cfg)
	tid := model.TraceID("0102030405060708090a0b0c0d0e0f10")
	for _, svc := range []string{"gateway", "auth", "billing"} {
		d1, _ := s1.Decide(tid, sp(svc, nil), false, false)
		d2, _ := s2.Decide(tid, sp(svc, nil), false, false)
		if d1 != d2 {
			t.Fatalf("verdict must be identical across instances/services: %v vs %v", d1, d2)
		}
	}
}

func TestRateBounds(t *testing.T) {
	tid := model.TraceID("0102030405060708090a0b0c0d0e0f10")
	if d, _ := New(Config{Rate: 0}).Decide(tid, sp("a", nil), false, false); d != Drop {
		t.Fatal("rate 0 must drop")
	}
	if d, _ := New(Config{Rate: 1}).Decide(tid, sp("a", nil), false, false); d != Keep {
		t.Fatal("rate 1 must keep")
	}
	// Distribution check at rate 0.5: must be neither 0 nor 100%.
	s := New(Config{Rate: 0.5})
	kept := 0
	for i := 0; i < 200; i++ {
		idBytes := make([]byte, 32)
		idBytes[0] = 'a'
		idBytes[30] = byte('0' + i/10)
		idBytes[31] = byte('0' + i%10)
		if d, _ := s.Decide(model.TraceID(idBytes), sp("a", nil), false, false); d == Keep {
			kept++
		}
	}
	if kept < 50 || kept > 150 {
		t.Fatalf("rate 0.5 distribution implausible: kept=%d/200", kept)
	}
}

func TestOrderedRulesAndServiceSelector(t *testing.T) {
	cfg := Config{
		Rate: 1, // would keep everything
		Rules: []Rule{
			{Service: "billing", Key: "env", Value: "canary", Decision: Drop},
			{Key: "error", Value: "true", Decision: Keep},
		},
	}
	s := New(cfg)
	tid := model.TraceID("0102030405060708090a0b0c0d0e0f10")
	d, reason := s.Decide(tid, sp("billing", map[string]string{"env": "canary", "error": "true"}), false, false)
	if d != Drop {
		t.Fatalf("first matching rule must win, got %s (%s)", d, reason)
	}
	d, _ = s.Decide(tid, sp("web", map[string]string{"error": "true"}), false, false)
	if d != Keep {
		t.Fatal("key-only scoped rule should match other services")
	}
	d, _ = s.Decide(tid, sp("web", map[string]string{"env": "canary"}), false, false)
	if d != Keep {
		t.Fatal("service-scoped drop rule must not apply to other services")
	}
}

func TestHintNeverOverrides(t *testing.T) {
	tid := model.TraceID("0102030405060708090a0b0c0d0e0f10")
	// rate 0 but hint says keep -> still drop, conflict recorded
	s := New(Config{Rate: 0})
	d, reason := s.Decide(tid, sp("a", nil), true, true)
	if d != Drop || reason == "" {
		t.Fatalf("hint must not override drop: %s %s", d, reason)
	}
	// rate 1 but hint says drop -> still keep
	s = New(Config{Rate: 1})
	d, reason = s.Decide(tid, sp("a", nil), true, false)
	if d != Keep {
		t.Fatalf("hint must not override keep: %s %s", d, reason)
	}
	// absent hint must not be reported as conflict
	s = New(Config{Rate: 0})
	_, reason = s.Decide(tid, sp("a", nil), false, false)
	if contains(reason, "conflict") {
		t.Fatalf("missing hint must not be a conflict: %s", reason)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
