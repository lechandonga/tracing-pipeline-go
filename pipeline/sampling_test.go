package pipeline

import (
	"fmt"
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func TestSamplerStickyDecision(t *testing.T) {
	s := NewSampler(0.5, nil)
	sp := span.Span{TraceID: "trace-x", SpanID: "a"}
	first := s.Decide(sp)
	// 乱序/重复到达的同追踪跨度结论必须一致。
	for i := 0; i < 100; i++ {
		if got := s.Decide(span.Span{TraceID: "trace-x", SpanID: fmt.Sprintf("s%d", i)}); got != first {
			t.Fatalf("decision flipped: first=%v got=%v", first, got)
		}
	}
}

func TestSamplerAttrRuleOverridesRatio(t *testing.T) {
	s := NewSampler(0, map[string]string{"tier": "critical"})
	keep := s.Decide(span.Span{TraceID: "t1", Attributes: map[string]string{"tier": "critical"}})
	if !keep {
		t.Fatal("attribute rule should force keep")
	}
	drop := s.Decide(span.Span{TraceID: "t2", Attributes: map[string]string{"tier": "batch"}})
	if drop {
		t.Fatal("ratio 0 without matching attr should drop")
	}
}

func TestSamplerConfigChangeKeepsOldDecisions(t *testing.T) {
	s := NewSampler(1, nil)
	if !s.Decide(span.Span{TraceID: "old-trace"}) {
		t.Fatal("ratio 1 should keep")
	}
	s.UpdateConfig(0, nil)
	if !s.Decide(span.Span{TraceID: "old-trace"}) {
		t.Fatal("config change must not flip decided trace")
	}
	if s.Decide(span.Span{TraceID: "new-trace"}) {
		t.Fatal("new trace should follow new ratio 0")
	}
}

func TestSamplerRatioConsistency(t *testing.T) {
	// 相同 traceID 在不同 Sampler 实例（不同服务/进程）结论一致。
	a, b := NewSampler(0.5, nil), NewSampler(0.5, nil)
	for i := 0; i < 200; i++ {
		tid := fmt.Sprintf("trace-%d", i)
		if a.Decide(span.Span{TraceID: tid}) != b.Decide(span.Span{TraceID: tid}) {
			t.Fatalf("inconsistent decision for %s", tid)
		}
	}
}
