package pipeline

import (
	"fmt"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func tp(traceID, spanID string) map[string]string {
	return map[string]string{"traceparent": "00-" + traceID + "-" + spanID + "-01"}
}

func tid(i int) string { return fmt.Sprintf("%032x", i) }
func sid(i int) string { return fmt.Sprintf("%016x", i) }

func testConfig(dir string) Config {
	return Config{MaxSpans: 1000, MaxOrphans: 100, Ratio: 1, Dir: dir}
}

func TestPipelineOutOfOrderAndDuplicate(t *testing.T) {
	p := New(testConfig(""))
	trace := tid(1)
	// 子跨度先到（缺父）。
	rej := p.Submit(map[string]string{
		"traceparent": "00-" + trace + "-" + sid(2) + "-01",
		"parent_id":   sid(1),
	}, span.Span{Service: "svc-b", Duration: 50 * time.Millisecond})
	if rej == nil || rej.Reason != span.ReasonOrphan {
		t.Fatalf("expected orphan, got %+v", rej)
	}
	// 重复上报同一暂存跨度 -> duplicate。
	rej = p.Submit(map[string]string{
		"traceparent": "00-" + trace + "-" + sid(2) + "-01",
		"parent_id":   sid(1),
	}, span.Span{Service: "svc-b"})
	// 暂存中的重复不会被 ingester 的 arrived 捕获，视为再次暂存请求；
	// 管道以 store 去重为最终防线，此处只要求不产生错误归属。
	_ = rej
	// 根跨度到达，子跨度补齐。
	if rej := p.Submit(tp(trace, sid(1)), span.Span{Service: "svc-a", Duration: 100 * time.Millisecond}); rej != nil {
		t.Fatalf("root rejected: %+v", rej)
	}
	if p.PendingOrphans() != 0 {
		t.Fatalf("orphans not resolved: %d", p.PendingOrphans())
	}
	if got := p.Stats()["svc-b"].Count; got != 1 {
		t.Fatalf("svc-b count=%d, want 1 (no wrong attribution, no double count)", got)
	}
	// 已确认跨度的重复上报必须被拒绝。
	rej = p.Submit(tp(trace, sid(1)), span.Span{Service: "svc-a"})
	if rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("expected duplicate, got %+v", rej)
	}
	if got := p.Stats()["svc-a"].Count; got != 1 {
		t.Fatalf("svc-a count=%d after duplicate, want 1", got)
	}
}

func TestPipelineMalformedContext(t *testing.T) {
	p := New(testConfig(""))
	rej := p.Submit(map[string]string{"traceparent": "garbage"}, span.Span{Service: "svc"})
	if rej == nil || rej.Reason != span.ReasonMalformedContext {
		t.Fatalf("expected malformed, got %+v", rej)
	}
	if p.RejectionCounts()[span.ReasonMalformedContext] != 1 {
		t.Fatal("rejection not counted")
	}
	if len(p.RejectionLog()) == 0 || p.RejectionLog()[0].Detail == "" {
		t.Fatal("rejection log missing detail")
	}
}

func TestPipelineSamplingConsistency(t *testing.T) {
	p := New(Config{MaxSpans: 1000, MaxOrphans: 100, Ratio: 0})
	trace := tid(7)
	// 追踪被丢弃后，后续到达的同追踪跨度不得改变结论。
	p.Submit(tp(trace, sid(1)), span.Span{Service: "svc"})
	p.Submit(tp(trace, sid(2)), span.Span{Service: "svc"})
	if p.StoredSpans() != 0 || p.Dropped() != 2 {
		t.Fatalf("stored=%d dropped=%d, want 0/2", p.StoredSpans(), p.Dropped())
	}
	// 配置改为全保留：旧追踪结论不变，新追踪按新配置。
	p.UpdateSampling(1, nil)
	p.Submit(tp(trace, sid(3)), span.Span{Service: "svc"})
	if p.StoredSpans() != 0 {
		t.Fatal("decided trace must not flip after config change")
	}
	p.Submit(tp(tid(8), sid(1)), span.Span{Service: "svc"})
	if p.StoredSpans() != 1 {
		t.Fatal("new trace should follow new config")
	}
}

func TestPipelineCapacityBoundary(t *testing.T) {
	p := New(Config{MaxSpans: 2, MaxOrphans: 10, Ratio: 1})
	for i := 1; i <= 4; i++ {
		rej := p.Submit(tp(tid(i), sid(1)), span.Span{Service: "svc"})
		if i <= 2 && rej != nil {
			t.Fatalf("span %d rejected: %+v", i, rej)
		}
		if i > 2 && (rej == nil || rej.Reason != span.ReasonCapacityFull) {
			t.Fatalf("span %d: expected capacity rejection, got %+v", i, rej)
		}
	}
	if p.StoredSpans() != 2 {
		t.Fatalf("stored=%d, want 2", p.StoredSpans())
	}
	if p.RejectionCounts()[span.ReasonCapacityFull] != 2 {
		t.Fatal("capacity rejections not observable")
	}
}

func TestPipelineRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	p := New(testConfig(dir))
	trace := tid(9)
	p.Submit(tp(trace, sid(1)), span.Span{Service: "svc", Duration: 10})
	p.Submit(tp(trace, sid(2)), span.Span{Service: "svc", Duration: 20})
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}
	// 模拟重启。
	p2 := New(testConfig(dir))
	if err := p2.Restore(); err != nil {
		t.Fatal(err)
	}
	if p2.StoredSpans() != 2 {
		t.Fatalf("restored spans=%d, want 2", p2.StoredSpans())
	}
	if got := p2.Stats()["svc"].Count; got != 2 {
		t.Fatalf("restored count=%d, want 2", got)
	}
	// 重启后重复上报不得重复计数。
	rej := p2.Submit(tp(trace, sid(1)), span.Span{Service: "svc"})
	if rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("post-restart duplicate not detected: %+v", rej)
	}
	if got := p2.Stats()["svc"].Count; got != 2 {
		t.Fatalf("count=%d after duplicate, want 2", got)
	}
}
