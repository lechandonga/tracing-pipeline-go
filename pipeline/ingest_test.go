package pipeline

import (
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func TestIngesterOutOfOrder(t *testing.T) {
	ing := NewIngester(100)
	// 孙子先于父到达。
	ready, rej := ing.Ingest(span.Span{TraceID: "t", SpanID: "c", ParentID: "b"})
	if len(ready) != 0 || rej == nil || rej.Reason != span.ReasonOrphan {
		t.Fatalf("expected orphan, ready=%v rej=%+v", ready, rej)
	}
	ready, rej = ing.Ingest(span.Span{TraceID: "t", SpanID: "b", ParentID: "a"})
	if len(ready) != 0 || rej == nil || rej.Reason != span.ReasonOrphan {
		t.Fatalf("expected orphan, ready=%v rej=%+v", ready, rej)
	}
	if ing.PendingOrphans() != 2 {
		t.Fatalf("pending=%d, want 2", ing.PendingOrphans())
	}
	// 根到达后应级联补齐 b、c。
	ready, rej = ing.Ingest(span.Span{TraceID: "t", SpanID: "a"})
	if rej != nil {
		t.Fatalf("root rejected: %+v", rej)
	}
	if len(ready) != 3 {
		t.Fatalf("ready=%v, want 3 spans", ready)
	}
	if ing.PendingOrphans() != 0 {
		t.Fatalf("pending=%d, want 0", ing.PendingOrphans())
	}
}

func TestIngesterDuplicate(t *testing.T) {
	ing := NewIngester(100)
	ing.Ingest(span.Span{TraceID: "t", SpanID: "a"})
	_, rej := ing.Ingest(span.Span{TraceID: "t", SpanID: "a"})
	if rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("expected duplicate, got %+v", rej)
	}
}

func TestIngesterOrphanCapacity(t *testing.T) {
	ing := NewIngester(1)
	ing.Ingest(span.Span{TraceID: "t", SpanID: "b", ParentID: "a"})
	_, rej := ing.Ingest(span.Span{TraceID: "t", SpanID: "c", ParentID: "a"})
	if rej == nil || rej.Reason != span.ReasonCapacityFull {
		t.Fatalf("expected capacity rejection, got %+v", rej)
	}
}
