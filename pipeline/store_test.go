package pipeline

import (
	"fmt"
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func TestStoreDedupe(t *testing.T) {
	s := NewStore(10, "")
	sp := span.Span{TraceID: "t", SpanID: "a"}
	if rej := s.Add(sp); rej != nil {
		t.Fatalf("first add rejected: %+v", rej)
	}
	rej := s.Add(sp)
	if rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("expected duplicate rejection, got %+v", rej)
	}
	if s.Len() != 1 || s.Rejections()[span.ReasonDuplicate] != 1 {
		t.Fatalf("bad state: len=%d rej=%v", s.Len(), s.Rejections())
	}
}

func TestStoreCapacity(t *testing.T) {
	s := NewStore(2, "")
	for i := 0; i < 2; i++ {
		s.Add(span.Span{TraceID: "t", SpanID: fmt.Sprintf("s%d", i)})
	}
	rej := s.Add(span.Span{TraceID: "t", SpanID: "s2"})
	if rej == nil || rej.Reason != span.ReasonCapacityFull {
		t.Fatalf("expected capacity rejection, got %+v", rej)
	}
	if s.Len() != 2 {
		t.Fatalf("store grew beyond capacity: %d", s.Len())
	}
}

func TestStoreSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(10, dir)
	s.Add(span.Span{TraceID: "t1", SpanID: "a", Service: "svc"})
	s.Add(span.Span{TraceID: "t1", SpanID: "a"}) // duplicate
	if err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	s2 := NewStore(10, dir)
	if err := s2.Restore(); err != nil {
		t.Fatal(err)
	}
	if s2.Len() != 1 {
		t.Fatalf("restored len=%d, want 1", s2.Len())
	}
	if _, ok := s2.Get("t1", "a"); !ok {
		t.Fatal("restored span missing")
	}
	// 重启后重复上报仍被识别为重复，不重复计数。
	if rej := s2.Add(span.Span{TraceID: "t1", SpanID: "a"}); rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("post-restore duplicate not detected: %+v", rej)
	}
}
