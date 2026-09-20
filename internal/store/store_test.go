package store

import (
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

func mkSpan(tid string, sid, pid string, start, end int64) model.Span {
	return model.Span{
		TraceID: model.TraceID(tid), SpanID: model.SpanID(sid), ParentSpanID: model.SpanID(pid),
		Service: "svc", StartUnixNano: start, EndUnixNano: end,
		Attributes: map[string]string{},
	}
}

const tid = "0102030405060708090a0b0c0d0e0f10"

func TestOutOfOrderReassembly(t *testing.T) {
	s := New(Config{})
	// grandchild -> child -> root delivered backwards
	r1 := s.Add(mkSpan(tid, "3333333333333333", "2222222222222222", 20, 30), false)
	if !r1.Accepted || r1.State != StatePartial {
		t.Fatalf("orphan expected partial, got %+v", r1)
	}
	r2 := s.Add(mkSpan(tid, "2222222222222222", "1111111111111111", 10, 30), false)
	if !r2.Accepted || r2.State != StatePartial || r2.Status != StatusResolvedOrphan {
		t.Fatalf("child should resolve one orphan but stay partial, got %+v", r2)
	}
	r3 := s.Add(mkSpan(tid, "1111111111111111", "", 0, 40), false)
	if !r3.Accepted || r3.State != StateComplete || r3.Status != StatusResolvedOrphan {
		t.Fatalf("root should complete trace, got %+v", r3)
	}
	v := s.Get(model.TraceID(tid))
	if len(v.OrphanSpanIDs) != 0 || len(v.RootSpanIDs) != 1 {
		t.Fatalf("unexpected links: %+v", v)
	}
}

func TestDuplicateAndCollision(t *testing.T) {
	s := New(Config{})
	a := mkSpan(tid, "1111111111111111", "", 0, 10)
	if r := s.Add(a, false); !r.Accepted {
		t.Fatal(r)
	}
	if r := s.Add(a, false); r.Status != StatusDuplicate || !r.Accepted {
		t.Fatalf("exact duplicate must be idempotent accepted, got %+v", r)
	}
	if s.SpanCount() != 1 {
		t.Fatalf("duplicate must not add a span: %d", s.SpanCount())
	}
	b := a
	b.EndUnixNano = 999 // same identity, different payload
	if r := s.Add(b, false); r.Accepted || r.Status != RejectSpanCollision {
		t.Fatalf("payload collision must be rejected, got %+v", r)
	}
}

func TestPerTraceAndTotalCapacityReject(t *testing.T) {
	s := New(Config{MaxSpansPerTrace: 1, MaxTotalSpans: 1})
	if r := s.Add(mkSpan(tid, "1111111111111111", "", 0, 1), false); !r.Accepted {
		t.Fatal(r)
	}
	if r := s.Add(mkSpan(tid, "2222222222222222", "1111111111111111", 1, 2), false); r.Accepted || r.Status != RejectPerTraceCap {
		t.Fatalf("per-trace cap expected, got %+v", r)
	}

	tid2 := "0202030405060708090a0b0c0d0e0f10"
	if r := s.Add(mkSpan(tid2, "1111111111111111", "", 0, 1), false); r.Accepted || r.Status != RejectTotalCapacity {
		t.Fatalf("total cap expected, got %+v", r)
	}
}

func TestMaxTracesEvictOldest(t *testing.T) {
	clock := time.Unix(0, 0)
	s := New(Config{MaxTraces: 1, EvictOldest: true})
	s.now = func() time.Time { return clock }

	r := s.Add(mkSpan(tid, "1111111111111111", "", 0, 1), false)
	if !r.Accepted {
		t.Fatal(r)
	}
	clock = clock.Add(time.Second)
	tid2 := "0202030405060708090a0b0c0d0e0f10"
	r = s.Add(mkSpan(tid2, "1111111111111111", "", 0, 1), false)
	if !r.Accepted || len(r.Evicted) != 1 || r.Evicted[0].Reason != ReasonEvictedOldest {
		t.Fatalf("oldest trace should be evicted to make room, got %+v", r)
	}
	if s.Get(model.TraceID(tid)) != nil || s.TraceCount() != 1 {
		t.Fatal("evicted trace must be gone")
	}
}

func TestMaxTracesRejectWhenDegradeDisabled(t *testing.T) {
	s := New(Config{MaxTraces: 1})
	if r := s.Add(mkSpan(tid, "1111111111111111", "", 0, 1), false); !r.Accepted {
		t.Fatal(r)
	}
	tid2 := "0202030405060708090a0b0c0d0e0f10"
	r := s.Add(mkSpan(tid2, "1111111111111111", "", 0, 1), false)
	if r.Accepted || r.Status != RejectTraceCapacity {
		t.Fatalf("expected trace-cap rejection, got %+v", r)
	}
}

func TestTTLEviction(t *testing.T) {
	s := New(Config{RetentionTTL: time.Minute})
	s.now = func() time.Time { return time.Unix(0, 0) }
	if r := s.Add(mkSpan(tid, "1111111111111111", "", 0, 1), false); !r.Accepted {
		t.Fatal(r)
	}
	if ev := s.EvictExpired(time.Unix(30, 0)); len(ev) != 0 {
		t.Fatal("trace must survive within TTL")
	}
	ev := s.EvictExpired(time.Unix(61, 0))
	if len(ev) != 1 || s.TraceCount() != 0 {
		t.Fatalf("idle trace must be evicted once, got %v", ev)
	}
}

func TestSpanCycleNeverCompletes(t *testing.T) {
	s := New(Config{})
	if r := s.Add(mkSpan(tid, "1111111111111111", "2222222222222222", 0, 10), false); r.State != StatePartial {
		t.Fatal("a->b with b missing must be partial")
	}
	if r := s.Add(mkSpan(tid, "2222222222222222", "1111111111111111", 0, 10), false); r.State != StatePartial {
		t.Fatal("a cycle a<->b must never be marked complete")
	}
	v := s.Get(model.TraceID(tid))
	if len(v.RootSpanIDs) != 0 || v.State != StatePartial {
		t.Fatalf("cycle must have no root and stay partial: %+v", v)
	}
}
