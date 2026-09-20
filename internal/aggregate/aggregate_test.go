package aggregate

import (
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

func mkSpan(tid string, sid, svc string, start, end int64, errored bool) model.Span {
	st := model.StatusOK
	if errored {
		st = model.StatusError
	}
	return model.Span{
		TraceID: model.TraceID(tid), SpanID: model.SpanID(sid), Service: svc,
		StartUnixNano: start, EndUnixNano: end, Status: st,
		Attributes: map[string]string{},
	}
}

const t1 = "0102030405060708090a0b0c0d0e0f10"
const t2 = "0202030405060708090a0b0c0d0e0f10"

func TestIdempotentAndOrderIndependent(t *testing.T) {
	spans := []model.Span{
		mkSpan(t1, "1111111111111111", "a", 0, 100, false),
		mkSpan(t1, "2222222222222222", "a", 50, 80, true),
		mkSpan(t1, "3333333333333333", "b", 120, 130, false),
		mkSpan(t2, "4444444444444444", "b", 0, 10, true),
	}
	apply := func(order []int) Snapshot {
		a := New()
		for _, i := range order {
			a.ApplySpan(spans[i])
		}
		return a.Snapshot()
	}
	s1 := apply([]int{0, 1, 2, 3})
	s2 := apply([]int{3, 2, 1, 0})
	if !snapEqual(s1, s2) {
		t.Fatalf("aggregates depend on arrival order:\n%+v\n%+v", s1, s2)
	}

	// Duplicates must not change anything.
	a := New()
	for _, sp := range spans {
		a.ApplySpan(sp)
	}
	before := a.Snapshot()
	if a.ApplySpan(spans[1]) {
		t.Fatal("duplicate span reported as applied")
	}
	if !snapEqual(before, a.Snapshot()) {
		t.Fatal("duplicate changed aggregates")
	}

	var tr *TraceStat
	for i := range s1.Traces {
		if string(s1.Traces[i].TraceID) == t1 {
			tr = &s1.Traces[i]
		}
	}
	if tr.SpanCount != 3 || tr.ErrorCount != 1 || tr.Duration != 130 || tr.TotalLatency != 100+30+10 {
		t.Fatalf("bad trace rollup: %+v", tr)
	}
}

func TestLateSpanUpdatesCurrentSnapshotOnly(t *testing.T) {
	a := New()
	a.ApplySpan(mkSpan(t1, "1111111111111111", "a", 0, 10, false))
	published := a.Snapshot()
	a.ApplySpan(mkSpan(t1, "2222222222222222", "a", 10, 30, false))
	if published.Traces[0].SpanCount != 1 {
		t.Fatal("published snapshot must be immutable")
	}
	if a.Snapshot().Traces[0].SpanCount != 2 {
		t.Fatal("late span must be counted in current snapshot")
	}
}

func TestRemoveRetractsExactly(t *testing.T) {
	a := New()
	s1 := mkSpan(t1, "1111111111111111", "a", 0, 10, false)
	s2 := mkSpan(t1, "2222222222222222", "a", 5, 80, true)
	a.ApplySpan(s1)
	a.ApplySpan(s2)
	a.RemoveSpan(s2)
	row := a.Snapshot()
	var tr TraceStat
	for _, x := range row.Traces {
		if string(x.TraceID) == t1 {
			tr = x
		}
	}
	if tr.ErrorCount != 0 || tr.SpanCount != 1 || tr.MaxEnd != 10 {
		t.Fatalf("removal did not retract exactly: %+v", tr)
	}
	// Removing twice / missing must be a no-op.
	a.RemoveSpan(s2)
	a.RemoveSpan(mkSpan(t1, "9999999999999999", "a", 0, 1, false))
	if a.Snapshot().Traces[0].SpanCount != 1 {
		t.Fatal("idempotent removal violated")
	}
}

func snapEqual(a, b Snapshot) bool {
	if len(a.Traces) != len(b.Traces) || len(a.Services) != len(b.Services) {
		return false
	}
	for i := range a.Traces {
		if a.Traces[i] != b.Traces[i] {
			return false
		}
	}
	for i := range a.Services {
		if a.Services[i] != b.Services[i] {
			return false
		}
	}
	return true
}
