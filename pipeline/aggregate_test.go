package pipeline

import (
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func TestAggregatorIdempotent(t *testing.T) {
	a := NewAggregator()
	sp := span.Span{TraceID: "t", SpanID: "s1", Service: "svc", Duration: 100 * time.Millisecond, HasError: true}
	if !a.Observe(sp) {
		t.Fatal("first observe should count")
	}
	// 重复/迟到上报不得重复计数。
	for i := 0; i < 5; i++ {
		if a.Observe(sp) {
			t.Fatal("duplicate observe counted")
		}
	}
	st := a.Snapshot()["svc"]
	if st.Count != 1 || st.ErrorCnt != 1 || st.TotalDurNs != int64(100*time.Millisecond) {
		t.Fatalf("bad stats: %+v", st)
	}
}

func TestAggregatorOrderIndependent(t *testing.T) {
	spans := []span.Span{
		{TraceID: "t", SpanID: "a", Service: "s1", Duration: 10},
		{TraceID: "t", SpanID: "b", Service: "s2", Duration: 20, HasError: true},
		{TraceID: "t", SpanID: "c", Service: "s1", Duration: 30},
	}
	a1, a2 := NewAggregator(), NewAggregator()
	for _, sp := range spans {
		a1.Observe(sp)
	}
	for i := len(spans) - 1; i >= 0; i-- {
		a2.Observe(spans[i])
	}
	s1, s2 := a1.Snapshot(), a2.Snapshot()
	if len(s1) != len(s2) {
		t.Fatal("snapshot mismatch")
	}
	for k, v := range s1 {
		if s2[k] != v {
			t.Fatalf("order-dependent stats for %s: %+v vs %+v", k, v, s2[k])
		}
	}
}

func TestAggregatorPersistence(t *testing.T) {
	dir := t.TempDir()
	a := NewAggregator()
	a.Observe(span.Span{TraceID: "t", SpanID: "a", Service: "svc", Duration: 5})
	if err := a.Save(dir); err != nil {
		t.Fatal(err)
	}
	b := NewAggregator()
	if err := b.Load(dir); err != nil {
		t.Fatal(err)
	}
	// 恢复后重复上报不得重复计数。
	if b.Observe(span.Span{TraceID: "t", SpanID: "a", Service: "svc", Duration: 5}) {
		t.Fatal("post-restore duplicate counted")
	}
	if got := b.Snapshot()["svc"].Count; got != 1 {
		t.Fatalf("count=%d, want 1", got)
	}
}
