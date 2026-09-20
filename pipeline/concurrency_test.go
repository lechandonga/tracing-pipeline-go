package pipeline

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func TestPipelineConcurrentWrites(t *testing.T) {
	const workers = 8
	const perWorker = 200
	p := New(Config{MaxSpans: workers * perWorker, MaxOrphans: 100, Ratio: 1})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n := w*perWorker + i + 1
				// 每条跨度上报两次，验证并发下去重稳定。
				p.Submit(tp(tid(n), sid(1)), span.Span{Service: fmt.Sprintf("svc-%d", w), Duration: 1})
				p.Submit(tp(tid(n), sid(1)), span.Span{Service: fmt.Sprintf("svc-%d", w), Duration: 1})
			}
		}(w)
	}
	wg.Wait()
	if p.StoredSpans() != workers*perWorker {
		t.Fatalf("stored=%d, want %d", p.StoredSpans(), workers*perWorker)
	}
	var total int64
	for _, st := range p.Stats() {
		total += st.Count
	}
	if total != workers*perWorker {
		t.Fatalf("aggregated=%d, want %d", total, workers*perWorker)
	}
	if got := p.RejectionCounts()[span.ReasonDuplicate]; got != workers*perWorker {
		t.Fatalf("duplicates=%d, want %d", got, workers*perWorker)
	}
}
