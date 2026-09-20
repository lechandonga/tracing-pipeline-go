package pipeline_test

import (
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/pipeline"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
)

// TestTTLExpiresKeptAndDropped covers the retention cap for both trace
// categories via the exported RunTTL test hook in the same module style.
func TestTTLExpiresKeptAndDropped(t *testing.T) {
	cfg := pipeline.Config{
		Store: store.Config{
			MaxTraces: 64, MaxTotalSpans: 1000,
			RetentionTTL: 20 * time.Millisecond,
		},
		Sampling: sampling.Config{Rate: 1},
	}
	p, _ := pipeline.New(cfg)
	defer p.Close()

	kept := testTraceID('1')
	p.Ingest(model.Report{Span: mkSpan(kept, testSpanID("1111111111111111"), "", "a", 0, 10, nil)})
	if p.TraceView(kept) == nil {
		t.Fatal("kept trace missing")
	}

	// dropped via rate 0 config on a second pipeline
	cfg2 := cfg
	cfg2.Sampling = sampling.Config{Rate: 0}
	p2, _ := pipeline.New(cfg2)
	defer p2.Close()
	dropped := testTraceID('2')
	if r := p2.Ingest(model.Report{Span: mkSpan(dropped, testSpanID("2222222222222222"), "", "a", 0, 10, nil)}); r.Decision != sampling.Drop {
		t.Fatal(r)
	}

	time.Sleep(40 * time.Millisecond)
	p.RunTTL(time.Now())
	p2.RunTTL(time.Now())

	if p.TraceView(kept) != nil || len(p.Aggregates().Traces) != 0 {
		t.Fatal("kept trace should expire and retract aggregates")
	}
	if p.Metrics().ExpiredTraces != 1 {
		t.Fatalf("expected 1 expired kept trace, got %d", p.Metrics().ExpiredTraces)
	}
	// Dropped tombstone expired: a later span recomputes a fresh verdict.
	if p2.Metrics().ExpiredTraces != 1 {
		t.Fatalf("expected 1 expired dropped tombstone, got %d", p2.Metrics().ExpiredTraces)
	}
}
