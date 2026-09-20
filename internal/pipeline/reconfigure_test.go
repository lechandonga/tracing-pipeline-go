package pipeline_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/pipeline"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
)

// TestConfigChangeKeepsPinnedDecision: a trace dropped under an old rule must
// stay dropped for later spans even after the sampler config is replaced, and
// vice versa for kept traces.
func TestConfigChangeKeepsPinnedDecision(t *testing.T) {
	tid := testTraceID('4')
	p, err := pipeline.New(pipeline.Config{
		Store:    store.Config{MaxTraces: 64, MaxTotalSpans: 1000, RetentionTTL: time.Hour},
		Sampling: sampling.Config{Rate: 0}, // everything dropped
	})
	if err != nil {
		t.Fatal(err)
	}
	r := p.Ingest(model.Report{Span: mkSpan(tid, testSpanID("1111111111111111"), "", "api", 0, 10, nil)})
	if r.Decision != sampling.Drop {
		t.Fatal(r)
	}
	// Simulate a config reload that would keep this trace if decided anew.
	p.SetSampling(sampling.Config{Rate: 1})
	r = p.Ingest(model.Report{Span: mkSpan(tid, testSpanID("2222222222222222"), testSpanID("1111111111111111"), "db", 1, 9, nil)})
	if r.Decision != sampling.Drop || len(p.Aggregates().Traces) != 0 {
		t.Fatalf("pinned decision must survive config reload, got %+v", r)
	}
	p.Close()

	// The symmetric case: pinned keep must stay kept after a reload to drop.
	p2, _ := pipeline.New(pipeline.Config{
		Store:    store.Config{MaxTraces: 64, MaxTotalSpans: 1000, RetentionTTL: time.Hour},
		Sampling: sampling.Config{Rate: 1},
	})
	tid2 := testTraceID('5')
	p2.Ingest(model.Report{Span: mkSpan(tid2, testSpanID("1111111111111111"), "", "api", 0, 10, nil)})
	p2.SetSampling(sampling.Config{Rate: 0})
	r = p2.Ingest(model.Report{Span: mkSpan(tid2, testSpanID("2222222222222222"), testSpanID("1111111111111111"), "db", 1, 9, nil)})
	if r.Decision != sampling.Keep || len(p2.Aggregates().Traces) != 1 {
		t.Fatalf("pinned keep must survive config reload, got %+v", r)
	}
	p2.Close()
}

// TestRecoveryAfterSnapshotCompaction verifies state survives the
// snapshot+log-rotation boundary with no duplication.
func TestRecoveryAfterSnapshotCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := pipeline.Config{
		DataDir: dir,
		Store: store.Config{
			MaxTraces: 128, MaxTotalSpans: 10000, RetentionTTL: time.Hour,
		},
		Sampling:       sampling.Config{Rate: 1},
		SnapshotEveryN: 3,
	}
	p1, _ := pipeline.New(cfg)
	for i := 0; i < 7; i++ { // crosses two compaction thresholds
		tid := testTraceID(byte('a' + i))
		p1.Ingest(model.Report{Span: mkSpan(tid, testSpanID("1111111111111111"), "", "svc", 0, int64(10+i), nil)})
	}
	before := p1.Aggregates()
	p1.Close()

	p2, _ := pipeline.New(cfg)
	defer p2.Close()
	after := p2.Aggregates()
	if len(after.Traces) != len(before.Traces) {
		t.Fatalf("traces lost/duplicated across compaction: %d vs %d", len(after.Traces), len(before.Traces))
	}
	for i := range before.Traces {
		if before.Traces[i].SpanCount != after.Traces[i].SpanCount ||
			before.Traces[i].TotalLatency != after.Traces[i].TotalLatency {
			t.Fatalf("row %d differs after recovery: %+v vs %+v", i, before.Traces[i], after.Traces[i])
		}
	}
}
