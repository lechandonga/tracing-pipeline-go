package pipeline_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/pipeline"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
)

func testTraceID(nibble byte) model.TraceID {
	b := make([]byte, 32)
	for i := range b {
		b[i] = '0'
	}
	b[31] = nibble
	return model.TraceID(b)
}

func testSpanID(s string) model.SpanID {
	b := make([]byte, 16)
	for i := range b {
		b[i] = '0'
	}
	copy(b, s)
	return model.SpanID(b)
}

func baseConfig(t *testing.T, dir string, rate float64, rules ...sampling.Rule) pipeline.Config {
	return pipeline.Config{
		DataDir: dir,
		Store: store.Config{
			MaxTraces: 128, MaxSpansPerTrace: 256, MaxTotalSpans: 10000,
			RetentionTTL: time.Hour,
		},
		Sampling:       sampling.Config{Rate: rate, Rules: rules},
		TTLInterval:    time.Hour,
		SnapshotEveryN: 4,
	}
}

func mkSpan(tid model.TraceID, sid, pid model.SpanID, svc string, start, end int64, attrs map[string]string) model.Span {
	return model.Span{
		TraceID: tid, SpanID: sid, ParentSpanID: pid, Service: svc, Name: "op",
		Kind: model.KindServer, StartUnixNano: start, EndUnixNano: end,
		Attributes: attrs, Status: model.StatusOK,
	}
}

func ingestAll(t *testing.T, p *pipeline.Pipeline, spans []model.Span, orders ...[]int) {
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	if len(orders) > 0 {
		order = orders[0]
	}
	for _, i := range order {
		r := p.Ingest(model.Report{Span: spans[i]})
		if !r.Accepted {
			t.Fatalf("span %d rejected: %s %s", i, r.Code, r.Reason)
		}
	}
}

// TestOrderIndependentResult is the headline consistency check: the same
// batch in different arrival orders must produce identical traces and
// aggregates.
func TestOrderIndependentResult(t *testing.T) {
	tid := testTraceID('1')
	root, a, b := testSpanID("1111111111111111"), testSpanID("2222222222222222"), testSpanID("3333333333333333")
	spans := []model.Span{
		mkSpan(tid, root, "", "api", 0, 100, nil),
		mkSpan(tid, a, root, "auth", 20, 80, nil),
		mkSpan(tid, b, root, "db", 30, 90, map[string]string{"q": "get"}),
	}
	run := func(order []int) (string, int, int) {
		p, err := pipeline.New(baseConfig(t, "", 1))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		ingestAll(t, p, spans, order)
		snap := p.Aggregates()
		v := p.TraceView(tid)
		if v == nil || v.State != store.StateComplete {
			t.Fatalf("trace missing/incomplete: %+v", v)
		}
		totalErr, totalCount, totalLat := 0, 0, int64(0)
		for _, s := range snap.Services {
			totalErr += s.ErrorCount
			totalCount += s.SpanCount
			totalLat += s.TotalLatency
		}
		return string(v.State), totalErr*0 + totalCount, int(totalLat)
	}
	s1, c1, l1 := run([]int{0, 1, 2})
	s2, c2, l2 := run([]int{2, 1, 0})
	s3, c3, l3 := run([]int{1, 0, 2})
	if s1 != s2 || s2 != s3 || c1 != c2 || c2 != c3 || l1 != l2 || l2 != l3 {
		t.Fatalf("results depend on order: (%s,%d,%d) (%s,%d,%d) (%s,%d,%d)",
			s1, c1, l1, s2, c2, l2, s3, c3, l3)
	}
	if c1 != 3 || l1 != 100+60+60 {
		t.Fatalf("unexpected rollup count=%d lat=%d", c1, l1)
	}
}

func TestDuplicateReportsCountOnce(t *testing.T) {
	p, err := pipeline.New(baseConfig(t, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tid := testTraceID('2')
	s := mkSpan(tid, testSpanID("1111111111111111"), "", "api", 0, 10, nil)
	for i := 0; i < 5; i++ {
		r := p.Ingest(model.Report{Span: s})
		if !r.Accepted {
			t.Fatal(r)
		}
	}
	snap := p.Aggregates()
	if snap.Traces[0].SpanCount != 1 || snap.Services[0].SpanCount != 1 {
		t.Fatalf("duplicate counted: %+v", snap)
	}
	if m := p.Metrics(); m.Duplicates != 4 {
		t.Fatalf("expected 4 duplicates, got %d", m.Duplicates)
	}
}

func TestContextConflictRejects(t *testing.T) {
	p, err := pipeline.New(baseConfig(t, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tid := testTraceID('3')
	s := mkSpan(tid, testSpanID("1111111111111111"), "", "api", 0, 10, nil)
	// Header names a different trace id: must be a hard rejection, no state.
	hdr := "00-ffffffffffffffffffffffffffffffff-2222222222222222-01"
	r := p.Ingest(model.Report{ContextHeader: hdr, Span: s})
	if r.Code != pipeline.CodeContextConflict {
		t.Fatalf("expected conflict, got %s/%s", r.Code, r.Reason)
	}
	if p.TraceView(tid) != nil || len(p.Aggregates().Traces) != 0 {
		t.Fatal("conflicting report must not create state")
	}
}

func TestContextDegradationRecorded(t *testing.T) {
	p, err := pipeline.New(baseConfig(t, "", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tid := testTraceID('4')
	s := mkSpan(tid, testSpanID("1111111111111111"), "", "api", 0, 10, nil)
	// v01 header with trailing vendor field -> downgrade, still ingested.
	hdr := "01-" + string(tid) + "-1111111111111111-01-vendorextra"
	r := p.Ingest(model.Report{ContextHeader: hdr, Span: s})
	if !r.Accepted || p.Metrics().DegradedContexts != 1 {
		t.Fatalf("downgrade must be accepted and counted: %+v m=%+v", r, p.Metrics())
	}
}

func TestSamplingStickyAndWholeTrace(t *testing.T) {
	// Rule drops traces tagged canary; rate would otherwise keep everything.
	p, err := pipeline.New(baseConfig(t, "", 1, sampling.Rule{Key: "canary", Value: "1", Decision: sampling.Drop}))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	tid := testTraceID('5')
	root := mkSpan(tid, testSpanID("1111111111111111"), "", "api", 0, 50, map[string]string{"canary": "1"})
	if r := p.Ingest(model.Report{Span: root}); !r.Accepted || r.Decision != sampling.Drop {
		t.Fatalf("first span must pin drop: %+v", r)
	}
	// Later span from ANOTHER service without the attribute must stay dropped
	// and must not appear in aggregates.
	late := mkSpan(tid, testSpanID("2222222222222222"), testSpanID("1111111111111111"), "billing", 10, 40, nil)
	r := p.Ingest(model.Report{Span: late})
	if r.Decision != sampling.Drop || p.TraceView(tid) != nil || len(p.Aggregates().Services) != 0 {
		t.Fatalf("late span must follow pinned drop and aggregate nothing, got %+v", r)
	}
	m := p.Metrics()
	if m.DroppedTraces != 1 {
		t.Fatalf("exactly one dropped trace, got %d", m.DroppedTraces)
	}
}

func TestSamplingConsistentAcrossServicesByFirstSpan(t *testing.T) {
	// The hash verdict for a trace id is identical regardless of which
	// service's span arrives first, because it only depends on trace id.
	tid := testTraceID('6')
	first := func(svc string) sampling.Decision {
		p, _ := pipeline.New(baseConfig(t, "", 0.37))
		defer p.Close()
		s := mkSpan(tid, testSpanID("1111111111111111"), "", svc, 0, 10, nil)
		return p.Ingest(model.Report{Span: s}).Decision
	}
	d1, d2 := first("api"), first("billing")
	if d1 != d2 {
		t.Fatalf("same trace sampled differently by first service: %v vs %v", d1, d2)
	}
}

func TestCapacityBoundaryRejectedAndObservable(t *testing.T) {
	cfg := baseConfig(t, "", 1)
	cfg.Store.MaxTraces = 1
	cfg.Store.EvictOldest = false
	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	t1 := testTraceID('7')
	t2 := testTraceID('8')
	if r := p.Ingest(model.Report{Span: mkSpan(t1, testSpanID("1111111111111111"), "", "a", 0, 1, nil)}); !r.Accepted {
		t.Fatal(r)
	}
	r := p.Ingest(model.Report{Span: mkSpan(t2, testSpanID("1111111111111111"), "", "a", 0, 1, nil)})
	if r.Code != pipeline.CodeCapTraces {
		t.Fatalf("expected capacity rejection, got %s", r.Code)
	}
	if p.Metrics().RejectedCapacity != 1 {
		t.Fatal("rejection must be observable via metrics")
	}
}

func TestCapacityEvictOldestRetractsAggregates(t *testing.T) {
	cfg := baseConfig(t, "", 1)
	cfg.Store.MaxTraces = 1
	cfg.Store.EvictOldest = true
	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	t1, t2 := testTraceID('9'), testTraceID('a')
	p.Ingest(model.Report{Span: mkSpan(t1, testSpanID("1111111111111111"), "", "a", 0, 10, nil)})
	p.Ingest(model.Report{Span: mkSpan(t2, testSpanID("2222222222222222"), "", "a", 0, 5, nil)})
	snap := p.Aggregates()
	if len(snap.Traces) != 1 || string(snap.Traces[0].TraceID) != string(t2) {
		t.Fatalf("oldest trace aggregates must be retracted, got %+v", snap.Traces)
	}
	if p.Metrics().EvictedTraces != 1 {
		t.Fatal("eviction must be counted")
	}
}

func TestRestartRecoveryNoLossNoDuplication(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := baseConfig(t, dir, 1)

	tid := testTraceID('b')
	root, child := testSpanID("1111111111111111"), testSpanID("2222222222222222")

	p1, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Out of order + duplicate before "crash".
	p1.Ingest(model.Report{Span: mkSpan(tid, child, root, "db", 20, 60, nil)})
	p1.Ingest(model.Report{Span: mkSpan(tid, child, root, "db", 20, 60, nil)})
	p1.Ingest(model.Report{Span: mkSpan(tid, root, "", "api", 0, 100, nil)})
	before := p1.Aggregates()
	if err := p1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: recovered state must match exactly.
	p2, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	after := p2.Aggregates()
	if len(after.Traces) != 1 || after.Traces[0].SpanCount != 2 {
		t.Fatalf("recovery lost or duplicated spans: %+v", after.Traces)
	}
	if after.Traces[0].TotalLatency != before.Traces[0].TotalLatency {
		t.Fatal("recovered aggregates must match pre-crash aggregates")
	}
	v := p2.TraceView(tid)
	if v == nil || v.State != store.StateComplete || !v.DecisionSet {
		t.Fatalf("recovered trace incomplete or decision lost: %+v", v)
	}
	// A duplicate re-report after restart must still count once.
	r := p2.Ingest(model.Report{Span: mkSpan(tid, child, root, "db", 20, 60, nil)})
	if r.Code != pipeline.CodeDuplicate {
		t.Fatalf("post-restart duplicate not idempotent: %s", r.Code)
	}
	if p2.Aggregates().Traces[0].SpanCount != 2 {
		t.Fatal("post-restart duplicate double counted")
	}
}

func TestRestartRecoveryDroppedTombstoneSurvives(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := baseConfig(t, dir, 0) // drop everything
	p1, _ := pipeline.New(cfg)
	tid := testTraceID('c')
	r := p1.Ingest(model.Report{Span: mkSpan(tid, testSpanID("1111111111111111"), "", "a", 0, 1, nil)})
	if r.Decision != sampling.Drop {
		t.Fatal(r)
	}
	p1.Close()

	p2, _ := pipeline.New(cfg)
	defer p2.Close()
	// Change rate to keep-all: the pre-crash verdict must still dominate.
	cfg2 := cfg
	cfg2.Sampling = sampling.Config{Rate: 1}
	p2, _ = pipeline.New(cfg2)
	defer p2.Close()
	r = p2.Ingest(model.Report{Span: mkSpan(tid, testSpanID("2222222222222222"), testSpanID("1111111111111111"), "a", 1, 2, nil)})
	if r.Decision != sampling.Drop || len(p2.Aggregates().Traces) != 0 {
		t.Fatalf("pinned drop must survive restart and config change: %+v", r)
	}
}

func TestConcurrentWritersHaveConsistentSnapshots(t *testing.T) {
	cfg := baseConfig(t, "", 0.6)
	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				tid := testTraceID(byte('d' + w%16))
				sid := testSpanID(hex16(byte(w), byte(i)))
				root := testSpanID("r111111111111111")
				pid := model.SpanID("")
				if i%2 == 1 {
					pid = root
				}
				p.Ingest(model.Report{Span: mkSpan(tid, sid, pid, "svc", int64(i), int64(i)+1, nil)})
				_ = p.Aggregates()
				_ = p.TraceViews()
			}
		}(w)
	}
	wg.Wait()
	// Final invariants: aggregate counts never exceed stored kept spans.
	snap := p.Aggregates()
	total := 0
	for _, s := range snap.Services {
		total += s.SpanCount
	}
	if int64(total) > p.Metrics().Accepted {
		t.Fatalf("aggregate count %d exceeds accepted %d", total, p.Metrics().Accepted)
	}
}

func hex16(a, b byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 16)
	for i := range out {
		out[i] = '0'
	}
	out[0] = h[a&0xf]
	out[1] = h[(a>>4)&0xf]
	out[2] = h[b&0xf]
	out[3] = h[(b>>4)&0xf]
	return string(out)
}

func TestDroppedTombstoneCountsAgainstMaxTraces(t *testing.T) {
	cfg := baseConfig(t, "", 0) // all traces dropped
	cfg.Store.MaxTraces = 1
	cfg.Store.EvictOldest = false
	p, err := pipeline.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r1 := p.Ingest(model.Report{Span: mkSpan(testTraceID('1'), testSpanID("1111111111111111"), "", "a", 0, 1, nil)})
	if r1.Decision != sampling.Drop || !r1.Accepted {
		t.Fatal(r1)
	}
	r2 := p.Ingest(model.Report{Span: mkSpan(testTraceID('2'), testSpanID("2222222222222222"), "", "a", 0, 1, nil)})
	if r2.Code != pipeline.CodeCapTraces {
		t.Fatalf("dropped tombstone must occupy a MaxTraces slot, got %s/%s", r2.Code, r2.Reason)
	}
}

func TestDroppedTombstoneEvictsOldestAndCanBeRecreated(t *testing.T) {
	cfg := baseConfig(t, "", 0)
	cfg.Store.MaxTraces = 1
	cfg.Store.EvictOldest = true
	p, _ := pipeline.New(cfg)
	defer p.Close()
	t1, t2 := testTraceID('1'), testTraceID('2')
	p.Ingest(model.Report{Span: mkSpan(t1, testSpanID("1111111111111111"), "", "a", 0, 1, nil)})
	r := p.Ingest(model.Report{Span: mkSpan(t2, testSpanID("2222222222222222"), "", "a", 0, 1, nil)})
	if !r.Accepted || p.Metrics().EvictedTraces != 1 {
		t.Fatalf("oldest dropped tombstone must be evicted: %+v", r)
	}
	// Re-arrival for the evicted trace: a fresh drop verdict is recomputed,
	// no stale tombstone resurrects aggregate data.
	r = p.Ingest(model.Report{Span: mkSpan(t1, testSpanID("3333333333333333"), "", "a", 0, 1, nil)})
	if !r.Accepted || r.Decision != sampling.Drop || len(p.Aggregates().Traces) != 0 {
		t.Fatalf("evicted dropped trace must be safely re-creatable: %+v", r)
	}
}

func TestConflictingSamplingHintsAcrossServicesResolveSticky(t *testing.T) {
	// rate 1 keeps; service A sends hint keep=0, service B hint keep=1. The
	// pipeline verdict is deterministic and identical regardless of order.
	tid := testTraceID('3')
	mk := func(sid, pid, svc string, hintFlags string) model.Report {
		s := mkSpan(tid, testSpanID(sid), model.SpanID(pid), svc, 0, 10, nil)
		hdr := "00-" + string(tid) + "-" + sid + "-" + hintFlags
		if pid == "" {
			// root reports its own id as parent-id field
			hdr = "00-" + string(tid) + "-" + sid + "-" + hintFlags
			s.ParentSpanID = ""
		}
		return model.Report{ContextHeader: hdr, Span: s}
	}
	run := func(first string) (sampling.Decision, string) {
		p, _ := pipeline.New(baseConfig(t, "", 1))
		defer p.Close()
		root := mk("1111111111111111", "", "api", first)
		// root header parent-id equal to its own span id => treated as body
		// parent authority (empty), header parent equal to span id is ignored.
		child := model.Report{
			ContextHeader: "00-" + string(tid) + "-1111111111111111-01",
			Span:          mkSpan(tid, testSpanID("2222222222222222"), testSpanID("1111111111111111"), "billing", 1, 9, nil),
		}
		p.Ingest(root)
		r := p.Ingest(child)
		return r.Decision, r.Code
	}
	d1, _ := run("00")
	d2, _ := run("01")
	if d1 != sampling.Keep || d2 != sampling.Keep {
		t.Fatalf("hints must not change the rate verdict: %v vs %v", d1, d2)
	}
}

func TestInvalidBodiesRejectedAndNotCounted(t *testing.T) {
	p, _ := pipeline.New(baseConfig(t, "", 1))
	defer p.Close()
	bad := model.Span{
		TraceID: "short", SpanID: testSpanID("1111111111111111"),
		StartUnixNano: 0, EndUnixNano: 1, Attributes: nil,
	}
	r := p.Ingest(model.Report{Span: bad})
	if r.Code != pipeline.CodeInvalidBody {
		t.Fatalf("invalid body must be rejected, got %s", r.Code)
	}
	if len(p.Aggregates().Traces) != 0 || p.Metrics().RejectedInvalid != 1 {
		t.Fatal("invalid body must neither aggregate nor be silently dropped")
	}
}
