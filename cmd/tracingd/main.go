// Command tracingd is a minimal demo of the local tracing pipeline: it feeds a
// handful of traces out of order, with duplicates, context degradations and
// capacity pressure, then prints the consistent aggregate snapshot.
package main

import (
	"fmt"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/pipeline"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
)

func span(tid model.TraceID, sid, pid model.SpanID, svc, name string, start, dur int64, errSpan bool, attrs map[string]string) model.Span {
	st := model.StatusOK
	if errSpan {
		st = model.StatusError
	}
	return model.Span{
		TraceID: tid, SpanID: sid, ParentSpanID: pid, Service: svc, Name: name,
		Kind: model.KindServer, StartUnixNano: start, EndUnixNano: start + dur,
		Attributes: attrs, Status: st,
	}
}

func main() {
	cfg := pipeline.Config{
		DataDir: "", // in-memory for the demo
		Store: store.Config{
			MaxTraces: 8, MaxSpansPerTrace: 64, MaxTotalSpans: 1024,
			RetentionTTL: 30 * time.Minute, EvictOldest: true,
		},
		Sampling: sampling.Config{
			Rate: 1.0,
			Rules: []sampling.Rule{
				{Key: "force.drop", Value: "1", Decision: sampling.Drop},
			},
		},
	}
	p, err := pipeline.New(cfg)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	tid := model.TraceID("0102030405060708090a0b0c0d0e0f10")
	root := model.SpanID("1111111111111111")
	child := model.SpanID("2222222222222222")
	tail := model.SpanID("3333333333333333")
	base := time.Now().UnixNano()

	// Arrive out of order: tail -> child -> root.
	for _, s := range []model.Span{
		span(tid, tail, child, "billing", "charge", base+200, 30, true, nil),
		span(tid, child, root, "gateway", "proxy", base+100, 150, false, nil),
		span(tid, root, "", "api", "GET /x", base, 400, false, nil),
	} {
		res := p.Ingest(model.Report{Span: s})
		fmt.Printf("ingest %s -> %s (%s) state=%s\n", s.Name, res.Code, res.Reason, res.State)
	}

	// Duplicate report must not double count.
	res := p.Ingest(model.Report{Span: span(tid, child, root, "gateway", "proxy", base+100, 150, false, nil)})
	fmt.Printf("duplicate -> %s\n", res.Code)

	// Dropped trace tombstone.
	tid2 := model.TraceID("a102030405060708090a0b0c0d0e0f10")
	r2 := model.SpanID("bbbbbbbbbbbbbbbb")
	res = p.Ingest(model.Report{Span: span(tid2, r2, "", "api", "drop-me", base, 10, false, map[string]string{"force.drop": "1"})})
	fmt.Printf("forced drop -> %s decision=%s\n", res.Code, res.Decision)
	res = p.Ingest(model.Report{Span: span(tid2, model.SpanID("cccccccccccccccc"), r2, "api", "late-span", base+5, 5, false, map[string]string{"force.drop": "1"})})
	fmt.Printf("late span on dropped trace -> decision=%s\n", res.Decision)

	snap := p.Aggregates()
	fmt.Println("\nservice aggregates:")
	for _, s := range snap.Services {
		fmt.Printf("  %-8s count=%d errors=%d min=%d max=%d total=%d\n",
			s.Service, s.SpanCount, s.ErrorCount, s.MinLatency, s.MaxLatency, s.TotalLatency)
	}
	m := p.Metrics()
	fmt.Printf("\nmetrics: accepted=%d duplicates=%d kept=%d dropped=%d orphans-resolved=%d degraded=%d\n",
		m.Accepted, m.Duplicates, m.KeptTraces, m.DroppedTraces, m.ResolvedOrphans, m.DegradedContexts)
}
