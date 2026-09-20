// Package aggregate computes trace- and service-level latency/error rollups.
//
// Consistency guarantees:
//
//   - every span is incorporated at most once: the key is (trace id, span id)
//     and the payload is verified identical on repeat application, so duplicate
//     or retried reports never double count;
//   - aggregates never depend on arrival order: counts and sums combine
//     commutatively and the extrema are derived from the retained span set;
//   - late spans update the current trace/service rows only; previously taken
//     snapshots were point-in-time copies and are not mutated;
//   - RemoveSpan exactly retracts a span (used by TTL/overflow eviction), so
//     removed data cannot keep being counted;
//   - Snapshot returns deep-copied, sorted rows, hence a consistent external
//     view safe to publish while concurrent writes continue.
package aggregate

import (
	"sort"
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

// ServiceStat is the per-service rollup over individual span latencies.
type ServiceStat struct {
	Service      string
	SpanCount    int
	ErrorCount   int
	MinLatency   int64
	MaxLatency   int64
	TotalLatency int64
}

// AvgLatencyMillis is kept out of the struct to avoid divergent rounding;
// consumers compute TotalLatency/SpanCount themselves.

// TraceStat is the per-trace rollup. Duration is the wall-clock envelope
// max(End)-min(Start), not the sum of span latencies.
type TraceStat struct {
	TraceID      model.TraceID
	SpanCount    int
	ErrorCount   int
	MinStart     int64
	MaxEnd       int64
	Duration     int64
	TotalLatency int64
}

type spanFact struct {
	service string
	latency int64
	start   int64
	end     int64
	errored bool
}

type traceRow struct {
	spans    map[model.SpanID]spanFact
	count    int
	errors   int
	minStart int64
	maxEnd   int64
	totalLat int64
}

type serviceRow struct {
	spans   map[string]int64 // key: traceID/spanID -> latency
	errKeys map[string]bool
	count   int
	errors  int
	min     int64
	max     int64
	total   int64
}

// Aggregator maintains rollups keyed by span identity.
type Aggregator struct {
	mu       sync.Mutex
	traces   map[model.TraceID]*traceRow
	services map[string]*serviceRow
}

// New constructs an empty Aggregator.
func New() *Aggregator {
	return &Aggregator{traces: map[model.TraceID]*traceRow{}, services: map[string]*serviceRow{}}
}

// ApplySpan idempotently incorporates one span. It reports true when the span
// actually changed aggregate state; exact duplicates return false.
func (a *Aggregator) ApplySpan(sp model.Span) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	tr := a.traces[sp.TraceID]
	if tr == nil {
		tr = &traceRow{spans: map[model.SpanID]spanFact{}, minStart: sp.StartUnixNano}
		a.traces[sp.TraceID] = tr
	}
	if _, exists := tr.spans[sp.SpanID]; exists {
		return false // duplicate of an already counted span
	}

	fact := spanFact{service: sp.Service, latency: sp.Latency(), start: sp.StartUnixNano, end: sp.EndUnixNano, errored: sp.Status == model.StatusError}
	tr.spans[sp.SpanID] = fact
	tr.count++
	if fact.errored {
		tr.errors++
	}
	if fact.start < tr.minStart || tr.count == 1 {
		tr.minStart = fact.start
	}
	if fact.end > tr.maxEnd {
		tr.maxEnd = fact.end
	}
	tr.totalLat += fact.latency

	a.applyServiceLocked(sp.TraceID, sp.SpanID, sp.Service, fact)
	return true
}

func (a *Aggregator) applyServiceLocked(tid model.TraceID, sid model.SpanID, service string, fact spanFact) {
	key := string(tid) + "/" + string(sid)
	sr := a.services[service]
	if sr == nil {
		sr = &serviceRow{spans: map[string]int64{}, errKeys: map[string]bool{}, min: fact.latency}
		a.services[service] = sr
	}
	sr.spans[key] = fact.latency
	sr.count++
	sr.total += fact.latency
	if fact.errored {
		sr.errors++
		sr.errKeys[key] = true
	}
	if len(sr.spans) == 1 || fact.latency < sr.min {
		sr.min = fact.latency
	}
	if fact.latency > sr.max {
		sr.max = fact.latency
	}
}

// RemoveSpan exactly retracts one previously applied span. Missing spans are a
// no-op (idempotent), so retracting a duplicate report cannot corrupt rows.
func (a *Aggregator) RemoveSpan(sp model.Span) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tr := a.traces[sp.TraceID]
	if tr == nil {
		return
	}
	fact, ok := tr.spans[sp.SpanID]
	if !ok {
		return
	}
	delete(tr.spans, sp.SpanID)
	tr.count--
	if fact.errored {
		tr.errors--
	}
	tr.totalLat -= fact.latency
	if len(tr.spans) == 0 {
		delete(a.traces, sp.TraceID)
	} else {
		// Extrema must be recomputed from the retained set: retraction is
		// order-independent.
		tr.minStart, tr.maxEnd = 1<<63-1, int64(0)
		for _, f := range tr.spans {
			if f.start < tr.minStart {
				tr.minStart = f.start
			}
			if f.end > tr.maxEnd {
				tr.maxEnd = f.end
			}
		}
	}

	a.removeServiceLocked(sp.TraceID, sp.SpanID, sp.Service, fact)
}

func (a *Aggregator) removeServiceLocked(tid model.TraceID, sid model.SpanID, service string, fact spanFact) {
	sr := a.services[service]
	if sr == nil {
		return
	}
	key := string(tid) + "/" + string(sid)
	if _, ok := sr.spans[key]; !ok {
		return
	}
	delete(sr.spans, key)
	delete(sr.errKeys, key)
	sr.count--
	sr.total -= fact.latency
	if fact.errored {
		sr.errors--
	}
	if sr.count == 0 {
		delete(a.services, service)
		return
	}
	sr.min, sr.max = 1<<63-1, int64(0)
	for _, lat := range sr.spans {
		if lat < sr.min {
			sr.min = lat
		}
		if lat > sr.max {
			sr.max = lat
		}
	}
}

// Snapshot is a consistent, detached copy of all rollup rows.
type Snapshot struct {
	Traces   []TraceStat
	Services []ServiceStat
}

// Snapshot returns all rows sorted by identity: publishing this never exposes
// internal maps or mid-update states.
func (a *Aggregator) Snapshot() Snapshot {
	a.mu.Lock()
	defer a.mu.Unlock()

	snap := Snapshot{
		Traces:   make([]TraceStat, 0, len(a.traces)),
		Services: make([]ServiceStat, 0, len(a.services)),
	}
	for id, tr := range a.traces {
		snap.Traces = append(snap.Traces, TraceStat{
			TraceID: id, SpanCount: tr.count, ErrorCount: tr.errors,
			MinStart: tr.minStart, MaxEnd: tr.maxEnd, Duration: tr.maxEnd - tr.minStart,
			TotalLatency: tr.totalLat,
		})
	}
	for name, sr := range a.services {
		snap.Services = append(snap.Services, ServiceStat{
			Service: name, SpanCount: sr.count, ErrorCount: sr.errors,
			MinLatency: sr.min, MaxLatency: sr.max, TotalLatency: sr.total,
		})
	}
	sort.Slice(snap.Traces, func(i, j int) bool { return snap.Traces[i].TraceID < snap.Traces[j].TraceID })
	sort.Slice(snap.Services, func(i, j int) bool { return snap.Services[i].Service < snap.Services[j].Service })
	return snap
}
