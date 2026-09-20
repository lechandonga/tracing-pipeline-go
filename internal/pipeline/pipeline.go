// Package pipeline wires ingestion, context resolution, sticky sampling,
// bounded storage, idempotent aggregation and durable recovery into one
// concurrency-safe entry point.
//
// Ordering rule for one report:
//
//  1. validate body identifiers/timestamps -> reject invalid-* if malformed;
//  2. resolve the trace context -> hard conflict rejects, degradations count;
//  3. look up the trace's pinned sampling decision; if absent compute it from
//     THIS span, so every service reaches the same verdict regardless of
//     arrival order;
//  4. dropped traces are retained only as tombstones (verdict + span count);
//     kept traces go through capacity-checked insertion in the store;
//  5. a kept span is persisted to the WAL (fsync) before it becomes
//     aggregate-visible; on persistence failure the in-memory span is rolled
//     back, so no unconfirmed data can be observed or snapshotted;
//  6. aggregates are applied idempotently and capacity/TTL evictions retract
//     aggregate contributions exactly once.
//
// All steps share one lock and the durable append happens while holding it,
// giving linearizable ingestions and consistent external snapshots. Recovery
// completes fully inside New before the pipeline is returned, so intermediate
// recovery state is never visible.
package pipeline

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/aggregate"
	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
	"github.com/lechandonga/tracing-pipeline-go/internal/tracecontext"
	"github.com/lechandonga/tracing-pipeline-go/internal/wal"
)

// Result codes (determination basis for every ingestion attempt).
const (
	CodeAccepted        = "accepted"
	CodeDuplicate       = "duplicate-span"
	CodeInvalidBody     = "invalid-body"
	CodeContextConflict = "context-conflict"
	CodeSpanCollision   = "span-id-collision"
	CodeCapTraces       = "capacity-max-traces"
	CodeCapPerTrace     = "capacity-max-spans-per-trace"
	CodeCapTotal        = "capacity-max-total-spans"
	CodePersistFailed   = "persist-failed"
)

// Config configures the pipeline.
type Config struct {
	DataDir        string // empty disables durability (in-memory mode)
	Store          store.Config
	Sampling       sampling.Config
	SnapshotEveryN int64 // compact WAL after this many confirmed changes; 0 disables
	TTLInterval    time.Duration
}

// Metrics is the observable counter set.
type Metrics struct {
	Received          int64
	Accepted          int64
	Duplicates        int64
	RejectedInvalid   int64
	RejectedConflict  int64
	RejectedCollision int64
	RejectedCapacity  int64
	PersistFailures   int64
	DegradedContexts  int64
	OrphanSpans       int64
	ResolvedOrphans   int64
	KeptTraces        int64
	DroppedTraces     int64
	EvictedTraces     int64
	ExpiredTraces     int64
}

// Pipeline is the entry point of the local tracing pipeline.
type Pipeline struct {
	mu      sync.Mutex
	cfg     Config
	st      *store.Store
	agg     *aggregate.Aggregator
	sampler *sampling.Sampler
	w       *wal.Log

	// dropped holds traces pinned as Drop: only the distinct span-id set
	// plus timestamps are retained (no payloads, no aggregates). Tombstones
	// count against MaxTraces and expire under the same TTL as kept traces.
	dropped map[model.TraceID]*droppedInfo

	changes       int64
	keepDecisions map[model.TraceID]string // recovery-only staging
	metrics       Metrics
	stopTTL       func()
}

// droppedInfo is the retained tombstone for a Drop-verdict trace.
type droppedInfo struct {
	spans     map[model.SpanID]bool
	firstNano int64
	lastNano  int64
}

// Result is the outcome of one ingestion attempt.
type Result struct {
	Accepted bool
	Code     string
	Reason   string
	TraceID  model.TraceID
	Decision sampling.Decision
	State    store.TraceState
}

// New creates a pipeline and atomically restores durable state before
// returning: no snapshot or query can observe partially recovered state.
func New(cfg Config) (*Pipeline, error) {
	p := &Pipeline{
		cfg:     cfg,
		st:      store.New(cfg.Store),
		agg:     aggregate.New(),
		sampler: sampling.New(cfg.Sampling),
		dropped: map[model.TraceID]*droppedInfo{},
	}
	if cfg.DataDir != "" {
		wl, err := wal.Open(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		p.w = wl
		if err := p.recover(); err != nil {
			return nil, err
		}
	}
	if cfg.Store.RetentionTTL > 0 && cfg.TTLInterval > 0 {
		p.startTTLEviction()
	}
	return p, nil
}

// Ingest handles one inbound report and returns its classification with a
// machine-readable reason. Exactly one outcome happens per call and it is
// counted in Metrics.
func (p *Pipeline) Ingest(r model.Report) Result {
	// Step 1: body validation (outside the lock; pure function).
	sp := r.Span
	if err := model.ValidateSpan(sp); err != nil {
		return p.record(Result{Code: CodeInvalidBody, Reason: err.Error(), TraceID: sp.TraceID}, &p.metrics.RejectedInvalid)
	}
	sp = sp.Normalize()

	// Step 2: context resolution.
	parsed, err := tracecontext.Parse(r.ContextHeader, sp)
	if err != nil {
		return p.record(Result{Code: CodeContextConflict, Reason: err.Error(), TraceID: sp.TraceID}, &p.metrics.RejectedConflict)
	}
	sp.ParentSpanID = parsed.ParentSpanID

	p.mu.Lock()
	defer p.mu.Unlock()

	nowNano := time.Now().UnixNano()
	p.metrics.Received++
	if parsed.Degraded {
		p.metrics.DegradedContexts++
	}

	// Step 3: pinned verdict lookup / fresh decision.
	decision, reason, pinned := p.decisionForLocked(sp.TraceID)
	if !pinned {
		decision, reason = p.sampler.Decide(sp.TraceID, sp, parsed.HintPresent, parsed.Sampled)
	}

	// Step 4a: dropped trace -> tombstone only, bounded by the same limits.
	if decision == sampling.Drop {
		info, existed := p.dropped[sp.TraceID]
		if !existed {
			// New dropped tombstone needs a MaxTraces slot (kept + dropped).
			if p.cfg.Store.MaxTraces > 0 && p.st.TraceCount()+len(p.dropped) >= p.cfg.Store.MaxTraces {
				if !p.cfg.Store.EvictOldest {
					return p.recordLocked(Result{Code: CodeCapTraces, Reason: store.RejectTraceCapacity, TraceID: sp.TraceID, Decision: decision}, &p.metrics.RejectedCapacity)
				}
				if ev := p.evictOldestLocked(nowNano); !ev {
					return p.recordLocked(Result{Code: CodeCapTraces, Reason: store.RejectTraceCapacity, TraceID: sp.TraceID, Decision: decision}, &p.metrics.RejectedCapacity)
				}
			}
			info = &droppedInfo{spans: map[model.SpanID]bool{}, firstNano: nowNano, lastNano: nowNano}
			p.dropped[sp.TraceID] = info
			p.metrics.DroppedTraces++
			if p.w != nil {
				if perr := p.appendDecisionLocked(sp.TraceID, decision, reason); perr != nil {
					delete(p.dropped, sp.TraceID)
					p.metrics.DroppedTraces--
					p.metrics.PersistFailures++
					return Result{Code: CodePersistFailed, Reason: perr.Error(), TraceID: sp.TraceID, Decision: decision}
				}
			}
		}
		if info.spans[sp.SpanID] {
			p.metrics.Duplicates++
			return Result{Accepted: true, Code: CodeDuplicate, Reason: store.StatusDuplicate,
				TraceID: sp.TraceID, Decision: decision}
		}
		info.spans[sp.SpanID] = true
		info.lastNano = nowNano
		if p.w != nil {
			// Persist dropped-span identity too, so restart recovery keeps
			// the exact distinct-span set and remains idempotent.
			if perr := p.appendSpanLocked(sp, parsed.ParentInferred, nowNano); perr != nil {
				delete(info.spans, sp.SpanID)
				p.metrics.PersistFailures++
				return Result{Code: CodePersistFailed, Reason: perr.Error(), TraceID: sp.TraceID, Decision: decision}
			}
		}
		p.metrics.Accepted++
		return Result{Accepted: true, Code: CodeAccepted,
			Reason:   "trace-dropped:" + p.reasonWithContext(reason, parsed),
			TraceID:  sp.TraceID,
			Decision: decision}
	}

	// Step 4b: capacity-checked insertion for kept traces.
	ar := p.st.Add(sp, parsed.ParentInferred)
	if !ar.Accepted {
		counter := &p.metrics.RejectedCapacity
		code := rejectCode(ar.Status)
		if ar.Status == store.RejectSpanCollision {
			counter = &p.metrics.RejectedCollision
			code = CodeSpanCollision
		}
		return p.recordLocked(Result{Code: code, Reason: ar.Status, TraceID: sp.TraceID, Decision: decision, State: ar.State}, counter)
	}
	switch ar.Status {
	case store.StatusDuplicate:
		p.metrics.Duplicates++
		return Result{Accepted: true, Code: CodeDuplicate, Reason: store.StatusDuplicate,
			TraceID: sp.TraceID, Decision: decision, State: ar.State}
	case store.StatusResolvedOrphan:
		p.metrics.ResolvedOrphans++
	}

	// Step 5: durable confirmation BEFORE aggregate visibility. The verdict
	// is persisted before the span so that, even if a crash (or snapshot
	// compaction) interleaves, replay can never see a span whose trace
	// decision is missing.
	if !pinned && p.w != nil {
		if perr := p.appendDecisionLocked(sp.TraceID, decision, reason); perr != nil {
			p.st.RemoveSpan(sp.TraceID, sp.SpanID)
			p.metrics.PersistFailures++
			return Result{Code: CodePersistFailed, Reason: perr.Error(), TraceID: sp.TraceID, Decision: decision}
		}
	}
	if p.w != nil {
		if perr := p.appendSpanLocked(sp, parsed.ParentInferred, nowNano); perr != nil {
			p.st.RemoveSpan(sp.TraceID, sp.SpanID)
			p.metrics.PersistFailures++
			return Result{Code: CodePersistFailed, Reason: perr.Error(), TraceID: sp.TraceID, Decision: decision}
		}
	}
	if !pinned && p.st.SetDecision(sp.TraceID, decision, reason) {
		p.metrics.KeptTraces++
	}

	// Step 6: idempotent aggregate; retract evicted traces exactly once.
	p.agg.ApplySpan(sp)
	for _, ev := range ar.Evicted {
		p.applyEvictionLocked(ev)
	}
	p.metrics.Accepted++
	p.changes++
	p.maybeCompactLocked()

	return Result{Accepted: true, Code: CodeAccepted,
		Reason:   p.reasonWithContext(reason, parsed),
		TraceID:  sp.TraceID,
		Decision: decision,
		State:    ar.State}
}

// decisionForLocked returns the already-pinned verdict for a trace, if any.
func (p *Pipeline) decisionForLocked(id model.TraceID) (sampling.Decision, string, bool) {
	if d, reason, ok := p.st.Decision(id); ok {
		return d, reason, true
	}
	if _, ok := p.dropped[id]; ok {
		return sampling.Drop, "pinned-drop-tombstone", true
	}
	return sampling.Drop, "", false
}

func (p *Pipeline) reasonWithContext(reason string, pc tracecontext.Parsed) string {
	if pc.Degraded {
		return reason + ";context=" + pc.Reason
	}
	return reason
}

func rejectCode(status string) string {
	switch status {
	case store.RejectTraceCapacity:
		return CodeCapTraces
	case store.RejectPerTraceCap:
		return CodeCapPerTrace
	case store.RejectTotalCapacity:
		return CodeCapTotal
	case store.RejectSpanCollision:
		return CodeSpanCollision
	default:
		return status
	}
}

func (p *Pipeline) record(res Result, counter *int64) Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	*counter = *counter + 1
	return res
}

func (p *Pipeline) recordLocked(res Result, counter *int64) Result {
	*counter = *counter + 1
	return res
}

func (p *Pipeline) applyEvictionLocked(ev store.EvictedTrace) {
	for _, ss := range ev.Trace.StoredSpans() {
		p.agg.RemoveSpan(ss.Span)
	}
	p.metrics.EvictedTraces++
	p.changes++
	if p.w != nil {
		_ = p.appendEvictLocked(ev.Trace.TraceID, ev.Reason)
	}
}

// --- persistence helpers (callers hold p.mu) ---

type walSpan struct {
	TraceID        string     `json:"trace_id"`
	Span           model.Span `json:"span"`
	ParentInferred bool       `json:"parent_inferred"`
	ArrivalNano    int64      `json:"arrival_nano"`
}

func (p *Pipeline) appendSpanLocked(sp model.Span, inferred bool, arrivalNano int64) error {
	b, err := json.Marshal(walSpan{TraceID: string(sp.TraceID), Span: sp, ParentInferred: inferred, ArrivalNano: arrivalNano})
	if err != nil {
		return err
	}
	return p.w.Append(wal.TypeSpan, b)
}

func (p *Pipeline) appendDecisionLocked(id model.TraceID, d sampling.Decision, reason string) error {
	b, err := json.Marshal(wal.DecisionRecord{TraceID: string(id), Decision: d.String(), Reason: reason})
	if err != nil {
		return err
	}
	return p.w.Append(wal.TypeDecision, b)
}

func (p *Pipeline) appendEvictLocked(id model.TraceID, reason string) error {
	b, err := json.Marshal(wal.EvictRecord{TraceID: string(id), Reason: reason})
	if err != nil {
		return err
	}
	return p.w.Append(wal.TypeEvict, b)
}

func (p *Pipeline) maybeCompactLocked() {
	if p.cfg.SnapshotEveryN <= 0 || p.w == nil || p.changes < p.cfg.SnapshotEveryN {
		return
	}
	data, err := p.buildSnapshotLocked()
	if err != nil {
		return
	}
	if err := p.w.SaveSnapshot(data); err != nil {
		return
	}
	p.changes = 0
}

// SetSampling atomically replaces the sampling configuration. Verdicts
// already pinned on existing traces are unaffected; the new config only
// governs traces decided after the call.
func (p *Pipeline) SetSampling(cfg sampling.Config) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sampler = sampling.New(cfg)
}

// Close flushes durable state and stops background tasks.
func (p *Pipeline) Close() error {
	if p.stopTTL != nil {
		p.stopTTL()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.w != nil {
		return p.w.Close()
	}
	return nil
}
