package pipeline

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/aggregate"
	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
	"github.com/lechandonga/tracing-pipeline-go/internal/store"
	"github.com/lechandonga/tracing-pipeline-go/internal/wal"
)

// snapshotJSON is the full durable state written by SaveSnapshot. It contains
// exactly what recovery needs; aggregate rows are not persisted because they
// are deterministically derivable from retained spans.
type snapshotJSON struct {
	Version int            `json:"version"`
	Traces  []persistTrace `json:"traces"`
	Dropped []persistDrop  `json:"dropped"`
}

type persistTrace struct {
	TraceID      string             `json:"trace_id"`
	Spans        []store.StoredSpan `json:"spans"`
	Decision     string             `json:"decision"`
	Reason       string             `json:"decision_reason"`
	Decided      bool               `json:"decided"`
	FirstArrival int64              `json:"first_arrival_nano"`
	LastUpdate   int64              `json:"last_update_nano"`
}

type persistDrop struct {
	TraceID   string   `json:"trace_id"`
	SpanIDs   []string `json:"span_ids"`
	FirstNano int64    `json:"first_nano"`
	LastNano  int64    `json:"last_nano"`
}

// buildSnapshotLocked serializes current state. Caller holds p.mu.
func (p *Pipeline) buildSnapshotLocked() ([]byte, error) {
	snap := snapshotJSON{Version: 1}
	for _, v := range p.st.Views() {
		tr := p.traceForSnapshot(v.TraceID)
		snap.Traces = append(snap.Traces, tr)
	}
	for id, set := range p.dropped {
		sids := make([]string, 0, len(set.spans))
		for sid := range set.spans {
			sids = append(sids, string(sid))
		}
		sort.Strings(sids)
		snap.Dropped = append(snap.Dropped, persistDrop{
			TraceID: string(id), SpanIDs: sids,
			FirstNano: set.firstNano, LastNano: set.lastNano,
		})
	}
	return json.Marshal(snap)
}

func (p *Pipeline) traceForSnapshot(id model.TraceID) persistTrace {
	v := p.st.Get(id)
	if v == nil {
		return persistTrace{TraceID: string(id)}
	}
	return persistTrace{
		TraceID:      string(id),
		Spans:        p.st.SpansOf(id),
		Decision:     v.Decision.String(),
		Reason:       v.DecisionReason,
		Decided:      v.DecisionSet,
		FirstArrival: v.FirstArrivalNano,
		LastUpdate:   v.LastUpdateNano,
	}
}

// recover loads snapshot then replays the surviving WAL idempotently. The
// pipeline is not returned until this completes, hence no intermediate state
// is ever observable.
func (p *Pipeline) recover() error {
	snap, err := p.w.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		if err := p.loadSnapshotLocked(snap); err != nil {
			return err
		}
	}
	type rec struct {
		t byte
		p []byte
	}
	var records []rec
	collect := func() error {
		return p.w.Replay(func(recType byte, payload []byte) error {
			records = append(records, rec{recType, append([]byte(nil), payload...)})
			return nil
		})
	}
	if err := collect(); err != nil {
		return err
	}
	// Pass 1: decisions and evictions establish trace verdicts and removals.
	p.keepDecisions = map[model.TraceID]string{}
	p.mu.Lock()
	for _, r := range records {
		switch r.t {
		case wal.TypeDecision:
			var d wal.DecisionRecord
			if err := json.Unmarshal(r.p, &d); err != nil {
				p.mu.Unlock()
				return err
			}
			p.applyRecoveredDecision(d)
		case wal.TypeEvict:
			var e wal.EvictRecord
			if err := json.Unmarshal(r.p, &e); err != nil {
				p.mu.Unlock()
				return err
			}
			p.applyRecoveredEvict(e)
		}
	}
	// Pass 2: spans land only into traces already carrying their verdict.
	for _, r := range records {
		if r.t != wal.TypeSpan {
			continue
		}
		var sp walSpan
		if err := json.Unmarshal(r.p, &sp); err != nil {
			p.mu.Unlock()
			return err
		}
		p.applyRecoveredSpan(sp)
	}
	// Pin keep verdicts whose decision record preceded the trace's first
	// span in the log (normal ordering) so SetDecision now finds the trace.
	for id, reason := range p.keepDecisions {
		if p.st.SetDecision(id, sampling.Keep, reason) {
			p.metrics.KeptTraces++
		}
	}
	p.keepDecisions = nil
	p.mu.Unlock()
	return nil
}

func (p *Pipeline) loadSnapshotLocked(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var snap snapshotJSON
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	p.dropped = map[model.TraceID]*droppedInfo{}
	p.st = store.New(p.cfg.Store)
	p.agg = aggregate.New()
	for _, t := range snap.Traces {
		var d sampling.Decision
		if t.Decision == "keep" {
			d = sampling.Keep
		}
		p.st.RestoreTraceData(model.TraceID(t.TraceID), t.Spans, d, t.Reason, t.Decided, t.FirstArrival, t.LastUpdate)
		for _, ss := range t.Spans {
			p.agg.ApplySpan(ss.Span)
		}
		if t.Decided && d == sampling.Keep {
			p.metrics.KeptTraces++
		}
	}
	for _, d := range snap.Dropped {
		set := map[model.SpanID]bool{}
		for _, sid := range d.SpanIDs {
			set[model.SpanID(sid)] = true
		}
		p.dropped[model.TraceID(d.TraceID)] = &droppedInfo{
			spans: set, firstNano: d.FirstNano, lastNano: d.LastNano,
		}
		p.metrics.DroppedTraces++
	}
	return nil
}

func (p *Pipeline) applyRecoveredSpan(rec walSpan) {
	sp := rec.Span
	// Dropped-trace span identities live only in the tombstone set.
	if info, dropped := p.dropped[sp.TraceID]; dropped {
		info.spans[sp.SpanID] = true
		if rec.ArrivalNano > info.lastNano {
			info.lastNano = rec.ArrivalNano
		}
		return
	}
	// Duplicate (snapshot + log, or repeated replay) => store/agg dedupe.
	p.st.Add(sp, rec.ParentInferred)
	p.agg.ApplySpan(sp)
}

func (p *Pipeline) applyRecoveredDecision(rec wal.DecisionRecord) {
	id := model.TraceID(rec.TraceID)
	if rec.Decision == "drop" {
		if _, ok := p.dropped[id]; !ok {
			p.dropped[id] = &droppedInfo{spans: map[model.SpanID]bool{}}
			p.metrics.DroppedTraces++
		}
		return
	}
	if rec.Decision == "keep" {
		// Trace may not exist yet (decision is fsynced before its first
		// span); buffer it for pinning after the span pass.
		if p.keepDecisions != nil {
			p.keepDecisions[id] = rec.Reason
		} else if p.st.SetDecision(id, sampling.Keep, rec.Reason) {
			p.metrics.KeptTraces++
		}
	}
}

func (p *Pipeline) applyRecoveredEvict(rec wal.EvictRecord) {
	id := model.TraceID(rec.TraceID)
	v := p.st.Get(id)
	if v == nil {
		delete(p.dropped, id)
		return
	}
	// Retract aggregates via stored spans, then drop the trace from the store.
	for _, ss := range p.storedSpansFor(id) {
		p.agg.RemoveSpan(ss.Span)
	}
	p.st.EvictTrace(id)
}

// storedSpansFor returns the retained spans of a trace (or nil).
func (p *Pipeline) storedSpansFor(id model.TraceID) []store.StoredSpan {
	// Store does not expose a raw *Trace by id publicly except via eviction;
	// Views gives identity; fetch spans through a dedicated accessor.
	return p.st.SpansOf(id)
}

// evictOldestLocked removes the globally oldest-idle trace across kept traces
// (store) and dropped tombstones. Returns false if nothing could be removed.
func (p *Pipeline) evictOldestLocked(_ int64) bool {
	var (
		oldestKind int // 0 none, 1 kept, 2 dropped
		oldestNano int64
		oldestID   model.TraceID
	)
	for _, v := range p.st.Views() {
		if oldestKind == 0 || v.LastUpdateNano < oldestNano {
			oldestKind, oldestNano, oldestID = 1, v.LastUpdateNano, v.TraceID
		}
	}
	for id, info := range p.dropped {
		if oldestKind == 0 || info.lastNano < oldestNano {
			oldestKind, oldestNano, oldestID = 2, info.lastNano, id
		}
	}
	switch oldestKind {
	case 1:
		spans := p.st.SpansOf(oldestID)
		for _, ss := range spans {
			p.agg.RemoveSpan(ss.Span)
		}
		p.st.EvictTrace(oldestID)
		p.metrics.EvictedTraces++
		p.changes++
		if p.w != nil {
			_ = p.appendEvictLocked(oldestID, store.ReasonEvictedOldest)
		}
		return true
	case 2:
		delete(p.dropped, oldestID)
		p.metrics.EvictedTraces++
		p.changes++
		if p.w != nil {
			_ = p.appendEvictLocked(oldestID, store.ReasonEvictedOldest)
		}
		return true
	default:
		return false
	}
}

func (p *Pipeline) startTTLEviction() {
	ticker := time.NewTicker(p.cfg.TTLInterval)
	done := make(chan struct{})
	p.stopTTL = func() {
		ticker.Stop()
		close(done)
	}
	go func() {
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				p.runTTL(now)
			}
		}
	}()
}

// RunTTL triggers one retention sweep immediately (also useful in tests
// instead of waiting for the background ticker).
func (p *Pipeline) RunTTL(now time.Time) { p.runTTL(now) }

func (p *Pipeline) runTTL(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ev := range p.st.EvictExpired(now) {
		for _, ss := range ev.Trace.StoredSpans() {
			p.agg.RemoveSpan(ss.Span)
		}
		p.metrics.ExpiredTraces++
		p.changes++
		if p.w != nil {
			_ = p.appendEvictLocked(ev.Trace.TraceID, ev.Reason)
		}
	}
	if p.cfg.Store.RetentionTTL > 0 {
		cutoff := now.Add(-p.cfg.Store.RetentionTTL).UnixNano()
		for id, info := range p.dropped {
			if info.lastNano <= cutoff {
				delete(p.dropped, id)
				p.metrics.ExpiredTraces++
				p.changes++
				if p.w != nil {
					_ = p.appendEvictLocked(id, "evicted-ttl")
				}
			}
		}
	}
}

// --- public read API (consistent snapshots) ---

// TraceView returns the reassembly view of one trace, or nil.
func (p *Pipeline) TraceView(id model.TraceID) *store.View {
	return p.st.Get(id)
}

// TraceViews returns a consistent point-in-time snapshot of all traces.
func (p *Pipeline) TraceViews() []*store.View {
	return p.st.Views()
}

// Aggregates returns a consistent detached copy of all aggregate rows.
func (p *Pipeline) Aggregates() aggregate.Snapshot {
	return p.agg.Snapshot()
}

// Metrics returns a consistent copy of the observable counters plus the
// current orphan count (derived from store views).
func (p *Pipeline) Metrics() Metrics {
	p.mu.Lock()
	m := p.metrics
	p.mu.Unlock()
	var orphans int64
	for _, v := range p.st.Views() {
		orphans += int64(len(v.OrphanSpanIDs))
	}
	m.OrphanSpans = orphans
	return m
}
