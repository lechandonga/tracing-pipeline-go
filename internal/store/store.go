// Package store reassembles inbound spans into traces under explicit, bounded
// capacity rules.
//
// Reassembly guarantees:
//
//   - spans are keyed by (trace id, span id); a span whose parent has not
//     arrived yet is stored in the same trace and marked Orphan instead of
//     being attached to a guessed parent;
//   - when the missing parent later arrives the orphan is linked automatically,
//     so out-of-order delivery never causes mis-attribution;
//   - exact duplicates (same span id and identical payload) are accepted as
//     idempotent no-ops; a same-id span with a different payload is a
//     collision and rejected, since the correct body cannot be guessed;
//   - a trace is Complete only when every span's parent is present in the
//     trace; traces with a dangling link are Partial, empty traces never
//     exist;
//   - MaxSpansPerTrace, MaxTraces and MaxTotalSpans are hard limits enforced
//     up front; when EvictOldest is configured an oldest-idle trace is evicted
//     to make room, otherwise the span is rejected. Either event is returned
//     with a machine-readable reason so callers can metric it;
//   - retention is bounded by RetentionTTL: EvictExpired removes traces idle
//     (no new span) for longer than the TTL and returns them so callers can
//     retract their contributions from published aggregates.
//
// All exported operations are safe for concurrent use.
package store

import (
	"sync"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
	"github.com/lechandonga/tracing-pipeline-go/internal/sampling"
)

// Reason / status codes returned by Add.
const (
	StatusAccepted       = "accepted"
	StatusDuplicate      = "duplicate-span"
	StatusResolvedOrphan = "resolved-orphan"

	RejectSpanCollision = "span-id-collision"
	RejectTraceCapacity = "capacity-max-traces"
	RejectPerTraceCap   = "capacity-max-spans-per-trace"
	RejectTotalCapacity = "capacity-max-total-spans"

	ReasonEvictedOldest = "evicted-oldest"
)

// TraceState distinguishes a fully linked trace from one with a dangling link.
type TraceState string

const (
	// StatePartial means at least one span's parent is absent.
	StatePartial TraceState = "partial"
	// StateComplete means every parent reference resolves inside the trace.
	StateComplete TraceState = "complete"
)

// Config are the capacity / retention limits. Zero means "no limit" for the
// maximums; RetentionTTL zero disables TTL eviction.
type Config struct {
	MaxTraces        int
	MaxSpansPerTrace int
	MaxTotalSpans    int
	RetentionTTL     time.Duration
	// EvictOldest enables degrade-on-overflow: the trace with the oldest
	// last-updated timestamp is evicted instead of rejecting the new span.
	EvictOldest bool
}

// spanEntry holds one span plus its link classification.
type spanEntry struct {
	span            model.Span
	parentInferred  bool // parent came from the context header, not the body
	arrivalUnixNano int64
}

// Trace is the stored state for one trace id.
type Trace struct {
	TraceID model.TraceID
	// Spans keyed by span id.
	spans map[model.SpanID]spanEntry
	// Orphans are span ids whose parent is not currently present.
	orphans map[model.SpanID]bool
	// Roots are span ids with no parent reference at all.
	roots map[model.SpanID]bool

	firstArrivalUnixNano int64
	lastUpdateUnixNano   int64

	// Sampling: pinned on first decision (first accepted span).
	decision       sampling.Decision
	decisionSet    bool
	decisionReason string
}

// State computes whether the trace is fully reconstructable: every parent
// reference must resolve AND at least one span must be reachable as a root.
// A cycle (e.g. a<->b with no root) keeps the trace Partial forever, because
// its parent/child timeline cannot be explained.
func (t *Trace) State() TraceState {
	if len(t.orphans) > 0 || len(t.spans) == 0 {
		return StatePartial
	}
	if len(t.roots) == 0 {
		return StatePartial
	}
	// Every span must be reachable from some root; otherwise a cycle is
	// detached from the rooted portion of the graph.
	reachable := make(map[model.SpanID]bool, len(t.spans))
	children := map[model.SpanID][]model.SpanID{}
	for id, e := range t.spans {
		if pid := e.span.ParentSpanID; pid != "" {
			children[pid] = append(children[pid], id)
		}
	}
	stack := make([]model.SpanID, 0, len(t.roots))
	for r := range t.roots {
		stack = append(stack, r)
	}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if reachable[cur] {
			continue
		}
		reachable[cur] = true
		stack = append(stack, children[cur]...)
	}
	if len(reachable) != len(t.spans) {
		return StatePartial
	}
	return StateComplete
}

// SpanCount returns the number of stored span payloads.
func (t *Trace) SpanCount() int { return len(t.spans) }

// View is an immutable-by-convention snapshot of a trace.
type View struct {
	TraceID          model.TraceID
	State            TraceState
	SpanCount        int
	OrphanSpanIDs    []model.SpanID
	RootSpanIDs      []model.SpanID
	Services         []string
	MinStartUnixNano int64
	MaxEndUnixNano   int64
	FirstArrivalNano int64
	LastUpdateNano   int64
	Decision         sampling.Decision
	DecisionSet      bool
	DecisionReason   string
}

// AddResult is the outcome of one Add call.
type AddResult struct {
	Status   string // accepted | duplicate-span | resolved-orphan or a Reject* code
	Accepted bool
	Evicted  []EvictedTrace // populated when room was made by eviction
	State    TraceState
}

// EvictedTrace pairs an evicted trace with the reason it was removed, so the
// pipeline can retract aggregate contributions exactly once.
type EvictedTrace struct {
	Trace  *Trace
	Reason string
}

// Store holds in-memory trace state.
type Store struct {
	mu  sync.Mutex
	cfg Config

	traces     map[model.TraceID]*Trace
	totalSpans int
	now        func() time.Time
}

// New constructs a Store.
func New(cfg Config) *Store {
	return &Store{cfg: cfg, traces: map[model.TraceID]*Trace{}, now: time.Now}
}

// Add inserts a span (already validated and normalized). inferredParent marks
// a parent adopted from the context header. It never partially mutates state
// on rejection.
func (s *Store) Add(span model.Span, inferredParent bool) AddResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	nowNano := s.now().UnixNano()
	t := s.traces[span.TraceID]

	// Duplicate / collision classification.
	if t != nil {
		if existing, ok := t.spans[span.SpanID]; ok {
			if spansEqual(existing.span, span) {
				// Exact duplicate: idempotent no-op. Do not bump the trace's
				// last-update time: duplicates are not activity.
				return AddResult{Status: StatusDuplicate, Accepted: true, State: t.State()}
			}
			return AddResult{Status: RejectSpanCollision, Accepted: false, State: t.State()}
		}
	}

	// Capacity enforcement happens before any mutation. Evictions performed
	// to make room are returned so the pipeline can retract aggregates and
	// persist the removal atomically with the insertion.
	var evicted []EvictedTrace
	if s.cfg.MaxSpansPerTrace > 0 && t != nil && len(t.spans) >= s.cfg.MaxSpansPerTrace {
		return AddResult{Status: RejectPerTraceCap, Accepted: false, State: t.State()}
	}
	if t == nil && s.cfg.MaxTraces > 0 && len(s.traces) >= s.cfg.MaxTraces {
		if !s.cfg.EvictOldest {
			return AddResult{Status: RejectTraceCapacity, Accepted: false}
		}
		e := s.evictOldestLocked(nowNano)
		if e == nil {
			return AddResult{Status: RejectTraceCapacity, Accepted: false}
		}
		evicted = append(evicted, *e)
	}
	if s.cfg.MaxTotalSpans > 0 && s.totalSpans >= s.cfg.MaxTotalSpans {
		if !s.cfg.EvictOldest {
			status := RejectTotalCapacity
			return AddResult{Status: status, Accepted: false, State: traceStateOrNil(t)}
		}
		// Evict until one slot frees.
		for s.totalSpans >= s.cfg.MaxTotalSpans {
			e := s.evictOldestLocked(nowNano)
			if e == nil {
				return AddResult{Status: RejectTotalCapacity, Accepted: false}
			}
			evicted = append(evicted, *e)
		}
	}

	if t == nil {
		t = NewTrace(span.TraceID)
		t.firstArrivalUnixNano = nowNano
		s.traces[span.TraceID] = t
	}

	entry := spanEntry{span: span, parentInferred: inferredParent, arrivalUnixNano: nowNano}
	t.spans[span.SpanID] = entry
	s.totalSpans++
	t.lastUpdateUnixNano = nowNano

	// Link / orphan bookkeeping. Adding a span can resolve orphanage for
	// other spans and can itself be orphaned; recompute locally.
	resolved := s.reclassifyAfterInsertLocked(t, span.SpanID)

	state := t.State()
	status := StatusAccepted
	if resolved > 0 {
		status = StatusResolvedOrphan
	}
	return AddResult{Status: status, Accepted: true, Evicted: evicted, State: state}
}

// reclassifyAfterInsertLocked updates root/orphan sets when a new span id has
// just been inserted. It returns how many previously orphaned spans became
// linked. Caller holds the lock.
func (s *Store) reclassifyAfterInsertLocked(t *Trace, inserted model.SpanID) int {
	newEntry := t.spans[inserted]
	resolved := 0

	// The new span may satisfy waiting children.
	for childID := range t.orphans {
		if t.spans[childID].span.ParentSpanID == inserted {
			delete(t.orphans, childID)
			resolved++
		}
	}

	// Classify the inserted span itself.
	parent := newEntry.span.ParentSpanID
	switch {
	case parent == "":
		t.roots[inserted] = true
	case s.parentPresentLocked(t, parent):
		delete(t.roots, inserted)
	default:
		// Parent referenced but not here: dangling, not guessed.
		t.orphans[inserted] = true
		delete(t.roots, inserted)
	}
	return resolved
}

func (s *Store) parentPresentLocked(t *Trace, id model.SpanID) bool {
	_, ok := t.spans[id]
	return ok
}

// evictOldestLocked removes the trace with the oldest last-update timestamp.
// Returns nil when nothing can be evicted. Caller holds the lock.
func (s *Store) evictOldestLocked(nowNano int64) *EvictedTrace {
	var oldestID model.TraceID
	var oldestNano int64
	first := true
	for id, tr := range s.traces {
		if first || tr.lastUpdateUnixNano < oldestNano {
			oldestID, oldestNano, first = id, tr.lastUpdateUnixNano, false
		}
	}
	if first {
		return nil
	}
	tr := s.traces[oldestID]
	delete(s.traces, oldestID)
	s.totalSpans -= len(tr.spans)
	return &EvictedTrace{Trace: tr, Reason: ReasonEvictedOldest}
}

func spansEqual(a, b model.Span) bool {
	return a.TraceID == b.TraceID &&
		a.SpanID == b.SpanID &&
		a.ParentSpanID == b.ParentSpanID &&
		a.Service == b.Service &&
		a.Name == b.Name &&
		a.Kind == b.Kind &&
		a.StartUnixNano == b.StartUnixNano &&
		a.EndUnixNano == b.EndUnixNano &&
		a.Status == b.Status &&
		a.StatusMessage == b.StatusMessage &&
		attrsEqual(a.Attributes, b.Attributes)
}

func attrsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func traceStateOrNil(t *Trace) TraceState {
	if t == nil {
		return ""
	}
	return t.State()
}

// Get returns an immutable view of one trace, or nil.
func (s *Store) Get(id model.TraceID) *View {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.traces[id]; ok {
		return viewOf(t)
	}
	return nil
}

// Views returns views of all traces sorted by trace id: a consistent
// point-in-time snapshot taken under the single store lock.
func (s *Store) Views() []*View {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*View, 0, len(s.traces))
	ids := make([]string, 0, len(s.traces))
	byID := make(map[string]*Trace, len(s.traces))
	for id, t := range s.traces {
		ids = append(ids, string(id))
		byID[string(id)] = t
	}
	sortStrings(ids)
	for _, id := range ids {
		out = append(out, viewOf(byID[id]))
	}
	return out
}

// TraceCount returns the number of traces currently held.
func (s *Store) TraceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.traces)
}

// SpanCount returns the number of span payloads currently held.
func (s *Store) SpanCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalSpans
}

// SetDecision pins the sticky sampling verdict on a trace. A second call with
// a different verdict is ignored and reports false: later associated spans can
// never change a decision already made.
func (s *Store) SetDecision(id model.TraceID, d sampling.Decision, reason string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.traces[id]
	if !ok {
		return false
	}
	if t.decisionSet {
		return false
	}
	t.decision, t.decisionSet, t.decisionReason = d, true, reason
	return true
}

// Decision returns the pinned decision, if any.
func (s *Store) Decision(id model.TraceID) (sampling.Decision, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.traces[id]
	if !ok || !t.decisionSet {
		return sampling.Drop, "", false
	}
	return t.decision, t.decisionReason, true
}

// SpansOf returns retained span copies of one trace sorted by span id, or nil.
func (s *Store) SpansOf(id model.TraceID) []StoredSpan {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.traces[id]
	if !ok {
		return nil
	}
	return t.StoredSpans()
}

// EvictTrace unconditionally removes one trace (used by TTL / WAL evict
// replay). It reports whether it existed.
func (s *Store) EvictTrace(id model.TraceID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.traces[id]
	if !ok {
		return false
	}
	delete(s.traces, id)
	s.totalSpans -= len(t.spans)
	return true
}

// RemoveSpan removes one span (used to roll back an unconfirmed insertion).
// Children of the removed span become orphans again; empty traces are dropped.
// It reports whether the span existed.
func (s *Store) RemoveSpan(traceID model.TraceID, spanID model.SpanID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.traces[traceID]
	if t == nil {
		return false
	}
	if _, ok := t.spans[spanID]; !ok {
		return false
	}
	delete(t.spans, spanID)
	delete(t.roots, spanID)
	delete(t.orphans, spanID)
	s.totalSpans--
	// Children now dangle until the parent re-arrives.
	for id, e := range t.spans {
		if e.span.ParentSpanID == spanID {
			t.orphans[id] = true
			delete(t.roots, id)
		}
	}
	if len(t.spans) == 0 {
		delete(s.traces, traceID)
	}
	return true
}

// EvictExpired removes traces whose last update is older than now-ttl and
// returns them (exactly once) so the pipeline can retract aggregates and
// persist deletions. No-op when TTL is zero (retention disabled).
func (s *Store) EvictExpired(now time.Time) []EvictedTrace {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.RetentionTTL <= 0 {
		return nil
	}
	cutoff := now.Add(-s.cfg.RetentionTTL).UnixNano()
	var out []EvictedTrace
	for id, t := range s.traces {
		if t.lastUpdateUnixNano <= cutoff {
			delete(s.traces, id)
			s.totalSpans -= len(t.spans)
			out = append(out, EvictedTrace{Trace: t, Reason: "evicted-ttl"})
		}
	}
	return out
}

// viewOf builds a copy-on-read snapshot. Caller holds the lock.
func viewOf(t *Trace) *View {
	v := &View{
		TraceID:          t.TraceID,
		State:            t.State(),
		SpanCount:        len(t.spans),
		MinStartUnixNano: 1<<63 - 1,
		FirstArrivalNano: t.firstArrivalUnixNano,
		LastUpdateNano:   t.lastUpdateUnixNano,
		Decision:         t.decision,
		DecisionSet:      t.decisionSet,
		DecisionReason:   t.decisionReason,
	}
	svcSet := map[string]bool{}
	for id, e := range t.spans {
		if t.orphans[id] {
			v.OrphanSpanIDs = append(v.OrphanSpanIDs, id)
		}
		if t.roots[id] {
			v.RootSpanIDs = append(v.RootSpanIDs, id)
		}
		if e.span.StartUnixNano < v.MinStartUnixNano {
			v.MinStartUnixNano = e.span.StartUnixNano
		}
		if e.span.EndUnixNano > v.MaxEndUnixNano {
			v.MaxEndUnixNano = e.span.EndUnixNano
		}
		if e.span.Service != "" {
			svcSet[e.span.Service] = true
		}
	}
	if len(t.spans) == 0 {
		v.MinStartUnixNano = 0
	}
	for svc := range svcSet {
		v.Services = append(v.Services, svc)
	}
	sortModelIDs(v.OrphanSpanIDs)
	sortModelIDs(v.RootSpanIDs)
	sortStrings(v.Services)
	return v
}

func sortStrings(a []string) {
	// avoid sort import churn: insertion sort is plenty for snapshot sizes
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

func sortModelIDs[T ~string](a []T) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j-1] > a[j]; j-- {
			a[j-1], a[j] = a[j], a[j-1]
		}
	}
}

// NewTrace builds an empty trace for recovery tooling.
func NewTrace(id model.TraceID) *Trace {
	return &Trace{TraceID: id, spans: map[model.SpanID]spanEntry{}, orphans: map[model.SpanID]bool{}, roots: map[model.SpanID]bool{}}
}

// StoredSpan is an exported copy of one stored span plus its linkage flags,
// used for aggregate retraction and durable persistence.
type StoredSpan struct {
	Span            model.Span
	ParentInferred  bool
	ArrivalUnixNano int64
}

// StoredSpans returns span copies sorted by span id (deterministic order for
// persistence and tests).
func (t *Trace) StoredSpans() []StoredSpan {
	ids := make([]model.SpanID, 0, len(t.spans))
	for id := range t.spans {
		ids = append(ids, id)
	}
	sortModelIDs(ids)
	out := make([]StoredSpan, 0, len(ids))
	for _, id := range ids {
		e := t.spans[id]
		out = append(out, StoredSpan{Span: e.span, ParentInferred: e.parentInferred, ArrivalUnixNano: e.arrivalUnixNano})
	}
	return out
}

// Timestamps returns the first-arrival and last-update times.
func (t *Trace) Timestamps() (firstArrival, lastUpdate int64) {
	return t.firstArrivalUnixNano, t.lastUpdateUnixNano
}

// DecisionFields returns the pinned decision state.
func (t *Trace) DecisionFields() (sampling.Decision, string, bool) {
	return t.decision, t.decisionReason, t.decisionSet
}

// RestoreTraceData rebuilds a trace from persisted data. It replays the same
// link classification as live ingestion, so recovered orphans/roots match the
// pre-crash state.
func (s *Store) RestoreTraceData(traceID model.TraceID, spans []StoredSpan, d sampling.Decision, reason string, decided bool, firstArrival, lastUpdate int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := NewTrace(traceID)
	for _, ss := range spans {
		t.spans[ss.Span.SpanID] = spanEntry{span: ss.Span, parentInferred: ss.ParentInferred, arrivalUnixNano: ss.ArrivalUnixNano}
		s.totalSpans++
	}
	// Recompute root/orphan classification from scratch.
	for id, e := range t.spans {
		parent := e.span.ParentSpanID
		switch {
		case parent == "":
			t.roots[id] = true
		default:
			if _, ok := t.spans[parent]; !ok {
				t.orphans[id] = true
			}
		}
	}
	t.firstArrivalUnixNano, t.lastUpdateUnixNano = firstArrival, lastUpdate
	if decided {
		t.decision, t.decisionSet, t.decisionReason = d, true, reason
	}
	if existing, ok := s.traces[traceID]; ok {
		s.totalSpans -= len(existing.spans)
	}
	s.traces[traceID] = t
}
