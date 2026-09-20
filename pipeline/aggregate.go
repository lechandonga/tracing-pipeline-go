package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Stats 是聚合统计。
type Stats struct {
	Service    string `json:"service,omitempty"`
	Count      int64  `json:"count"`
	ErrorCnt   int64  `json:"error_count"`
	TotalDurNs int64  `json:"total_dur_ns"`
}

// Aggregator 维护追踪与服务维度的聚合。
//
// 规则：
//  1. 以 traceID/spanID 去重，重复或迟到上报不得重复计数。
//  2. 聚合只接受采样结论为保留的跨度，口径与到达顺序无关。
//  3. Snapshot 在锁内拷贝，对外快照一致，不受并发写入影响。
//  4. Save/Load 保证重启后已确认聚合不丢失、不重复。
type Aggregator struct {
	mu        sync.Mutex
	seen      map[string]bool
	byService map[string]*Stats
	byTrace   map[string]*Stats
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		seen:      map[string]bool{},
		byService: map[string]*Stats{},
		byTrace:   map[string]*Stats{},
	}
}

// Observe 记录一条已确认采样保留的跨度；重复跨度被忽略并返回 false。
func (a *Aggregator) Observe(sp span.Span) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := sp.TraceID + "/" + sp.SpanID
	if a.seen[k] {
		return false
	}
	a.seen[k] = true
	addTo(a.byService, sp.Service, sp)
	addTo(a.byTrace, sp.TraceID, sp)
	return true
}

func addTo(m map[string]*Stats, key string, sp span.Span) {
	st, ok := m[key]
	if !ok {
		st = &Stats{Service: key}
		m[key] = st
	}
	st.Count++
	st.TotalDurNs += int64(sp.Duration)
	if sp.HasError {
		st.ErrorCnt++
	}
}

// Snapshot 返回服务维度聚合的一致快照。
func (a *Aggregator) Snapshot() map[string]Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]Stats, len(a.byService))
	for k, v := range a.byService {
		out[k] = *v
	}
	return out
}

// TraceStats 返回单条追踪的聚合。
func (a *Aggregator) TraceStats(traceID string) (Stats, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.byTrace[traceID]
	if !ok {
		return Stats{}, false
	}
	return *st, true
}

type aggSnapshot struct {
	Seen      []string          `json:"seen"`
	ByService map[string]*Stats `json:"by_service"`
	ByTrace   map[string]*Stats `json:"by_trace"`
}

// Save 将聚合状态原子落盘。
func (a *Aggregator) Save(dir string) error {
	if dir == "" {
		return nil
	}
	a.mu.Lock()
	snap := aggSnapshot{
		Seen:      make([]string, 0, len(a.seen)),
		ByService: a.byService,
		ByTrace:   a.byTrace,
	}
	for k := range a.seen {
		snap.Seen = append(snap.Seen, k)
	}
	a.mu.Unlock()
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "aggregate.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "aggregate.json"))
}

// Load 从快照恢复聚合状态；无快照时从空状态开始。
func (a *Aggregator) Load(dir string) error {
	if dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "aggregate.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap aggSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("聚合快照损坏: %w", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, k := range snap.Seen {
		a.seen[k] = true
	}
	for k, v := range snap.ByService {
		a.byService[k] = v
	}
	for k, v := range snap.ByTrace {
		a.byTrace[k] = v
	}
	return nil
}
