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
//  4. Save/Load 保证重启后已确认聚合不丢失、不重复；Load 在本地构建后一次性替换，
//     恢复中间状态不对外可见，非法统计项按规则跳过并记录于 Notes。
type Aggregator struct {
	mu        sync.Mutex
	seen      map[string]bool
	byService map[string]*Stats
	byTrace   map[string]*Stats
	notes     []string
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
		if v == nil {
			continue
		}
		out[k] = *v
	}
	return out
}

// TraceStats 返回单条追踪的聚合。
func (a *Aggregator) TraceStats(traceID string) (Stats, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.byTrace[traceID]
	if !ok || st == nil {
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

// Notes 返回从聚合快照恢复时的降级依据（拷贝）。
func (a *Aggregator) Notes() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.notes...)
}

// Load 从快照恢复聚合状态；无快照时从空状态开始。
//
// 恢复全程在本地构建、最后一次性替换，对外没有逐条加载的中间状态。
// 单个统计项为空(null)、含负计数、或错误数大于总数时：跳过该项并写入 Notes，
// 其余聚合正常恢复、正常查询，绝不让非法值对外发布，也不带崩进程。
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
	seen := map[string]bool{}
	for _, k := range snap.Seen {
		seen[k] = true
	}
	notes := []string{}
	services := make(map[string]*Stats, len(snap.ByService))
	for k, v := range snap.ByService {
		services[k] = v
	}
	traces := make(map[string]*Stats, len(snap.ByTrace))
	for k, v := range snap.ByTrace {
		traces[k] = v
	}
	sanitizeStats(services, "聚合快照 by_service", &notes)
	sanitizeStats(traces, "聚合快照 by_trace", &notes)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = seen
	a.byService = services
	a.byTrace = traces
	a.notes = append(a.notes, notes...)
	return nil
}
