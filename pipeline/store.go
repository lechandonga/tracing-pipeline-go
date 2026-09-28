package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Store 是有容量上限的跨度存储。
//
// 规则：
//  1. 以 traceID/spanID 为键去重，重复上报返回 ReasonDuplicate，不重复计数。
//  2. 超出 maxSpans 时拒绝并返回 ReasonCapacityFull，拒绝计数可观测。
//  3. Snapshot/Restore 保证重启后已确认跨度不丢失、不重复；
//     快照通过临时文件 + rename 原子落盘，恢复过程中的中间状态不对外可见。
type Store struct {
	mu       sync.RWMutex
	maxSpans int
	dir      string
	spans    map[string]span.Span
	rejected map[span.RejectionReason]int64
}

// NewStore 创建存储，maxSpans 为容量上限，dir 为快照目录（空表示不持久化）。
func NewStore(maxSpans int, dir string) *Store {
	return &Store{
		maxSpans: maxSpans,
		dir:      dir,
		spans:    map[string]span.Span{},
		rejected: map[span.RejectionReason]int64{},
	}
}

func keyOf(sp span.Span) string { return sp.TraceID + "/" + sp.SpanID }

// Add 写入跨度，重复或超容量时返回拒绝原因。
func (s *Store) Add(sp span.Span) *span.Rejection {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := keyOf(sp)
	if _, ok := s.spans[k]; ok {
		s.rejected[span.ReasonDuplicate]++
		return &span.Rejection{Reason: span.ReasonDuplicate, Detail: k}
	}
	if len(s.spans) >= s.maxSpans {
		s.rejected[span.ReasonCapacityFull]++
		return &span.Rejection{Reason: span.ReasonCapacityFull,
			Detail: fmt.Sprintf("容量 %d 已满", s.maxSpans)}
	}
	s.spans[k] = sp
	return nil
}

// Get 读取单条跨度。
func (s *Store) Get(traceID, spanID string) (span.Span, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sp, ok := s.spans[traceID+"/"+spanID]
	return sp, ok
}

// Len 返回当前跨度数。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.spans)
}

// Rejections 返回各类拒绝的计数快照。
func (s *Store) Rejections() map[span.RejectionReason]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[span.RejectionReason]int64, len(s.rejected))
	for k, v := range s.rejected {
		out[k] = v
	}
	return out
}

type storeSnapshot struct {
	Spans    []span.Span                    `json:"spans"`
	Rejected map[span.RejectionReason]int64 `json:"rejected"`
}

// Snapshot 将已确认状态原子落盘。
func (s *Store) Snapshot() error {
	if s.dir == "" {
		return nil
	}
	s.mu.RLock()
	snap := storeSnapshot{Spans: make([]span.Span, 0, len(s.spans)), Rejected: s.Rejections()}
	for _, sp := range s.spans {
		snap.Spans = append(snap.Spans, sp)
	}
	s.mu.RUnlock()
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "store.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "store.json"))
}

// Restore 从快照恢复；无快照时从空状态开始。
//
// 全程在本地构建，最后一次性替换并合并拒绝计数：恢复完成前对外查询不到
// 只加载了一半的中间状态；快照超出容量的部分按最旧顺序截断（容量边界不放宽）。
func (s *Store) Restore() error {
	if s.dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(s.dir, "store.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap storeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("快照损坏: %w", err)
	}
	spans := make(map[string]span.Span, len(snap.Spans))
	for _, sp := range snap.Spans {
		if len(spans) >= s.maxSpans {
			break
		}
		spans[keyOf(sp)] = sp
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = spans // 原子替换，不存在逐条加载的中间状态
	for k, v := range snap.Rejected {
		s.rejected[k] += v
	}
	return nil
}
