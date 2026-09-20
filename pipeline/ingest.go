package pipeline

import (
	"fmt"
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Ingester 接收跨度并重建追踪内的父子关系。
//
// 规则：
//  1. 乱序到达不产生错误归属：父跨度未到达时，子跨度进入暂存区，
//     返回 ReasonOrphan 以便区分，不会被挂到错误节点。
//  2. 父跨度到达时，暂存的子孙跨度按链补齐并随返回值一起释放。
//  3. 重复上报返回 ReasonDuplicate，不会重复进入下游。
//  4. 暂存区有容量上限，超限拒绝并返回 ReasonCapacityFull（可观测）。
type Ingester struct {
	mu         sync.Mutex
	maxOrphans int
	arrived    map[string]map[string]bool        // traceID -> spanID 集合
	orphans    map[string]map[string][]span.Span // traceID -> parentID -> 等待中的子跨度
	orphanKeys map[string]bool                   // traceID/spanID -> 是否已在暂存区
	orphanCnt  int
}

func NewIngester(maxOrphans int) *Ingester {
	return &Ingester{
		maxOrphans: maxOrphans,
		arrived:    map[string]map[string]bool{},
		orphans:    map[string]map[string][]span.Span{},
		orphanKeys: map[string]bool{},
	}
}

// Ingest 接收一条跨度。
// 返回值 ready 包含本次可直接进入下游的跨度（本跨度及被补齐的暂存跨度）；
// 缺父暂存时 ready 为空且 rej.Reason == ReasonOrphan。
func (i *Ingester) Ingest(sp span.Span) (ready []span.Span, rej *span.Rejection) {
	i.mu.Lock()
	defer i.mu.Unlock()
	set, ok := i.arrived[sp.TraceID]
	if !ok {
		set = map[string]bool{}
		i.arrived[sp.TraceID] = set
	}
	if set[sp.SpanID] {
		return nil, &span.Rejection{Reason: span.ReasonDuplicate,
			Detail: sp.TraceID + "/" + sp.SpanID}
	}
	if sp.ParentID != "" && !set[sp.ParentID] {
		// 父跨度未到达：暂存，可区分、可补齐。
		key := sp.TraceID + "/" + sp.SpanID
		if i.orphanKeys[key] {
			return nil, &span.Rejection{Reason: span.ReasonDuplicate, Detail: key}
		}
		if i.orphanCnt >= i.maxOrphans {
			return nil, &span.Rejection{Reason: span.ReasonCapacityFull,
				Detail: fmt.Sprintf("暂存区容量 %d 已满", i.maxOrphans)}
		}
		m, ok := i.orphans[sp.TraceID]
		if !ok {
			m = map[string][]span.Span{}
			i.orphans[sp.TraceID] = m
		}
		m[sp.ParentID] = append(m[sp.ParentID], sp)
		i.orphanKeys[sp.TraceID+"/"+sp.SpanID] = true
		i.orphanCnt++
		return nil, &span.Rejection{Reason: span.ReasonOrphan,
			Detail: fmt.Sprintf("等待父跨度 %s", sp.ParentID)}
	}
	set[sp.SpanID] = true
	ready = append(ready, sp)
	// 释放以本跨度为父的暂存跨度（可能级联）。
	ready = i.drainLocked(sp.TraceID, sp.SpanID, ready)
	return ready, nil
}

func (i *Ingester) drainLocked(traceID, spanID string, ready []span.Span) []span.Span {
	m := i.orphans[traceID]
	if m == nil {
		return ready
	}
	children := m[spanID]
	if len(children) == 0 {
		return ready
	}
	delete(m, spanID)
	set := i.arrived[traceID]
	for _, c := range children {
		i.orphanCnt--
		delete(i.orphanKeys, c.TraceID+"/"+c.SpanID)
		set[c.SpanID] = true
		ready = append(ready, c)
		ready = i.drainLocked(traceID, c.SpanID, ready)
	}
	if len(m) == 0 {
		delete(i.orphans, traceID)
	}
	return ready
}

// PendingOrphans 返回当前暂存跨度数（用于观测断链情况）。
func (i *Ingester) PendingOrphans() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.orphanCnt
}
