package pipeline

import (
	"hash/fnv"
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Sampler 决定追踪级采样结论。
//
// 规则：
//  1. 结论按追踪维度粘性保存：一旦确定，后续到达的同追踪跨度不得改变结论，
//     因此同一追踪要么全部保留要么全部丢弃，不存在部分保留。
//  2. 按属性规则优先于按比例：任一属性命中规则即保留。
//  3. 按比例采样使用 traceID 的确定性哈希，保证同一追踪在不同服务、
//     不同进程（配置相同）下结论一致。
//  4. 配置更新（UpdateConfig）只影响尚未决策的追踪，已决策追踪不受影响。
type Sampler struct {
	mu        sync.Mutex
	ratio     float64
	attrRules map[string]string
	decisions map[string]bool
}

// NewSampler 创建采样器：ratio 为按比例采样率 [0,1]，attrRules 为按属性规则。
func NewSampler(ratio float64, attrRules map[string]string) *Sampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &Sampler{ratio: ratio, attrRules: attrRules, decisions: map[string]bool{}}
}

// Decide 返回追踪的采样结论；同一追踪多次调用结论一致。
func (s *Sampler) Decide(sp span.Span) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.decisions[sp.TraceID]; ok {
		return d
	}
	d := s.decideLocked(sp)
	s.decisions[sp.TraceID] = d
	return d
}

func (s *Sampler) decideLocked(sp span.Span) bool {
	for k, v := range s.attrRules {
		if sp.Attributes[k] == v {
			return true
		}
	}
	return hashBucket(sp.TraceID) < s.ratio
}

// hashBucket 将 traceID 映射到 [0,1) 的稳定桶。
func hashBucket(traceID string) float64 {
	h := fnv.New64a()
	h.Write([]byte(traceID))
	return float64(h.Sum64()>>11) / float64(1<<53)
}

// UpdateConfig 更新采样配置；已决策的追踪结论保持不变。
func (s *Sampler) UpdateConfig(ratio float64, attrRules map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	s.ratio = ratio
	s.attrRules = attrRules
}

// DecisionCount 返回已固化的决策数（用于容量观测）。
func (s *Sampler) DecisionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.decisions)
}
