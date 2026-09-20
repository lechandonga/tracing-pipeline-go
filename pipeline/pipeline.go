package pipeline

import (
	"sync"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Config 是管道配置。
type Config struct {
	MaxSpans   int               // 已确认跨度容量上限
	MaxOrphans int               // 断链暂存区容量上限
	Ratio      float64           // 按比例采样率 [0,1]
	AttrRules  map[string]string // 按属性采样规则
	Dir        string            // 快照目录，空表示不持久化
}

// Pipeline 串联上下文解析、采集、采样与聚合。
type Pipeline struct {
	cfg Config
	ing *Ingester
	sam *Sampler
	st  *Store
	agg *Aggregator

	mu         sync.Mutex
	rejections []span.Rejection // 有界判定日志
	counts     map[span.RejectionReason]int64
	dropped    int64 // 采样丢弃计数
}

const maxRejectionLog = 1000

func New(cfg Config) *Pipeline {
	return &Pipeline{
		cfg:    cfg,
		ing:    NewIngester(cfg.MaxOrphans),
		sam:    NewSampler(cfg.Ratio, cfg.AttrRules),
		st:     NewStore(cfg.MaxSpans, cfg.Dir),
		agg:    NewAggregator(),
		counts: map[span.RejectionReason]int64{},
	}
}

// Submit 接收一条原始跨度记录。
// raw 为追踪上下文字段（traceparent 或扁平字段），sp 为跨度业务数据；
// 上下文解析结果为准，覆盖 sp 中的 TraceID/SpanID/ParentID。
func (p *Pipeline) Submit(raw map[string]string, sp span.Span) *span.Rejection {
	ctx, rej := span.ParseContext(raw)
	if rej != nil {
		p.record(*rej)
		return rej
	}
	sp.TraceID, sp.SpanID, sp.ParentID = ctx.TraceID, ctx.SpanID, ctx.ParentID
	ready, rej := p.ing.Ingest(sp)
	if rej != nil {
		p.record(*rej)
		return rej
	}
	var last *span.Rejection
	for _, sp := range ready {
		if !p.sam.Decide(sp) {
			p.mu.Lock()
			p.dropped++
			p.mu.Unlock()
			continue
		}
		sp.Sampled = true
		if rej := p.st.Add(sp); rej != nil {
			p.record(*rej)
			last = rej
			continue
		}
		p.agg.Observe(sp)
	}
	return last
}

func (p *Pipeline) record(r span.Rejection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts[r.Reason]++
	if len(p.rejections) < maxRejectionLog {
		p.rejections = append(p.rejections, r)
	}
}

// RejectionCounts 返回各类拒绝的计数（判定依据可观测）。
func (p *Pipeline) RejectionCounts() map[span.RejectionReason]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[span.RejectionReason]int64, len(p.counts))
	for k, v := range p.counts {
		out[k] = v
	}
	return out
}

// RejectionLog 返回最近的判定日志。
func (p *Pipeline) RejectionLog() []span.Rejection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]span.Rejection(nil), p.rejections...)
}

// Dropped 返回采样丢弃的跨度数。
func (p *Pipeline) Dropped() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// Stats 返回服务维度聚合快照。
func (p *Pipeline) Stats() map[string]Stats { return p.agg.Snapshot() }

// TraceStats 返回单条追踪的聚合。
func (p *Pipeline) TraceStats(traceID string) (Stats, bool) { return p.agg.TraceStats(traceID) }

// PendingOrphans 返回断链暂存跨度数。
func (p *Pipeline) PendingOrphans() int { return p.ing.PendingOrphans() }

// StoredSpans 返回已确认跨度数。
func (p *Pipeline) StoredSpans() int { return p.st.Len() }

// UpdateSampling 更新采样配置；已决策追踪结论不变。
func (p *Pipeline) UpdateSampling(ratio float64, rules map[string]string) {
	p.sam.UpdateConfig(ratio, rules)
}

// Snapshot 将已确认状态落盘。
func (p *Pipeline) Snapshot() error {
	if err := p.st.Snapshot(); err != nil {
		return err
	}
	return p.agg.Save(p.cfg.Dir)
}

// Restore 从快照恢复；恢复完成前不应调用 Submit，恢复中的中间状态不对外可见。
func (p *Pipeline) Restore() error {
	if err := p.st.Restore(); err != nil {
		return err
	}
	return p.agg.Load(p.cfg.Dir)
}
