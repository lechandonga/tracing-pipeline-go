package pipeline

import (
	"sync"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// Config 是管道配置。
type Config struct {
	MaxSpans   int               // 已确认跨度容量上限
	MaxOrphans int               // 断链暂存区容量上限
	Ratio      float64           // 按比例采样率 [0,1]
	AttrRules  map[string]string // 按属性采样规则
	Dir        string            // 快照目录，空表示不持久化
	Window     WindowConfig      // 滚动时间窗口导出配置，Size<=0 表示禁用
	// Clock 为窗口归属与滚动提供时钟，便于测试注入；nil 取墙钟。
	Clock func() time.Time
}

// Pipeline 串联上下文解析、采集、采样与聚合。
type Pipeline struct {
	cfg Config
	ing *Ingester
	sam *Sampler
	st  *Store
	agg *Aggregator
	win *WindowExporter

	mu         sync.Mutex
	rejections []span.Rejection // 有界判定日志
	counts     map[span.RejectionReason]int64
	dropped    int64 // 采样丢弃计数
	ready      bool  // 窗口恢复完成门控：完成前窗口查询不可见
}

const maxRejectionLog = 1000

func New(cfg Config) *Pipeline {
	p := &Pipeline{
		cfg:    cfg,
		ing:    NewIngester(cfg.MaxOrphans),
		sam:    NewSampler(cfg.Ratio, cfg.AttrRules),
		st:     NewStore(cfg.MaxSpans, cfg.Dir),
		agg:    NewAggregator(),
		win:    NewWindowExporter(cfg.Window, cfg.Clock),
		counts: map[span.RejectionReason]int64{},
	}
	// 无持久化目录时不存在恢复阶段，直接就绪；
	// 有目录时必须等 Restore 完成后窗口查询才放行。
	p.ready = cfg.Dir == ""
	return p
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
		if p.agg.Observe(sp) && p.win.Enabled() {
			p.win.Observe(sp)
		}
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

// WindowQuery 返回指定窗口（起点自动对齐）的服务/追踪维度一致视图。
// 窗口导出未启用、恢复未完成或窗口已淘汰/不存在时返回 false。
func (p *Pipeline) WindowQuery(start time.Time) (WindowView, bool) {
	p.mu.Lock()
	if !p.ready || !p.win.Enabled() {
		p.mu.Unlock()
		return WindowView{}, false
	}
	p.mu.Unlock()
	return p.win.Query(start)
}

// Windows 返回保留中的窗口元数据；恢复未完成前返回 nil。
func (p *Pipeline) Windows() []WindowMeta {
	p.mu.Lock()
	ready := p.ready
	p.mu.Unlock()
	if !ready || !p.win.Enabled() {
		return nil
	}
	return p.win.Windows()
}

// WindowDisposals 返回窗口侧处置计数与有界处置日志（迟到/重复等可核对依据）。
func (p *Pipeline) WindowDisposals() (map[string]int64, []Disposal) {
	return p.win.Disposals()
}

// WindowDegradations 返回窗口快照恢复时的降级记录。
func (p *Pipeline) WindowDegradations() []string { return p.win.Degradations() }

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
	if err := p.agg.Save(p.cfg.Dir); err != nil {
		return err
	}
	return p.win.Save(p.cfg.Dir)
}

// Restore 从快照恢复；恢复完成前不应调用 Submit，恢复中的中间状态不对外可见。
func (p *Pipeline) Restore() error {
	if err := p.st.Restore(); err != nil {
		return err
	}
	if err := p.agg.Load(p.cfg.Dir); err != nil {
		return err
	}
	if err := p.win.Load(p.cfg.Dir); err != nil {
		return err
	}
	p.mu.Lock()
	p.ready = true
	p.mu.Unlock()
	return nil
}
