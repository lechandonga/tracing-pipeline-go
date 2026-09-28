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
	Window     WindowConfig      // 滚动时间窗口导出，Size<=0 表示不启用
}

// Pipeline 串联上下文解析、采集、采样与聚合。
//
// 恢复可见性：Restore 持有 restoreMu 写锁一次性完成存储、聚合、窗口三段恢复；
// 所有对外查询与 Submit 持读锁，恢复全部完成前会被阻塞，
// 任何时刻都查不到只恢复了一半的中间状态。
type Pipeline struct {
	cfg Config
	ing *Ingester
	sam *Sampler
	st  *Store
	agg *Aggregator
	win *WindowExporter

	restoreMu   sync.RWMutex
	restoreHook func(stage string) // 仅测试注入：恢复阶段探针

	mu         sync.Mutex
	rejections []span.Rejection // 有界判定日志
	counts     map[span.RejectionReason]int64
	dropped    int64 // 采样丢弃计数
}

const maxRejectionLog = 1000

func New(cfg Config) *Pipeline {
	p := &Pipeline{
		cfg:    cfg,
		ing:    NewIngester(cfg.MaxOrphans),
		sam:    NewSampler(cfg.Ratio, cfg.AttrRules),
		st:     NewStore(cfg.MaxSpans, cfg.Dir),
		agg:    NewAggregator(),
		counts: map[span.RejectionReason]int64{},
	}
	if cfg.Window.valid() {
		p.win = NewWindowExporter(cfg.Window)
	}
	return p
}

// Submit 接收一条原始跨度记录。
// raw 为追踪上下文字段（traceparent 或扁平字段），sp 为跨度业务数据；
// 上下文解析结果为准，覆盖 sp 中的 TraceID/SpanID/ParentID。
func (p *Pipeline) Submit(raw map[string]string, sp span.Span) *span.Rejection {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
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
		if p.agg.Observe(sp) && p.win != nil {
			if rej := p.win.Observe(sp); rej != nil {
				p.record(*rej)
				last = rej
			}
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
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
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
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]span.Rejection(nil), p.rejections...)
}

// Dropped 返回采样丢弃的跨度数。
func (p *Pipeline) Dropped() int64 {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dropped
}

// Stats 返回服务维度聚合快照。
func (p *Pipeline) Stats() map[string]Stats {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	return p.agg.Snapshot()
}

// TraceStats 返回单条追踪的聚合。
func (p *Pipeline) TraceStats(traceID string) (Stats, bool) {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	return p.agg.TraceStats(traceID)
}

// PendingOrphans 返回断链暂存跨度数。
func (p *Pipeline) PendingOrphans() int {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	return p.ing.PendingOrphans()
}

// WindowView 查询包含指定时刻的滚动窗口（起点自动对齐）的服务与追踪维度结果。
func (p *Pipeline) WindowView(at time.Time) (WindowView, bool) {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	if p.win == nil {
		return WindowView{}, false
	}
	return p.win.View(at)
}

// PublishedWindows 返回已封板窗口（结束且宽限期已过），按起点升序。
func (p *Pipeline) PublishedWindows() []WindowView {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	if p.win == nil {
		return nil
	}
	return p.win.Published()
}

// WindowMetrics 返回窗口导出器的处置计数：重复、超宽限期、未来窗口。
func (p *Pipeline) WindowMetrics() (dups, expired, future int64) {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	if p.win == nil {
		return 0, 0, 0
	}
	return p.win.Metrics()
}

// WindowLateLog 返回窗口侧最近的越界处置记录。
func (p *Pipeline) WindowLateLog() []LateRecord {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	if p.win == nil {
		return nil
	}
	return p.win.LateLog()
}

// WindowNotes 返回窗口快照恢复时的降级依据。
func (p *Pipeline) WindowNotes() []string {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	if p.win == nil {
		return nil
	}
	return p.win.Notes()
}

// StoredSpans 返回已确认跨度数。
func (p *Pipeline) StoredSpans() int {
	p.restoreMu.RLock()
	defer p.restoreMu.RUnlock()
	return p.st.Len()
}

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
	if p.win != nil {
		return p.win.Save(p.cfg.Dir)
	}
	return nil
}

// Restore 从快照恢复；持写锁一次性完成全部恢复，
// 恢复期间所有查询与 Submit 阻塞，中间状态不对外可见。
func (p *Pipeline) Restore() error {
	p.restoreMu.Lock()
	defer p.restoreMu.Unlock()
	if err := p.st.Restore(); err != nil {
		return err
	}
	p.markRestoreStage("store")
	if err := p.agg.Load(p.cfg.Dir); err != nil {
		return err
	}
	p.markRestoreStage("aggregate")
	if p.win != nil {
		if err := p.win.Load(p.cfg.Dir); err != nil {
			return err
		}
	}
	p.markRestoreStage("window")
	return nil
}

func (p *Pipeline) markRestoreStage(stage string) {
	if p.restoreHook != nil {
		p.restoreHook(stage)
	}
}
