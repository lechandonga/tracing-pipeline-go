package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// WindowConfig 配置滚动时间窗口导出。
//
// 语义：
//   - 所有窗口按 Size 对齐 Unix 纪元，跨度按 Start（而非到达时间）归属窗口，
//     窗口口径与跨度到达先后无关。
//   - 窗口结束后 Grace 宽限期内的迟到跨度并入其本应归属的窗口；
//     超出宽限期（或归属窗口已按保留上限淘汰）的跨度不静默丢弃、不计入当前窗口，
//     而是记录为 late_beyond_grace，可通过 Metrics/LateLog/RejectionCounts 核对。
//   - 只保留最近 Retain 个窗口，旧窗口连同其去重集合整体淘汰，辅助结构占用有界。
type WindowConfig struct {
	Size   time.Duration // 窗口长度，必须 > 0
	Retain int           // 保留的历史窗口数（含当前窗口），必须 > 0
	Grace  time.Duration // 迟到数据宽限期，>= 0
}

func (c WindowConfig) valid() bool { return c.Size > 0 && c.Retain > 0 && c.Grace >= 0 }

// WindowView 是单个窗口对外发布的一致视图。
type WindowView struct {
	Start    time.Time
	End      time.Time
	Sealed   bool // 窗口结束且宽限期已过，结果不再变化
	Services map[string]Stats
	Traces   map[string]Stats
}

// LateRecord 记录一条被明确处置的越界跨度（可核对依据）。
type LateRecord struct {
	TraceID string
	SpanID  string
	Reason  span.RejectionReason
	Detail  string
}

// WindowExporter 按固定窗口滚动产出服务维度与追踪维度统计。
//
// 规则：
//  1. 窗口按跨度 Start 对齐，宽限期内迟到数据回到归属窗口；超宽限期明确处置。
//  2. 每个窗口自带 traceID/spanID 去重集合：乱序、补齐、翻窗、重启后的重复上报
//     都不会重复计数；窗口淘汰时去重集合随之释放，占用有界。
//  3. 窗口淘汰只发生在写入路径；查询路径只读，持读锁拷贝，不阻塞写入。
type WindowExporter struct {
	mu      sync.RWMutex
	cfg     WindowConfig
	now     func() time.Time
	windows map[int64]*window
	dups    int64
	expired int64
	future  int64
	lateLog []LateRecord
	notes   []string
}

type window struct {
	services map[string]*Stats
	traces   map[string]*Stats
	seen     map[string]bool
}

const maxLateLog = 1000

// NewWindowExporter 创建滚动窗口导出器。
func NewWindowExporter(cfg WindowConfig) *WindowExporter {
	w := &WindowExporter{
		cfg:     cfg,
		now:     time.Now,
		windows: map[int64]*window{},
	}
	return w
}

func (w *WindowExporter) indexOf(t time.Time) int64 {
	return t.UnixNano() / w.cfg.Size.Nanoseconds()
}

func (w *WindowExporter) windowStart(idx int64) time.Time {
	return time.Unix(0, idx*w.cfg.Size.Nanoseconds()).UTC()
}

// Observe 将一条已确认跨度并入其归属窗口。
// 返回 nil 表示已计入；返回非 nil 表示被明确处置（超宽限期/未来窗口），调用方应记录。
func (w *WindowExporter) Observe(sp span.Span) *span.Rejection {
	now := w.now()
	curIdx := w.indexOf(now)
	idx := w.indexOf(sp.Start)
	switch {
	case idx > curIdx:
		// 起点落在当前窗口之后：不猜测归属、不挤压当前窗口，明确拒绝并留痕。
		return w.recordOutOfBounds(now, sp, idx, span.ReasonFutureWindow,
			"跨度起点晚于当前窗口")
	case now.Sub(w.windowStart(idx).Add(w.cfg.Size)) > w.cfg.Grace:
		// 已超过归属窗口的宽限期：不计入任何窗口，明确留痕。
		return w.recordOutOfBounds(now, sp, idx, span.ReasonLateExpired,
			"跨度超出迟到宽限期")
	case idx < curIdx-int64(w.cfg.Retain)+1:
		// 宽限期配置大于保留范围时，归属窗口已按保留上限淘汰：明确留痕。
		return w.recordOutOfBounds(now, sp, idx, span.ReasonLateExpired,
			"归属窗口已按保留上限淘汰")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	win := w.windows[idx]
	if win == nil {
		win = &window{
			services: map[string]*Stats{},
			traces:   map[string]*Stats{},
			seen:     map[string]bool{},
		}
		w.windows[idx] = win
	}
	k := sp.TraceID + "/" + sp.SpanID
	if win.seen[k] {
		w.dups++
		return nil
	}
	win.seen[k] = true
	addTo(win.services, sp.Service, sp)
	addTo(win.traces, sp.TraceID, sp)
	w.evictLocked(curIdx)
	return nil
}

func (w *WindowExporter) recordOutOfBounds(now time.Time, sp span.Span, idx int64,
	reason span.RejectionReason, msg string) *span.Rejection {
	start := w.windowStart(idx)
	detail := fmt.Sprintf("%s: 归属窗口 [%s,%s) 处理时刻 %s",
		msg, start.Format(time.RFC3339Nano), start.Add(w.cfg.Size).Format(time.RFC3339Nano),
		now.UTC().Format(time.RFC3339Nano))
	w.mu.Lock()
	switch reason {
	case span.ReasonLateExpired:
		w.expired++
	case span.ReasonFutureWindow:
		w.future++
	}
	if len(w.lateLog) < maxLateLog {
		w.lateLog = append(w.lateLog, LateRecord{
			TraceID: sp.TraceID, SpanID: sp.SpanID, Reason: reason, Detail: detail,
		})
	}
	w.mu.Unlock()
	return &span.Rejection{Reason: reason, Detail: detail}
}

// evictLocked 仅保留最近 Retain 个窗口（含当前），旧窗口及去重集合整体释放。
func (w *WindowExporter) evictLocked(curIdx int64) {
	minIdx := curIdx - int64(w.cfg.Retain) + 1
	for idx := range w.windows {
		if idx < minIdx {
			delete(w.windows, idx)
		}
	}
}

// View 查询包含指定时刻的窗口（起点自动对齐）的一致快照。
func (w *WindowExporter) View(at time.Time) (WindowView, bool) {
	idx := w.indexOf(at)
	now := w.now()
	w.mu.RLock()
	defer w.mu.RUnlock()
	win := w.windows[idx]
	if win == nil {
		return WindowView{}, false
	}
	return w.snapshotLocked(idx, win, now), true
}

// Published 返回已封板（窗口结束且宽限期已过）的窗口视图，按起点升序。
func (w *WindowExporter) Published() []WindowView {
	now := w.now()
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]WindowView, 0, len(w.windows))
	for idx, win := range w.windows {
		if !now.Before(w.windowStart(idx).Add(w.cfg.Size).Add(w.cfg.Grace)) {
			out = append(out, w.snapshotLocked(idx, win, now))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// Windows 返回当前保留的全部窗口视图，按起点升序（含未封板窗口）。
func (w *WindowExporter) Windows() []WindowView {
	now := w.now()
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]WindowView, 0, len(w.windows))
	for idx, win := range w.windows {
		out = append(out, w.snapshotLocked(idx, win, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

func (w *WindowExporter) snapshotLocked(idx int64, win *window, now time.Time) WindowView {
	start := w.windowStart(idx)
	v := WindowView{
		Start:    start,
		End:      start.Add(w.cfg.Size),
		Sealed:   !now.Before(start.Add(w.cfg.Size).Add(w.cfg.Grace)),
		Services: make(map[string]Stats, len(win.services)),
		Traces:   make(map[string]Stats, len(win.traces)),
	}
	for k, s := range win.services {
		if s == nil {
			continue // 防御：非法统计在恢复时已剔除，这里兜底保证查询不崩
		}
		v.Services[k] = *s
	}
	for k, s := range win.traces {
		if s == nil {
			continue
		}
		v.Traces[k] = *s
	}
	return v
}

// Metrics 返回导出器自身的处置计数：重复、超宽限期、未来窗口。
func (w *WindowExporter) Metrics() (dups, expired, future int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.dups, w.expired, w.future
}

// LateLog 返回最近的越界处置记录（拷贝）。
func (w *WindowExporter) LateLog() []LateRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]LateRecord(nil), w.lateLog...)
}

// Notes 返回恢复过程中的降级依据（拷贝）。
func (w *WindowExporter) Notes() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]string(nil), w.notes...)
}

// windowFormatVersion 是窗口快照格式版本；跨版本读取规则见 Load。
const windowFormatVersion = 1

type windowSnapshotFile struct {
	Version int           `json:"version"`
	Windows []windowEntry `json:"windows"`
	Dups    int64         `json:"dups"`
	Expired int64         `json:"expired"`
	Future  int64         `json:"future"`
	LateLog []LateRecord  `json:"late_log"`
	// 指针形态用于区分“字段缺失”（nil，老版本/外来存档）与合法零值：
	// 缺失即无法核对窗口边界，按降级规则拒收窗口，绝不猜测。
	SizeNs  *int64 `json:"size_ns"`
	GraceNs *int64 `json:"grace_ns"`
}

// windowEntry 是单个窗口的持久化形态。
type windowEntry struct {
	Index    int64             `json:"index"`
	Services map[string]*Stats `json:"services"`
	Traces   map[string]*Stats `json:"traces"`
	Seen     []string          `json:"seen"`
}

// Save 将窗口状态通过临时文件 + rename 原子落盘。
func (w *WindowExporter) Save(dir string) error {
	if dir == "" || !w.cfg.valid() {
		return nil
	}
	w.mu.RLock()
	sizeNs, graceNs := int64(w.cfg.Size), int64(w.cfg.Grace)
	snap := windowSnapshotFile{
		Version: windowFormatVersion,
		SizeNs:  &sizeNs,
		GraceNs: &graceNs,
		Windows: make([]windowEntry, 0, len(w.windows)),
		Dups:    w.dups,
		Expired: w.expired,
		Future:  w.future,
		LateLog: append([]LateRecord(nil), w.lateLog...),
	}
	for idx, win := range w.windows {
		e := windowEntry{
			Index:    idx,
			Services: win.services,
			Traces:   win.traces,
			Seen:     make([]string, 0, len(win.seen)),
		}
		for k := range win.seen {
			e.Seen = append(e.Seen, k)
		}
		snap.Windows = append(snap.Windows, e)
	}
	w.mu.RUnlock()
	sort.Slice(snap.Windows, func(i, j int) bool {
		return snap.Windows[i].Index < snap.Windows[j].Index
	})
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "windows.json")
	if err := os.WriteFile(tmp+".tmp", data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp+".tmp", tmp)
}

// Load 从快照恢复窗口状态；无快照时从空状态开始。
//
// 跨重启/跨版本/损坏数据的降级规则（每条都写入 Notes，可核对，绝不猜测归属）：
//  1. 版本号未知（非 windowFormatVersion）→ 整份快照忽略，从空窗口启动；
//  2. size_ns/grace_ns 字段缺失 → 无法核对窗口边界，拒收存档窗口（处置计数仍恢复）；
//  3. 存档 size/grace 与当前配置不一致 → 为保证已发布窗口的边界与归属跟关机前
//     完全一致，拒收存档窗口、从空窗口启动（处置计数仍恢复），绝不按新配置静默
//     重新解释旧窗口 index；
//  4. 单条窗口 index 非法、缺少 seen/services/traces → 跳过该条并记录；
//  5. 单条统计值为空、含负数计数、错误数超过总数 → 跳过该条统计并记录，
//     同窗口其余合法统计与其余窗口不受影响；
//  6. 窗口数超过 Retain → 只恢复最近 Retain 个，其余跳过并记录；
//  7. 文件 JSON 整体非法 → 返回错误，调用方不应带着猜测数据对外服务。
func (w *WindowExporter) Load(dir string) error {
	if dir == "" || !w.cfg.valid() {
		return nil
	}
	path := filepath.Join(dir, "windows.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap windowSnapshotFile
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("窗口快照损坏: %w", err)
	}
	notes := []string{}
	if snap.Version != windowFormatVersion {
		w.mu.Lock()
		w.notes = append(w.notes, fmt.Sprintf(
			"窗口快照版本 %d 与当前版本 %d 不兼容，已忽略整份快照，从空窗口启动",
			snap.Version, windowFormatVersion))
		w.mu.Unlock()
		return nil
	}
	// 边界核对：存档的窗口长度/宽限期必须与当前配置一致，否则同一 index 会被
	// 解释成另一段时间，已发布窗口的边界与归属被悄悄挪动。宁可拒收也不猜测。
	restoreWindows := true
	switch {
	case snap.SizeNs == nil || snap.GraceNs == nil:
		notes = append(notes, "窗口快照缺少 size_ns/grace_ns 字段，无法核对窗口边界，"+
			"已拒收存档窗口、从空窗口启动（处置计数与日志仍恢复）")
		restoreWindows = false
	case *snap.SizeNs != int64(w.cfg.Size) || *snap.GraceNs != int64(w.cfg.Grace):
		notes = append(notes, fmt.Sprintf(
			"存档窗口配置 size=%s grace=%s 与当前配置 size=%s grace=%s 不一致："+
				"为保证已发布窗口边界与归属不变，已拒收存档窗口、从空窗口启动（处置计数与日志仍恢复）",
			time.Duration(*snap.SizeNs), time.Duration(*snap.GraceNs), w.cfg.Size, w.cfg.Grace))
		restoreWindows = false
	}
	var entries []windowEntry
	if restoreWindows {
		entries = snap.Windows
		if len(entries) > w.cfg.Retain {
			sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })
			skipped := len(entries) - w.cfg.Retain
			notes = append(notes, fmt.Sprintf("快照窗口数 %d 超过保留上限 %d，跳过最旧的 %d 个",
				len(entries), w.cfg.Retain, skipped))
			entries = entries[skipped:]
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if e.Index < 0 || e.Services == nil || e.Traces == nil || e.Seen == nil {
			notes = append(notes, fmt.Sprintf(
				"窗口条目 index=%d 缺少必填字段(seen/services/traces)，按降级规则跳过", e.Index))
			continue
		}
		win := &window{services: e.Services, traces: e.Traces, seen: map[string]bool{}}
		for _, k := range e.Seen {
			win.seen[k] = true
		}
		// 统计值合法性校验：空值/负数/明显不合理的统计不对外发布，
		// 只跳过该条统计，同窗口其余统计与其余窗口不受影响。
		notes = append(notes, sanitizeWindowStats(e.Index, "services", win.services)...)
		notes = append(notes, sanitizeWindowStats(e.Index, "traces", win.traces)...)
		if _, dup := w.windows[e.Index]; dup {
			notes = append(notes, fmt.Sprintf("窗口条目 index=%d 在快照中重复，以后一条为准", e.Index))
		}
		w.windows[e.Index] = win
	}
	w.dups += snap.Dups
	w.expired += snap.Expired
	w.future += snap.Future
	if len(snap.LateLog) > 0 {
		w.lateLog = append(w.lateLog, snap.LateLog...)
		if len(w.lateLog) > maxLateLog {
			w.lateLog = w.lateLog[len(w.lateLog)-maxLateLog:]
		}
	}
	w.notes = append(w.notes, notes...)
	return nil
}

// sanitizeWindowStats 剔除非法统计值并返回可核对的降级依据。
func sanitizeWindowStats(idx int64, dim string, m map[string]*Stats) []string {
	var notes []string
	for k, s := range m {
		if reason := invalidStat(s); reason != "" {
			delete(m, k)
			notes = append(notes, fmt.Sprintf(
				"窗口 index=%d 的 %s[%q] 统计非法（%s），已跳过该条统计，其余结果不受影响",
				idx, dim, k, reason))
		}
	}
	return notes
}

// invalidStat 判定统计值是否明显不合法；返回空串表示合法。
func invalidStat(s *Stats) string {
	switch {
	case s == nil:
		return "统计值为空"
	case s.Count < 0 || s.ErrorCnt < 0 || s.TotalDurNs < 0:
		return fmt.Sprintf("含负数计数 count=%d error_count=%d total_dur_ns=%d",
			s.Count, s.ErrorCnt, s.TotalDurNs)
	case s.ErrorCnt > s.Count:
		return fmt.Sprintf("错误数 %d 超过总数 %d", s.ErrorCnt, s.Count)
	}
	return ""
}
