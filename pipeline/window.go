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
//
// 跨重启/跨配置：存档窗口自带 size/grace/绝对起点。当前配置与存档不一致时，
// 存档窗口按其原始边界恢复为历史窗口（归属与封板口径完全不变），新跨度进入按
// 当前 Size 对齐的新窗口；两套网格互不挪动，处置依据见 Notes()。
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
//  4. Load 期间 ready=false，查询不到任何中间状态；恢复成功或按规则降级后才放行。
//  5. 存档窗口保留其原始 size/grace/起点：配置改小窗口或宽限期后重启，已发布窗口
//     的边界、归属与封板结论与关机前完全一致，不会按新配置重新切分。
type WindowExporter struct {
	mu      sync.RWMutex
	cfg     WindowConfig
	now     func() time.Time
	windows map[int64]*window // 键为窗口绝对起点（UnixNano）
	ready   bool              // 恢复是否已完成（新建未持久化的导出器立即可用）
	dups    int64
	expired int64
	future  int64
	recover int64 // 恢复期间到达、被明确拒绝的跨度
	lateLog []LateRecord
	notes   []string
	// testPreCommitHook 仅用于测试：Load 完成解析、提交新状态之前调用。
	testPreCommitHook func()
}

// window 是窗口的内部形态。
//
// sizeNs/graceNs 随存档持久化：从旧配置存档恢复的历史窗口保留各自的原始口径，
// 不被当前配置改写；当前进程新建窗口的 sizeNs/graceNs 恒等于当前配置。
type window struct {
	startNs  int64
	sizeNs   int64
	graceNs  int64
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
		ready:   true,
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
// 返回 nil 表示已计入（或识别为重复）；返回非 nil 表示被明确处置，调用方应记录。
func (w *WindowExporter) Observe(sp span.Span) *span.Rejection {
	now := w.now()
	curIdx := w.indexOf(now)
	newIdx := w.indexOf(sp.Start)
	if newIdx > curIdx {
		// 起点落在当前窗口之后：不猜测归属、不挤压当前窗口，明确拒绝并留痕。
		start := w.windowStart(newIdx)
		return w.recordOutOfBounds(now, sp, span.ReasonFutureWindow, "跨度起点晚于当前窗口",
			start, w.cfg.Size)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.ready {
		// 恢复未完成：不写入任何半成品窗口，明确拒绝并计数。
		w.recover++
		return &span.Rejection{Reason: span.ReasonRecoveryInProgress,
			Detail: "窗口状态恢复中，暂不接受跨度"}
	}
	k := sp.TraceID + "/" + sp.SpanID
	// 跨网格/跨窗口全局去重：重启后重复上报、窗口口径切换都不会重复计数。
	for _, win := range w.windows {
		if win.seen[k] {
			w.dups++
			return nil
		}
	}
	newStartNs := w.windowStart(newIdx).UnixNano()
	cfgSizeNs := w.cfg.Size.Nanoseconds()
	// 选窗口：跨度起点落在某个仍保留的存档历史窗口内时归属该历史窗口（按存档宽限
	// 判定迟到）；历史窗口未保留（已淘汰）时不做槽位推断、不猜测归属，按下面
	// 当前配置网格的规则明确处置。其余跨度归属当前配置对齐出的新网格窗口。
	var target *window
	for _, win := range w.windows {
		if win.sizeNs == cfgSizeNs {
			continue
		}
		st := sp.Start.UnixNano()
		if st >= win.startNs && st < win.startNs+win.sizeNs {
			target = win
			break
		}
	}
	if target == nil {
		start := w.windowStart(newIdx)
		// 新网格沿用当前配置的宽限期与保留上限判定。
		if now.Sub(start.Add(w.cfg.Size)) > w.cfg.Grace {
			return w.rejectLocked(now, sp, span.ReasonLateExpired, "跨度超出迟到宽限期",
				start, w.cfg.Size)
		}
		if newIdx < curIdx-int64(w.cfg.Retain)+1 {
			return w.rejectLocked(now, sp, span.ReasonLateExpired, "归属窗口已按保留上限淘汰",
				start, w.cfg.Size)
		}
		target = w.windows[newStartNs]
		if target == nil {
			target = &window{
				startNs:  newStartNs,
				sizeNs:   cfgSizeNs,
				graceNs:  int64(w.cfg.Grace),
				services: map[string]*Stats{},
				traces:   map[string]*Stats{},
				seen:     map[string]bool{},
			}
			w.windows[newStartNs] = target
		}
	} else {
		// 历史窗口按其存档时的宽限期判定迟到，改配置不会让封板窗口重新开口。
		end := time.Unix(0, target.startNs+target.sizeNs)
		if now.Sub(end) > time.Duration(target.graceNs) {
			return w.rejectLocked(now, sp, span.ReasonLateExpired,
				"跨度超出历史窗口的存档宽限期", time.Unix(0, target.startNs),
				time.Duration(target.sizeNs))
		}
	}
	target.seen[k] = true
	addTo(target.services, sp.Service, sp)
	addTo(target.traces, sp.TraceID, sp)
	w.evictLocked()
	return nil
}

// rejectLocked 在已持锁状态下记录一次越界处置并返回拒绝结论。
func (w *WindowExporter) rejectLocked(now time.Time, sp span.Span,
	reason span.RejectionReason, msg string, start time.Time, size time.Duration) *span.Rejection {
	detail := fmt.Sprintf("%s: 归属窗口 [%s,%s) 处理时刻 %s",
		msg, start.Format(time.RFC3339Nano), start.Add(size).Format(time.RFC3339Nano),
		now.UTC().Format(time.RFC3339Nano))
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
	return &span.Rejection{Reason: reason, Detail: detail}
}

// recordOutOfBounds 在无需持锁的判定路径（未来窗口）上计数并留痕。
func (w *WindowExporter) recordOutOfBounds(now time.Time, sp span.Span,
	reason span.RejectionReason, msg string, start time.Time, size time.Duration) *span.Rejection {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rejectLocked(now, sp, reason, msg, start, size)
}

// evictLocked 只保留最近 Retain 个窗口（按绝对起点取最新），旧窗口及去重集合整体释放。
func (w *WindowExporter) evictLocked() {
	for len(w.windows) > w.cfg.Retain {
		var oldest int64
		first := true
		for ns := range w.windows {
			if first || ns < oldest {
				oldest, first = ns, false
			}
		}
		delete(w.windows, oldest)
	}
}

// View 查询包含指定时刻的窗口的一致快照。
// 优先匹配当前配置对齐窗口；否则匹配覆盖该时刻的历史窗口；恢复期间一律查不到。
func (w *WindowExporter) View(at time.Time) (WindowView, bool) {
	idx := w.indexOf(at)
	now := w.now()
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.ready {
		return WindowView{}, false
	}
	win := w.windows[w.windowStart(idx).UnixNano()]
	if win == nil {
		st := at.UnixNano()
		for _, cand := range w.windows {
			if st >= cand.startNs && st < cand.startNs+cand.sizeNs {
				win = cand
				break
			}
		}
	}
	if win == nil {
		return WindowView{}, false
	}
	return snapshotLocked(win, now), true
}

// Published 返回已封板（窗口结束且该窗口自身宽限期已过）的窗口视图，按起点升序。
// 历史窗口按存档口径判定封板，已封板结论不因当前配置变化而改变。
func (w *WindowExporter) Published() []WindowView {
	now := w.now()
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.ready {
		return nil
	}
	out := make([]WindowView, 0, len(w.windows))
	for _, win := range w.windows {
		if !now.Before(time.Unix(0, win.startNs+win.sizeNs+win.graceNs)) {
			out = append(out, snapshotLocked(win, now))
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
	if !w.ready {
		return nil
	}
	out := make([]WindowView, 0, len(w.windows))
	for _, win := range w.windows {
		out = append(out, snapshotLocked(win, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

func snapshotLocked(win *window, now time.Time) WindowView {
	start := time.Unix(0, win.startNs)
	v := WindowView{
		Start:    start,
		End:      start.Add(time.Duration(win.sizeNs)),
		Sealed:   !now.Before(time.Unix(0, win.startNs+win.sizeNs+win.graceNs)),
		Services: make(map[string]Stats, len(win.services)),
		Traces:   make(map[string]Stats, len(win.traces)),
	}
	for k, s := range win.services {
		if s == nil { // 防御：任何路径都不应让空统计项崩掉查询
			continue
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

// Metrics 返回导出器自身的处置计数：重复、超宽限期、未来窗口、恢复期间拒绝。
func (w *WindowExporter) Metrics() (dups, expired, future, recovering int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.dups, w.expired, w.future, w.recover
}

// LateLog 返回最近的越界处置记录（拷贝）。
func (w *WindowExporter) LateLog() []LateRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.ready {
		return nil
	}
	return append([]LateRecord(nil), w.lateLog...)
}

// Notes 返回恢复过程中的降级依据（拷贝），任何时刻均可读，便于拒收现场核对。
func (w *WindowExporter) Notes() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]string(nil), w.notes...)
}

// windowFormatVersion 是当前窗口快照格式版本；跨版本读取规则见 Load。
const windowFormatVersion = 2

// 历史可读版本：v1 的窗口按文件级 size_ns/grace_ns + index 还原边界。
const windowFormatV1 = 1

type windowSnapshotFile struct {
	Version int           `json:"version"`
	Windows []windowEntry `json:"windows"`
	Dups    int64         `json:"dups"`
	Expired int64         `json:"expired"`
	Future  int64         `json:"future"`
	LateLog []LateRecord  `json:"late_log"`
	SizeNs  int64         `json:"size_ns"`  // v1：文件级窗口长度；v2：保存时当前配置
	GraceNs int64         `json:"grace_ns"` // v1：文件级宽限期；v2：保存时当前配置
}

// windowEntry 是单个窗口的持久化形态。
// v2 每个窗口自带口径；v1 只写 index，读取时用文件级 size/grace 补齐。
type windowEntry struct {
	Index    int64             `json:"index,omitempty"` // v1
	StartNs  int64             `json:"start_ns,omitempty"`
	SizeNs   int64             `json:"size_ns,omitempty"`
	GraceNs  int64             `json:"grace_ns,omitempty"`
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
	if !w.ready {
		w.mu.RUnlock()
		return nil
	}
	snap := windowSnapshotFile{
		Version: windowFormatVersion,
		SizeNs:  int64(w.cfg.Size),
		GraceNs: int64(w.cfg.Grace),
		Windows: make([]windowEntry, 0, len(w.windows)),
		Dups:    w.dups,
		Expired: w.expired,
		Future:  w.future,
		LateLog: append([]LateRecord(nil), w.lateLog...),
	}
	for ns, win := range w.windows {
		e := windowEntry{
			StartNs:  ns,
			SizeNs:   win.sizeNs,
			GraceNs:  win.graceNs,
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
		return snap.Windows[i].StartNs < snap.Windows[j].StartNs
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
// 恢复全程在本地构建、最后一次性替换，ready 在恢复前置为 false，
// 对外永远查不到加载了一半的中间状态。
//
// 跨版本/跨配置/损坏数据的处置规则（每条都写入 Notes，可核对，绝不猜测归属）：
//  1. 文件整体非法 JSON 或无法读取 → 返回错误、保持不可服务状态，原文件保留待查；
//  2. 版本号未知（非 1/2），或 v1 缺少有效的文件级 size_ns（无法确定边界）
//     → 整份快照忽略，按空窗口启动，并记录依据；
//  3. 单条窗口起点非法、未按自身 size 对齐、缺 seen/services/traces
//     → 跳过该条并记录，其余条目正常恢复；
//  4. 单个统计项为空（null）、计数为负、错误数大于总数 → 丢弃该项并记录，
//     窗口其余统计仍可查询，绝不把非法值对外发布，也不带崩进程；
//  5. 窗口起点重复 → 只保留第一条，其余跳过并记录；
//  6. 窗口数超过 Retain → 只恢复最近 Retain 个，其余跳过并记录；
//  7. 存档 size/grace 与当前配置不一致 → 存档窗口按原始边界恢复为历史窗口，
//     边界、归属与封板口径保持关机前一致，并记录差异；
//  8. 处置计数为负 → 丢弃该计数并记录；LateLog 原样保留。
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
		// 规则 1：不猜测、不覆盖，保持不可服务状态，原文件保留待人工核对。
		return fmt.Errorf("窗口快照损坏: %w", err)
	}

	// 进入恢复：放行业务之前，对外不可见任何窗口。
	w.mu.Lock()
	w.ready = false
	w.mu.Unlock()

	notes := []string{}
	entries := append([]windowEntry(nil), snap.Windows...)

	switch snap.Version {
	case windowFormatVersion:
		// v2：逐条校验自带口径。
		kept := entries[:0]
		for _, e := range entries {
			if e.SizeNs <= 0 || e.StartNs < 0 || e.StartNs%e.SizeNs != 0 {
				notes = append(notes, fmt.Sprintf(
					"窗口条目 start_ns=%d size_ns=%d 起点或长度非法/未对齐，按降级规则跳过",
					e.StartNs, e.SizeNs))
				continue
			}
			if e.GraceNs < 0 {
				notes = append(notes, fmt.Sprintf(
					"窗口条目 start_ns=%d grace_ns=%d 为负，宽限期按 0 处理", e.StartNs, e.GraceNs))
				e.GraceNs = 0
			}
			kept = append(kept, e)
		}
		entries = kept
	case windowFormatV1:
		// v1：边界由文件级 size 与 index 推导；推导不出来只能整份忽略，不能猜。
		if snap.SizeNs <= 0 {
			notes = append(notes, fmt.Sprintf(
				"窗口快照 v1 缺少有效的文件级 size_ns（=%d），无法确定窗口边界，"+
					"整份快照忽略，按空窗口启动；原文件保留于 %s", snap.SizeNs, path))
			return w.finishDegradedLoad(nil, 0, 0, 0, nil, notes)
		}
		fileGrace := snap.GraceNs
		if fileGrace < 0 {
			notes = append(notes, fmt.Sprintf(
				"窗口快照 v1 grace_ns=%d 为负，宽限期按 0 处理", fileGrace))
			fileGrace = 0
		}
		converted := make([]windowEntry, 0, len(entries))
		for _, e := range entries {
			if e.Index < 0 {
				notes = append(notes, fmt.Sprintf(
					"窗口条目 index=%d 为负，按降级规则跳过", e.Index))
				continue
			}
			e.StartNs = e.Index * snap.SizeNs
			e.SizeNs = snap.SizeNs
			e.GraceNs = fileGrace
			converted = append(converted, e)
		}
		entries = converted
	default:
		notes = append(notes, fmt.Sprintf(
			"窗口快照版本 %d 为未知版本（当前支持 %d/%d），整份快照忽略，"+
				"按空窗口启动；原文件保留于 %s",
			snap.Version, windowFormatV1, windowFormatVersion, path))
		return w.finishDegradedLoad(nil, 0, 0, 0, nil, notes)
	}

	loaded := map[int64]*window{}
	for _, e := range entries {
		// 规则 3：必填字段缺失 → 跳过整条。
		if e.Services == nil || e.Traces == nil || e.Seen == nil {
			notes = append(notes, fmt.Sprintf(
				"窗口条目 start_ns=%d 缺少必填字段(seen/services/traces)，按降级规则跳过",
				e.StartNs))
			continue
		}
		services := make(map[string]*Stats, len(e.Services))
		for k, st := range e.Services {
			services[k] = st
		}
		traces := make(map[string]*Stats, len(e.Traces))
		for k, st := range e.Traces {
			traces[k] = st
		}
		// 规则 4：非法/空统计项逐项丢弃，不连累整个窗口。
		sanitizeStats(services, fmt.Sprintf("窗口 start_ns=%d services", e.StartNs), &notes)
		sanitizeStats(traces, fmt.Sprintf("窗口 start_ns=%d traces", e.StartNs), &notes)
		// 规则 5：起点重复只保留第一条。
		if _, dup := loaded[e.StartNs]; dup {
			notes = append(notes, fmt.Sprintf(
				"窗口条目 start_ns=%d 与已恢复条目重复，跳过该条", e.StartNs))
			continue
		}
		win := &window{
			startNs:  e.StartNs,
			sizeNs:   e.SizeNs,
			graceNs:  e.GraceNs,
			services: services,
			traces:   traces,
			seen:     map[string]bool{},
		}
		for _, k := range e.Seen {
			win.seen[k] = true
		}
		loaded[e.StartNs] = win
	}

	// 规则 6：只保留最新 Retain 个。
	if len(loaded) > w.cfg.Retain {
		starts := make([]int64, 0, len(loaded))
		for ns := range loaded {
			starts = append(starts, ns)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		drop := len(loaded) - w.cfg.Retain
		for _, ns := range starts[:drop] {
			delete(loaded, ns)
		}
		notes = append(notes, fmt.Sprintf("快照窗口数超过保留上限 %d，跳过最旧的 %d 个",
			w.cfg.Retain, drop))
	}

	// 规则 7：配置与存档口径不一致时明确留痕；窗口按原始边界保留，绝不重新切分。
	cfgSizeNs := w.cfg.Size.Nanoseconds()
	cfgGraceNs := int64(w.cfg.Grace)
	mismatch := false
	for _, win := range loaded {
		if win.sizeNs != cfgSizeNs || win.graceNs != cfgGraceNs {
			mismatch = true
			break
		}
	}
	if mismatch {
		notes = append(notes, fmt.Sprintf(
			"当前配置 size=%s grace=%s 与存档口径不一致：%d 个存档窗口按各自原始边界/宽限期"+
				"恢复为历史窗口（边界、归属与封板结论保持关机前一致）；重启后的新跨度按当前"+
				"配置归属新窗口，两套网格互不挪动",
			w.cfg.Size, w.cfg.Grace, len(loaded)))
	}

	// 规则 8：负计数不采纳。
	dups, expired, future := snap.Dups, snap.Expired, snap.Future
	if dups < 0 {
		notes = append(notes, fmt.Sprintf("快照处置计数 dups=%d 为负，已丢弃", dups))
		dups = 0
	}
	if expired < 0 {
		notes = append(notes, fmt.Sprintf("快照处置计数 expired=%d 为负，已丢弃", expired))
		expired = 0
	}
	if future < 0 {
		notes = append(notes, fmt.Sprintf("快照处置计数 future=%d 为负，已丢弃", future))
		future = 0
	}
	lateLog := append([]LateRecord(nil), snap.LateLog...)
	if len(lateLog) > maxLateLog {
		lateLog = lateLog[len(lateLog)-maxLateLog:]
	}
	if w.testPreCommitHook != nil {
		w.testPreCommitHook()
	}
	return w.finishDegradedLoad(loaded, dups, expired, future, lateLog, notes)
}

// finishDegradedLoad 一次性替换全部状态并放行，保证对外不存在中间状态。
func (w *WindowExporter) finishDegradedLoad(loaded map[int64]*window,
	dups, expired, future int64, lateLog []LateRecord, notes []string) error {
	if loaded == nil {
		loaded = map[int64]*window{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.windows = loaded
	w.dups, w.expired, w.future = dups, expired, future
	w.lateLog = lateLog
	w.notes = append(w.notes, notes...)
	w.ready = true
	return nil
}

// sanitizeStats 丢弃空/非法统计项并逐条记录降级依据，保证其余数据可正常发布。
func sanitizeStats(m map[string]*Stats, where string, notes *[]string) {
	for k, st := range m {
		switch {
		case st == nil:
			delete(m, k)
			*notes = append(*notes, fmt.Sprintf(
				"%s 中键 %q 的统计值为空(null)，已跳过该统计项，窗口其余数据正常恢复", where, k))
		case st.Count < 0 || st.ErrorCnt < 0 || st.TotalDurNs < 0:
			delete(m, k)
			*notes = append(*notes, fmt.Sprintf(
				"%s 中键 %q 的统计含负数(count=%d,error=%d,dur=%d)，非法数据不对外发布，"+
					"已跳过该统计项", where, k, st.Count, st.ErrorCnt, st.TotalDurNs))
		case st.ErrorCnt > st.Count:
			delete(m, k)
			*notes = append(*notes, fmt.Sprintf(
				"%s 中键 %q 的错误数 %d 大于总数 %d，数据不合理，已跳过该统计项",
				where, k, st.ErrorCnt, st.Count))
		}
	}
}
