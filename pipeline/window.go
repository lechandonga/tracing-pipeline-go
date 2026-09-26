package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// 处置原因：迟到/重复数据不会被静默丢弃，也不会被算进当前窗口，
// 而是记录为可观测、可解释的处置结果。
const (
	DisposeTooLate       = "too_late"       // 超出宽限期，归属窗口已关闭
	DisposeWindowEvicted = "window_evicted" // 归属窗口已按保留上限淘汰
	DisposeDuplicate     = "duplicate"      // 重复上报，窗口内已计数
	DisposeClockRewind   = "clock_rewind"   // 跨度起点晚于当前时钟，无法归属
)

const (
	windowFormatVersion = 1
	maxDisposalLog      = 1000
	maxDegradationLog   = 100
)

// WindowConfig 配置滚动时间窗口导出。
//
// 窗口按跨度 Start 时间向下对齐到 Size 的整数倍归属，口径与到达先后无关。
// 窗口在 End+Grace 之前允许迟到数据并入，之后关闭；历史窗口按 Retain 上限淘汰。
type WindowConfig struct {
	Size   time.Duration // 窗口长度，<=0 表示禁用窗口导出
	Retain int           // 保留的窗口个数（含当前窗口），<=0 取 1
	Grace  time.Duration // 迟到数据宽限期（从窗口结束起算），<0 取 0
}

// WindowView 是某个窗口对外发布的一致视图（拷贝，后续写入不影响已返回结果）。
type WindowView struct {
	Start     time.Time
	End       time.Time
	Closed    bool // 是否已过宽限期（之后到达的归属跨度将被处置而非并入）
	ByService map[string]Stats
	ByTrace   map[string]Stats
}

// WindowMeta 描述一个保留中的窗口的元数据。
type WindowMeta struct {
	Start  time.Time
	End    time.Time
	Closed bool
	Spans  int64
}

// Disposal 记录一条被明确处置的跨度及其判定依据。
type Disposal struct {
	Reason  string
	SpanKey string
	Window  time.Time // 它本应归属的窗口起点
	At      time.Time // 到达时刻
	Detail  string
}

// window 是单个窗口的内部状态；seen 随窗口一起淘汰，去重结构占用有界。
type window struct {
	start     time.Time
	seen      map[string]bool
	byService map[string]*Stats
	byTrace   map[string]*Stats
}

func newWindow(start time.Time) *window {
	return &window{
		start:     start,
		seen:      map[string]bool{},
		byService: map[string]*Stats{},
		byTrace:   map[string]*Stats{},
	}
}

// WindowExporter 按固定时间窗口滚动产出服务/追踪维度统计。
//
// 规则：
//  1. 归属口径只看跨度 Start 对齐到的窗口，不随到达先后改变；
//     宽限期内的迟到跨度并入原窗口，超宽限期按 too_late 明确处置。
//  2. 每个窗口自带 seen 去重集，随窗口淘汰；重复上报不会二次计数
//     （补齐释放、翻窗、重启后重报都受此与上游 Store/Aggregator 双重约束）。
//  3. 历史窗口按 Retain 淘汰；淘汰、切换与查询都在同一把锁内完成，
//     查询拿到的是拷贝，不会观察到只更新了一半的窗口，也不会长时间阻塞写入。
//  4. Save/Load 原子落盘与换入：恢复完成前外部看到的始终是旧完整状态或空状态。
//  5. 快照带版本；缺失/非法/未知字段与未知版本按明确降级规则处理并记录依据。
type WindowExporter struct {
	mu   sync.Mutex
	cfg  WindowConfig
	now  func() time.Time
	wins map[int64]*window // 窗口起点 UnixNano -> 窗口

	disposed    map[string]int64 // 处置原因 -> 计数
	disposalLog []Disposal       // 有界处置日志
	degraded    []string         // 恢复降级记录（有界）
}

// NewWindowExporter 创建窗口导出器；now 为时钟（可注入以便测试），nil 取墙钟。
func NewWindowExporter(cfg WindowConfig, now func() time.Time) *WindowExporter {
	if cfg.Retain <= 0 {
		cfg.Retain = 1
	}
	if cfg.Grace < 0 {
		cfg.Grace = 0
	}
	if now == nil {
		now = time.Now
	}
	return &WindowExporter{
		cfg:      cfg,
		now:      now,
		wins:     map[int64]*window{},
		disposed: map[string]int64{},
	}
}

// Enabled 报告窗口导出是否启用。
func (e *WindowExporter) Enabled() bool { return e != nil && e.cfg.Size > 0 }

func (e *WindowExporter) align(t time.Time) time.Time {
	// UnixNano 截断避免 Truncate 对亚纳秒/单调钟的边界问题。
	return time.Unix(0, t.UnixNano()-(t.UnixNano()%int64(e.cfg.Size))).UTC()
}

// evictLocked 按当前时钟与 Retain 淘汰过旧窗口；调用方持锁。
func (e *WindowExporter) evictLocked() {
	cur := e.align(e.now())
	cutoff := cur.Add(-time.Duration(e.cfg.Retain-1) * e.cfg.Size)
	for idx, w := range e.wins {
		if w.start.Before(cutoff) {
			delete(e.wins, idx)
		}
	}
}

// Observe 将一条已确认（已采样保留、已通过上游去重）跨度并入其归属窗口。
// 返回 false 表示该跨度被明确处置，可通过 Disposals 核对依据。
func (e *WindowExporter) Observe(sp span.Span) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.now()
	st := sp.Start
	if st.IsZero() {
		st = now // 缺失起点不猜测历史归属，按到达时刻计入当前窗口
	}
	st = st.UTC()
	if st.After(now) {
		// 时钟倒挂/未来时间：无法可信归属，不悄悄算进当前窗口。
		e.disposeLocked(DisposeClockRewind, sp, time.Time{}, now,
			"跨度起点晚于当前时钟")
		return false
	}
	ws := e.align(st)
	key := sp.TraceID + "/" + sp.SpanID

	e.evictLocked()

	w := e.wins[ws.UnixNano()]
	if w == nil {
		cur := e.align(now)
		cutoff := cur.Add(-time.Duration(e.cfg.Retain-1) * e.cfg.Size)
		switch {
		case ws.Before(cutoff):
			e.disposeLocked(DisposeWindowEvicted, sp, ws, now,
				fmt.Sprintf("归属窗口 %s 已超出保留上限 %d", ws.Format(time.RFC3339), e.cfg.Retain))
			return false
		case !now.Before(ws.Add(e.cfg.Size).Add(e.cfg.Grace)):
			e.disposeLocked(DisposeTooLate, sp, ws, now,
				fmt.Sprintf("超出宽限期 %s（窗口 %s 已于 %s 关闭）",
					e.cfg.Grace, ws.Format(time.RFC3339),
					ws.Add(e.cfg.Size).Add(e.cfg.Grace).Format(time.RFC3339)))
			return false
		}
		w = newWindow(ws)
		e.wins[ws.UnixNano()] = w
	} else if !now.Before(ws.Add(e.cfg.Size).Add(e.cfg.Grace)) {
		// 窗口仍存在但已关闭：迟到跨度不得再并入。
		e.disposeLocked(DisposeTooLate, sp, ws, now,
			fmt.Sprintf("超出宽限期 %s（窗口 %s 已于 %s 关闭）",
				e.cfg.Grace, ws.Format(time.RFC3339),
				ws.Add(e.cfg.Size).Add(e.cfg.Grace).Format(time.RFC3339)))
		return false
	} else if w.seen[key] {
		// 防御性窗口内去重：即使上游重放也不会二次计数。
		e.disposeLocked(DisposeDuplicate, sp, ws, now, "窗口内已计数")
		return false
	}
	w.seen[key] = true
	addTo(w.byService, sp.Service, sp)
	addTo(w.byTrace, sp.TraceID, sp)
	return true
}

func (e *WindowExporter) disposeLocked(reason string, sp span.Span, ws, at time.Time, detail string) {
	e.disposed[reason]++
	if len(e.disposalLog) < maxDisposalLog {
		e.disposalLog = append(e.disposalLog, Disposal{
			Reason:  reason,
			SpanKey: sp.TraceID + "/" + sp.SpanID,
			Window:  ws,
			At:      at.UTC(),
			Detail:  detail,
		})
	}
}

// Query 返回指定窗口起点（自动对齐到窗口边界）的一致视图。
// 已淘汰或从未建立的窗口返回 false；视图为拷贝，不阻塞也不受后续写入影响。
func (e *WindowExporter) Query(start time.Time) (WindowView, bool) {
	if !e.Enabled() {
		return WindowView{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	ws := e.align(start.UTC())
	w := e.wins[ws.UnixNano()]
	if w == nil {
		return WindowView{}, false
	}
	closed := !e.now().Before(ws.Add(e.cfg.Size).Add(e.cfg.Grace))
	sv := map[string]Stats{}
	for k, v := range w.byService {
		sv[k] = *v
	}
	tv := map[string]Stats{}
	for k, v := range w.byTrace {
		tv[k] = *v
	}
	return WindowView{
		Start:     ws,
		End:       ws.Add(e.cfg.Size),
		Closed:    closed,
		ByService: sv,
		ByTrace:   tv,
	}, true
}

// Windows 返回当前保留中的窗口元数据（按起点升序）。
func (e *WindowExporter) Windows() []WindowMeta {
	if !e.Enabled() {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]WindowMeta, 0, len(e.wins))
	for _, w := range e.wins {
		out = append(out, WindowMeta{
			Start:  w.start,
			End:    w.start.Add(e.cfg.Size),
			Closed: !e.now().Before(w.start.Add(e.cfg.Size).Add(e.cfg.Grace)),
			Spans:  int64(len(w.seen)),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// Disposals 返回处置计数（拷贝）与有界处置日志（拷贝）。
func (e *WindowExporter) Disposals() (map[string]int64, []Disposal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	counts := make(map[string]int64, len(e.disposed))
	for k, v := range e.disposed {
		counts[k] = v
	}
	log := append([]Disposal(nil), e.disposalLog...)
	return counts, log
}

// Degradations 返回恢复阶段的降级记录（可核对依据）。
func (e *WindowExporter) Degradations() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.degraded...)
}

func (e *WindowExporter) recordDegradation(msg string) {
	if len(e.degraded) < maxDegradationLog {
		e.degraded = append(e.degraded, msg)
	}
}

// ---- 持久化 -------------------------------------------------------------

type windowEntryJSON struct {
	StartUnixNano int64             `json:"start_unix_nano"`
	Seen          []string          `json:"seen"`
	ByService     map[string]*Stats `json:"by_service"`
	ByTrace       map[string]*Stats `json:"by_trace"`
}

type windowSnapshotJSON struct {
	Version  int               `json:"version"`
	Windows  []windowEntryJSON `json:"windows"`
	Disposed map[string]int64  `json:"disposed,omitempty"`
}

// Save 将窗口状态原子落盘（临时文件 + rename）。
func (e *WindowExporter) Save(dir string) error {
	if dir == "" || !e.Enabled() {
		return nil
	}
	e.mu.Lock()
	snap := windowSnapshotJSON{
		Version:  windowFormatVersion,
		Windows:  make([]windowEntryJSON, 0, len(e.wins)),
		Disposed: make(map[string]int64, len(e.disposed)),
	}
	for _, w := range e.wins {
		en := windowEntryJSON{
			StartUnixNano: w.start.UnixNano(),
			Seen:          make([]string, 0, len(w.seen)),
			ByService:     w.byService,
			ByTrace:       w.byTrace,
		}
		for k := range w.seen {
			en.Seen = append(en.Seen, k)
		}
		snap.Windows = append(snap.Windows, en)
	}
	sort.Slice(snap.Windows, func(i, j int) bool {
		return snap.Windows[i].StartUnixNano < snap.Windows[j].StartUnixNano
	})
	for k, v := range e.disposed {
		snap.Disposed[k] = v
	}
	e.mu.Unlock()

	data, err := json.Marshal(&snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, "windows.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "windows.json"))
}

// Load 从快照恢复。
//
// 降级规则（均留下 Degradations 记录，不猜测归属）：
//   - 文件不存在：从空状态开始（正常首启，不记降级）；
//   - JSON 非法：返回错误，由调用方决定是否启动；
//   - 版本缺失：按 v1 解读并记录；版本未知：整份快照跳过；
//   - 单条窗口记录非法（起点未对齐、计数为负等）：跳过该条并记录；
//   - 出现未知字段：已知字段照常读取（向前兼容），记录未知字段名。
//
// 状态先在本地构建，最后在锁内一次性换入，恢复完成前不暴露中间状态。
func (e *WindowExporter) Load(dir string) error {
	if dir == "" || !e.Enabled() {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, "windows.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var notes []string
	if unknown, err := unknownFields(data, windowSnapshotJSON{}); err != nil {
		return fmt.Errorf("窗口快照损坏: %w", err)
	} else if len(unknown) > 0 {
		notes = append(notes, fmt.Sprintf("windows.json 含未知字段（已忽略）: %v", unknown))
	}

	var raw struct {
		Version  int               `json:"version"`
		Windows  []windowEntryJSON `json:"windows"`
		Disposed map[string]int64  `json:"disposed"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("窗口快照损坏: %w", err)
	}
	switch raw.Version {
	case 0:
		notes = append(notes, "windows.json 缺少版本号，按 v1 解读")
	case windowFormatVersion:
	default:
		notes = append(notes, fmt.Sprintf("windows.json 版本 %d 不受支持（当前支持 <= %d），整份窗口快照已跳过",
			raw.Version, windowFormatVersion))
		e.mu.Lock()
		for _, n := range notes {
			e.recordDegradation(n)
		}
		e.mu.Unlock()
		return nil
	}

	size := int64(e.cfg.Size)
	fresh := map[int64]*window{}
	for i, en := range raw.Windows {
		if en.StartUnixNano <= 0 || en.StartUnixNano%size != 0 {
			notes = append(notes, fmt.Sprintf("第 %d 条窗口起点 %d 未对齐到窗口长度 %d，已跳过",
				i, en.StartUnixNano, size))
			continue
		}
		bad := false
		for k, v := range en.ByService {
			if v == nil || v.Count < 0 || v.ErrorCnt < 0 || v.TotalDurNs < 0 {
				notes = append(notes, fmt.Sprintf("窗口 %d 服务维度 %q 统计非法，已跳过该窗口",
					en.StartUnixNano, k))
				bad = true
				break
			}
		}
		if bad {
			continue
		}
		for k, v := range en.ByTrace {
			if v == nil || v.Count < 0 || v.ErrorCnt < 0 || v.TotalDurNs < 0 {
				notes = append(notes, fmt.Sprintf("窗口 %d 追踪维度 %q 统计非法，已跳过该窗口",
					en.StartUnixNano, k))
				bad = true
				break
			}
		}
		if bad {
			continue
		}
		w := newWindow(time.Unix(0, en.StartUnixNano).UTC())
		for _, k := range en.Seen {
			w.seen[k] = true
		}
		for k, v := range en.ByService {
			w.byService[k] = v
		}
		for k, v := range en.ByTrace {
			w.byTrace[k] = v
		}
		fresh[en.StartUnixNano] = w
	}
	disp := map[string]int64{}
	for k, v := range raw.Disposed {
		if v < 0 {
			notes = append(notes, fmt.Sprintf("处置计数 %q 为负，已忽略", k))
			continue
		}
		disp[k] = v
	}

	e.mu.Lock()
	e.wins = fresh
	e.disposed = disp
	for _, n := range notes {
		e.recordDegradation(n)
	}
	e.mu.Unlock()
	return nil
}

// unknownFields 用严格解码探测 JSON 中的未知字段名（已知字段仍按宽松规则读取）。
func unknownFields(data []byte, _ windowSnapshotJSON) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var probe windowSnapshotJSON
	if err := dec.Decode(&probe); err != nil {
		if bytes.Contains([]byte(err.Error()), []byte("unknown field")) {
			// 提取字段名：形如 `json: unknown field "xxx"`
			msg := err.Error()
			start := bytes.IndexByte([]byte(msg), '"')
			end := bytes.LastIndexByte([]byte(msg), '"')
			if start >= 0 && end > start {
				return []string{msg[start+1 : end]}, nil
			}
			return []string{"?"}, nil
		}
		return nil, err
	}
	return nil, nil
}
