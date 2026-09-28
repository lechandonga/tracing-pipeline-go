package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// 改小窗口长度后重启：已发布窗口必须按存档的原始边界恢复，归属不挪动。
func TestRestoreSizeMismatchKeepsLegacyBoundaries(t *testing.T) {
	dir := t.TempDir()
	now := winBase.Add(2 * time.Minute)
	old := newTestWindow(2*time.Minute, 30*time.Second, 5, &now)
	// 一个 2 分钟窗口，跨度起点 70s 归属窗口 [0,120s)。
	if rej := old.Observe(wspan("t1", "s1", "svc-a", winBase.Add(70*time.Second), 100)); rej != nil {
		t.Fatal(rej)
	}
	if err := old.Save(dir); err != nil {
		t.Fatal(err)
	}
	before := old.Windows()
	if len(before) != 1 || !before[0].Start.Equal(winBase) ||
		!before[0].End.Equal(winBase.Add(2*time.Minute)) {
		t.Fatalf("unexpected pre-restart view: %+v", before)
	}

	// 新配置窗口长度改为 1 分钟：存档窗口不得按新网格重新切分。
	restored := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	if err := restored.Load(dir); err != nil {
		t.Fatal(err)
	}
	views := restored.Windows()
	if len(views) != 1 {
		t.Fatalf("legacy windows=%d, want 1", len(views))
	}
	if !views[0].Start.Equal(winBase) || !views[0].End.Equal(winBase.Add(2*time.Minute)) {
		t.Fatalf("legacy boundary moved after config change: %+v", views[0])
	}
	if views[0].Services["svc-a"].Count != 1 || views[0].Traces["t1"].TotalDurNs != 100 {
		t.Fatalf("legacy attribution changed: %+v", views[0])
	}
	// 同一跨度重报：回到原历史窗口去重，结果不翻倍。
	if rej := restored.Observe(wspan("t1", "s1", "svc-a", winBase.Add(70*time.Second), 100)); rej != nil {
		t.Fatalf("resubmit into legacy window rejected: %+v", rej)
	}
	// 宽限期内的迟到跨度（起点仍在历史窗口内）并入历史窗口，不进 1 分钟新网格。
	if rej := restored.Observe(wspan("t2", "s1", "svc-b", winBase.Add(100*time.Second), 50)); rej != nil {
		t.Fatalf("late span into legacy window rejected: %+v", rej)
	}
	v, ok := restored.View(winBase.Add(70 * time.Second))
	if !ok {
		t.Fatal("legacy window not found via View")
	}
	if v.Services["svc-a"].Count != 1 || v.Services["svc-b"].Count != 1 {
		t.Fatalf("legacy window stats wrong: %+v", v.Services)
	}
	// 新网格的 60s 边界处不得存在被“挪”出来的新窗口：61s 时刻只能命中历史窗口，
	// 且其边界仍是 [0,120s)，归属数据不变。
	cover, ok := restored.View(winBase.Add(61 * time.Second))
	if !ok || !cover.Start.Equal(winBase) || !cover.End.Equal(winBase.Add(2*time.Minute)) {
		t.Fatalf("61s must resolve to the legacy [0,120s) window, got ok=%v %+v", ok, cover)
	}
	if cover.Services["svc-a"].Count != 1 {
		t.Fatalf("legacy attribution must not move: %+v", cover.Services)
	}
	if n := len(restored.Windows()); n != 1 {
		t.Fatalf("new-grid leak window created, windows=%d", n)
	}
	// 口径差异必须留痕，可核对。
	notes := restored.Notes()
	found := false
	for _, n := range notes {
		if strings.Contains(n, "历史窗口") && strings.Contains(n, "不一致") {
			found = true
		}
	}
	if !found {
		t.Fatalf("config mismatch not recorded in notes: %v", notes)
	}
}

// 改小宽限期后重启：已封板窗口不得重新开口，迟到跨度按存档宽限期拒收。
func TestRestoreGraceMismatchKeepsSeal(t *testing.T) {
	dir := t.TempDir()
	// 旧宽限 90s：先在宽限期内计入跨度，再推进到封板。
	ingestNow := winBase.Add(120 * time.Second)
	old := newTestWindow(time.Minute, 90*time.Second, 5, &ingestNow)
	if rej := old.Observe(wspan("t1", "s1", "svc", winBase.Add(10*time.Second), 10)); rej != nil {
		t.Fatal(rej)
	}
	now := winBase.Add(160 * time.Second) // 60+90=150 已过，窗口封板
	old.now = func() time.Time { return now }
	pub := old.Published()
	if len(pub) != 1 {
		t.Fatalf("precondition: want 1 sealed, got %d", len(pub))
	}
	if err := old.Save(dir); err != nil {
		t.Fatal(err)
	}
	// 新宽限改成 1s：存档窗口仍按 90s 口径封板（now=160s 已过 150s），结论不变。
	restored := newTestWindow(time.Minute, time.Second, 5, &now)
	if err := restored.Load(dir); err != nil {
		t.Fatal(err)
	}
	pub2 := restored.Published()
	if len(pub2) != 1 || !pub2[0].Sealed ||
		!pub2[0].Start.Equal(winBase) || pub2[0].Services["svc"].Count != 1 {
		t.Fatalf("sealed window changed after grace shrink: %+v", pub2)
	}
	// 反方向：旧宽限 1s 已封板的窗口，新配置改成 90s 也不得重新开口。
	dir2 := t.TempDir()
	now2 := winBase.Add(61 * time.Second)
	old2 := newTestWindow(time.Minute, time.Second, 5, &now2)
	if rej := old2.Observe(wspan("t1", "s1", "svc", winBase.Add(10*time.Second), 10)); rej != nil {
		t.Fatal(rej)
	}
	if err := old2.Save(dir2); err != nil {
		t.Fatal(err)
	}
	restored2 := newTestWindow(time.Minute, 90*time.Second, 5, &now2)
	if err := restored2.Load(dir2); err != nil {
		t.Fatal(err)
	}
	if p := restored2.Published(); len(p) != 1 || !p[0].Sealed {
		t.Fatalf("sealed window reopened after grace enlarge: %+v", p)
	}
}

// v1 存档（index + 文件级 size_ns）在新代码下恢复：边界由存档参数推导，且与配置不一致时保留。
func TestRestoreV1ArchiveBoundaries(t *testing.T) {
	dir := t.TempDir()
	writeWindowsJSON(t, dir, map[string]any{
		"version":  windowFormatV1,
		"size_ns":  int64(2 * time.Minute),
		"grace_ns": int64(30 * time.Second),
		"dups":     2,
		"expired":  1,
		"future":   0,
		"late_log": []any{},
		"windows": []any{
			map[string]any{
				"index": 0,
				"services": map[string]any{
					"svc-a": map[string]any{"service": "svc-a", "count": 3,
						"error_count": 1, "total_dur_ns": 300},
				},
				"traces": map[string]any{
					"t1": map[string]any{"count": 3, "error_count": 1, "total_dur_ns": 300},
				},
				"seen": []any{"t1/s1"},
			},
		},
	})
	now := winBase.Add(3 * time.Minute)
	restored := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	if err := restored.Load(dir); err != nil {
		t.Fatal(err)
	}
	views := restored.Windows()
	if len(views) != 1 {
		t.Fatalf("want 1 legacy window from v1 archive, got %d", len(views))
	}
	if !views[0].Start.Equal(winBase) || !views[0].End.Equal(winBase.Add(2*time.Minute)) {
		t.Fatalf("v1 boundary wrong: %+v", views[0])
	}
	if views[0].Services["svc-a"].Count != 3 || views[0].Traces["t1"].ErrorCnt != 1 {
		t.Fatalf("v1 stats wrong: %+v", views[0])
	}
	if !views[0].Sealed {
		t.Fatal("v1 window should be sealed under archived grace")
	}
	dups, expired, _, _ := restored.Metrics()
	if dups != 2 || expired != 1 {
		t.Fatalf("counters not restored: dups=%d expired=%d", dups, expired)
	}
}

// 损坏/非法存档：不崩溃，非法项被跳过且降级依据可核对，其余窗口照常查询。
func TestRestoreCorruptAndIllegalArchiveDegraded(t *testing.T) {
	dir := t.TempDir()
	writeWindowsJSON(t, dir, map[string]any{
		"version": windowFormatVersion,
		"windows": []any{
			// 0: 统计值为 null（历史上直接崩查询）。
			map[string]any{
				"start_ns": int64(0), "size_ns": int64(time.Minute), "grace_ns": int64(0),
				"services": map[string]any{"svc-null": nil},
				"traces": map[string]any{
					"t-good": map[string]any{"count": 2, "error_count": 0, "total_dur_ns": 20},
				},
				"seen": []any{"t/s"},
			},
			// 1: 服务统计出现负数，追踪统计错误数大于总数。
			map[string]any{
				"start_ns": int64(time.Minute), "size_ns": int64(time.Minute), "grace_ns": int64(0),
				"services": map[string]any{
					"svc-neg": map[string]any{"count": -5, "error_count": 0, "total_dur_ns": 10},
					"svc-ok":  map[string]any{"count": 1, "error_count": 0, "total_dur_ns": 10},
				},
				"traces": map[string]any{
					"t-bad": map[string]any{"count": 1, "error_count": 3, "total_dur_ns": 10},
				},
				"seen": []any{"t2/s1"},
			},
			// 2: 起点未按 size 对齐 → 整条跳过。
			map[string]any{
				"start_ns": int64(17 * time.Second), "size_ns": int64(time.Minute),
				"grace_ns": int64(0),
				"services": map[string]any{}, "traces": map[string]any{}, "seen": []any{},
			},
			// 3: 缺 seen → 整条跳过。
			map[string]any{
				"start_ns": int64(2 * time.Minute), "size_ns": int64(time.Minute),
				"grace_ns": int64(0),
				"services": map[string]any{}, "traces": map[string]any{},
			},
			// 4: 起点重复（与窗口 0 相同）→ 跳过。
			map[string]any{
				"start_ns": int64(0), "size_ns": int64(time.Minute), "grace_ns": int64(0),
				"services": map[string]any{}, "traces": map[string]any{}, "seen": []any{},
			},
		},
		"dups": -3, // 负计数 → 丢弃
	})
	now := winBase.Add(10 * time.Minute)
	w := newTestWindow(time.Minute, 0, 10, &now)
	if err := w.Load(dir); err != nil {
		t.Fatalf("illegal entries must degrade, not error: %v", err)
	}
	// 进程不能崩：所有查询都要能跑完。
	views := w.Windows()
	pub := w.Published()
	if _, ok := w.View(winBase); !ok {
		t.Fatal("window 0 should still be queryable")
	}
	_ = pub
	if len(views) != 2 {
		t.Fatalf("want 2 recovered windows (illegal entries skipped), got %d: %+v", len(views), views)
	}
	v0 := views[0]
	if _, exists := v0.Services["svc-null"]; exists {
		t.Fatal("null stats must not be published")
	}
	if v0.Traces["t-good"].Count != 2 {
		t.Fatalf("good stats alongside null entry must survive: %+v", v0.Traces)
	}
	v1 := views[1]
	if _, exists := v1.Services["svc-neg"]; exists {
		t.Fatal("negative count must not be published")
	}
	if v1.Services["svc-ok"].Count != 1 {
		t.Fatalf("valid sibling stats must survive: %+v", v1.Services)
	}
	if _, exists := v1.Traces["t-bad"]; exists {
		t.Fatal("error_count>count must not be published")
	}
	if dups, _, _, _ := w.Metrics(); dups != 0 {
		t.Fatalf("negative counter must be dropped, got %d", dups)
	}
	notes := w.Notes()
	if len(notes) < 4 {
		t.Fatalf("degradation decisions must be auditable, notes=%v", notes)
	}

	// 整体非法 JSON：返回错误，原文件保留，不能带猜测数据服务。
	if err := os.WriteFile(filepath.Join(dir, "windows.json"), []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	w2 := newTestWindow(time.Minute, 0, 10, &now)
	if err := w2.Load(dir); err == nil {
		t.Fatal("corrupt JSON must return error")
	}
	// 失败后保持不可服务：查询拿不到任何窗口。
	if len(w2.Windows()) != 0 {
		t.Fatal("after failed load no window may be visible")
	}
	if _, err := os.Stat(filepath.Join(dir, "windows.json")); err != nil {
		t.Fatalf("corrupt original file must be kept for inspection: %v", err)
	}
}

// 聚合存档中的空/负数统计不崩溃、不发布，并留下可核对依据。
func TestRestoreAggregatorIllegalStats(t *testing.T) {
	dir := t.TempDir()
	raw := map[string]any{
		"seen": []any{"t1/s1", "t2/s1"},
		"by_service": map[string]any{
			"svc-null": nil,
			"svc-neg":  map[string]any{"count": -1, "error_count": 0, "total_dur_ns": 0},
			"svc-ok":   map[string]any{"service": "svc-ok", "count": 4, "error_count": 1, "total_dur_ns": 400},
		},
		"by_trace": map[string]any{
			"t1": map[string]any{"count": 4, "error_count": 1, "total_dur_ns": 400},
		},
	}
	data, _ := json.Marshal(raw)
	if err := os.WriteFile(filepath.Join(dir, "aggregate.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	a := NewAggregator()
	if err := a.Load(dir); err != nil {
		t.Fatalf("illegal stats should degrade, not error: %v", err)
	}
	snap := a.Snapshot()
	if _, exists := snap["svc-null"]; exists {
		t.Fatal("null aggregate must not be published")
	}
	if _, exists := snap["svc-neg"]; exists {
		t.Fatal("negative aggregate must not be published")
	}
	if snap["svc-ok"].Count != 4 {
		t.Fatalf("valid aggregate must survive: %+v", snap)
	}
	if st, ok := a.TraceStats("t1"); !ok || st.Count != 4 {
		t.Fatalf("trace stats wrong: %+v ok=%v", st, ok)
	}
	if len(a.Notes()) < 2 {
		t.Fatalf("aggregator degradation must be auditable: %v", a.Notes())
	}
}

// 恢复进行中的查询拿不到半成品；恢复完成后结果原子出现。
func TestRestoreNoIntermediateStateWindow(t *testing.T) {
	dir := t.TempDir()
	now := winBase.Add(5*time.Minute + 2*time.Second)
	old := newTestWindow(time.Minute, 10*time.Minute, 10, &now)
	for i := 0; i < 5; i++ {
		if rej := old.Observe(wspan("t", "s"+string(rune('a'+i)), "svc",
			winBase.Add(time.Duration(i)*time.Minute+time.Second), 10)); rej != nil {
			t.Fatal(rej)
		}
	}
	if err := old.Save(dir); err != nil {
		t.Fatal(err)
	}

	// 通过钩子让 Load 在解析完成、提交状态前停住，并发查询必须始终看到 0 或完整 5 个。
	w := NewWindowExporter(WindowConfig{Size: time.Minute, Retain: 10, Grace: 10 * time.Minute})
	w.now = func() time.Time { return now }
	release := make(chan struct{})
	entered := make(chan struct{})
	w.testPreCommitHook = func() {
		close(entered)
		<-release
	}
	var done atomic.Bool
	go func() {
		if err := w.Load(dir); err != nil {
			t.Errorf("load: %v",
				err)
		}
		done.Store(true)
	}()
	<-entered
	// 恢复中：任何查询都不能看到部分窗口。
	for i := 0; i < 200; i++ {
		if n := len(w.Windows()); n != 0 {
			t.Fatalf("intermediate state visible during restore: %d windows", n)
		}
		if n := len(w.Published()); n != 0 {
			t.Fatalf("intermediate published state visible: %d", n)
		}
		if _, ok := w.View(winBase); ok {
			t.Fatal("window visible before restore committed")
		}
		// 恢复期间写入被明确拒绝，绝不混入半成品。
		if rej := w.Observe(wspan("x", "y", "svc", now.Add(-time.Second), 1)); rej == nil ||
			rej.Reason != span.ReasonRecoveryInProgress {
			t.Fatalf("observe during restore must be rejected, got %+v", rej)
		}
	}
	close(release)
	// 等 Load 返回。
	for !done.Load() {
		time.Sleep(time.Millisecond)
	}
	if n := len(w.Windows()); n != 5 {
		t.Fatalf("post-restore windows=%d, want 5", n)
	}
	if _, _, _, recovering := w.Metrics(); recovering == 0 {
		t.Fatal("recovery-period rejections must be counted")
	}
}

// 管道级恢复闸门：Restore 全程查询为空、Submit 被拒；成功后原子可见。
func TestRestorePipelineGate(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 10, Grace: 10 * time.Minute}
	p := New(cfg)
	now := winBase.Add(85 * time.Second)
	p.win.now = func() time.Time { return now }
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 100})
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}

	p2 := New(cfg)
	p2.win.now = func() time.Time { return now }
	// 直接构造恢复中途状态：ready=false 时所有对外面必须是“空且明确”。
	p2.readyMu.Lock()
	p2.ready = false
	p2.readyMu.Unlock()
	if p2.Ready() {
		t.Fatal("expected not ready")
	}
	if rej := p2.Submit(tp(tid(2), sid(1)), span.Span{Service: "svc-b", Start: now}); rej == nil ||
		rej.Reason != span.ReasonRecoveryInProgress {
		t.Fatalf("submit during restore must be rejected, got %+v", rej)
	}
	if len(p2.Stats()) != 0 || p2.StoredSpans() != 0 || len(p2.PublishedWindows()) != 0 ||
		len(p2.RejectionLog()) != 0 || p2.Dropped() != 0 || p2.PendingOrphans() != 0 {
		t.Fatal("no half-restored state may be observable")
	}
	if _, ok := p2.WindowView(winBase); ok {
		t.Fatal("window view must be unavailable during restore")
	}
	// 恢复完成后结果原子出现。
	if err := p2.Restore(); err != nil {
		t.Fatal(err)
	}
	if !p2.Ready() {
		t.Fatal("expected ready after restore")
	}
	if n := p2.StoredSpans(); n != 1 {
		t.Fatalf("stored spans after restore=%d, want 1", n)
	}
	v, ok := p2.WindowView(winBase.Add(10 * time.Second))
	if !ok || v.Services["svc-a"].Count != 1 {
		t.Fatalf("restored window not visible atomically: %+v ok=%v", v, ok)
	}

	// 并发：Restore 持锁期间大量查询，只可能阻塞到恢复后看到完整状态，不可能看到半成品。
	p3 := New(cfg)
	p3.win.now = func() time.Time { return now }
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				n := p3.StoredSpans()
				if n != 0 && n != 1 {
					t.Errorf("intermediate store count %d", n)
					return
				}
				if wn := len(p3.PublishedWindows()); wn < 0 || wn > 1 {
					t.Errorf("intermediate window count %d", wn)
					return
				}
			}
		}()
	}
	if err := p3.Restore(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
