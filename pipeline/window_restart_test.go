package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// 改配置重启：存档窗口边界不得被按新配置静默重新解释，
// 要么原样恢复（配置一致），要么明确拒收并留下可核对的说明。
func TestWindowConfigChangeRestartRejectsArchive(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 3, Grace: 45 * time.Second}
	now := winBase.Add(85 * time.Second) // 提交时窗口 0 尚在宽限期
	p := New(cfg)
	p.win.now = func() time.Time { return now }
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 100})
	now = winBase.Add(125 * time.Second) // 落盘时窗口 0 已封板
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if got := p.PublishedWindows(); len(got) != 1 {
		t.Fatalf("published before restart=%d, want 1", len(got))
	}

	// 重启且窗口长度改小：不得把旧 index 按 30s 重新解释成另一段时间。
	cfg2 := cfg
	cfg2.Window.Size = 30 * time.Second
	p2 := New(cfg2)
	p2.win.now = func() time.Time { return now }
	if err := p2.Restore(); err != nil {
		t.Fatal(err)
	}
	if got := p2.PublishedWindows(); len(got) != 0 {
		t.Fatalf("archived windows must be rejected on size change, got %+v", got)
	}
	notes := p2.WindowNotes()
	if len(notes) == 0 || !strings.Contains(notes[0], "size=1m0s") || !strings.Contains(notes[0], "size=30s") {
		t.Fatalf("rejection must leave auditable note with both configs, notes=%v", notes)
	}
	// 拒收后新配置正常工作：新跨度按新边界（30s）归属。
	p2.Submit(tp(tid(2), sid(1)), span.Span{Service: "svc-b", Start: winBase.Add(100 * time.Second), Duration: 50})
	v, ok := p2.WindowView(winBase.Add(100 * time.Second))
	if !ok || v.End.Sub(v.Start) != 30*time.Second || v.Services["svc-b"].Count != 1 {
		t.Fatalf("new-config window wrong after rejection: %+v ok=%v", v, ok)
	}
}

// 宽限期变化、边界字段缺失同样触发拒收；配置一致时边界与归属原样恢复。
func TestWindowLoadConfigMismatchDegradation(t *testing.T) {
	dir := t.TempDir()
	cfg := WindowConfig{Size: time.Minute, Retain: 3, Grace: 45 * time.Second}
	now := winBase.Add(10 * time.Second)
	w := NewWindowExporter(cfg)
	w.now = func() time.Time { return now }
	if rej := w.Observe(wspan("t1", "s1", "svc", winBase.Add(5*time.Second), 100)); rej != nil {
		t.Fatalf("observe rejected: %+v", rej)
	}
	if err := w.Save(dir); err != nil {
		t.Fatal(err)
	}

	// 仅宽限期变化：同样拒收并留痕（封板口径不同，不能悄悄挪）。
	w2 := NewWindowExporter(WindowConfig{Size: time.Minute, Retain: 3, Grace: 10 * time.Second})
	w2.now = func() time.Time { return now }
	if err := w2.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(w2.Windows()) != 0 {
		t.Fatalf("grace change must reject archived windows, got %+v", w2.Windows())
	}
	if notes := w2.Notes(); len(notes) == 0 || !strings.Contains(notes[0], "grace") {
		t.Fatalf("grace-change rejection must leave note, notes=%v", notes)
	}

	// 缺少 size_ns/grace_ns（老版本/外来存档）：无法核对边界，拒收并留痕。
	writeWindowsJSON(t, dir, map[string]any{
		"version": windowFormatVersion,
		"windows": []any{
			map[string]any{"index": 0, "services": map[string]any{}, "traces": map[string]any{},
				"seen": []any{"t1/s1"}},
		},
	})
	w3 := NewWindowExporter(cfg)
	w3.now = func() time.Time { return now }
	if err := w3.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(w3.Windows()) != 0 {
		t.Fatalf("missing boundary fields must reject archived windows, got %+v", w3.Windows())
	}
	if notes := w3.Notes(); len(notes) == 0 || !strings.Contains(notes[0], "size_ns") {
		t.Fatalf("missing-field rejection must leave note, notes=%v", notes)
	}

	// 配置一致：边界与归属原样恢复，跟关机前完全一致。
	if err := w.Save(dir); err != nil {
		t.Fatal(err)
	}
	w4 := NewWindowExporter(cfg)
	w4.now = func() time.Time { return now }
	if err := w4.Load(dir); err != nil {
		t.Fatal(err)
	}
	views := w4.Windows()
	if len(views) != 1 || !views[0].Start.Equal(winBase) || !views[0].End.Equal(winBase.Add(time.Minute)) {
		t.Fatalf("boundaries changed across restart: %+v", views)
	}
	if views[0].Services["svc"].Count != 1 || views[0].Services["svc"].TotalDurNs != 100 {
		t.Fatalf("attribution changed across restart: %+v", views[0].Services)
	}
}

// 损坏/非法统计值：不崩溃、不对外发布，按规则跳过并留痕，其余窗口正常查询。
func TestWindowLoadInvalidStatsDegrade(t *testing.T) {
	dir := t.TempDir()
	cfg := WindowConfig{Size: time.Minute, Retain: 5, Grace: 30 * time.Second}
	writeWindowsJSON(t, dir, map[string]any{
		"version":  windowFormatVersion,
		"size_ns":  int64(cfg.Size),
		"grace_ns": int64(cfg.Grace),
		"windows": []any{
			map[string]any{
				"index": 0,
				"services": map[string]any{
					"svc-null":         nil, // 空统计值：历史上直接让查询崩溃
					"svc-neg":          map[string]any{"count": -3, "error_count": 0, "total_dur_ns": 10},
					"svc-inconsistent": map[string]any{"count": 1, "error_count": 5, "total_dur_ns": 10},
					"svc-good":         map[string]any{"count": 2, "error_count": 1, "total_dur_ns": 100},
				},
				"traces": map[string]any{
					"t-neg":  map[string]any{"count": 1, "error_count": -1, "total_dur_ns": 5},
					"t-good": map[string]any{"count": 1, "error_count": 0, "total_dur_ns": 5},
				},
				"seen": []any{"t1/s1"},
			},
			map[string]any{
				"index":    1,
				"services": map[string]any{"svc-other": map[string]any{"count": 4, "error_count": 0, "total_dur_ns": 40}},
				"traces":   map[string]any{},
				"seen":     []any{},
			},
		},
	})
	w := NewWindowExporter(cfg)
	now := winBase.Add(10 * time.Second)
	w.now = func() time.Time { return now }
	if err := w.Load(dir); err != nil {
		t.Fatal(err)
	}
	// 查询不崩溃，非法统计不发布，合法统计保留。
	v, ok := w.View(winBase.Add(5 * time.Second))
	if !ok {
		t.Fatal("window 0 should survive partial degradation")
	}
	for _, bad := range []string{"svc-null", "svc-neg", "svc-inconsistent"} {
		if _, ok := v.Services[bad]; ok {
			t.Fatalf("invalid stat %q must not be published: %+v", bad, v.Services[bad])
		}
	}
	if v.Services["svc-good"].Count != 2 || v.Services["svc-good"].ErrorCnt != 1 {
		t.Fatalf("valid stat lost: %+v", v.Services["svc-good"])
	}
	if _, ok := v.Traces["t-neg"]; ok {
		t.Fatal("negative trace stat must not be published")
	}
	if v.Traces["t-good"].Count != 1 {
		t.Fatalf("valid trace stat lost: %+v", v.Traces)
	}
	// 其余窗口不受影响。
	v1, ok := w.View(winBase.Add(65 * time.Second))
	if !ok || v1.Services["svc-other"].Count != 4 {
		t.Fatalf("other windows must stay queryable: %+v ok=%v", v1, ok)
	}
	// 每条降级都有可核对依据：4 条非法统计各一条。
	if notes := w.Notes(); len(notes) != 4 {
		t.Fatalf("expected 4 degradation notes, got %d: %v", len(notes), notes)
	}
	// 全量遍历（含封板判定）不崩溃。
	_ = w.Windows()
	_ = w.Published()
}

// 恢复全部完成之前，对外查询拿不到任何中间状态。
func TestPipelineRestoreHidesIntermediateState(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 3, Grace: 45 * time.Second}
	now := winBase.Add(85 * time.Second)
	p := New(cfg)
	p.win.now = func() time.Time { return now }
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 100})
	now = winBase.Add(125 * time.Second)
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}

	p2 := New(cfg)
	p2.win.now = func() time.Time { return now }
	entered := make(chan struct{})
	release := make(chan struct{})
	p2.restoreHook = func(stage string) {
		if stage == "store" {
			close(entered)
			<-release // 卡在恢复中段：存储已恢复，聚合与窗口尚未恢复
		}
	}
	restoreDone := make(chan error, 1)
	go func() { restoreDone <- p2.Restore() }()
	<-entered

	// 恢复中段发起查询：必须阻塞，不得返回只恢复了一半的结果。
	queryDone := make(chan map[string]Stats, 1)
	go func() { queryDone <- p2.Stats() }()
	select {
	case got := <-queryDone:
		t.Fatalf("query returned during restore, intermediate state visible: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-restoreDone; err != nil {
		t.Fatal(err)
	}
	stats := <-queryDone
	if stats["svc-a"].Count != 1 {
		t.Fatalf("after restore, stats=%+v, want svc-a count 1", stats)
	}
	if got := p2.PublishedWindows(); len(got) != 1 || got[0].Services["svc-a"].Count != 1 {
		t.Fatalf("published after restore incomplete: %+v", got)
	}
}
