package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

var winBase = time.Unix(0, 0).UTC()

func newTestWindow(size, grace time.Duration, retain int, now *time.Time) *WindowExporter {
	w := NewWindowExporter(WindowConfig{Size: size, Retain: retain, Grace: grace})
	w.now = func() time.Time { return *now }
	return w
}

func wspan(trace, id, svc string, start time.Time, dur time.Duration) span.Span {
	return span.Span{TraceID: trace, SpanID: id, Service: svc, Start: start, Duration: dur}
}

func TestWindowLateAndOutOfOrder(t *testing.T) {
	now := winBase.Add(89 * time.Second) // 当前窗口 idx=1，窗口 0 尚在宽限期
	w := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	// 乱序：先报当前窗口的跨度，再报上一窗口的迟到跨度（宽限期内）。
	if rej := w.Observe(wspan("t1", "s1", "svc-a", winBase.Add(70*time.Second), 100)); rej != nil {
		t.Fatalf("current-window span rejected: %+v", rej)
	}
	if rej := w.Observe(wspan("t1", "s2", "svc-b", winBase.Add(10*time.Second), 200)); rej != nil {
		t.Fatalf("late span within grace rejected: %+v", rej)
	}
	v0, ok := w.View(winBase.Add(10 * time.Second))
	if !ok {
		t.Fatal("window 0 missing")
	}
	if v0.Services["svc-b"].Count != 1 || v0.Traces["t1"].TotalDurNs != 200 {
		t.Fatalf("late span not merged into its own window: %+v", v0)
	}
	v1, ok := w.View(winBase.Add(70 * time.Second))
	if !ok || v1.Services["svc-a"].Count != 1 {
		t.Fatalf("window 1 wrong: %+v ok=%v", v1, ok)
	}
	// 窗口口径不随到达先后改变：迟到跨度不得落入当前窗口。
	if v1.Services["svc-b"].Count != 0 {
		t.Fatal("late span leaked into current window")
	}
	if v0.Sealed || v1.Sealed {
		t.Fatal("windows should not be sealed yet")
	}
	// 推进到宽限期之后（90s），窗口 0 封板并出现在已发布列表。
	now = winBase.Add(91 * time.Second)
	pub := w.Published()
	if len(pub) != 1 || !pub[0].Start.Equal(winBase) || !pub[0].Sealed {
		t.Fatalf("published=%+v, want only sealed window 0", pub)
	}
}

func TestWindowGraceBoundary(t *testing.T) {
	now := winBase.Add(90 * time.Second) // 窗口 0 宽限期恰好结束
	w := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	// 迟到量 == 宽限期：仍应并入归属窗口。
	if rej := w.Observe(wspan("t1", "s1", "svc", winBase.Add(59*time.Second), 10)); rej != nil {
		t.Fatalf("span at grace boundary rejected: %+v", rej)
	}
	// 超出宽限期 1ns：明确拒绝，不计入任何窗口。
	now = now.Add(time.Nanosecond)
	rej := w.Observe(wspan("t1", "s2", "svc", winBase.Add(59*time.Second), 10))
	if rej == nil || rej.Reason != span.ReasonLateExpired {
		t.Fatalf("expected late_beyond_grace, got %+v", rej)
	}
	_, expired, _, _ := w.Metrics()
	if expired != 1 {
		t.Fatalf("expired=%d, want 1", expired)
	}
	if log := w.LateLog(); len(log) != 1 || log[0].SpanID != "s2" || log[0].Detail == "" {
		t.Fatalf("late log missing evidence: %+v", log)
	}
	v, _ := w.View(winBase)
	if v.Services["svc"].Count != 1 {
		t.Fatalf("expired span must not be counted: %+v", v.Services["svc"])
	}
}

func TestWindowFutureSpanRejected(t *testing.T) {
	now := winBase.Add(10 * time.Second)
	w := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	rej := w.Observe(wspan("t1", "s1", "svc", winBase.Add(70*time.Second), 10))
	if rej == nil || rej.Reason != span.ReasonFutureWindow {
		t.Fatalf("expected future_window, got %+v", rej)
	}
	_, _, future, _ := w.Metrics()
	if future != 1 {
		t.Fatalf("future=%d, want 1", future)
	}
	if len(w.Windows()) != 0 {
		t.Fatal("future span must not create a window")
	}
}

func TestWindowDuplicateNotDoubleCounted(t *testing.T) {
	now := winBase.Add(10 * time.Second)
	w := newTestWindow(time.Minute, 30*time.Second, 5, &now)
	sp := wspan("t1", "s1", "svc", winBase.Add(5*time.Second), 100)
	w.Observe(sp)
	w.Observe(sp) // 重复上报
	// 翻窗后再次重复上报（窗口仍保留）。
	now = winBase.Add(70 * time.Second)
	w.Observe(sp)
	dups, _, _, _ := w.Metrics()
	if dups != 2 {
		t.Fatalf("dups=%d, want 2", dups)
	}
	v, _ := w.View(winBase)
	if v.Services["svc"].Count != 1 || v.Services["svc"].TotalDurNs != 100 {
		t.Fatalf("duplicate double counted: %+v", v.Services["svc"])
	}
}

func TestWindowRolloverAndEviction(t *testing.T) {
	now := winBase
	w := newTestWindow(time.Minute, 30*time.Second, 2, &now)
	for i := 0; i < 4; i++ {
		now = winBase.Add(time.Duration(i) * time.Minute)
		if rej := w.Observe(wspan("t", fmt.Sprintf("s%d", i), "svc", now.Add(time.Second), 10)); rej != nil {
			t.Fatalf("span %d rejected: %+v", i, rej)
		}
	}
	views := w.Windows()
	if len(views) != 2 {
		t.Fatalf("retained windows=%d, want 2", len(views))
	}
	if !views[0].Start.Equal(winBase.Add(2*time.Minute)) || !views[1].Start.Equal(winBase.Add(3*time.Minute)) {
		t.Fatalf("wrong windows retained: %+v", views)
	}
	// 已淘汰窗口的迟到跨度：明确拒绝（超宽限期）。
	rej := w.Observe(wspan("t", "late", "svc", winBase.Add(5*time.Second), 10))
	if rej == nil || rej.Reason != span.ReasonLateExpired {
		t.Fatalf("expected late_beyond_grace for evicted window, got %+v", rej)
	}
	// 宽限期大于保留范围时，归属窗口已淘汰也要明确拒绝。
	now = winBase.Add(3 * time.Minute)
	w2 := newTestWindow(time.Minute, 10*time.Minute, 2, &now)
	rej = w2.Observe(wspan("t", "old", "svc", winBase.Add(5*time.Second), 10))
	if rej == nil || rej.Reason != span.ReasonLateExpired {
		t.Fatalf("expected rejection for evicted-range window, got %+v", rej)
	}
	if len(w2.Windows()) != 0 {
		t.Fatal("rejected span must not create a window")
	}
}

func TestWindowConcurrentQueryAndWrite(t *testing.T) {
	// 原子时钟：写入、查询、窗口滚动并发进行。
	var nowNs atomic.Int64
	nowNs.Store(winBase.Add(10 * time.Second).UnixNano())
	// 保留上限大于测试期间产生的窗口数，淘汰不影响最终对账。
	w := NewWindowExporter(WindowConfig{Size: time.Minute, Retain: 10, Grace: 30 * time.Second})
	w.now = func() time.Time { return time.Unix(0, nowNs.Load()).UTC() }
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				n := time.Unix(0, nowNs.Load()).UTC()
				// 起点贴近当前时刻，落在当前或紧邻的上一窗口（保留且在宽限期内）。
				w.Observe(wspan(fmt.Sprintf("t%d", g), fmt.Sprintf("s%d", i), "svc",
					n.Add(-time.Duration(i%5)*time.Second), 10))
			}
		}(g)
	}
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				w.View(winBase)
				w.Published()
				w.Windows()
				nowNs.Add(int64(time.Second)) // 查询期间窗口持续滚动、淘汰
			}
		}()
	}
	wg.Wait()
	var total int64
	for _, v := range w.Windows() {
		for _, c := range v.Services {
			total += c.Count
		}
	}
	dups, expired, future, _ := w.Metrics()
	if got := total + dups + expired + future; got != 4*200 {
		t.Fatalf("counted=%d dups=%d expired=%d future=%d, want sum=%d",
			total, dups, expired, future, 4*200)
	}
	if len(w.Windows()) > 10 {
		t.Fatalf("retention not bounded: %d windows", len(w.Windows()))
	}
}

func TestWindowRestartConsistency(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 3, Grace: 45 * time.Second}
	submitNow := winBase.Add(85 * time.Second) // 窗口 0 尚在宽限期，迟到跨度可并入
	p := New(cfg)
	p.win.now = func() time.Time { return submitNow }
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 100})
	p.Submit(tp(tid(2), sid(1)), span.Span{Service: "svc-b", Start: winBase.Add(70 * time.Second), Duration: 200})
	now := winBase.Add(125 * time.Second) // 推进后窗口 0 已封板，窗口 1 未封板
	p.win.now = func() time.Time { return now }
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}
	before := p.PublishedWindows()
	if len(before) != 1 {
		t.Fatalf("published before restart=%d, want 1", len(before))
	}
	// 模拟重启。
	p2 := New(cfg)
	p2.win.now = func() time.Time { return now }
	if err := p2.Restore(); err != nil {
		t.Fatal(err)
	}
	after := p2.PublishedWindows()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("published windows differ after restart:\nbefore=%+v\nafter=%+v", before, after)
	}
	// 重启后同一批跨度重新上报：窗口结果不得翻倍。
	p2.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 100})
	v, ok := p2.WindowView(winBase.Add(10 * time.Second))
	if !ok || v.Services["svc-a"].Count != 1 {
		t.Fatalf("window result doubled after restart+resubmit: %+v", v.Services["svc-a"])
	}
	// 重启后宽限期内的迟到跨度仍并入归属窗口（窗口 1，迟到 5s < 45s）。
	p2.Submit(tp(tid(3), sid(1)), span.Span{Service: "svc-c", Start: winBase.Add(80 * time.Second), Duration: 50})
	v, _ = p2.WindowView(winBase.Add(60 * time.Second))
	if v.Services["svc-c"].Count != 1 {
		t.Fatalf("late span after restart not merged: %+v", v.Services)
	}
}

func TestWindowLoadDegradation(t *testing.T) {
	dir := t.TempDir()
	cfg := WindowConfig{Size: time.Minute, Retain: 2, Grace: 30 * time.Second}
	// 未知版本：整份忽略并留痕。
	writeWindowsJSON(t, dir, map[string]any{"version": 99, "windows": []any{}})
	w := NewWindowExporter(cfg)
	if err := w.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(w.Windows()) != 0 || len(w.Notes()) == 0 {
		t.Fatalf("unknown version should be ignored with note, windows=%d notes=%v",
			len(w.Windows()), w.Notes())
	}
	// v1 快照：必须带文件级 size_ns 才能确定边界；缺少时整份忽略并留痕，绝不猜测归属。
	writeWindowsJSON(t, dir, map[string]any{
		"version": windowFormatV1,
		"windows": []any{
			map[string]any{"index": 0, "services": map[string]any{}, "traces": map[string]any{},
				"seen": []any{}},
		},
	})
	w1b := NewWindowExporter(cfg)
	if err := w1b.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(w1b.Windows()) != 0 {
		t.Fatalf("v1 without size_ns must be fully ignored, got %d windows", len(w1b.Windows()))
	}
	// 单条缺 seen：跳过该条并留痕，其余正常恢复（v1 带 size_ns/grace_ns）。
	writeWindowsJSON(t, dir, map[string]any{
		"version":  windowFormatV1,
		"size_ns":  int64(time.Minute),
		"grace_ns": int64(30 * time.Second),
		"windows": []any{
			map[string]any{"index": 1, "services": map[string]any{}, "traces": map[string]any{}},
			map[string]any{"index": 2, "services": map[string]any{}, "traces": map[string]any{},
				"seen": []any{"t/s"}},
		},
	})
	w2 := NewWindowExporter(cfg)
	if err := w2.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(w2.Windows()) != 1 || len(w2.Notes()) == 0 {
		t.Fatalf("entry missing seen should be skipped with note: windows=%d notes=%v",
			len(w2.Windows()), w2.Notes())
	}
	// 整体非法 JSON：返回错误，不得猜测。
	if err := os.WriteFile(filepath.Join(dir, "windows.json"), []byte("{bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := NewWindowExporter(cfg).Load(dir); err == nil {
		t.Fatal("corrupt snapshot must return error")
	}
}

func writeWindowsJSON(t *testing.T, dir string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "windows.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineWindowLateObservable(t *testing.T) {
	cfg := testConfig("")
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 2, Grace: 30 * time.Second}
	now := winBase.Add(10 * time.Minute)
	p := New(cfg)
	p.win.now = func() time.Time { return now }
	// 远超出宽限期的跨度：存储与聚合仍确认，但窗口侧明确拒绝且可观测。
	rej := p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc", Start: winBase.Add(5 * time.Second)})
	if rej == nil || rej.Reason != span.ReasonLateExpired {
		t.Fatalf("expected late_beyond_grace, got %+v", rej)
	}
	if p.RejectionCounts()[span.ReasonLateExpired] != 1 {
		t.Fatal("late rejection not observable in pipeline counts")
	}
	if len(p.WindowLateLog()) != 1 {
		t.Fatal("late log not exposed via pipeline")
	}
	if got := p.Stats()["svc"].Count; got != 1 {
		t.Fatalf("aggregate count=%d, want 1 (window rejection独立于聚合)", got)
	}
}
