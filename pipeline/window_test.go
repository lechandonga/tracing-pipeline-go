package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

// fakeClock 是可注入的测试时钟。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var winBase = time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)

func testWindowCfg() WindowConfig {
	return WindowConfig{Size: time.Minute, Retain: 3, Grace: 10 * time.Second}
}

func wsp(trace, id int, svc string, start time.Time, dur time.Duration) span.Span {
	return span.Span{
		TraceID:  tid(trace),
		SpanID:   sid(id),
		Service:  svc,
		Start:    start,
		Duration: dur,
	}
}

func TestWindowAttributionIndependentOfArrival(t *testing.T) {
	cfg := testWindowCfg()
	cfg.Grace = 3 * time.Minute
	clk := newFakeClock(winBase.Add(2*time.Minute + 30*time.Second))
	e := NewWindowExporter(cfg, clk.now)

	// 乱序上报：先报窗口2的跨度，再报窗口0/1的跨度。
	e.Observe(wsp(1, 1, "svc-a", winBase.Add(2*time.Minute+5*time.Second), 10))
	e.Observe(wsp(1, 2, "svc-a", winBase.Add(5*time.Second), 20))
	e.Observe(wsp(2, 1, "svc-b", winBase.Add(time.Minute+3*time.Second), 30))

	v0, ok := e.Query(winBase)
	if !ok {
		t.Fatal("window 0 missing")
	}
	if v0.ByService["svc-a"].Count != 1 || v0.ByService["svc-a"].TotalDurNs != 20 {
		t.Fatalf("window0 svc-a=%+v, want count=1 dur=20", v0.ByService["svc-a"])
	}
	if v0.Start != winBase || v0.End != winBase.Add(time.Minute) {
		t.Fatalf("window0 bounds %v~%v", v0.Start, v0.End)
	}
	v1, ok := e.Query(winBase.Add(time.Minute))
	if !ok || v1.ByService["svc-b"].Count != 1 {
		t.Fatalf("window1=%+v ok=%v", v1, ok)
	}
	// 查询起点未对齐时自动对齐到所属窗口。
	v1b, ok := e.Query(winBase.Add(time.Minute + 42*time.Second))
	if !ok || v1b.Start != winBase.Add(time.Minute) {
		t.Fatalf("unaligned query not normalized: %+v", v1b)
	}
	// 追踪维度结果同样可查。
	if v1.ByTrace[tid(2)].Count != 1 {
		t.Fatalf("window1 trace dim missing: %+v", v1.ByTrace)
	}
}

func TestWindowLateWithinGraceMerges(t *testing.T) {
	clk := newFakeClock(winBase.Add(time.Minute + 5*time.Second)) // 窗口0已结束，宽限期内
	e := NewWindowExporter(testWindowCfg(), clk.now)

	if !e.Observe(wsp(1, 1, "svc-a", winBase.Add(30*time.Second), 10)) {
		t.Fatal("late span within grace must merge into home window")
	}
	v, ok := e.Query(winBase)
	if !ok || v.ByService["svc-a"].Count != 1 {
		t.Fatalf("late span not merged: %+v ok=%v", v, ok)
	}
	if v.Closed {
		t.Fatal("window within grace must not be closed")
	}
	counts, _ := e.Disposals()
	if len(counts) != 0 {
		t.Fatalf("unexpected disposals: %v", counts)
	}
}

func TestWindowGraceBoundaryAndTooLate(t *testing.T) {
	clk := newFakeClock(winBase.Add(30 * time.Second))
	e := NewWindowExporter(testWindowCfg(), clk.now)
	e.Observe(wsp(1, 1, "svc-a", winBase.Add(time.Second), 10))

	// 推进到窗口0关闭时刻（End+Grace）之前最后一纳秒：仍可并入。
	clk.set(winBase.Add(time.Minute + 10*time.Second - time.Nanosecond))
	if !e.Observe(wsp(1, 2, "svc-a", winBase.Add(2*time.Second), 10)) {
		t.Fatal("span arriving just before close must merge")
	}
	// 到达关闭时刻：窗口关闭，再到的归属跨度按 too_late 处置。
	clk.set(winBase.Add(time.Minute + 10*time.Second))
	if e.Observe(wsp(1, 3, "svc-a", winBase.Add(3*time.Second), 10)) {
		t.Fatal("span at/after close must be disposed, not merged")
	}
	v, ok := e.Query(winBase)
	if !ok {
		t.Fatal("closed window must remain queryable while retained")
	}
	if !v.Closed {
		t.Fatal("window must report closed after grace")
	}
	if v.ByService["svc-a"].Count != 2 {
		t.Fatalf("closed window count=%d, want 2 (too-late span must not be counted)", v.ByService["svc-a"].Count)
	}
	counts, log := e.Disposals()
	if counts[DisposeTooLate] != 1 {
		t.Fatalf("too_late count=%d, want 1", counts[DisposeTooLate])
	}
	if len(log) != 1 || log[0].Window != winBase || log[0].SpanKey != tid(1)+"/"+sid(3) {
		t.Fatalf("disposal log missing evidence: %+v", log)
	}
	// 超宽限期的跨度不得悄悄落进当前窗口。
	cur, ok := e.Query(clk.now())
	if ok && cur.ByService["svc-a"].Count != 0 {
		t.Fatalf("too-late span leaked into current window: %+v", cur)
	}
}

func TestWindowDuplicateNotCountedTwice(t *testing.T) {
	cfg := testWindowCfg()
	cfg.Grace = 5 * time.Minute
	clk := newFakeClock(winBase.Add(30 * time.Second))
	e := NewWindowExporter(cfg, clk.now)

	sp := wsp(1, 1, "svc-a", winBase.Add(time.Second), 10)
	if !e.Observe(sp) {
		t.Fatal("first observe must succeed")
	}
	if e.Observe(sp) {
		t.Fatal("duplicate observe must be disposed")
	}
	// 翻窗后重放同一跨度：窗口仍在保留期内，仍不得二次计数。
	clk.advance(2 * time.Minute)
	if e.Observe(sp) {
		t.Fatal("replayed span after rotation must not be recounted")
	}
	v, _ := e.Query(winBase)
	if v.ByService["svc-a"].Count != 1 {
		t.Fatalf("count=%d after duplicates, want 1", v.ByService["svc-a"].Count)
	}
	counts, _ := e.Disposals()
	if counts[DisposeDuplicate] != 2 {
		t.Fatalf("duplicate disposals=%d, want 2", counts[DisposeDuplicate])
	}
}

func TestWindowRotationAndEviction(t *testing.T) {
	cfg := testWindowCfg()
	cfg.Retain = 2
	clk := newFakeClock(winBase.Add(30 * time.Second))
	e := NewWindowExporter(cfg, clk.now)

	e.Observe(wsp(1, 1, "svc-a", winBase.Add(time.Second), 10))
	clk.set(winBase.Add(time.Minute + 30*time.Second))
	e.Observe(wsp(1, 2, "svc-a", winBase.Add(time.Minute+time.Second), 10))
	clk.set(winBase.Add(2*time.Minute + 30*time.Second))
	e.Observe(wsp(1, 3, "svc-a", winBase.Add(2*time.Minute+time.Second), 10))

	metas := e.Windows()
	if len(metas) != 2 {
		t.Fatalf("retained windows=%d, want 2", len(metas))
	}
	if metas[0].Start != winBase.Add(time.Minute) || metas[1].Start != winBase.Add(2*time.Minute) {
		t.Fatalf("retained windows wrong: %+v", metas)
	}
	// 已淘汰窗口不可查询。
	if _, ok := e.Query(winBase); ok {
		t.Fatal("evicted window must not be queryable")
	}
	// 迟到到已淘汰窗口的跨度：明确处置为 window_evicted，不重建窗口。
	if e.Observe(wsp(1, 4, "svc-a", winBase.Add(5*time.Second), 10)) {
		t.Fatal("span for evicted window must be disposed")
	}
	counts, log := e.Disposals()
	if counts[DisposeWindowEvicted] != 1 {
		t.Fatalf("window_evicted=%d, want 1", counts[DisposeWindowEvicted])
	}
	if len(log) != 1 || log[0].Window != winBase {
		t.Fatalf("eviction disposal evidence wrong: %+v", log)
	}
	if _, ok := e.Query(winBase); ok {
		t.Fatal("disposed span must not resurrect evicted window")
	}
}

func TestWindowZeroStartAndFutureStart(t *testing.T) {
	clk := newFakeClock(winBase.Add(time.Minute))
	e := NewWindowExporter(testWindowCfg(), clk.now)

	// 缺失起点：按到达时刻计入当前窗口，不猜测历史归属。
	if !e.Observe(wsp(1, 1, "svc-a", time.Time{}, 10)) {
		t.Fatal("zero-start span should fall into current window")
	}
	v, ok := e.Query(winBase.Add(time.Minute))
	if !ok || v.ByService["svc-a"].Count != 1 {
		t.Fatalf("zero-start span not in current window: %+v ok=%v", v, ok)
	}
	// 未来起点：无法可信归属，明确处置，不进当前窗口。
	if e.Observe(wsp(1, 2, "svc-a", clk.now().Add(time.Hour), 10)) {
		t.Fatal("future-start span must be disposed")
	}
	counts, _ := e.Disposals()
	if counts[DisposeClockRewind] != 1 {
		t.Fatalf("clock_rewind=%d, want 1", counts[DisposeClockRewind])
	}
	if v.ByService["svc-a"].Count != 1 {
		t.Fatal("future-start span leaked into current window")
	}
}

func TestWindowConcurrentWriteAndQuery(t *testing.T) {
	clk := newFakeClock(winBase)
	e := NewWindowExporter(testWindowCfg(), clk.now)

	const writers = 8
	const perWriter = 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := w*perWriter + i + 1
				// 分布在当前窗口与上一窗口（宽限期内），并夹杂查询与翻窗。
				start := winBase.Add(time.Duration(id%50) * time.Second)
				e.Observe(wsp(id, id, fmt.Sprintf("svc-%d", id%4), start, time.Duration(id)))
				if id%17 == 0 {
					e.Query(winBase)
					e.Windows()
				}
			}
		}(w)
	}
	// 并发翻窗推进时钟。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			clk.advance(10 * time.Second)
		}
	}()
	wg.Wait()

	// 对外视图必须完整一致：服务维度计数之和等于追踪维度计数之和。
	var svcSum, traceSum int64
	for _, m := range e.Windows() {
		v, ok := e.Query(m.Start)
		if !ok {
			t.Fatalf("listed window %v not queryable", m.Start)
		}
		var s, tr int64
		for _, st := range v.ByService {
			s += st.Count
		}
		for _, st := range v.ByTrace {
			tr += st.Count
		}
		if s != tr {
			t.Fatalf("window %v inconsistent: service=%d trace=%d", m.Start, s, tr)
		}
		svcSum += s
		traceSum += tr
	}
	counts, _ := e.Disposals()
	var disposed int64
	for _, c := range counts {
		disposed += c
	}
	if svcSum+disposed != writers*perWriter {
		t.Fatalf("counted=%d disposed=%d total=%d, want %d (no span lost or double counted)",
			svcSum, disposed, svcSum+disposed, writers*perWriter)
	}
}

func TestWindowSaveLoadConsistency(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(winBase.Add(2 * time.Minute))
	cfg := testWindowCfg()
	cfg.Grace = 5 * time.Minute
	e := NewWindowExporter(cfg, clk.now)

	e.Observe(wsp(1, 1, "svc-a", winBase.Add(10*time.Second), 10))
	e.Observe(wsp(1, 2, "svc-a", winBase.Add(20*time.Second), 30))
	e.Observe(wsp(2, 1, "svc-b", winBase.Add(time.Minute+10*time.Second), 20))
	// 触发一次重复处置，验证处置计数也持久化。
	e.Observe(wsp(1, 1, "svc-a", winBase.Add(10*time.Second), 10))
	if err := e.Save(dir); err != nil {
		t.Fatal(err)
	}

	clk2 := newFakeClock(winBase.Add(2 * time.Minute))
	e2 := NewWindowExporter(cfg, clk2.now)
	if err := e2.Load(dir); err != nil {
		t.Fatal(err)
	}
	v0, ok := e2.Query(winBase)
	if !ok || v0.ByService["svc-a"].Count != 2 || v0.ByService["svc-a"].TotalDurNs != 40 {
		t.Fatalf("restored window0 wrong: %+v ok=%v", v0, ok)
	}
	if v0.ByTrace[tid(1)].Count != 2 {
		t.Fatalf("restored trace dim wrong: %+v", v0.ByTrace)
	}
	v1, ok := e2.Query(winBase.Add(time.Minute))
	if !ok || v1.ByService["svc-b"].Count != 1 {
		t.Fatalf("restored window1 wrong: %+v ok=%v", v1, ok)
	}
	counts, _ := e2.Disposals()
	if counts[DisposeDuplicate] != 1 {
		t.Fatalf("restored disposals wrong: %v", counts)
	}
	// 恢复后重放同一批跨度：既有窗口内去重，也有上游去重，这里直接验证窗口层不翻倍。
	if e2.Observe(wsp(1, 1, "svc-a", winBase.Add(10*time.Second), 10)) {
		t.Fatal("replayed span after restore must not recount")
	}
	v0b, _ := e2.Query(winBase)
	if v0b.ByService["svc-a"].Count != 2 {
		t.Fatalf("count doubled after restore replay: %d", v0b.ByService["svc-a"].Count)
	}
	metas := e2.Windows()
	if len(metas) != 2 || metas[0].Start != winBase || metas[1].Start != winBase.Add(time.Minute) {
		t.Fatalf("restored window boundaries wrong: %+v", metas)
	}
}

func TestWindowLoadDegradationRules(t *testing.T) {
	dir := t.TempDir()
	cfg := testWindowCfg()

	// 未知版本：整份跳过，不猜测归属。
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("windows.json", `{"version":99,"windows":[{"start_unix_nano":0,"by_service":{}}]}`)
	e := NewWindowExporter(cfg, newFakeClock(winBase).now)
	if err := e.Load(dir); err != nil {
		t.Fatal(err)
	}
	if len(e.Windows()) != 0 {
		t.Fatal("unsupported version snapshot must be skipped entirely")
	}
	notes := e.Degradations()
	if len(notes) != 1 || !strings.Contains(notes[0], "99") {
		t.Fatalf("unsupported-version degradation note missing: %v", notes)
	}

	// 缺失版本按 v1 解读；未对齐窗口与非法统计逐条跳过；未知字段忽略并记录。
	write("windows.json", `{
		"windows":[
			{"start_unix_nano":0,"by_service":{}},
			{"start_unix_nano":123,"by_service":{}},
			{"start_unix_nano":60000000000,"by_service":{"svc":{"count":-1,"error_count":0,"total_dur_ns":0}}},
			{"start_unix_nano":60000000000,"by_service":{"svc":{"count":2,"error_count":0,"total_dur_ns":5}},"by_trace":{"t":{"count":2,"error_count":0,"total_dur_ns":5}}}
		],
		"future_field":"x"}`)
	e2 := NewWindowExporter(cfg, newFakeClock(winBase.Add(time.Minute)).now)
	if err := e2.Load(dir); err != nil {
		t.Fatal(err)
	}
	v, ok := e2.Query(time.Unix(0, 60000000000))
	if !ok || v.ByService["svc"].Count != 2 {
		t.Fatalf("valid window not restored: %+v ok=%v", v, ok)
	}
	if _, ok := e2.Query(time.Time{}); ok {
		t.Fatal("zero/misaligned windows must not be restored")
	}
	notes = e2.Degradations()
	joined := strings.Join(notes, "|")
	if !strings.Contains(joined, "缺少版本号") || !strings.Contains(joined, "未对齐") ||
		!strings.Contains(joined, "非法") || !strings.Contains(joined, "future_field") {
		t.Fatalf("degradation evidence incomplete: %v", notes)
	}

	// 非法 JSON：返回错误，不静默吞掉。
	write("windows.json", `{not json`)
	e3 := NewWindowExporter(cfg, newFakeClock(winBase).now)
	if err := e3.Load(dir); err == nil {
		t.Fatal("corrupt snapshot must return error")
	}
}
