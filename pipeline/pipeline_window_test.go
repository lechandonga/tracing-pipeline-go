package pipeline

import (
	"testing"
	"time"

	"github.com/lechandonga/tracing-pipeline-go/span"
)

func windowTestConfig(dir string, clk *fakeClock) Config {
	cfg := testConfig(dir)
	cfg.Window = WindowConfig{Size: time.Minute, Retain: 3, Grace: 5 * time.Minute}
	cfg.Clock = clk.now
	return cfg
}

func TestPipelineWindowReadyGate(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(winBase)
	p := New(windowTestConfig(dir, clk))
	// 有持久化目录时，恢复完成前窗口查询不可见。
	if _, ok := p.WindowQuery(winBase); ok {
		t.Fatal("window query must be gated before restore completes")
	}
	if p.Windows() != nil {
		t.Fatal("window list must be gated before restore completes")
	}
	if err := p.Restore(); err != nil {
		t.Fatal(err)
	}
	// 恢复完成后可正常写入与查询。
	clk.set(winBase.Add(30 * time.Second))
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(time.Second), Duration: 10})
	v, ok := p.WindowQuery(winBase)
	if !ok || v.ByService["svc-a"].Count != 1 {
		t.Fatalf("window query after restore: %+v ok=%v", v, ok)
	}
}

func TestPipelineWindowRestartConsistency(t *testing.T) {
	dir := t.TempDir()
	clk := newFakeClock(winBase.Add(30 * time.Second))
	cfg := windowTestConfig(dir, clk)
	p := New(cfg)
	if err := p.Restore(); err != nil {
		t.Fatal(err)
	}

	// 窗口0两条、窗口1一条（乱序提交）。
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(5 * time.Second), Duration: 10})
	clk.set(winBase.Add(time.Minute + 30*time.Second))
	p.Submit(tp(tid(2), sid(1)), span.Span{Service: "svc-b", Start: winBase.Add(time.Minute + 5*time.Second), Duration: 20})
	p.Submit(tp(tid(1), sid(2)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 30})
	if err := p.Snapshot(); err != nil {
		t.Fatal(err)
	}
	// 快照后继续写入然后“被终止”（未再 Snapshot）。
	p.Submit(tp(tid(3), sid(1)), span.Span{Service: "svc-c", Start: winBase.Add(time.Minute + 10*time.Second), Duration: 40})

	// 重启：已发布窗口结果与边界不丢失，崩溃前未落盘的写入不出现。
	p2 := New(cfg)
	if _, ok := p2.WindowQuery(winBase); ok {
		t.Fatal("query must be gated until restore completes")
	}
	if err := p2.Restore(); err != nil {
		t.Fatal(err)
	}
	v0, ok := p2.WindowQuery(winBase)
	if !ok || v0.ByService["svc-a"].Count != 2 || v0.ByService["svc-a"].TotalDurNs != 40 {
		t.Fatalf("restored window0: %+v ok=%v", v0, ok)
	}
	v1, ok := p2.WindowQuery(winBase.Add(time.Minute))
	if !ok || v1.ByService["svc-b"].Count != 1 {
		t.Fatalf("restored window1: %+v ok=%v", v1, ok)
	}
	if _, lost := v1.ByService["svc-c"]; lost {
		t.Fatal("span written after last snapshot must not appear after crash")
	}
	metas := p2.Windows()
	if len(metas) != 2 || metas[0].Start != winBase || metas[1].Start != winBase.Add(time.Minute) {
		t.Fatalf("window boundaries lost across restart: %+v", metas)
	}

	// 同一批跨度重新上报：窗口结果不得翻倍。
	p2.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(5 * time.Second), Duration: 10})
	p2.Submit(tp(tid(1), sid(2)), span.Span{Service: "svc-a", Start: winBase.Add(10 * time.Second), Duration: 30})
	p2.Submit(tp(tid(2), sid(1)), span.Span{Service: "svc-b", Start: winBase.Add(time.Minute + 5*time.Second), Duration: 20})
	v0b, _ := p2.WindowQuery(winBase)
	if v0b.ByService["svc-a"].Count != 2 {
		t.Fatalf("window0 doubled after replay: %d", v0b.ByService["svc-a"].Count)
	}
	v1b, _ := p2.WindowQuery(winBase.Add(time.Minute))
	if v1b.ByService["svc-b"].Count != 1 {
		t.Fatalf("window1 doubled after replay: %d", v1b.ByService["svc-b"].Count)
	}
}

func TestPipelineWindowLateAndDuplicateThroughPipeline(t *testing.T) {
	clk := newFakeClock(winBase.Add(30 * time.Second))
	cfg := windowTestConfig("", clk)
	cfg.Window.Retain = 10 // 保留期需覆盖宽限期，否则迟到跨度先撞上淘汰
	p := New(cfg)

	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(5 * time.Second), Duration: 10})
	// 迟到但在宽限期内：并入原窗口。
	clk.set(winBase.Add(time.Minute + 5*time.Second))
	p.Submit(tp(tid(1), sid(2)), span.Span{Service: "svc-a", Start: winBase.Add(8 * time.Second), Duration: 20})
	v, ok := p.WindowQuery(winBase)
	if !ok || v.ByService["svc-a"].Count != 2 {
		t.Fatalf("late span not merged via pipeline: %+v ok=%v", v, ok)
	}
	// 管道层重复上报：被拒绝且窗口不重复计数。
	rej := p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc-a", Start: winBase.Add(5 * time.Second), Duration: 10})
	if rej == nil || rej.Reason != span.ReasonDuplicate {
		t.Fatalf("pipeline duplicate not rejected: %+v", rej)
	}
	v, _ = p.WindowQuery(winBase)
	if v.ByService["svc-a"].Count != 2 {
		t.Fatalf("duplicate counted in window: %d", v.ByService["svc-a"].Count)
	}
	// 超出宽限期：明确处置，不进当前窗口。
	clk.set(winBase.Add(time.Minute + 5*time.Minute + time.Second))
	p.Submit(tp(tid(1), sid(3)), span.Span{Service: "svc-a", Start: winBase.Add(9 * time.Second), Duration: 5})
	counts, log := p.WindowDisposals()
	if counts[DisposeTooLate] != 1 || len(log) != 1 {
		t.Fatalf("too-late disposal not observable: %v %+v", counts, log)
	}
	v, _ = p.WindowQuery(winBase)
	if v.ByService["svc-a"].Count != 2 {
		t.Fatalf("too-late span merged into closed window: %d", v.ByService["svc-a"].Count)
	}
	cur, ok := p.WindowQuery(clk.now())
	if ok && cur.ByService["svc-a"].Count != 0 {
		t.Fatal("too-late span leaked into current window")
	}
}

func TestPipelineWindowDisabledByDefault(t *testing.T) {
	p := New(testConfig(""))
	p.Submit(tp(tid(1), sid(1)), span.Span{Service: "svc", Start: winBase})
	if _, ok := p.WindowQuery(winBase); ok {
		t.Fatal("window export must be disabled when Size<=0")
	}
	if got := p.Stats()["svc"].Count; got != 1 {
		t.Fatalf("existing aggregation regressed: %d", got)
	}
}
