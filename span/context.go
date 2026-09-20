package span

import (
	"fmt"
	"strings"
)

// Context 是从上报报文中解析出的追踪上下文。
type Context struct {
	TraceID  string
	SpanID   string
	ParentID string
	Sampled  bool
	// Version 为解析出的协议版本。
	Version int
	// Degraded 为 true 表示按降级规则处理（如未知版本、忽略无法解析的可选字段）。
	Degraded bool
	// DegradeReason 记录降级依据，保证可解释。
	DegradeReason string
}

const (
	traceIDLen = 32 // 16 字节 hex
	spanIDLen  = 16 // 8 字节 hex
)

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	for _, c := range s {
		if c != '0' {
			return false
		}
	}
	return true
}

// ParseContext 解析追踪上下文。
//
// 既定规则：
//  1. 优先解析 W3C traceparent（version-traceid-spanid-flags）。
//  2. version 为 "ff" 或格式非法 -> 拒绝（ReasonMalformedContext）。
//  3. version 非 "00" 但前四段可解析 -> 降级接受：只取已知字段，忽略后续扩展，
//     标记 Degraded，不产生错误关联。
//  4. 缺少 traceparent 时回退到扁平字段 trace_id/span_id/parent_id/sampled。
//  5. trace-id 或 span-id 缺失、非 hex、长度错误或全零 -> 拒绝，
//     不得用猜测值静默关联。
func ParseContext(raw map[string]string) (Context, *Rejection) {
	if tp, ok := raw["traceparent"]; ok && tp != "" {
		return parseTraceparent(tp, raw["parent_id"])
	}
	return parseFlat(raw)
}

func parseTraceparent(tp, parentID string) (Context, *Rejection) {
	parts := strings.Split(tp, "-")
	if len(parts) < 4 {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: fmt.Sprintf("traceparent 段数不足: %q", tp)}
	}
	ver := parts[0]
	if len(ver) != 2 || !isHex(ver) || ver == "ff" {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: fmt.Sprintf("非法版本号: %q", ver)}
	}
	tid, sid, flags := parts[1], parts[2], parts[3]
	if len(tid) != traceIDLen || !isHex(tid) || allZero(tid) {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: fmt.Sprintf("非法 trace-id: %q", tid)}
	}
	if len(sid) != spanIDLen || !isHex(sid) || allZero(sid) {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: fmt.Sprintf("非法 span-id: %q", sid)}
	}
	if len(flags) != 2 || !isHex(flags) {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: fmt.Sprintf("非法 flags: %q", flags)}
	}
	ctx := Context{
		TraceID:  tid,
		SpanID:   sid,
		ParentID: parentID,
		Sampled: flags[1] == '1' || flags[1] == '3' || flags[1] == '5' || flags[1] == '7' ||
			flags[1] == '9' || flags[1] == 'b' || flags[1] == 'd' || flags[1] == 'f',
	}
	var v int
	fmt.Scanf(ver, "%02x", &v)
	ctx.Version = v
	if v != 0 {
		// 未知版本：按既定规则降级，仅使用可解析的已知字段。
		ctx.Degraded = true
		ctx.DegradeReason = fmt.Sprintf("unsupported_version %d: 仅解析已知字段", v)
	}
	if len(parts) > 4 && v == 0 {
		// version 00 不允许有扩展字段，多余字段忽略并标记降级。
		ctx.Degraded = true
		ctx.DegradeReason = "version 00 含多余扩展字段，已忽略"
	}
	return ctx, nil
}

func parseFlat(raw map[string]string) (Context, *Rejection) {
	tid, sid := raw["trace_id"], raw["span_id"]
	if tid == "" || sid == "" {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: "缺少 trace_id 或 span_id"}
	}
	if !isHex(tid) || allZero(tid) || !isHex(sid) || allZero(sid) {
		return Context{}, &Rejection{Reason: ReasonMalformedContext,
			Detail: "trace_id/span_id 非 hex 或全零"}
	}
	sampled := raw["sampled"] == "1" || raw["sampled"] == "true"
	return Context{TraceID: tid, SpanID: sid, ParentID: raw["parent_id"], Sampled: sampled}, nil
}
