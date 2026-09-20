// Package span 定义跨度数据模型与追踪上下文。
package span

import "time"

// Span 表示一条已解析的跨度记录。
type Span struct {
	TraceID    string
	SpanID     string
	ParentID   string // 为空表示根跨度
	Service    string
	Name       string
	Start      time.Time
	Duration   time.Duration
	HasError   bool
	Attributes map[string]string
	// Sampled 由采样器写入的追踪级结论。
	Sampled bool
}

// RejectionReason 区分各类拒绝/降级原因，保证异常可区分、可解释。
type RejectionReason string

const (
	ReasonMalformedContext RejectionReason = "malformed_context"
	ReasonUnsupportedVer   RejectionReason = "unsupported_version"
	ReasonDuplicate        RejectionReason = "duplicate"
	ReasonCapacityFull     RejectionReason = "capacity_full"
	ReasonOrphan           RejectionReason = "orphan_pending_parent"
)

// Rejection 记录一次拒绝及其判定依据。
type Rejection struct {
	Reason RejectionReason
	Detail string
}
