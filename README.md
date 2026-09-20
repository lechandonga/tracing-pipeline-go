# tracing-pipeline-go

本地链路追踪的采集、采样与聚合管道。目标：同一批跨度在**乱序到达、重复上报、
采样配置变化与进程重启**后仍得到一致且可解释的结果。

## 数据模型

- `span.Span`：一条跨度，含 `TraceID/SpanID/ParentID/Service/Name/Start/Duration/HasError/Attributes`。
  `ParentID` 为空表示根跨度。
- `span.Context`：从上报报文解析出的追踪上下文（见下文兼容规则），含 `Degraded/DegradeReason`
  以记录降级依据。
- `span.Rejection`：一次拒绝/降级的判定记录，`Reason` 区分各类异常：
  - `malformed_context`：上下文缺失、非 hex、长度错误、全零 ID、非法版本号 `ff`；
  - `unsupported_version`：未知协议版本（按规则降级接受，见下文）；
  - `duplicate`：重复上报（采集层与存储层双重去重）；
  - `capacity_full`：已确认存储或断链暂存区达到容量上限；
  - `orphan_pending_parent`：父跨度未到达，跨度被暂存（非错误，可后续补齐）。

## 上下文解析与版本兼容规则（span.ParseContext）

1. 优先解析 W3C `traceparent`（`version-traceid-spanid-flags`）。
2. 版本号为 `ff` 或格式非法 → 拒绝（`malformed_context`），不得用猜测值静默关联。
3. 版本号非 `00` 但前四段可解析 → **降级接受**：只取已知字段、忽略扩展，
   标记 `Degraded` 并记录 `DegradeReason`；version `00` 携带多余扩展字段同样降级忽略。
4. 缺少 `traceparent` 时回退到扁平字段 `trace_id/span_id/parent_id/sampled`。
5. `trace-id`/`span-id` 缺失、非 hex、长度错误或全零 → 拒绝。

## 采集与断链补齐（pipeline.Ingester）

- 乱序到达不产生错误归属：父跨度未到达时子跨度进入暂存区，返回 `orphan_pending_parent`。
- 父跨度到达时，暂存的子孙跨度**级联补齐**并释放进入下游。
- 暂存区容量上限为 `Config.MaxOrphans`，超限拒绝（`capacity_full`）并计数。

## 采样规则（pipeline.Sampler）

- 结论按**追踪维度粘性保存**：一旦确定，后续到达的同追踪跨度不得改变结论，
  同一追踪要么全部保留要么全部丢弃，不存在部分保留。
- 按属性规则（`AttrRules`）优先于按比例：任一属性命中即保留。
- 按比例采样使用 traceID 的 FNV 哈希映射到 `[0,1)` 稳定桶，
  配置相同时不同服务/进程结论一致。
- `UpdateSampling` 只影响尚未决策的追踪，已决策追踪结论不变。

## 聚合口径（pipeline.Aggregator）

- 服务维度与追踪维度统计 `Count/ErrorCnt/TotalDurNs`。
- 以 `traceID/spanID` 去重，重复或迟到上报不重复计数；只统计采样保留的跨度。
- `Snapshot()` 在锁内拷贝，并发写入下对外快照一致；聚合结果与到达顺序无关。

## 容量与持久化

- `Config.MaxSpans`：已确认跨度容量上限，超限拒绝并计数（`RejectionCounts` 可观测）。
- `Config.Dir`：快照目录。`Snapshot()` 将存储与聚合状态通过**临时文件 + rename**
  原子落盘；`Restore()` 在启动时一次性恢复，恢复中的中间状态不对外可见。
- 重启后已确认跨度与聚合不丢失、不重复：去重集合随快照持久化，
  重启后的重复上报仍被识别为 `duplicate`。

## 使用示例

```go
p := pipeline.New(pipeline.Config{
    MaxSpans: 10000, MaxOrphans: 1000,
    Ratio: 0.5, AttrRules: map[string]string{"tier": "critical"},
    Dir: "/var/lib/tracing",
})
_ = p.Restore()
rej := p.Submit(map[string]string{"traceparent": "00-<tid>-<sid>-01"},
    span.Span{Service: "checkout", Duration: 120 * time.Millisecond})
// rej == nil 表示已确认；否则 rej.Reason 给出判定依据。
stats := p.Stats()          // 服务维度一致快照
_ = p.Snapshot()            // 优雅退出前落盘
```

## 验证方法

```sh
go build ./...
go test ./...                 # 单元 + 集成测试
CGO_ENABLED=1 go test -race ./...  # 并发一致性（竞态检测）
```

测试覆盖：乱序与级联补齐、重复上报去重、上下文非法/未知版本降级、
采样粘性与配置变更、容量边界拒绝、重启恢复不丢不重、并发写入一致性。
