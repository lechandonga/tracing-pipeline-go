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
  - `orphan_pending_parent`：父跨度未到达，跨度被暂存（非错误，可后续补齐）；
  - `late_beyond_grace`：超出窗口宽限期或归属窗口已淘汰（窗口导出层处置）；
  - `future_window`：跨度起点落在当前窗口之后（窗口导出层处置）。

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

## 滚动时间窗口导出（pipeline.WindowExporter）

在聚合之上按固定时间窗口滚动产出对外统计视图，供报表按窗口消费。
配置 `Config.Window`（`Size<=0` 表示不启用）：

- `Size`：窗口长度。所有窗口按 `Size` 对齐 Unix 纪元，跨度按 **`Start`（而非到达时间）**
  归属窗口，窗口口径不随跨度到达先后改变。
- `Retain`：保留的历史窗口数（含当前窗口）。超出即整体淘汰最旧窗口，
  窗口内的统计与去重集合一并释放，辅助结构占用有界、不随历史追踪数增长。
- `Grace`：迟到宽限期。窗口结束后 `Grace` 内的迟到跨度**并入其本应归属的窗口**；
  超出宽限期、归属窗口已淘汰、或起点落在当前窗口之后的跨度：
  **不静默丢弃、不悄悄计入当前窗口**，而是记为 `late_beyond_grace` / `future_window`，
  可通过 `RejectionCounts`、`WindowMetrics()`（重复/超宽限/未来窗口计数）与
  `WindowLateLog()`（有界处置日志，含判定依据）核对。

查询与一致性：

- `WindowView(at)`：取包含 `at` 的窗口（起点自动对齐）的服务维度与追踪维度结果；
  `PublishedWindows()`：只返回已封板（窗口结束且宽限期已过、结果不再变化）的窗口。
- 每个窗口自带 `traceID/spanID` 去重集合：乱序、断链补齐、翻窗、重启后的
  重复上报都不会重复计数。
- 查询持读锁拷贝，返回完整一致视图；淘汰只发生在写入路径，查询不阻塞写入，
  也不会读到更新了一半的窗口。

跨重启与跨版本：

- 窗口状态（含各窗口去重集合、处置计数与日志）随 `Snapshot()` 以
  **临时文件 + rename** 原子落盘（`windows.json`，带格式版本号与落盘时的
  `size_ns/grace_ns`），`Restore()` 一并恢复。
- **`Restore()` 全程持有恢复写锁，所有对外查询与 `Submit` 持读锁**：
  恢复全部完成之前，任何查询都拿不到只恢复了一半的中间状态
  （阻塞至恢复结束，随后看到完整状态）。
- 重启后已发布窗口结果与窗口边界不丢失、不重复：同一批跨度重新上报仍被去重，
  窗口结果不翻倍。
- **边界一致性**：窗口 index 的含义由 `Size` 决定，落盘时的 `size_ns/grace_ns`
  会在恢复时与当前配置逐一核对。配置改小/改大后重启，旧 index **绝不会**被按
  新配置静默重新解释成另一段时间。
- 跨版本/损坏读取的降级规则（每条都写入 `WindowNotes()`，可核对，绝不猜测归属）：
  1. 版本号未知 → 整份快照忽略、从空窗口启动；
  2. `size_ns/grace_ns` 字段缺失（老版本/外来存档）→ 无法核对边界，
     拒收存档窗口、从空窗口启动（处置计数与日志仍恢复，审计线索不丢）；
  3. 存档 `size/grace` 与当前配置不一致 → 为保证已发布窗口边界与归属不变，
     拒收存档窗口、从空窗口启动（处置计数与日志仍恢复），
     说明中同时记录存档值与当前值；
  4. 单条窗口缺 `seen/services/traces` 或 index 非法 → 跳过该条，其余正常恢复；
  5. 单条统计值为空、含负数计数、错误数超过总数 → 不崩溃、不对外发布，
     只跳过该条统计，同窗口其余统计与其余窗口不受影响；
  6. 窗口数超过 `Retain` → 只恢复最近 `Retain` 个；
  7. 文件整体非法 JSON → 返回错误，不带猜测数据对外服务。

## 容量与持久化

- `Config.MaxSpans`：已确认跨度容量上限，超限拒绝并计数（`RejectionCounts` 可观测）。
- `Config.Dir`：快照目录。`Snapshot()` 将存储与聚合状态通过**临时文件 + rename**
  原子落盘；`Restore()` 在恢复写锁内一次性完成存储、聚合、窗口三段恢复，
  恢复期间所有查询与 `Submit` 阻塞，中间状态不对外可见。
- 重启后已确认跨度与聚合不丢失、不重复：去重集合随快照持久化，
  重启后的重复上报仍被识别为 `duplicate`。

## 使用示例

```go
p := pipeline.New(pipeline.Config{
    MaxSpans: 10000, MaxOrphans: 1000,
    Ratio: 0.5, AttrRules: map[string]string{"tier": "critical"},
    Dir: "/var/lib/tracing",
    Window: pipeline.WindowConfig{Size: time.Minute, Retain: 60, Grace: 30 * time.Second},
})
_ = p.Restore()
rej := p.Submit(map[string]string{"traceparent": "00-<tid>-<sid>-01"},
    span.Span{Service: "checkout", Start: time.Now(), Duration: 120 * time.Millisecond})
// rej == nil 表示已确认；否则 rej.Reason 给出判定依据。
stats := p.Stats()               // 服务维度一致快照
view, _ := p.WindowView(at)      // 指定窗口的服务/追踪维度结果
pub := p.PublishedWindows()      // 已封板窗口（报表消费口径）
_ = p.Snapshot()                 // 优雅退出前落盘
```

## 验证方法

```sh
go build ./...
go test ./...                 # 单元 + 集成测试
CGO_ENABLED=1 go test -race ./...  # 并发一致性（竞态检测）
```

测试覆盖：乱序与级联补齐、重复上报去重、上下文非法/未知版本降级、
采样粘性与配置变更、容量边界拒绝、重启恢复不丢不重、并发写入一致性；
窗口层另覆盖：乱序与迟到数据、宽限期边界、重复上报、窗口滚动与淘汰、
并发查询与写入、重启前后窗口结果一致、改配置重启后边界不漂移（拒收+留痕）、
损坏/非法统计值降级不崩溃、恢复期间查询不可见中间状态、跨版本/损坏快照降级。

针对本次修复的关键用例可单独运行：

```sh
go test ./pipeline/ -run 'TestWindowConfigChangeRestartRejectsArchive|TestWindowLoadConfigMismatchDegradation|TestWindowLoadInvalidStatsDegrade|TestPipelineRestoreHidesIntermediateState' -v
```
