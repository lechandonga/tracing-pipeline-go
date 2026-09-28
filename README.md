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
  - `future_window`：跨度起点落在当前窗口之后（窗口导出层处置）；
  - `recovery_in_progress`：状态恢复尚未完成时到达的写入/查询，明确拒绝或返回空结果，
    绝不发布加载了一半的中间状态。

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

跨重启、跨版本与跨配置读取（`Restore()`/`WindowExporter.Load`）：

- 窗口状态（含各窗口去重集合、处置计数与日志）随 `Snapshot()` 以
  **临时文件 + rename** 原子落盘（`windows.json`），`Restore()` 一并恢复。
- **恢复原子性（查不到半成品）**：`Pipeline.Restore()` 全程持有恢复写锁，
  `WindowExporter.Load()` 在本地完成全部解析与校验后**一次性替换**内存状态；
  恢复期间（`Ready()==false`）所有查询返回明确空结果（`Stats()` 空表、
  `WindowView/PublishedWindows` 为空、计数为 0），`Submit` 返回
  `recovery_in_progress` 拒绝（窗口侧第 4 个 `WindowMetrics()` 计数可核对）。
  任一组件恢复失败，管道保持不可服务状态并返回错误，原文件保留待查，不带猜测数据上线。
- **改配置重启后边界与归属不变**：快照格式自 v2 起为每个窗口记录绝对起点
  `start_ns` 与该窗口自身的 `size_ns/grace_ns`；v1 存档（只有 `index`）
  用文件级 `size_ns/grace_ns` 推导边界。重启时若当前 `Size/Grace` 与存档不一致：
  存档窗口按其**原始边界与宽限期**恢复为历史窗口，封板结论（`Sealed`）完全沿用
  关机前口径——改小窗口长度不会把同一条跨度挪到新网格，改小/改大宽限期都不会让
  已封板窗口重新开口；重启后到达的新跨度按当前配置归属新窗口，两套网格互不挪动，
  差异写入 `WindowNotes()`。跨度起点落在某个保留中的历史窗口内时仍按存档宽限并入，
  历史窗口已淘汰则按 `late_beyond_grace` 明确处置，不做槽位推断。
- **缺失/非法/不合理数据的降级**（窗口与聚合通用，逐条留痕，绝不带崩进程、
  绝不对外发布非法值）：
  - 文件整体非法 JSON 或无法读取 → 返回错误、保持不可服务，原文件保留；
  - 未知版本号、或 v1 缺少有效的文件级 `size_ns`（无法确定边界）→
    整份快照忽略、按空窗口启动，记录原因与文件路径，绝不按当前配置猜测切分；
  - 单条窗口起点非法/未按自身 `size` 对齐、缺 `seen/services/traces`、起点重复
    → 跳过该条并记录，其余条目正常恢复；
  - 单个统计项为 `null`、计数为负、错误数大于总数 → 丢弃该统计项并记录，
    窗口/聚合内其余数据照常可查；
  - 窗口数超过 `Retain` → 只恢复最新 `Retain` 个，跳过最旧的并记录；
  - 处置计数（dups/expired/future）为负 → 丢弃该计数并记录。
- 所有降级依据可通过 `WindowNotes()` 与 `AggregateNotes()`（管道上为
  `WindowNotes()`/`AggregateNotes()`）核对，记录包含窗口起点、键名与具体非法值。
- 版本策略：写出恒为当前版本（v2）；读取兼容 v1/v2，其余版本按上条整份忽略。

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
go test ./...                       # 单元 + 集成测试（含跨重启/跨版本/降级/恢复闸门）
go test -run 'TestRestore' ./...    # 只跑本轮新增的恢复语义用例
CGO_ENABLED=1 go test -race ./...   # 并发一致性（竞态检测）
```

本地手工核对恢复语义：

```sh
# 1. 用旧 Size/Grace 跑一段并落盘（windows.json 内含每窗口 start_ns/size_ns/grace_ns）
# 2. 改小 Window.Size / Window.Grace 后重启：PublishedWindows() 中旧窗口的 Start/End/Sealed
#    与关机前完全一致，WindowNotes() 含“存档窗口按原始边界恢复为历史窗口”的说明；
# 3. 手工把 windows.json 中某个统计改成 null 或负数后重启：进程不崩，
#    WindowView 仍可查、非法项不出现在结果里，WindowNotes() 给出键名与非法值；
# 4. 把 windows.json 改成非法 JSON：Restore() 返回错误、Ready()==false，
#    原文件保留；修复或移走文件后重新 Restore() 即可恢复服务。
```

测试覆盖：乱序与级联补齐、重复上报去重、上下文非法/未知版本降级、
采样粘性与配置变更、容量边界拒绝、重启恢复不丢不重、并发写入一致性；
窗口层另覆盖：乱序与迟到数据、宽限期边界、重复上报、窗口滚动与淘汰、
并发查询与写入、重启前后窗口结果一致；以及本轮新增的：

- 改小/改大窗口长度与宽限期重启后，历史窗口边界、归属、封板结论不变（v1 与 v2 存档）；
- 存档含 null 统计、负数计数、错误数大于总数、未对齐/重复/缺字段条目、负处置计数时，
  进程不崩、非法数据不发布、其余窗口照常可查、降级依据可核对；
- 整体非法 JSON 明确返回错误且保持不可服务，原文件保留；
- 恢复期间查询拿不到半成品、写入被明确拒绝，恢复完成后结果原子出现（含并发压测）。
