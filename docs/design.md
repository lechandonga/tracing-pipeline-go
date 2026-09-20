# 设计规则说明

## 1. 数据模型 (`internal/model`)

- `TraceID`：32 个小写十六进制字符（16 字节），全零保留且非法。
- `SpanID`：16 个小写十六进制字符（8 字节），全零保留且非法。
- `Span`：
  - 必填且必须合法：`TraceID`、`SpanID`；
  - `ParentSpanID` 可空（root）；非空时必须格式合法；
  - `StartUnixNano / EndUnixNano` 非负且 `end >= start`；
  - `Service / Name` 缺失降级为空串；未知 `Kind` 降级为 `internal`；
    未知 `Status` 降级为 `unset`；`Attributes` 复制一份存储，调用方后续修改不影响内部状态。
- 校验失败返回明确错误（`ErrBadTraceID / ErrBadSpanID / 时间戳非法`），
  在管道入口归类为 `invalid-body`，不产生任何状态。

## 2. 追踪上下文与版本兼容 (`internal/tracecontext`)

头部格式：`version-traceid-spanid-flags[-扩展…]`（W3C traceparent 风格）。

| 输入 | 结论 | 对外行为 |
|------|------|----------|
| 无头 | `no-context-header` | body 即权威，不降级 |
| v00 四字段合法 | 正常 | trace/span 与 body 交叉校验 |
| v00 字段数不对 / 字段为空 / 非十六进制 | `context-header-malformed` | **放弃头部**，body 权威，标记降级 |
| v01（更新的小版本） | `context-minor-version-downgrade` | 解析前四个标准字段，忽略尾部未知扩展 |
| 其它版本（含保留值 ff） | `context-version-unsupported` | 不按其关联，body 权威，标记降级 |
| 头部 trace-id 与 body 不一致 | `ErrIdentityConflict` | **硬拒绝整份上报**，杜绝静默错挂 |
| body 有父且与头部父不同 | `context-parent-mismatch-body-kept` | 以 body 父为准，记录分歧 |
| body 无父、头部父有效且不等于自身 | `context-parent-inferred-from-header` | 采用头部父为**显式标记的推断父** |
| flags 非法（非 1 字节十六进制） | `context-header-malformed` | 同上，body 权威 |

flags bit0（sampled）只作为**建议**（`HintPresent` 区分“无 hint”与“hint=drop”），
不会改变管道自己的采样结论；不一致时结论原因里带 `hint=...-conflict-ignored`。

## 3. 采样规则 (`internal/sampling`)

追踪级（trace-level）决策，同一追踪内所有服务共享同一结论。

1. **有序属性规则**先于比例判定，首条命中即结束：
   - `Service==""` 匹配所有服务，否则精确匹配；
   - `Value==""` 表示“键存在即可”，否则属性必须相等；
2. 未命中规则时按 `Rate ∈ [0,1]` 判定：`FNV-1a(traceID) / 2^32 < Rate`，
   纯函数、跨进程稳定，与哪个服务的跨度先到无关；
3. **粘性**：首个跨度触发决策并钉定在追踪状态上；之后关联跨度只读取结论。
   `SetSampling` 热更配置不影响已决策追踪；
4. **整体性**：keep 轨迹保留全部跨度；drop 轨迹不保留载荷、不进聚合，
   只保留「决策 + 已见到的去重 spanID 集合 + 时间戳」的墓碑，
   后续跨度无法翻盘，也不会只保留半个追踪。

判定原因形如 `attribute-rule#2:...->drop` 或 `rate:3367825970-bucket<0.5000->keep`，
采样冲突以 `hint=keep-conflict-ignored` 附加，可解释、可测试。

## 4. 重建与断链 (`internal/store`)

- 以 `(traceID, spanID)` 为主键，root / orphan 集合随每次写入增量维护；
- 父缺失 → 放入同一条 trace 并标记 orphan，**绝不猜测归属**；
  父到达后，等待的子跨度自动解除断链（返回 `resolved-orphan`）；
- 追踪状态 `State()`：
  - 有任何断链 → `partial`；
  - 无 root（例如 a↔b 环）→ `partial`（父/子时间线无法解释）；
  - 存在 root 但有跨度从 root 不可达（root 之外另有环）→ `partial`；
  - 其余 → `complete`；
- 重复：载荷完全一致 → `duplicate-span`（不更新活动时间、不计数）；
  同 ID 不同载荷 → `span-id-collision` 拒绝（无法判断哪个正确）。

## 5. 聚合口径 (`internal/aggregate`)

- 幂等键 = `traceID/spanID`（service 维度同样基于该键去重）；
- trace 行：跨度数、错误数、时间盒 `max(End)-min(Start)`（不是跨度时长之和）、总延迟；
- service 行：跨度数、错误数、min/max/总延迟；
- 组合仅使用计数/求和/由现存集合重算的极值，因此**与到达顺序无关**；
- 迟到跨度只更新当前行；过去通过 `Snapshot()` 取得的是深拷贝，不会被回改；
- 跨度被 TTL/容量驱逐时 `RemoveSpan` 精确回退（极值从剩余集合重算），
  重复回退、回退不存在的跨度均为幂等 no-op。

## 6. 容量与保留 (`store.Config` + 墓碑策略)

| 参数 | 含义 | 超限策略 |
|------|------|----------|
| `MaxSpansPerTrace` | 单追踪跨度上限（0=不限） | 直接拒绝 `capacity-max-spans-per-trace` |
| `MaxTraces` | 保留 trace + drop 墓碑总数（0=不限） | `EvictOldest=false` 拒绝；否则驱逐最旧空闲项 |
| `MaxTotalSpans` | 保留跨度总数（墓碑只占 trace 槽，不占跨度槽） | 同上，可连续驱逐直到有空位 |
| `RetentionTTL` | 距离上次新跨度的空闲超时（0=永不过期） | 后台 ticker 删除并精确回退聚合 |

拒绝与驱逐都通过 `Result.Code/Reason` 和 `Metrics.RejectedCapacity/EvictedTraces/
ExpiredTraces` 可观测。驱逐项（含丢弃墓碑）在 keep/drop 两类中全局取最旧。

## 7. 持久化与重启 (`internal/wal` + 恢复)

- `events.log` 帧：`"TWAL" | ver | type | len(4) | payload | crc32(4)`，
  CRC 覆盖 ver..payload；`Append` 成功返回前 `fsync`，这是“已确认”边界；
- 帧类型：`span / decision / evict`。写入顺序：**先 decision，再 span**
  （drop 墓碑同序），保证任何崩溃时刻日志都不会出现“有跨度无结论”；
- 帧尾写一半（崩溃截断）→ 回放时作为未提交尾部干净忽略；
  已完整成帧但 CRC 不符 → `ErrCorrupt` 直接失败，不静默丢数据；
- `snapshot.json` 走「临时文件 + fsync + rename + fsync 目录」原子替换，
  随后原子轮换 events.log；快照后旧日志残留也安全（记录全部幂等）；
- 启动恢复：载入快照 → 收集日志 → **两遍回放**（先 decision/evict，再 span），
  早到的 keep 决策先暂存、跨度恢复后再钉定；`New` 返回前恢复全部完成，
  外部看不到中间状态。

## 8. 并发模型

管道入口单一互斥锁；WAL 追加在持锁状态下完成，使每次摄入线性化。
`TraceViews / Aggregates / Metrics` 均在锁内取点并深拷贝返回，
并发写入下对外快照不会撕裂。TTL 后台任务与摄入共用同一把锁。
