# 本地链路追踪采集 / 采样 / 聚合管道 (Go)

一个**本地单进程**的链路追踪管道骨架实现：从本地数据源接收跨度（span），
在乱序、重复、上下文版本不一致、采样冲突、容量受限以及进程重启的情况下，
仍然产出**一致且可解释**的追踪与聚合结果。

## 快速开始

```bash
go build ./...
go test ./...          # 全部单元 / 集成测试
go run ./cmd/tracingd  # 乱序 + 重复 + 丢弃 + 容量演示
```

## 目录结构

```
internal/model         数据模型：TraceID/SpanID/Span 校验与归一化
internal/tracecontext  W3C 风格 traceparent 解析与版本兼容 / 降级规则
internal/sampling      比例（FNV 哈希）+ 有序属性规则；追踪级粘性决策
internal/store         乱序重建、断链/孤立区分、重复检测、容量与 TTL
internal/aggregate     幂等的 trace / service 延迟与错误聚合，精确回退
internal/wal           CRC 校验的追加日志 + 原子快照压缩（崩溃恢复）
internal/pipeline      串联全部组件：线性化写入、一致快照、后台 TTL
cmd/tracingd           最小演示程序
docs/                  设计规则与验证方法说明
```

## 数据流

```
Report ──► 1. body 校验 (model)
         ──► 2. 上下文解析/降级 (tracecontext)
         ──► 3. 追踪级粘性采样决策 (sampling)
         ──► 4. 容量受控写入 (store)          ── 丢弃轨迹只留墓碑
         ──► 5. WAL fsync 确认 (wal)          ── 未确认不可见
         ──► 6. 幂等聚合 + 驱逐精确回退 (aggregate)
```

## 核心一致性承诺

| 场景 | 行为 |
|------|------|
| 乱序到达 | 父缺失的跨度记为 **orphan（断链）**，父到达后自动挂接；环或无 root 永远保持 `partial` |
| 重复上报 | `(traceID, spanID)` + 载荷比对：完全相同幂等忽略；同 ID 不同载荷判为 **碰撞拒绝** |
| 上下文不兼容 | trace-id 不一致 = 硬冲突拒绝；畸形 / 不支持版本 = 按规则降级且记录原因，绝不静默错挂 |
| 采样冲突 | 决策只取决于 traceID 与首个跨度；W3C sampled hint **仅建议**，冲突被记录但不能改变结论 |
| 配置变更 | 已钉定的追踪结论不受 `SetSampling` 影响；新配置只管新追踪 |
| 容量上限 | `MaxTraces / MaxSpansPerTrace / MaxTotalSpans` 硬上限；拒绝或驱逐最旧，二选一，均计入指标 |
| 长期运行 | TTL 驱逐保留与丢弃墓碑；并发下单一锁 + 快照深拷贝，对外视图始终一致 |
| 进程重启 | 已 fsync 的跨度/决策/驱逐按序回放（两遍：先决策驱逐、后跨度）；快照原子替换 |

结果中的 `Code` 与 `Reason` 字段、`Metrics` 计数器给出了每个判定的依据，
便于在测试与线上定位「为什么这个跨度被接受 / 拒绝 / 丢弃 / 降级」。

详见 [docs/design.md](docs/design.md) 与 [docs/verification.md](docs/verification.md)。
