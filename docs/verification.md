# 验证方法

## 运行

```bash
go test ./... -count=1      # 全部测试
go test -race ./...         # 竞态（需 CGO_ENABLED=1）
go vet ./...
go run ./cmd/tracingd       # 手工演示
```

## 测试矩阵（异常优先，且区分判定依据）

### 乱序与断链
- `TestOutOfOrderReassembly`（store）：孙→子→root 逆序到达，断链→补齐→complete。
- `TestSpanCycleNeverCompletes`（store）：a↔b 互指无 root，永远 partial。
- `TestOrderIndependentResult`（pipeline）：同一批跨度三种到达顺序，
  追踪状态、跨度总数、延迟总和逐字节一致。

### 重复与冲突
- `TestDuplicateAndCollision`：完全重复幂等；同 ID 不同载荷拒绝为碰撞。
- `TestDuplicateReportsCountOnce`（pipeline）：5 次同跨度只计 1 次，4 次 dup 指标。
- `TestRestartRecoveryNoLossNoDuplication`：崩溃前后 + 重启后重复均不双计。

### 上下文不兼容
- `tracecontext_test.go`：无头 / v00 / v01 降级 / ff 不支持 / 畸形 /
  trace-id 硬冲突 / body 父优先 / 头部推断父，七类结论逐一断言。
- `TestContextConflictRejects`：冲突上报不产生任何状态。
- `TestContextDegradationRecorded`：降级接受且 `DegradedContexts=1`。

### 采样一致性与冲突
- `TestDeterministicAcrossServicesAndInstances`：同 traceID 跨服务、跨实例同结论。
- `TestRateBounds / TestOrderedRulesAndServiceSelector`：边界比例与有序规则。
- `TestHintNeverOverrides / TestConflictingSamplingHintsAcrossServices`：
  hint 冲突被记录但无法改变结论。
- `TestSamplingStickyAndWholeTrace`：首跨度 drop 后，其他服务的迟到跨度仍 drop 且不进聚合。
- `TestConfigChangeKeepsPinnedDecision`：热更 Rate/规则不翻转已钉定追踪。

### 容量边界
- `TestPerTraceAndTotalCapacityReject / TestMaxTracesRejectWhenDegradeDisabled`：硬拒绝。
- `TestMaxTracesEvictOldest`：驱逐最旧并返回被驱逐项。
- `TestCapacityBoundaryRejectedAndObservable`：拒绝计入指标。
- `TestCapacityEvictOldestRetractsAggregates`：驱逐后聚合精确回退。
- `TestDroppedTombstoneCountsAgainstMaxTraces`：drop 墓碑同样受 MaxTraces 约束。
- `TestDroppedTombstoneEvictsOldestAndCanBeRecreated`：墓碑被驱逐后可安全重建。
- `TestTTLEviction`：TTL 内保留、超时只驱逐一次（keep 与 drop 两类同规则）。

### 持久化 / 重启
- `wal_test.go`：正常回放、截断尾部容忍、已提交帧 CRC 损坏报 `ErrCorrupt`、
  快照压缩后日志清空且可继续追加。
- `TestRestartRecoveryNoLossNoDuplication`：乱序 + 重复 → 关闭 → 重启，
  追踪 complete、决策保留、聚合与重启前一致。
- `TestRestartRecoveryDroppedTombstoneSurvives`：drop 结论跨重启、跨配置变更存活。
- `TestRecoveryAfterSnapshotCompaction`：跨多次快照压缩边界后恢复无丢失无重复。

### 并发
- `TestConcurrentWritersHaveConsistentSnapshots`：8 个写入协程 + 并发快照，
  最终不变式：聚合跨度总数不超过已接受数；快照过程无撕裂。

## 判定依据（可解释性）对照

| 现象 | Result.Code / Reason | 指标 |
|------|----------------------|------|
| 非法 ID / 时间戳 | `invalid-body` | `RejectedInvalid` |
| trace-id 冲突 | `context-conflict` | `RejectedConflict` |
| 上下文降级 | accepted, reason 含 `context=...` | `DegradedContexts` |
| 完全重复 | `duplicate-span` | `Duplicates` |
| 同 ID 不同载荷 | `span-id-collision` | `RejectedCollision` |
| 容量拒绝 | `capacity-max-*` | `RejectedCapacity` |
| 降级驱逐 | reason `evicted-oldest` | `EvictedTraces` |
| TTL 过期 | reason `evicted-ttl` | `ExpiredTraces` |
| 采样冲突 | reason 含 `hint=...-conflict-ignored` | — |
| 持久化失败 | `persist-failed`（内存回滚） | `PersistFailures` |
