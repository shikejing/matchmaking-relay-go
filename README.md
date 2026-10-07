# matchmaking-relay-go

本地进程内完成的在线匹配与对局中继服务：匹配与消息中继都在进程内进行，客户端连接用内存管道（loopback pipe）模拟，整条链路不依赖任何外部服务。

## 架构

```
service  门面：入队 / 取消 / 推进 / 重连 / 中继 / 查询
  │
  ├── match   按分区（partition）隔离的确定性匹配引擎
  │     ├── Enqueue   队伍作为整体入队，原子分配入队序号
  │     ├── Tick      逻辑时钟推进：先补位（backfill），再普通匹配
  │     └── PlanMatches / PlanBackfill  纯函数决策（无 map 遍历、无随机、无时钟）
  │
  ├── relay   中继会话生命周期与消息投递
  ├── rule    版本化规则（v1 / v2），注册表不可变
  ├── audit   有界环形审计日志，每次状态迁移与拒绝均可追溯
  └── transport  内存管道（客户端连接的本地回环模拟）
```

## 1. 匹配语义

### 队伍是不可拆分的最小单位

- 队伍（Team）作为整体入队，一经入队即被盖上当时的规则版本戳记，**不会被拆散、不会跨队重组**。
- 队伍分值在入队时按所属规则版本的合并规则一次性合并为快照：
  - v1：`合并分 = 成员分之和`
  - v2：`合并分 = 成员分之均值（向下取整）`
- 同一队伍被并发或重复提交时，只有一次成功，其余返回 `DUPLICATE_TEAM`；队伍在会话中的成员集合始终与入队时一致（测试 `TestConcurrentDuplicateTeam`、`TestMatchFormedAndTeamIntegrity`）。

### 确定性与可复现

匹配决策是纯函数 `f(入队顺序, 逻辑时钟, 队伍快照, 规则版本, 配置上限)`：

- **入队顺序**：入队时在分区锁内通过原子计数器分配单调递增的 `EnqueueSeq`，这是全序的入队次序。
- **逻辑时钟**：不读墙上时钟，等待时长以逻辑 tick 度量，因此与调度时序无关。
- **不使用随机数**：决策路径不消费任何随机源（配置中的 `Seed` 仅作为可复现契约记录在审计中，并可用于测试生成确定性输入）。
- **不依赖字典/集合遍历顺序**：所有候选排序与配对都在有序切片上完成。

裁决规则（`PlanMatches`）：

1. 候选队伍按 `(入队 tick, 入队 seq)` 升序排列 —— **等待时长是第一优先级**，等待最久的队伍成为锚点（anchor）。
2. 锚点与其余队伍中**有效分最接近**的队伍配对（绝对差值最小）。
3. 分值接近度相同（差值相等）时，按典范次序（更早的入队 seq）打破平局。
4. 每分区每 tick 最多形成 `MatchBatchSize` 场匹配；每场 `TeamsPerMatch` 支队伍。

有效分（EffectiveScore）由锚点队伍入队时的规则版本计算，双方队伍同算：

- v1：`有效分 = 合并分`（无等待加成）
- v2：`有效分 = 合并分 + 等待 tick × 2`（等待越久优先级越高，接近度仍在相近等待的队伍间裁决）

> 可复现保证：固定种子与相同入队顺序下，以任意并发度运行，匹配结果逐条一致（测试 `TestDeterminismUnderConcurrency`：并发入队后按观测到的入队次序串行重放，分组结果完全相同；并覆盖 GOMAXPROCS=1）。

### 饥饿防护

等待时长优先保证早入队的队伍先被匹配；补位请求在每个 tick 最先处理。任何分区都不会被长期饥饿（测试 `TestNoStarvation`：早到的低分队伍不会被后到的高分队伍挤掉）。

## 2. 中继会话生命周期

```
                         CreateSession（凑齐后建立，成员全部 Joined）
   ┌──────────────────────────────────────────────────────────────┐
   ▼                                                              │
SessionActive ──成员离线──▶ MemberDisconnected ──窗口内重连──▶ MemberJoined
   │                              │
   │                              │ 超过保留窗口
   │                              ▼
   │                    按规则版本的超时策略：
   │                    · v1：结束会话（SessionEnded）
   │                    · v2：空位进入补位队列，会话保持 Active
   │                              │
   │                    补位成功 ──▶ 新成员 Joined（会话继续）
   │                    补位超时 ──▶ 结束会话
   │
   └── 成员全部离开 / 显式 EndSession ──▶ SessionEnded（终态，拒绝一切迁移）
```

- 对外可见的会话与成员集合始终是**单个自洽快照**：所有读取都在锁内拷贝（`SessionView`），不存在半更新状态；快照是深拷贝，外部修改不会污染内部状态（测试 `TestSnapshotIsCopy`）。
- 任何非法迁移（对已结束会话操作、重复离线、重复结束等）都返回可区分的错误码，不产生半更新。

### 掉线与重连

- 成员掉线后进入 `MemberDisconnected`，在规则的保留窗口（v1=100 tick，v2=200 tick）内可重连恢复为 `MemberJoined`，**不会被重复计入**（测试 `TestReconnectDoesNotDoubleCount`）。
- 超过窗口：v1 结束会话；v2 将该成员置为离开并发起补位（backfill），会话继续。
- 重连不改变会话的规则版本，不产生新会话。

### 补位（backfill）

- v2 规则下，空位以补位票（backfill ticket）的形式进入匹配队列，**每会话同一时刻最多一张未决票**；重复推进逻辑时钟不会产生重复补位。
- 补位票在每个 tick 最先被满足：与**空位人数相同**的等待队伍中按入队次序最早者配对。
- 补位票盖上**会话建立时的规则版本**戳记，等待时长上限按该版本的 `BackfillTimeoutTicks`（v2=50）计算；之后切换默认规则不影响在途等待（测试 `TestRuleSwitchKeepsInFlightBackfill`）。
- 补位票超过 `BackfillTimeoutTicks` 仍无合适队伍时，会话**必须结束**，结束原因为 `BACKFILL_TIMEOUT`，不会无限期挂在活跃状态（测试 `TestBackfillTimeoutViaTicks`，通过真实推进逻辑时钟触发）。
- 补位成功或超时后，相关等待状态（会话上的等待标记、分区里的补位票）立即清理干净；之后出现新的空位可以再次进入补位流程（测试 `TestBackfillSuccessThenReBackfill`）。
- 补位等待期间出现的新空位会被记录为"未覆盖空位"，在当前补位票被满足后立即以新票进入补位流程，不会丢失也不会叠加成重复票。
- 补位严格按分区隔离：每个分区各自维护自己的补位票，A 分区的补位绝不消费 B 分区排队的队伍；某分区等待补位或补位超时不阻塞其他分区的匹配推进（测试 `TestBackfillDoesNotCrossPartitions`、`TestBackfillParallelPartitions`）。
- 会话因任何原因结束时，其未决补位票在下一个 tick 被清理，终态快照中不再显示等待标记（测试 `TestEndSessionClearsBackfillWait`）。

### 会话结束原因

会话快照（`SessionView.EndReason`）与审计记录（`SESSION_ENDED` 的 `Reason`）携带机器可读的结束原因，三种结果可明确区分：

| 结束原因 | 含义 |
|---|---|
| `BACKFILL_TIMEOUT` | 补位等待超过会话所属规则的时长，无合适替补 |
| `RETENTION_TIMEOUT` | 成员掉线超过保留窗口，且规则不启用补位（如 v1） |
| `MEMBER_VACATED` | 成员显式离开且规则不补位，或会话已无在线成员 |
| 其他（调用方自定） | 显式 `EndSession` 传入的原因原样记录 |

补位成功不是结束：会话保持活跃并继续（审计记录 `BACKFILL_SUCCEEDED`）。对外查询始终拿到**单个自洽快照**：成员状态、是否有未决补位、会话是否已结束、结束原因互相一致，不存在"空位挂起但会话既不结束也不继续"的中间态（测试 `TestEndReasonsDistinguishable`）。

## 3. 并发推进与隔离

- 每个匹配分区拥有独立互斥与（后台模式下的）独立 goroutine：慢分区不阻塞快分区（测试 `TestPartitionIsolationBackground`）。
- 所有容量均有上下界且可配置；启动时 `Config.Validate()` 同时校验下界与硬上界，越界配置被直接拒绝并返回可按字段区分的 `*config.LimitError`（测试 `TestConfigUpperBounds`）：

| 配置项 | 含义 | 默认值 | 允许范围 |
|---|---|---|---|
| `QueueCapacity` | 每分区等待队列上限 | 64 | 1 – 4096（`config.MaxQueueCapacity`） |
| `MatchBatchSize` | 每分区每 tick 最多匹配场数 | 8 | 1 – 256（`config.MaxMatchBatchSize`） |
| `MaxConcurrentSessions` | 全局并行会话上限 | 32 | 1 – 4096（`config.MaxConcurrentSessions`） |
| `TeamsPerMatch` | 每场匹配的队伍数 | 2 | ≥ 2 |
| `AuditCapacity` | 审计环形日志保留条数 | 4096 | 1 – 1048576（`config.MaxAuditCapacity`） |
| `MessageBufferSize` | 每成员消息管道缓冲 | 64 | 1 – 4096（`config.MaxMessageBufferSize`） |
| `TickInterval` | 后台推进间隔；0 为手动 `Tick()`（确定性测试用） | 0 | — |
| `Seed` | 确定性种子（仅记录，不参与决策） | 1 | — |
| `Partitions` | 启动时创建的分区 | `["default"]` | 非空、不重复 |

- 超出容量返回**可区分的拒绝结果**（`match.Error` / 审计事件码）：

| 错误码 | 触发条件 |
|---|---|
| `QUEUE_FULL` | 分区等待队列已满 |
| `DUPLICATE_TEAM` | 同一队伍重复/并发提交 |
| `TEAM_EMPTY` | 队伍无成员 |
| `RULE_NOT_FOUND` | 指定的规则版本未注册 |
| `PARTITION_NOT_FOUND` | 分区不存在 |
| `SESSION_AT_CAPACITY` | 并行会话数已达上限（队伍留在队列，下 tick 重试） |
| `SESSION_NOT_FOUND` / `SESSION_ENDED` | 会话不存在 / 已结束 |
| `MEMBER_NOT_FOUND` / `MEMBER_NOT_DISCONNECTED` | 成员不存在 / 状态不允许该迁移 |
| `RECONNECT_WINDOW_EXPIRED` | 重连超出保留窗口 |
| `CLIENT_SLOW` | 消息管道缓冲满（消息丢弃并审计，中继不阻塞） |
| `BACKFILL_TIMEOUT` | 补位超时，会话结束 |

- 高负载下无死锁、无内存无界增长：队列有界、审计环形有界、消息管道有界且满即跳过（测试 `TestNoDeadlockUnderLoad`、`TestQueueCapUnderBurst`、`TestAuditRingBounded`）。

## 4. 规则版本兼容

- 规则不可变，注册于 `rule.Registry`；队伍入队时盖版本戳，会话创建时记录版本号。
- 规则更替（`SetDefault`）只影响**之后入队**的队伍：已入队队伍按各自版本继续，已建立会话按其版本走完生命周期——包括补位票，它在开立时盖上会话的规则版本，等待时长与超时行为不因默认规则变化而改变——中途不改变已产生的匹配结果（测试 `TestRuleVersionSwitching`、`TestRuleSwitchKeepsInFlightBackfill`）。
- 跨版本差异可通过 `Registry.DescribeAll()` 查询；每次匹配的审计记录都包含当时使用的规则版本。

## 5. 可观测与审计

- 查询：`QueryQueue`（队列快照）、`QuerySession` / `ListSessions`（会话与成员快照）、`QueryTeam`、`ListPartitions`。
- 审计：`Audit().Query(Filter{...})` 支持按类型、分区、队伍、会话、序号过滤。
- 每次匹配记录 `MATCH_FORMED`：输入（队伍 ID、合并分、入队 seq、等待 tick、分差）与决策依据（锚点、配对选择、规则版本）。
- 每次拒绝记录 `REJECT` 与原因码，可定位到具体队伍/会话/成员对象。

## 6. 客户端连接模拟

`transport.Pipe` 是内存单向管道（中继 → 客户端），缓冲有界；`AttachPipe` 后会话消息投递给所有 `MemberJoined` 成员。客户端 → 中继的消息通过 `SendMessage` 在进程内直接投递。无网络、无外部服务。

## 7. 测试

```bash
go test ./...          # 全部测试
go test -race ./...    # 竞态检测（含多分区并发推进用例）
```

覆盖：纯匹配裁决（等待优先、分值接近、平局打破、批量上限、输入洗牌不变性）、并发确定性重放、队伍完整性、重复/容量拒绝、配置上下界校验、会话生命周期、掉线重连（窗口内/超时）、补位流程（成功、真实逻辑时钟触发的超时、成功后再次补位、跨分区隔离、多分区并发推进互不干扰）、规则版本切换（在途补位等待仍按建立时规则执行）、后台推进、消息中继与慢消费者、审计有界性。
