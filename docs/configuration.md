# 配置项、容量与并发隔离（Configuration & Concurrency）

`config.Config` / `config.PartitionConfig` 的所有容量都有硬上限
（`config` 包常量），启动时校验；超限提交得到**可区分的拒绝结果**而不是
无限排队。

## 1. 全局配置 `Config`

| 配置 | 含义 | 默认 | 硬上限 |
|------|------|------|--------|
| `Partitions` | 分区列表，ID 不可重复 | 1 个 default | 1024 |
| `MaxSessions` | 全局并存会话数硬上限，超出返回 `too_many_sessions` | 4096 | 100 000 |
| `EventHistoryCap` | 审计每对象环形缓冲容量（全局+每键各一份） | 1024 | 100 000 |
| `RandomSeed` | 规则随机种子（当前裁决不消耗随机，预留） | 1 | — |
| `TickIntervalMs` | 后台自动 tick 间隔；0 表示不自动推进（测试用手动 tick） | 50 | — |
| `PipeBuffer` | 每条内存管道每方向的信道缓冲 | 64 | 10 000 |

## 2. 分区配置 `PartitionConfig`

| 配置 | 含义 | 默认 | 硬上限 |
|------|------|------|--------|
| `ID` | 分区标识 | default | — |
| `Version` | 匹配规则版本（v1 均值 / v2 中位数） | v1 | — |
| `QueueCapacity` | 在队队伍数硬上限，超出返回 `queue_full` | 1024 | 100 000 |
| `MaxTeamSize` | 单队人数上限，超限返回 `oversized_team` | 5 | 100 |
| `TargetTeamSize` | 每局总人数（必须恰好凑满才成局） | 10 | — |
| `MaxBatch` | 单 tick 最多成局数，保证单批数量可配且有界 | 16 | 1 000 |
| `ScoreWindow` | 分值软屏障阈值（极差 ≤ 阈值才可成局） | 200 | — |
| `MaxWaitMs` | 耐心阈值；最老队伍等待超过它后放开分值屏障 | 30000 | — |
| `ReconnectHoldMs` | 掉线保留窗口（毫秒） | 10000 | — |
| `Reconnect` | 窗口超时策略：`end_session` / `hold_slot` / `backfill` | end_session | — |
| `SearchWindow` | 每锚点向后搜索组合的队伍数窗口 | 64 | 256 |

## 2. 拒绝结果一览（可区分）

| RejectReason | 触发条件 | `Detail` 定位 |
|---|---|---|
| `queue_full` | 分区队列达到 `QueueCapacity` | 分区 ID |
| `too_many_sessions` | 全局会话数达到 `MaxSessions` | 分区 ID（已取队伍会按原序号无损回退队列） |
| `duplicate_team` | 同 TeamID 已在队 / 会话 ID 重复 | 队伍 ID |
| `player_already_queued` | 玩家已在另一支在队队伍中 | 玩家 ID |
| `empty_team` | 空队伍或非法玩家 | 队伍 ID |
| `oversized_team` | 队伍人数超 `MaxTeamSize` | 队伍 ID |
| `unknown_partition` | 提交到不存在的分区 | 分区 ID |
| `unknown_session` | 操作不存在/已关闭的会话或未知成员 | 会话/玩家 ID |
| `version_incompatible` | 跨版本提交（队伍版本 ≠ 分区版本） | 版本号 |
| `backfill_unavailable` | 窗口外/已移除席位重连，或补位无替补 | 玩家/会话 ID |

## 3. 并发推进与隔离

- 每个分区拥有独立互斥锁与独立推进 goroutine（`Engine.Start`），并在
  新提交时唤醒；慢分区不会阻塞快分区。
- 固定按分区 ID 排序调度，且等待最久的队伍在裁决中优先，因此任何
  分区、任何队伍都不会被长期饥饿。
- 会话超时推进由独立循环处理，会话各自加锁，互不阻塞。
- 高负载保证：
  - 队列、会话、审计、管道全部有容量上限，超限显式拒绝，不存在无界
    内存增长；
  - 管道发送非阻塞，无“持锁等待慢消费者”路径，`-race` 高负载测试
    （`TestHighLoadBoundedNoDeadlock`）验证不死锁。
- 当 tick 已取出队伍但因全局会话上限无法全部建局时，未建局队伍会以
  原提交序号无损 `Restore` 回队列（保持原 FIFO），下一个 tick 可继续。

## 4. 版本兼容

- 队伍入队时绑定分区当前版本，会话成局时绑定版本；后续规则更替不
  影响已在队记录的冻结分值、已产生的匹配结果和进行中的会话。
- 跨版本提交直接 `version_incompatible` 拒绝；分区规则与版本差异可
  通过 `Engine.PartitionRules(id)` 与 `Engine.RuleVersions()` 查询，
  并在审计事件的 `Version` 字段中追溯每个对象所属版本。
  补位只接受同版本队伍。

## 5. 手动 tick 与确定性测试

测试中把 `TickIntervalMs` 设为 0（不启动后台循环），注入
`clock.FakeClock`，调用 `Engine.TickPartition(id)` 手动推进，配合固定
种子即可逐条复现匹配结果与超时行为。
