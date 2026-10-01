# 在线匹配与对局中继服务端（进程内实现）

纯 Go 实现的在线匹配 + 对局消息中继服务端。匹配与消息中继全部在本地
进程内完成，客户端连接用内存回环管道模拟（`transport` 包），**不依赖
任何外部服务**（无数据库、无网络端口、无第三方随机源）。

## 能力对照

| 需求 | 实现 |
|------|------|
| 组队整体匹配、分值合并、并发/重复提交只入队一次 | `queue`、`rules`、`engine` |
| 固定种子+相同入队顺序下任意并发结果一致，裁决规则文档化 | `match`、`rules`，见 `docs/matchmaking.md` |
| 会话生命周期明确，单一自洽快照，无半更新 | `session`，见 `docs/session-lifecycle.md` |
| 掉线/窗口内重连/超时补位或结束，不重复计数 | `session`（gathering/active/degraded/closed） |
| 分区并行隔离、无饥饿、容量与批量可配有上限、可区分拒绝 | `engine`、`config`，见 `docs/configuration.md` |
| 规则版本兼容、版本差异可查询追溯 | `rules` v1/v2、审计 `Version` 字段 |
| 状态/迁移历史/匹配输入与依据/异常对象定位 | `audit` |
| 自动化测试 | `go test ./...`（含 `-race`） |

## 快速开始

```bash
go test ./... -race -count=1
go run ./cmd/demo
```

## 包结构

```
types/     共享值类型、状态枚举、拒绝原因
clock/     可注入时钟（RealClock / FakeClock）
config/    配置、默认值、硬上限与校验
rules/     版本化匹配规则（v1 均值 / v2 中位数）与注册表
audit/     有界环形审计（全局 + 按队伍/会话/玩家）
transport/ 内存回环管道（Peer/ServerPipe，可模拟掉线）
queue/     分区队伍队列（容量、去重、FIFO、原子 Take/Restore）
match/     确定性匹配决策（锚点 DFS + 耐心/接近度/字典序裁决）
session/   中继会话生命周期、重连窗口、补位、消息扇出
engine/    分区编排、全局序号、并行推进、会话上限
cmd/demo/  端到端演示
docs/      语义文档
```

## 确定性示例

固定配置（含 `RandomSeed`）与相同入队顺序，任意并发 tick 得到相同成局
结果；详见 `docs/matchmaking.md` 与 `match/match_test.go`。
