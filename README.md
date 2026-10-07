# Metrics Alert Pipeline (Go, local process)

一套完全运行在本地进程内的**指标采集 → 聚合 → 告警判定**组件。无外部依赖（仅标准库），
重点保证并发上报、乱序样本与配置变更下结论可预期、可复现。

## 目录结构

| 包 | 职责 |
|---|---|
| `pkg/model` | 样本、标签、聚合结果、规则、静默、状态、事件等核心类型与校验 |
| `pkg/collector` | 采集入口：非法样本拒绝（可区分原因）、来源标签归并、重复识别 |
| `pkg/aggregator` | 并发安全的定长窗口聚合；乱序样本统一口径合并；幂等累加 |
| `pkg/engine` | 告警状态机 `inactive → pending → firing`，触发/恢复双侧持续时间与抖动保护，多轮（cycle）编号 |
| `pkg/silence` | 按时间窗 + 标签匹配的静默配置 |
| `pkg/notifier` | 按轮次去重（稳定标识 + cycle，容量有界）与静默抑制 |
| `pkg/pipeline` | 全链路编排、事件/统计、基于历史的确定性 Replay |
| `cmd/demo` | 本地可运行演示：并发/乱序/非法/抖动/静默/规则变更 |

## 快速开始

```bash
go run ./cmd/demo          # 本地端到端演示
go test ./...              # 全部单测（日志打印输入样本与判定依据）
go test -race ./...        # 并发安全验证
go test -race -count=10 ./pkg/pipeline   # 并发==串行的稳定性复跑
```

## 1. 多来源采集与标签归并

- 每个样本必须带 `Metric / Timestamp / Value / Source`。
- collector 为每个样本归并两类标签：
  1. 进程级 `ExtraLabels`（如 `env=local`），与样本同名标签冲突时**样本优先**；
  2. 保留标签 `__source__`，强制盖戳为 `Sample.Source`，客户端伪造或不一致会被拒绝。
- 序列身份 `SeriesKey = metric + "|" + labels 指纹`；标签指纹为标签集（按键排序，
  与构造顺序无关）的 SHA-256 截断值，因此来源不同的同一指标是**不同序列**，不会互相串数据。

### 非法样本：明确拒绝且原因可区分

校验在锁外先做，拒绝返回类型化 `RejectError`，原因枚举：

| 原因 | 含义 |
|---|---|
| `empty_metric_name` | 指标名为空/全空白 |
| `bad_timestamp` | 时间戳为零值 |
| `empty_source` | 来源为空 |
| `nan_value` / `infinite_value` | 值为 NaN / ±Inf |
| `invalid_label` | 标签名非法或标签重复 |
| `source_mismatch` | 样本自带 `__source__` 与声明来源不一致 |

### 重复上报：不丢、不重

- 样本唯一身份 `Identity = SeriesKey + "|" + timestamp(unixnano) + "|" + float64bits`。
- collector 与 aggregator 两层都做幂等：同身份样本只接受/累加一次。
- 所有写入路径持锁（map 非并发安全），`-race` 下验证：16 goroutine × 100 份相同批次，
  接受数恒等于不同身份数，无丢失、无重复累加。

## 2. 聚合口径

- 定长窗口，窗口起点对齐 Unix epoch：`windowStart = timestamp.Truncate(windowSize)`。
- 每个「序列 × 窗口」产出：`Count / Sum / Min / Max / First / Last`。
- **乱序统一口径**（与到达顺序无关）：
  - `Count/Sum/Min/Max` 天然交换律；
  - `First` = `(timestamp, identity)` 最小者的值，`Last` = 最大者的值；
  - 同 timestamp 不同值用 identity 做确定性平局裁决，结论唯一。
- `Aggregator.Snapshot()` 按序列键、窗口起点有序返回，方便并发结果与串行结果逐字节比较。
- 已累加的身份在窗口内记录，重复提交返回同一聚合且数值不变。

## 3. 告警状态机规则

每个「规则版本 × 序列」维护一个状态：

```
                 条件为真(首次)                 持续 >= for
   inactive ───────────────────────▶ pending ──────────────▶ firing
      ▲                                  │                      │
      │                                  │ 条件为假(任意时刻)    │ 反例持续 >= resolve
      └──────────────────────────────────┘                      ▼
                   (计时清零，回到 inactive，无状态/无事件)   resolved(恰好一次) → inactive
```

- **触发侧抖动保护**：pending 期间收到任意反例，立即回到 inactive 并清空等待计时；
  短时抖动永远不会触发。再次为真时重新计时。
- **触发持续时间**：`for=0` 表示当 tick/样本时刻立即触发；否则按样本时间戳在
  每个时间点「先 tick（用截至此刻最新值做提升）再处理样本」推进。
- **触发期重复上报无副作用**：firing 中条件持续为真的样本是 no-op，不会重复告警。
- **恢复侧持续时间与抖动保护**：`resolve` 时长独立配置（`Rule.ResolveDuration`），
  与 `for` 互不影响。firing 中收到反例只是开启「恢复等待」：反例必须**连续**保持
  `>= resolve` 才产出恰好一次恢复；等待期间一旦条件重新满足，恢复等待被静默取消
  （回到纯 firing，不产生任何通知），之后的反例重新计时。`resolve<=0` 保持旧行为：
  首个反例立即恢复。
- 判定时刻使用**样本时间戳**而非墙上时钟，因此离线 Replay 与在线处理口径一致。

### 多轮触发/恢复（cycle）

- 同一条序列恢复后条件重新持续满足时，会像第一次一样完整走一遍
  `pending → firing → resolved` 流程；每一次实际触发构成一个**轮次（cycle）**，
  从 1 开始编号，印在 `AlertState.Cycle` 上，通知事件随之携带，可区分「第几轮」。
- 每一轮内：触发通知恰好一次、恢复通知恰好一次；同轮重复/并发重复上报仍只投递一次；
  不同轮次互不顶掉（去重标识包含轮次号，见第 4 节）。
- 轮次号只在**真正触发**时分配：pending 被抖动重置不消耗轮次号。
- 规则版本变更会开启新的版本状态，轮次从 1 重新编号（新旧版本本就不共享结论）。

### 规则版本（配置变更可复现）

- 规则定义指纹包含 `metric / operator / threshold / for / resolve / match`；定义不变保留
  版本号，任何变化产生新版本 `#vN`。
- 规则修改/删除时，旧版本的 firing 状态会被终结为 inactive 并恰好产出一次恢复，
  新旧版本状态严格隔离，绝不把旧判定并入新规则。

## 4. 静默与去重语义

### 静默（Silence）

- 半开窗口 `[StartsAt, EndsAt)` + metric/标签选择器，按 ID 取第一个命中。
- **只抑制通知，不影响状态、轮次与统计**：firing/pending/resolved 照常发生、
  轮次照常编号、状态机统计照常累加，仅 notifier 把该次通知改写为
  `notification_suppressed` 事件。
- 静默**不占用任何去重名额**：窗口结束后，后续轮次的触发/恢复通知照常投递；
  同一轮转换若在窗口外被再次投递（如重放、补发），也仍会送达。

### 去重（Dedup）

- 稳定标识 `AlertKey = ruleID#version + "|" + SeriesKey` 再加上**轮次号 cycle**，
  firing 与 resolved 各自独立占槽，每轮最多各投递一次。
- notifier 是状态机幂等之外的第二道保险；多 goroutine 并发投递同一轮 firing，
  实际下发恰好 1 次。
- **容量有界**：每个 alert key 只保留最近 `RetainCycles` 轮的去重记忆
  （`Pipeline.Config.DedupRetainCycles`，默认 `notifier.DefaultRetainCycles=1024`）。
  轮次号单调递增，因此被淘汰的旧轮次绝不会误伤新轮次；旧轮次的迟到重复投递
  按重复处理（不会再次送达）。保留窗口内的每一轮仍然恰好一次。

## 5. 历史判定复现（Replay）

`Pipeline.Replay()` 使用**当前规则与静默**，对全部已接受样本：

1. 按 `(timestamp, identity)` 稳定排序（消除到达顺序/并发交错）；
2. 在每个时间点先 `engine.Tick(ts)` 再 `Observe(sample, ts)`（确定性持续时间判定）；
3. 用全新的聚合器、状态机、通知器重放，返回事件流、统计、聚合快照与最终状态。

因此：

- 并发乱序上报 vs 严格串行上报，聚合结果与 Replay 结论完全一致（测试逐项断言）；
- 改规则阈值后 Replay，可看到新版本状态机在同一批数据上的结论；
- 删除静默后 Replay，原本被抑制的 firing 变为已通知，且在线状态不被重放污染。

## 6. 验证方法与测试覆盖

```bash
go test -race -count=10 ./...
```

| 测试 | 覆盖点 |
|---|---|
| `TestConcurrentSubmitNoLossNoDoubleCount` | 并发采集不丢不重，拒绝原因可区分 |
| `TestOutOfOrderMergeUniform` / `TestConcurrentEqualsSerial` | 乱序合并口径、并发结果==串行、幂等 |
| `TestPendingFlapNeverFires` / `TestFiresAndResolvesExactlyOnce` | 触发侧抖动保护、for 时长、触发期重复无副作用、恢复恰好一次 |
| `TestMultiCycleReTrigger` / `TestPendingFlapDoesNotBurnCycle` | 多轮触发恢复：每轮触发/恢复各恰好一次、轮次可区分、pending 抖动不占轮次号 |
| `TestResolveFlapDoesNotResolve` / `TestResolveDurationViaTick` / `TestResolveDurationIndependentOfFor` | 恢复侧抖动不误恢复、恢复时长（样本驱动与 tick 驱动）、恢复时长与 for 独立 |
| `TestDedupPerCycle` / `TestDedupMemoryBounded` / `TestConcurrentDispatchPerCycleAtMostOnce` | 按轮去重、去重记忆容量上界与淘汰不误伤新轮、并发同轮仅一次 |
| `TestSilenceDoesNotConsumeCycleSlot` | 静默不占名额、跨轮次不影响后续投递 |
| `TestMultiCycleNotificationsExactlyOncePerRound` / `TestMultiCycleConcurrentEqualsSerial` / `TestMultiCycleDedupRetentionBounded` | 端到端多轮通知次数、静默跨轮、并发==串行==在线对拍、小容量保留下每轮仍恰好一次 |
| `TestRuleChangeIsolatesState` | 规则版本隔离与旧 firing 单次恢复 |
| `TestDedupStableKey` / `TestSilenceSuppressesOnly` / `TestConcurrentDispatchAtMostOnce` | 稳定标识去重、静默只抑制、并发仅投递一次 |
| `TestConcurrentIngestEqualsSerialViaReplay` | 并发 vs 串行：聚合/计数/Replay 结论一致 |
| `TestReplayReproducesAfterSilenceChange` | 静默变更后历史判定可复现，在线状态不变 |

所有单测均通过 `t.Logf` 打印**输入样本**（metric/source/value/timestamp/labels）与
**判定依据**（拒绝原因、first/last 口径、状态迁移条件、轮次号、排序与槽位规则）。

## 7. 与旧版结论的兼容边界

本次新增「多轮触发恢复 + 恢复侧持续时间 + 按轮去重（容量有界）」后，旧口径保持如下：

- **单周期行为不变**：`resolve` 不配置（<=0）时，恢复仍由首个反例立即触发；
  第一轮（cycle=1）的 pending/firing/resolved 转换条件、事件序列与旧版完全一致，
  原有测试全部不改断言继续通过。
- **重复/并发上报口径不变**：同一份样本仍只接受一次，同一轮同一转换仍只投递一次。
- **Replay 口径不变**：仍按 `(timestamp, identity)` 排序、先 tick 后 observe；
  多轮场景下在线（按时间有序到达）与 Replay 结论一致并有对拍测试。
- **有意的语义变化**（相对旧文档描述）：
  - 静默不再「占用」通知名额——旧版被静默的 firing 在窗口结束后不会补发，
    新版若同一转换在窗口外被再次投递则会送达；这只影响「窗口外重复投递同一转换」
    这一边界场景，正常单轮流程（引擎每轮只发一次转换）结论不变。
  - 去重标识增加了轮次号：旧版「同一序列恢复后再次触发会被当作重复吞掉」
    正是本次修复的问题，属于行为修正而非兼容承诺。
  - `Rule` 新增 `ResolveDuration`、`AlertState` 新增 `Cycle/ResolveSince` 字段，
    零值即旧行为/旧含义，存量构造代码无需修改。
