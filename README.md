# metrics-alert-pipeline-go

本地进程内运行的**指标聚合 + 告警评估**组件，用一条确定性链路模拟：

```
多来源样本(并发/乱序/重试)
      │  校验 + 标签归并 + 去重
      ▼
 窗口聚合器 (窗口 × 序列)
      │  每窗口一次
      ▼
 规则状态机 (inactive → pending → firing → inactive)
      │  to_firing / to_inactive
      ▼
 静默匹配 → 通知去重 → 对外通知 + 统计
```

无外部依赖，`go test ./...` 与 `go run ./cmd/demo` 即可验证全部语义。

## 目录结构

| 包 | 职责 |
|---|---|
| `pkg/metric` | 样本类型、合法性校验（可区分原因）、标签归并、并发安全的窗口聚合、样本 ID 去重 |
| `pkg/alert` | 告警规则、比较算子、规则指纹、`inactive/pending/firing` 状态机 |
| `pkg/silence` | 静默窗口配置与"某时刻 + 某标签集"的纯查询 |
| `pkg/engine` | 编排：上报→聚合→评估→静默→通知去重→统计；以及历史样本重放 `Replay` |
| `cmd/demo` | 单进程端到端演示 |

## 一、采集与聚合口径（`pkg/metric`）

### 1. 标签归并与序列标识
- 每个来源可配置默认标签（如 `env=prod`），上报时按 **来源默认标签 → 样本标签** 合并，样本标签优先。
- 聚合器注入保留标签 `__source=<来源>`，因此**样本禁止携带 `__` 前缀标签**（会被拒绝，原因 `reserved_label_key`）。
- 序列标识 `SeriesKey = 指标名 + 排序后的标签集`：标签 map 的写入顺序不影响 key，不同来源相同指标名+相同标签归并为同一条序列。

### 2. 窗口
- 窗口长度固定，按 **Unix 纪元整窗对齐**（`windowStart = ts.Truncate(window)`），口径全局统一、与时区无关。
- 每个"窗口 × 序列"产出一个聚合：`Count / Sum / Min / Max / First / Last`。
- **乱序安全**：`First/FirstTS` 与 `Last/LastTS` 严格按**采样时间戳**而非到达顺序选取；样本先到 w3 再到 w1 也各自进入正确窗口。对同一批样本，无论到达顺序如何，聚合结果完全一致。
- 判定默认使用窗口内 `Last`（采样时间最晚的值）。

### 3. 并发与重复上报
- 所有写入经同一把互斥锁串行化到内部状态，配合测试 `-race`，保证**不丢计数、不重复累加**。
- 样本可选携带稳定 `ID`：同一 ID 全局只生效一次；重复上报计入该窗口的 `Duplicate` 计数与全局 `TotalDup`，但**不累加任何值**，可安全重试。

### 4. 非法样本与可区分原因
非法样本不进入任何聚合，返回带原因的错误（`RejectedSample`）：

| 原因 | 含义 |
|---|---|
| `empty_source` / `empty_metric_name` | 来源 / 指标名为空 |
| `invalid_metric_name` / `invalid_label_key` | 命名不符合 `[A-Za-z_][A-Za-z0-9_:]*`（标签不允许冒号） |
| `reserved_label_key` | 标签键使用了保留前缀 `__` |
| `value_is_nan` / `value_is_inf` | 值为 NaN / ±Inf |
| `counter_value_negative` | counter 出现负值 |
| `timestamp_out_of_range` / `timestamp_in_future` | 时间戳越界（早于 Unix 纪元）/ 超前超过容忍偏斜（10 分钟） |

## 二、告警状态机（`pkg/alert`）

以**窗口为最小离散时间单位**对每条"规则 × 序列"独立推进；某窗口无数据视为"未越界"。

```
                 breach(连续) 达 ForDuration
  inactive ───────────────────────────────► firing
     ▲     │ breach(首个窗口)                   │
     │     ▼                                    │
     └─ pending（任一窗口恢复即立即回 inactive） │
          ▲ 短抖动在此被吞掉，不产生任何对外通知  │
     recovery(连续) 达 KeepDuration             │
  firing ──────────────────────────────────────┘
```

- **inactive → pending**：第一个越界窗口。
- **pending → firing**：从首个越界窗口起连续越界窗口数 × 窗口长度 ≥ `ForDuration`，产生**一次** `to_firing`。
- **pending 中任一窗口恢复**：立即回到 inactive，**不产生通知** —— 短时尖峰抖动不会误触发，也不会污染下一次判定（重新从 0 计时）。
- **firing 中持续/反复越界**：保持 firing，**绝不重复产生 `to_firing`**；恢复观察期间再次越界会取消恢复计时、继续留在 firing。
- **firing → inactive**：条件恢复后连续正常窗口数 × 窗口长度 ≥ `KeepDuration`，产生**恰好一次** `to_inactive`（恢复通知不会丢）。恢复保护期内的抖动同样被吸收。

> `ForDuration=0` 时首个越界窗口即触发；事件都带 `Reason` 字段，说明本窗口的值、比较式、持续时长与状态依据。

### 规则指纹与配置更新
`Rule.Fingerprint()` 对规则内容做**与 map 顺序无关**的稳定哈希（JSON 确定性序列化 + SHA-256 截断）。阈值/选择器/持续时间任意一项变化指纹即变。
`UpdateRules` 返回新增/变更/移除三类 ID，并清除受影响规则的序列状态，避免旧配置状态污染新结论。

## 三、静默与去重语义

### 静默（`pkg/silence`）
- 一条静默 = 标签相等选择器（空选择器匹配任意）+ `[StartsAt, EndsAt)` **左闭右开**窗口。
- `MatchAt(labels, at)` 返回该时刻同时满足"窗口生效 + 标签命中"的静默 ID（排序、确定）。
- **静默只抑制通知，不改变状态、不改变统计**：被静默期间状态机照常进入/退出 firing，`Firing/Resolved` 计数照常；只是通知 `Sent=false`，计入 `Silenced` 而非 `Notified`。
- 静默更新只影响之后窗口的通知；历史已形成的通知与状态不变。

### 通知去重（`pkg/engine`）
- 稳定通知标识：`ruleID | seriesKey | kind#occurrence`。`occurrence` 是该序列该类事件（`to_firing`/`to_inactive`）的递增轮次：首次触发 `#1`、恢复后再次触发 `#2`……既稳定又能区分不同生命周期。
- 双层保障：状态机天然不重复发事件；引擎再用已发送 ID 集合兜底，同一 ID 全局最多一次。
- 同一窗口重复 `RunWindow` 会被忽略（按窗口起点去重），防止评估重入造成重复通知。

### 配置变更后复现历史判定：`engine.Replay`
`Replay(samples, ReplayConfig{...})` 用一组**历史样本 + 一份配置快照**从零构建全新聚合器/评估器/静默存储：
1. 样本先按**采样时间**（同时间戳再按 ID/来源/指标名）稳定排序后喂入，因此结论与采集当时的到达顺序无关；
2. 逐窗口推进，产出每个窗口的内部事件（含 pending 流转依据）与对外通知（含被静默的）；
3. 重复 ID 仍只生效一次，非法样本单独汇总且原因可区分。

同一批历史样本换用新阈值/新选择器/新静默重跑，即可确定性得到"当时若是这套配置会怎样"，且与在线引擎当前状态完全解耦。测试 `TestReplayWithChangedConfig` 演示了阈值上调与静默开关下的复现差异。

## 四、并发与乱序的可预期性

- 聚合阶段对并发写入加锁；**评估阶段必须按窗口时间顺序调用**（聚合已把乱序样本归位，所以评估侧只需顺序消费窗口）。
- `TestConcurrentEqualsSerial`：同一批样本（含跨序列并发写入、同 ID 重试）分别用"串行"与"高并发"喂入，再按相同窗口顺序评估，断言两侧统计量、通知 ID/类型/值/发送与否**逐一相等**。
- 所有事件、通知、快照均按 ruleID / seriesKey / 窗口时间排序产出，避免 map 迭代随机性导致输出不稳定。

## 五、验证方法

```bash
# 全量单测（带竞态检测）
go test ./... -race

# 查看带输入样本与判定依据的详细日志
go test ./... -v

# 端到端演示：并发乱序上报 + 非法样本 + 抖动 + 静默 + 配置变更重放
go run ./cmd/demo
# 可复现不同并发/乱序排列：
go run ./cmd/demo -seed 1 -seed 2
```

测试覆盖：

| 测试 | 覆盖点 |
|---|---|
| `metric.TestValidateReasons` / `TestMoreRejectReasons` | 各类非法样本与可区分原因 |
| `metric.TestOutOfOrderMerge` / `TestGaugeFirstLastBySampleTime` / `TestCrossWindowOutOfOrder` | 同窗口与跨窗口乱序合并口径 |
| `metric.TestDuplicateID` | 同 ID 重试只生效一次 |
| `metric.TestConcurrentNoLoss` | 1 万条并发上报计数无丢失（`-race`） |
| `alert.TestPendingThenFire` / `TestShortFlapNoFire` | pending 持续时间与进入前抖动保护 |
| `alert.TestNoDuplicateAlertAndResolve` | firing 重复越界不重复告警、恢复恰好一次、退出抖动保护 |
| `alert.TestRuleFingerprint` | 指纹与 map 顺序无关、配置 diff |
| `silence.TestMatchAt` | 左闭右开时间边界与标签匹配 |
| `engine.TestConcurrentEqualsSerial` | 并发结果与串行结果逐项一致 |
| `engine.TestSilenceSuppressesOnly` | 静默仅抑制通知，状态统计不变 |
| `engine.TestReplayDeterministicAndReproducesOnline` | 重放确定、且与在线推进结论一致 |
| `engine.TestReplayWithChangedConfig` | 阈值/静默变更后历史结论按新配置复现 |

所有单测均用 `t.Logf` 打印**输入样本**与**判定依据**（值、比较式、持续时长、状态流转、静默命中等），便于审计"为什么是这个结论"。
