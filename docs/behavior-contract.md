# 网关行为约定（阶段 0）

这份文档定义网关面对失败、限流、预算、超时和关停时该怎么做。它是代码、配置和回归验证的共同依据。

**机器可检版本在 [`internal/policy`](../internal/policy/policy.go)：结构体是配置，`Validate()` 是交叉字段约束，`Classify()` / `Decide()` 是重试判定，`StreamCompletion.CountsAsSuccess()` 是流式成败判定。本文件与代码冲突时，以代码为准，并同步修改本文件。**

范围：仅现有 `POST /v1/chat/completions`。不含 Responses、Anthropic 原生 API、音视频、MCP、语义缓存。

**阶段 1 的交付已落地**：总时限、按结果分类的重试、退避与 `Retry-After`、流式不切换、SSE 硬上限、流式完成状态、排空与 `/upstreams`、统一错误码。第 11 节记录剩余差距（阶段 2 起处理）。

---

## 1. 时间预算

一个请求只有一个总时限，所有重试共用它，绝不因为换了一个部署就重新计时。

| 项目 | 默认 | 约束 |
| --- | --- | --- |
| `entrance.timeout` 入口/LB 超时 | 60s | 由部署侧保证 |
| `entrance.safety_margin` 安全余量 | 5s | 留给网关写错误响应 |
| `timeouts.total_deadline` 总时限 | 45s | `total + margin <= entrance.timeout` |
| `timeouts.attempt_timeout` 单次上游时限 | 20s | `<= total_deadline` |
| `timeouts.upstream_connect_timeout` 连接时限 | 5s | `<= attempt_timeout` |

- 剩余时限不足以完成一次尝试时，不再发起新尝试，直接返回 `upstream_timeout`。
- 每次尝试的 context 从**总 deadline**派生，不是从 `context.Background()` 派生。

## 2. 重试判定（核心）

一个尝试失败时，先判断上游**是否可能已经处理了它**，再决定能不能再来一次。判定输入是失败发生的阶段：

| 阶段 `Phase` | 含义 |
| --- | --- |
| `pre_send` | 请求字节还没有交给上游（DNS、建连、TLS 失败） |
| `awaiting_status` | 字节已发出，还没收到响应头 |
| `body_forwarding` | 已收到响应头，正在转发响应体或 SSE |

由此得到结果分类 `Outcome` 和允许的动作 `Decision`：

| 场景 | Outcome | 同一部署重放 | 切换部署 | 终止 |
| --- | --- | --- | --- | --- |
| 建连前失败 | `not_sent` | 允许 | 允许 | — |
| 上游 429 / 408 | `throttled` | **禁止** | 允许 | — |
| 上游其他 4xx | `rejected` | 禁止 | 禁止 | 终止 |
| 已发出后传输失败（超时、reset） | `unknown` | 禁止 | **默认禁止** | 终止 |
| 上游 5xx | `unknown` | 禁止 | **默认禁止** | 终止 |
| 200 但响应体转发中断 | `partial` | 禁止 | 禁止 | 终止 |
| 200 且完整结束 | `processed` | — | — | 完成 |

**默认配置下，凡是上游结果不确定的失败一律不重放**（`retry_on_unknown_outcome = false`）。这是「不造成重复生成」的底线。

只有同时打开 `retry.retry_on_unknown_outcome` **和** `fallback.allow_cross_provider_on_unknown_outcome` 时，才允许在 `unknown` 下切换供应商；即便如此，**同一部署重放永远禁止**（`Validate()` 强制）。

> 注意：这与当前实现相反。现行 `shouldRetry()` 把 5xx 和传输错误一律视为可重试，并在非流式响应体拷贝失败时 `continue` 到下一个部署 —— 那正是重复生成与重复扣费的来源。

实现约定：同一分类下既有「重放同一部署」又有「切换部署」两种许可时（如 `not_sent`），**优先切换部署**；只有该模型仅有一个部署时才重放同一部署。

退避与 `Retry-After`：

| 项目 | 默认 | 说明 |
| --- | --- | --- |
| `retry.max_attempts` | 3 | 所有尝试的总数上限，含首次 |
| `retry.backoff_initial` / `backoff_max` | 200ms / 2s | 指数退避，`backoff_multiplier = 2` |
| `retry.jitter` | true | 必须开启，避免同步重试风暴 |
| `retry.respect_retry_after` | true | 429/503 的 `Retry-After` 优先于本地退避 |
| `retry.max_retry_after` | 10s | 上游给的等待时间上限，超过则按 `backoff_max` |

退避等待计入总时限；等待期间总时限到点即终止。

## 3. 跨供应商回退

- 每个部署必须声明兼容组（`fallback.require_compatibility_group = true`）。只有同一兼容组内的部署才能互相回退。
- **模型别名相同不代表兼容**（`allow_alias_name_as_compatibility` 被 `Validate()` 硬性拒绝为 false）。兼容要显式声明能力、数据区域和输出约束。
- 响应体一旦开始写给客户端（含 SSE 首个字节），**禁止切换上游**，无论后续发生什么。
- 上游不支持请求中的字段（如 `tools`、`response_format`）时，不应通过回退掩盖，应明确报错或按适配矩阵转换。

## 4. 限流与预算

请求速率限制与 token/金额预算是两套东西，不能互相替代。

| 维度 | 粒度 | 说明 |
| --- | --- | --- |
| RPM | 每租户 | 请求速率 |
| TPM | 每租户 + 每模型 | token 速率 |
| 金额预算 | 每租户 + 平台级每供应商 | `budget_window`：`calendar_month` / `rolling_30d` / `rolling_24h`；币种固定为 3 位 ISO 4217 |
| 并发 | 每租户 + 全局 | 防止单租户打满全局信号量 |

执行方式：

- 请求**前**预留（reserve），响应完成**后**核账（settle）。
- 预留和核账以 `request_id` + 尝试序号作为幂等键，重试不会重复扣账。
- 预留带 TTL（`reservation_ttl`，默认 10m，上限 24h），进程崩溃后由清扫器回收，避免额度泄漏。清扫器每 `sweep_interval`（默认 1m，上限 1h）运行一次，扫描本副本见过的窗口键。

**窗口是固定分桶，不是滑动窗口。** 分桶边界处一个租户在相邻两个窗口内各花满一次是可能的（最坏 2 倍）。滑动窗口需要保留窗口内每一笔消费再做区间求和，在月度规模下不可行。若业务不能接受边界突发，应把预算窗口缩短并留出余量。

已实现：`internal/quota`
- `MemoryStore`：进程内实现，语义完整（幂等、过期回收、并发安全），用于 `mode: local` 与降级兜底。
- `RedisStore`：跨副本共享。全部判定逻辑放在 Lua 脚本里执行（`GET/INCRBY/DECRBY/ZADD/ZREM/ZRANGEBYSCORE/HSET`），保证「检查 + 占位」是原子的——两个副本不会同时抢到最后一份额度。客户端是自带的极简 RESP 实现，项目不引入第三方依赖。
- 每个窗口键有三个卫星键：`:u` 已核账、`:h` 在途预留、`:p` 未结算预留的 ZSET（score 为过期时间，用于崩溃回收）。
- 金额一律用 `int64` 微单位（1e-6 货币单位）累计，避免浮点漂移。

共享存储不可用时的降级（`quota.store.on_unavailable`）：

| 策略 | 行为 | 适用 |
| --- | --- | --- |
| `fail_closed` | 拒绝请求 | 预算是硬约束 |
| `fail_open` | 放行但标记为不可核账 | 可用性优先，接受超限 |
| `degrade`（默认） | 本地桶按 `degrade_factor`（默认 0.5）降额运行 | 默认选择 |

**计量要精确，执行可近似**：用量审计事件必须完整、持久；限额执行允许近实时（数十秒级滞后），不必把共享存储放进每次请求的关键路径。

## 5. 流式请求的预算

响应完成前 token 总量未知，因此：

- 按 `quota.stream_reserve_tokens`（默认 4096）预留输入 token + 预估输出上限。
- 超出预留时按 `quota.stream_overage` 处理：`abort`（默认，终止并记为 `truncated`）/ `allow`（允许超额，事后核账）/ `truncate`（向上游请求缩短输出）。
- 网关必须为支持的供应商注入 `stream_options: {include_usage: true}`（`streaming.require_usage_in_stream = true`）。不这样做，流式请求的用量恒为 0，成本核算对流式直接失效。

## 6. 用量缺失与对账

| 情况 | 处理 |
| --- | --- |
| 上游返回完整 usage | `reconciliation = ok` |
| usage 字段缺失 | 按 `usage.missing_usage_policy`：`estimate`（默认，估算并标记）/ `reject` / `allow_zero` |
| 流式中途中断 | `reconciliation = partial`，按已接收内容结算 |
| 供应商返回异常值（负数、超出合理范围） | `reconciliation = missing`，记告警事件 |

- 价格信息版本化，记录计价单位、币种和生效版本（`usage.price_version_required = true`）。
- 审计事件必须落到可查询的持久存储（`usage.audit_sink = both`），仅 stdout 不足以做成本对账。

## 7. 流式中断语义

SSE 响应的完成状态 `StreamCompletion`：

| 状态 | 含义 | 计入成功 |
| --- | --- | --- |
| `completed` | 收到终止事件（`[DONE]`）或正常结束 | 是 |
| `aborted_client` | 客户端断开 | 否 |
| `aborted_upstream` | 上游中断 | 否 |
| `aborted_gateway` | 网关主动中断（预算、超时、排空、限流） | 否 |
| `truncated` | 触发大小上限 | 否 |

**只有 `completed` 允许记为 2xx 成功请求**。日志、指标、审计事件中的状态字段必须与之一致。

内存与大小上限：

- `streaming.max_event_bytes`（默认 256 KiB）：单条 SSE 事件上限，必须**边读边判**，不能先读完一整行再判。
- `streaming.max_stream_bytes`（默认 16 MiB）：整个流式响应上限。
- `streaming.abort_on_client_gone`：客户端断开后立即取消上游请求，并记为 `aborted_client`，**不得归因于上游故障、不得触发熔断**。

## 8. 错误码

所有客户端可见错误使用统一信封 `{error:{message,type,code}}`，`code` 取自封闭集合：

`unauthorized` · `model_not_allowed` · `model_not_found` · `invalid_request` · `request_too_large` · `rate_limit_exceeded` · `budget_exceeded` · `gateway_overloaded` · `upstream_unavailable` · `upstream_rate_limited` · `upstream_timeout` · `upstream_error` · `stream_aborted` · `draining` · `not_found` · `internal_error`

- 上游错误默认归一为上述码；`error_codes.passthrough_statuses` 白名单内的状态码才透传原始响应体。
- 结果分类到错误码的映射：`throttled → upstream_rate_limited`，`rejected → upstream_error`，`unknown → upstream_unavailable`，`partial → stream_aborted`。
- 每个错误响应都带 `X-Request-ID`；4xx/5xx 中的可重试响应带 `Retry-After`。

## 9. 排空与探针

| 探针 | 语义 | 依赖上游 |
| --- | --- | --- |
| `GET /healthz` | 进程存活 | **否**，硬性要求 |
| `GET /readyz` | 可接收新流量；开始排空后返回 503 | 否 |
| `GET /upstreams`（新增） | 各供应商可用性，仅供观测 | 是 |

- 收到 SIGTERM 后：先切 readiness 为失败，再等待 `drain.timeout`（默认 30s，上限 10m）让在途请求结束。
- `liveness_depends_on_upstream` 必须为 false —— 上游故障绝不能导致全量 Pod 重启。
- 部署侧需配 `preStop`（等待端点摘除）+ `terminationGracePeriodSeconds > drain.timeout`，三者对齐。
- 排空期间新请求返回 503 + `draining`。

## 10. 观测

- 指标标签只允许白名单 `tenant / client / model / provider / route / status_class / outcome / error_code`，基数上限 200。**禁止**用 request_id、用户 ID 或任意用户值做标签。
- 必须包含：请求耗时直方图（有桶，能算分位数）、上游延迟与错误、重试/回退、限流拒绝、熔断状态、SSE 中断（按完成状态分）、token 用量、TTFT、流式输出节奏。
- 结构化审计事件至少含：request_id、租户、模型别名、供应商、上游模型、输入/输出 token、单价版本、金额、预留与核账 ID、对账状态、完成状态。

## 11. 剩余差距（阶段 2 起处理）

阶段 1 已修：总时限、按结果分类的重试（含 5xx 默认不重放、已发出不重放、响应已开始不切换）、退避与 `Retry-After`、SSE 单事件与总大小硬上限、流式完成状态与「不计成功」、客户端断连不归因上游、`/readyz` 排空态与 `/upstreams`、`/healthz` 不依赖上游、统一错误码与可选透传。

| # | 位置 | 剩余差距 | 阶段 |
| --- | --- | --- | --- |
| 1 | 用量 | 审计事件已可查（stdout / 追加写文件），但尚未接入可聚合查询的存储；流式 token 仍依赖上游上报 | 4 |
| 2 | 身份 | 无密钥轮换/撤销窗口，租户等于 `client_id`，密钥只在启动时读取 | 3 |
| 3 | 出口 | 上游仍可用明文 HTTP，无部署侧出口限制 | 3 |
| 4 | 部署 | `replicas: 1`、`image: latest`，无 PDB / NetworkPolicy / Redis 实例 | 4 |
| 5 | `metrics.go` | 无直方图桶、无标签，无法按租户/模型/供应商切分 | 4 |
| 6 | CI | 无预发环境故障场景验证；Redis 集成测试需显式开启 | 4 |

补充：跨供应商回退目前只按 `models[].deployments` 的声明顺序尝试，兼容组尚未进入配置文件；`fallback.require_compatibility_group` 已生效于契约层，配置结构在后续阶段落地。平台级供应商预算是**事后核账**（请求前不知道哪个供应商会应答），超额在下一请求被拒绝并记录事件。

补充：跨供应商回退目前只按 `models[].deployments` 的声明顺序尝试，兼容组尚未进入配置文件；`fallback.require_compatibility_group` 已生效于契约层，配置结构在阶段 2 落地。

## 12. 验收断言

契约层断言在 `internal/policy/policy_test.go`，请求路径断言在 `internal/gateway/gateway_test.go` 与 `stream_test.go`。两者都不得失败：

请求路径（`go test -race ./internal/gateway/`）：

1. `TestNoReplayOnUpstream5xx`：上游 5xx 时备用供应商不被调用，`replay_skipped` 计数 +1。
2. `TestNoReplayAfterRequestWasSent`：字节已发出后连接中断，备用供应商不被调用。
3. `TestTotalDeadlineIsSharedByAllAttempts`：总时限 1s 时整体耗时 < 1.2s，且不再发起新尝试。
4. `TestStreamIsNotHandedToAnotherProviderAfterWriting`：流式中途断流后备用供应商不被调用，且 `incomplete` +1（不计成功）。
5. `TestOversizedSSEEventIsTruncated` / `TestStreamTotalSizeLimit`：超大事件与超大流被截断，不缓冲完整事件。
6. `TestIncompleteNonStreamResponseIsNotRetried`：上游 200 但响应体不完整时不重试，返回 502。
7. `TestClientSideFailureDoesNotOpenCircuit`：客户端断连与网关超时不触发熔断。
8. `TestDrainingStopsNewTraffic` / `TestReadinessFailsWhileDrainingButLivenessDoesNot`：排空后拒绝新请求，`/readyz` 转 503，`/healthz` 恒 200。

共享限额与成本核算（`go test -race ./internal/quota/ ./internal/cost/ ./internal/gateway/`）：

9. `TestConcurrentReserveNeverOversells` / `TestRedisNeverOversellsUnderConcurrency`：并发预留绝不超卖（Redis 版本跑真实 Redis 与 Lua）。
10. `TestReserveIsIdempotentPerAttemptID` / `TestSettleIsIdempotent`：重试不会重复占用，重复核账不会重复扣款。
11. `TestExpiredHoldsAreReclaimed` / `TestRedisSweepReclaimsExpiredHolds`：预留过期后归还额度。
12. `TestReplicasShareOneTenantBudget`：两个副本共用一个租户预算，第二个副本被拒。
13. `TestBudgetExhaustionBlocksTheNextRequest` / `TestRequestRateLimitIsSeparateFromBudget`：金额与速率是两个维度，各自返回自己的错误码。
14. `TestUsageEventIsTraceableByTenantModelAndProvider`：完成请求可按租户、模型、供应商追溯用量与费用。
15. `TestUncertainFailureIsChargedOnce`：不确定结果的失败只预留一次、只核账一次。

Redis 集成测试默认跳过，设置 `AI_GATEWAY_REDIS_TEST_ADDR`（例如 `127.0.0.1:6399`）后运行。

契约层（`go test ./internal/policy/`）：

1. 默认契约下 `Decide(unknown)` 为终止（不重放、不切换）。
2. 默认契约下 `Decide(processed)` 与 `Decide(partial)` 均为终止。
3. `Decide(not_sent)` 允许重放；`Decide(throttled)` 只允许切换、不允许重放同一部署。
4. 除 `completed` 外，任何流式完成状态 `CountsAsSuccess()` 均为 false。
5. `total_deadline + safety_margin <= entrance.timeout`。
6. `liveness_depends_on_upstream` 与 `allow_alias_name_as_compatibility` 恒为 false。
7. 默认契约下审计事件落到持久存储。
