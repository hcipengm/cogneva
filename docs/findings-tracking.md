# 发现台账（会话跟踪）

建立：2026-11-11 · 最近更新：2026-11-11

本文件是本次会话发现的问题的唯一台账。主线是「全局禁止硬编码 / 声明只写一次」。
与主线无关的发现记在 N 系列，也照样跟踪，不因为不相干而丢弃。

规则：

- 每条都有状态：`已完成` / `进行中` / `未开始` / `待决策`。
- 只有关闭判据被一条真的跑绿的门（或一条已推送的提交）满足，才可改成 `已完成`；
  「讨论过了」「决定这么做」不是完成。
- 未完成项留到关闭为止。排序原则：覆盖缺口 > 真实失效 > 去重 / 搬迁。

## 总览

| ID | 状态 | 一句话 |
|---|---|---|
| M1 | 已完成 | 字面量读法收敛为一份，并有一道门判「不再长出第二份」 |
| M2 | 已完成 | Wall / Report 控制律一份；夹具里逐字重复的体已清 |
| M3 | 已完成 | 配置面读法一份；每个类型产生的旋钮至少被一份交付文档写出 |
| M4 | 已完成 | 嵌套配置节进入判据：类型图递归字段声明，内层与顶层同一条判据 |
| M5 | 已完成 | `produced` = 读回 ∪ 字段声明，`skip_serializing_if` 字段不再消失 |
| M6 | 未开始 | 9 个配置字段没有任何生产读取 |
| M7 | 未开始 | 4 个字段只有同名碰撞，没有配置读取 |
| M8 | 未开始 | `multi_backend_consumer.group` 被硬编码成 `"supervisor-multi"` |
| M9 | 未开始 | `/metrics` 在四处硬编码，`metrics.endpoint` 无人读 |
| M10 | 未开始 | `cogneva.example.json` 是手工维护的第二配置源 |
| M11 | 已完成 | 4 个藏起来的说明键与 19 处 `dormancy` 改名成 `_comment_*` |
| M12 | 未开始 | 读法普查的人口只有 `tests/`，且头部有一句与事实不符 |
| M13 | 未开始 | 纯词法读法该搬到 `cog-test-support`（dev-only 有门保证） |
| M14 | 未开始 | 生产侧字段消费普查尚未成为判据 |
| N1 | 待决策 | `cog-core::contract::reflection` 里的生产解析器与 core 的定位冲突 |

---

## 主线

### M1 字面量读法收敛为一份

- 状态：已完成（提交 `f117611`、`b979f2b`，已双推）
- 证据：`crates/cogneva/tests/common/declared_once.rs` 的
  `literals_in` / `literal_values`；`every_declared_once_gate_shares_the_one_reader_test`
  走遍各 crate 的 `tests/`，判「同一个读法不再长出第二份」，并给跨方言读取器留了
  带理由的登记表。
- 关闭判据：该门与全量套件跑绿。已满足。

### M2 Wall / Report 控制律一份

- 状态：已完成（提交 `f117611`、`b979f2b`，已双推）
- 证据：`Declared` / `Wall` / `Report` / `Site` / `Exemption`；
  `Wall::assert_clean` 把「取现场 + `Report::new` + `assert_clean`」收成一步；
  `the_fixture_reading_steps_are_written_once_test` 判夹具里不再有逐字重复的体。
- 关闭判据：全量套件 1416 条跑绿。已满足。

### M3 配置面读法与旋钮判据一份

- 状态：已完成（提交 `b979f2b`，已双推）
- 证据：`crates/cogneva/tests/common/config_surface.rs`；
  `every_config_knob_is_named_by_a_delivered_document_test` 判「类型产生的键至少
  被一份交付文档写出」，`CODE_ONLY` 登记了 40 条审计过的例外，`PRODUCED_FLOOR=300`、
  `DOCUMENT_FLOOR=5`。
- 关闭判据：该门跑绿。已满足。
- 注意：M4 关闭后，内层节与顶层走同一条判据。

### M4 嵌套配置节没有进判据

- 状态：已完成（提交 `d65dbc2`，已双推）
- 证据：
  - `common/config_surface.rs` 新增类型图：`TypeGraph` 从 `crates/*/src` 读声明，
    `Reader::of` 记下 `type_name`，`TypeInfo` / `FieldInfo` 保留字段名、别名、
    `skip` / `skip_deserializing` / `flatten` 与 `rename_all`；
  - `walk` 跟着字段类型下钻：`Option` / `Vec` / `Box` / `Arc` / `Cow` 剥壳，
    本地结构体递归，`HashMap` / `Value` 是动态键，数组元素用 `section[]` 作路径，
    带环保护；未知键在**任意一层**都是红；
  - `with_nested` 只剩 `self_evolution → artifact_evolution` 一条：父类型没有该字段，
    那是 cog-reflection 按 JSON 指针自己读的节；
  - 自证（`config_surface::self_proofs`）：
    `a_key_inside_a_nested_section_with_no_field_is_a_finding`、
    `a_key_inside_an_array_element_with_no_field_is_a_finding` 证内层未知键会红；
    `produced_is_read_from_the_declaration_not_from_a_default` 在临时树里删掉字段，
    下一次读 `produced` 少一个键。
  - 递归遍历与配平括号复用共享实现（`common::routes::walk` / `matching_brace`），
    夹具普查门跑绿。
- 顺带判出的真实缺陷（已随本条修掉）：
  - `deploy/helm/cogneva/files/cogneva.json` 的
    `observability.infra_watch.rules[]._comment_dormancy` 原名 `dormancy`，
    19 处，`InfraRule` 没有该字段，交付比较器也不丢它——说明文字被当成旋钮；
  - `_for_seconds_comment`、`_retired_rules_comment`、`_mirror_alerts_comment`
    不以 `_comment` 开头，同一形状。
  - 全部改名后重新渲染 k3s 静态清单与三份 profile。
- 嵌套判据上线后暴露 26 个「类型产生、交付文档没写」的旋钮，已逐条关闭：
  - 写进 chart（权威交付文档）：`dag_executor.nats.*` 4 条、`observability.loki.*`
    4 条、`self_evolution.build_gate.*` 10 条实测值；
  - `CODE_ONLY` 新增 8 条：`dag_executor.nats_url`、`bot_identity` 3 条、
    `data_volume_watch` 2 条、`deploy_admission.server_dry_run`、
    `system.enabled_plugins`，每条写明为什么只能由代码或环境变量给值。
- 关闭判据：两道门 + 自证 + 夹具普查门全绿。已满足。

### M5 `skip_serializing_if` 字段对读回不可见

- 状态：已完成（提交 `d65dbc2`，已双推）
- 证据：
  - `produced` 现在是两处之和：读回该节得到的键 ∪ 类型声明的字段键（减掉
    `#[serde(skip)]` 与 `#[serde(skip_deserializing)]`）；别名也算同一个旋钮；
  - 自证 `a_field_serialization_omits_is_still_produced`：`dag_executor` 的默认读回
    里没有 `nats_url`，而 `produced` 有它；同时断言内层
    `nats.stream_retention` / `nats.consumer_ack_wait_secs` 被走到；
  - `system.enabled_plugins` 在 `CODE_ONLY` 里写明「`None` = 全部插件，写进样例即
    变成白名单」。
- 关闭判据：上述自证跑绿。已满足。

### M6 9 个配置字段没有任何生产读取

- 状态：未开始
- 证据（探针：生产源剥掉注释与字符串后找 `.field` 读取；破坏性改写与解构读取
  需要在关闭时逐条人工确认，探针只作候选）：
  - `app.config_dir`：只有环境映射 `cogneva/src/config_loader.rs:64`；
    `cog-core/src/config_sections.rs:76` 还写着「config_dir … read at startup」——与代码不符；
  - `dag_executor.retry_delay_ms`：无读取；
  - `self_evolution.notify_on_success`：无读取；
  - `system.grpc_reconnect_interval_secs`、`system.probe_timeout_secs`、
    `system.http_timeout_secs`：只出现在 `config_loader.rs:406/410/414` 的环境映射里；
  - `system.memory_message_broadcast_capacity`：无读取；
  - `system.vector_backend_required`：无读取；
  - `tuning.max_summaries`：`TuningConfig::load()` 的调用点只读 `stream_capacity`
    （`cog-llm/src/plugin.rs:38`、`cogneva/src/lib.rs:426`、`eval_table.rs:2243`）。
- 关闭判据：每个字段二选一——接线（生产侧真的读它、有行为证据）或删除（字段、环境
  映射、文档键、`CODE_ONLY` 条目、`config_sections` 里的说明一起删）。零永久豁免。

### M7 4 个字段只有同名碰撞，没有配置读取

- 状态：未开始
- 证据：
  - `multi_backend_consumer.group`：字段存在、环境映射存在（`config_loader.rs:504`），
    但真正被读的是构造器里写死的 `"supervisor-multi"`（见 M8）；
  - `providers.vector`：`ProviderConfigs` 有该字段，生产侧只读到 `storage`、`wiki`、`media`；
  - `tuning.high_watermark` / `tuning.low_watermark`：命中的 `.high_watermark` /
    `.low_watermark` 都属于 `EventStream` 的配置（`cog-core/src/contract/event.rs`），
    不是 `cog_llm::config::TuningConfig`。
- 关闭判据：同 M6——接线或删除，零永久豁免。`group` 是真实失效，按 M8 接线。

### M8 `multi_backend_consumer.group` 被硬编码

- 状态：未开始
- 证据：`crates/cog-supervisor/src/multi_backend_consumer.rs:38`
  `group: "supervisor-multi".into()`；构造器 `new(primary, event_tx, channel)` 根本不收
  `group`，全树也没有 `with_group`。配置默认值 `"cogneva-consumer"` 从未到达这里。
- 影响：运维改 `multi_backend_consumer.group` 不会改变消费组，两个部署拼在同一组上
  时行为与文档相反。
- 关闭判据：构造器收到配置里的 `group`；一条门判「配置值改变 → 消费组改变」，并且
  删掉接线后该门红。

### M9 `/metrics` 四处硬编码，`metrics.endpoint` 无人读

- 状态：未开始
- 证据：
  - 生产路由：`crates/cog-extension/src/command_server.rs:828`、
    `crates/cog-gateway/src/lib.rs:702`、
    `crates/cog-gateway/src/security_gateway.rs:10809`、`:10855`；
  - `MetricsConfig.endpoint`（`cog-core/src/config.rs:800`）默认值在 `:842` 也是字面量；
    `observability_text.rs:140` 另有一份 `("endpoint", "/metrics")`；
  - 没有任何生产代码读 `metrics.endpoint`。
- 关闭判据：这个路径只声明一次；四处路由与说明文字从同一处取；一条门判「改声明值，
  所有站点跟着变」。

### M10 `cogneva.example.json` 是手工维护的第二配置源

- 状态：未开始
- 证据（2026-11-11 实测）：
  - example：438 个键 / 377 个叶子；chart：582 个键 / 509 个叶子；
  - example 独有的叶子只有两个：
    `observability.infra_watch._for_seconds_comment`（说明文字，见 M11）、
    `self_evolution.test_command`（真实字段，`cog-core/src/config.rs:1588`）；
  - `test_command` 在 chart、k3s ConfigMap、三份 rendered 里都不存在；
  - `deploy/README.md:21` 明说 Helm chart 是「应用拓扑唯一权威源」「唯一手改入口」；
    顶层与部署 README 都没有把 example 当入口介绍；
  - 自进化的 `unreachable_default_reason` 把 example 与 chart 并列为部署的两份配置
    （`cog-reflection/src/change_pipeline.rs` 的 `DOCUMENTS`）。
- 关闭判据：二选一并落地：
  - (a) 保留「小样例」角色：example 由 chart 生成（受控子集），重新生成必须逐字相同；
    额外项一个都不许有；
  - (b) 删除 example：把 `test_command` 迁进 chart，改自进化判据与所有引用它的测试。
  - 无论哪条，`test_command` 必须出现在 chart 里。

### M11 `_for_seconds_comment` 是藏起来的说明文字

- 状态：已完成（提交 `d65dbc2`，已双推）
- 证据：
  - 改名并同步所有交付文档：`_for_seconds_comment` → `_comment_for_seconds`、
    `_retired_rules_comment` → `_comment_retired_rules`、
    `_mirror_alerts_comment` → `_comment_mirror_alerts`；
  - 同一形状的 `dormancy`（chart 内 19 处）→ `_comment_dormancy`；
  - k3s ConfigMap 与三份 rendered profile 由 `deploy/scripts/render-deploy.sh`
    重新生成（Chart 仍是唯一手改入口）。
- 关闭判据：M4 的内层判据把「不以 `_comment` 开头的键」判成旋钮；改名后门绿。
  已满足。

### M12 读法普查的人口与头部事实

- 状态：未开始
- 证据：
  - `every_declared_once_gate_shares_the_one_reader_test` 只走各 crate 的 `tests/`；
    `crates/bootstrap/src/main.rs:4925` 的测试内联读取器在 `src/` 的 `#[cfg(test)]`
    模块里，普查看不见；
  - 头部说「`src/` 里的读取器（`bootstrap` 读模板、`takeover` 读迁移脚本）」，与事实
    不符：takeover 发的是静态 HTML，bootstrap 读的是 `/etc/os-release`、apt sources、
    kubespray 输出。
- 关闭判据：人口扩到 `src/` 的内联 `#[cfg(test)]` 模块（能剥出测试块），头部改成
  实测事实，下限随人口重算；bootstrap 那份要么改读共享实现，要么登记理由。

### M13 纯词法读法搬到 `cog-test-support`

- 状态：未开始
- 背景与决定：
  - `cog-core` 是契约宪法，不实现业务逻辑（`README.md:457`），词法扫描不进 core；
  - 生产侧不用这份读法：`contract/reflection.rs:1029` 的 `literal_field_assignment`
    是另一条判据（整行右值是不是字面量），不枚举、不认转义；
  - `cog-test-support` 是 dev-only，且不是口头约定：
    `crate_dependency_contract_test.rs` 把「正常依赖指向它」判成
    `linked into the artifact`、把「两节同声明」判红；
  - `the_fixture_reading_steps_are_written_once_test` 的 `SUPPORT_HOME` 会判
    「这里定义过的体，夹具里不许再写一遍」。
- 关闭判据：新模块放 `literals_in` / `literal_values`；`common/declared_once.rs` 改为
  re-export；普查的 `HOME` 与 `SUPPORT_ENTRY_POINTS` 更新；cog-gateway 的
  `OTHER_READERS` 条目可删（它加 dev 依赖就能用共享读法）；依赖门与全量套件跑绿。

### M14 生产侧字段消费普查尚未成为判据

- 状态：未开始
- 现状：M6/M7 的结论来自一次性探针（剥注释与字符串、按类型名或节路径锚定 `.field`），
  不是树上的门；探针看不见解构读取，也可能把同名碰撞当成读取。
- 关闭判据：门走生产源，剥注释与字符串，按「类型名或节路径」锚定读取；解构读取算读；
  零永久豁免。落地顺序在 M6–M9 修完之后，否则门一上来就红一片而只能靠豁免变绿。

---

## 非主线

### N1 `cog-core::contract::reflection` 里的生产解析器与 core 的定位冲突

- 状态：待决策
- 证据：
  - `README.md:457` / `README.zh-CN.md:455`：`cog-core` 只定义跨 crate 的 trait、类型、
    配置、错误与插件契约，不实现业务逻辑，零外部 IO；
  - `crates/cog-core/src/contract/reflection.rs:925` 的 `unreachable_defaults`、
    `:1029` 的 `literal_field_assignment`、`default_impl_blocks` 是生产解析器；
  - 唯一生产调用方是 `crates/cog-reflection/src/change_pipeline.rs:4544`。
- 与主线的关系：无关。主线的读法搬迁（M13）不得把它牵进来。
- 待决策：把它挪去 `cog-reflection`（调用方一侧），还是把「反思判据」明确写成
  core 契约的一部分。决定之后再排期；在决定之前不动它。

---

## 执行顺序

1. ~~M4 + M5 + M11：类型图递归（最大覆盖缺口，且能顺手判出 M11）。~~ 已完成。
2. M13 + M12：读法搬家与普查人口。
3. M8 + M9 + M7 + M6：真实失效与无读取字段（接线或删除）。
4. M10：example 去独立化（需要先定 (a) 还是 (b)）。
5. M14：消费普查成门（依赖 3 已清）。
6. N1：决策后再排。

## 变更记录

- 2026-11-11：建立台账；M1–M3 记为已完成（`f117611`、`b979f2b` 已双推）。
- 2026-11-11：M4 + M5 + M11 关闭：类型图递归、`produced` 并上字段声明、说明键改名；
  顺带修掉 19 处 `dormancy` 与 26 个嵌套旋钮的交付缺口（18 条写进 chart，
  8 条进 `CODE_ONLY`）。提交 `d65dbc2`，已双推。
