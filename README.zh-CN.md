<div align="center">

# 🌌 Cogneva

**认知如新星般涌现，赋予 AI 以觉醒的品味。**
*Where cognition emerges like a nova, awakening the taste of AI.*

[![License: Modified MIT](https://img.shields.io/badge/License-Modified%20MIT-blue.svg)](LICENSE)
[![Rust 1.85+](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/hcipengm/cogneva/actions/workflows/ci.yml/badge.svg)](https://github.com/hcipengm/cogneva/actions/workflows/ci.yml)
[![Crates](https://img.shields.io/badge/Crates-26-5e6ad2.svg)](Cargo.toml)

**Meta-bootstrapping · Real-autonomous · Omni-evolution**
元启动 · 真自治 · 全进化

[English](README.md) | **简体中文**

</div>

---

Cogneva（发音：/kɒɡˈneɪvə/）源自 **Cognition（认知）** + **Emerge（涌现）** + **Nova（新星）**，是一个用 Rust 编写的**分布式 AI 多智能体自治系统**。
三者不是孤立功能，而是同一套认知基础设施的三个连续阶段：

- 元启动提供**存在基础**；
- 真自治提供**运行能力**；
- 全进化提供**成长能力**。

## 🔱 三大支柱

Cogneva 不是静态的 AI 工具，而是一个能自己站起来、自己运转、自己成长的数字生命体。它的核心理念浓缩为三个词：

### 🌱 元启动（Meta-bootstrapping）

元启动解决的是**系统起源问题**：一个声称能自我运行、自我进化的 AI 系统，首先必须能把自己从虚无中召唤出来。如果连启动都需要人工一步步配置依赖、安装集群、调通网络，那它就不是真正的自治生命体，而只是一个需要人类保姆的脚本集合。

Cogneva 的元启动，从一台空白 Linux 机器开始，执行 `bootstrap.sh` 这一行 Shell 命令，系统便会自动完成环境探测、依赖自修复、K3s/K8s 自适应部署、安全网关配置、沙盒环境初始化，最终把控制权交给 WebUI。整个过程由两个核心思想驱动：

- **自举架构（Bootstrapping）**：用一个极小的 Rust 引导程序作为“系统之种”，它先检查自己、修复自己，再一层一层拉起网络、存储、消息队列、Agent 运行时、监控、网关，最终让整个 AI 基础设施开花结果。
- **管理即代码（Management as Code）**：所有部署逻辑、配置模板、资源清单都以代码形式固化在仓库中，不依赖某个运维人员的鼠标点击，也不依赖某份过期的文档。环境即代码，变更即可审计、可回滚、可复现。

元启动还内置了**环境自适应**：`cog-bootstrap` 引导器根据机器资源、内核能力、网络条件探测环境并做出部署决策；`cog-supervisor` 持续健康检查与自动恢复；`cog-gateway` 配置安全网关与网络策略；`cog-agent` 初始化沙盒运行时。系统会自动选择 K3s 轻量分支还是 K8s 生产分支，遇到缺失的工具或权限问题在沙盒模式下能自动修复。它不是“一键安装脚本”，而是让系统具备**从无到有的生存能力**。



### 🤖 真自治（Real-autonomous）

真自治解决的是**系统意志问题**：当系统已经站起来，它能否在没有人类逐步指令的情况下，持续朝着一个目标前进？传统 AI 是“你问一句，我答一句”的工具；真自治的 Cogneva 是“你给目标，我找路径”的生命体。人类从操作者退化为设定者，系统从执行者进化为主角。

这种自治不是单点能力，而是一个**分层闭环**：

- **感知层**：`cog-observability` 全链路采集指标、日志、事件、追踪；`cog-supervisor` 通过 Hook 订阅 Agent 状态变化，持续感知系统健康。
- **决策层**：`cog-orchestrator` 基于目标拆解任务，`MetaLearning` 根据历史成功率推荐策略，`SchedulerGate` 动态控制任务流。
- **执行层**：`cog-orchestrator` 的 **Process**（DAG 流程编排）驱动 `cog-collaboration` 的 **Squad** 自发组队，`cog-agent` 的 ReAct 内核在工具、记忆、知识库之间自主推理；`Respawner` 和 `BinarySwitcher` 在失败时自动恢复或回滚。

关键在于**没有预设的 DAG**。Agent 不按照人类提前画好的流程图机械执行，而是通过事件总线实时协作：一个 Agent 产生事件，其他 Agent 或 Hook 动态响应，拓扑随任务复杂度自适应展开。遇到意外，系统自己调整；遇到故障，系统自己治愈。

真自治产生的运行数据，是全进化的燃料；全进化生成的 Patch 和策略，又反过来增强真自治的决策质量。

**一句话**：真自治让 Cogneva 从一个“被启动的程序”，变成一个“有目标、能协作、会自愈、可持续学习”的自主系统。

所以真自治的本质是：**把“人类步步紧盯”变为“人类设定方向”，把“按脚本执行”变为“按目标生存”**。



### 🧬 全进化（Omni-evolution）

全进化是 Cogneva 的**认知闭合能力**——它让系统不仅能在运行中修复自己，还能在运行中**理解自己、改进自己、超越自己**。这不是简单的“AI 写代码”，而是一个完整的负反馈大循环：系统观测自身行为，从成功与失败中沉淀经验，把经验转化为可验证的代码改动，再把它安全地部署回自身，并用量化评估确认这次改动确实让系统变得更好。

#### 进化的两类输入源

进化不是只能等人下命令。它同时接收：

- **外部意图 / 目标输入**：人类或其他 AI 通过 WebUI、API、GitHub issue/PR/comment 等渠道向系统注入需求、问题与目标；`cog-github` 采集这些外部反馈并汇入全进化反馈回路，使其成为驱动系统进化的外部反馈源。未来 A2A 场景下，任意 AI 智能体均可直接提交进化意图。
- **系统自发现**：`cog-observability` 持续采集指标、日志、链路追踪；`cog-supervisor` 做健康检查与自动恢复；`cog-reflection` 把 Agent、Tool、Squad、Patch 的运行结果沉淀为 `Learning` / `ErrorEntry` / `SkillOutcome`。当某类错误反复出现、某个技能效果退化、某个模式成熟时，Reflection 会主动生成进化任务。

#### 双轨进化

Cogneva 的全进化不是单一通道，而是“源码级进化 + 产物级进化”双轨并行：

- **源码级进化**：作用于开源的引擎框架（MIT），通过 Patch 修改 `.rs` 文件，改算法、改框架、改数据结构。
- **产物级进化**：作用于私有的策略产物（Protobuf 元策略、阈值、规则、经验库），运行时热替换，不改源码即可改行为。


#### 负反馈闭环

```text
Observability 采集系统运行数据
        ↓
Supervisor 健康检查与自动恢复
        ↓
Reflection 沉淀错误/经验并触发进化任务
        ↓
Orchestrator 统一决策与路由
        ↓
Collaboration 生成 .patch
        ↓
PatchPipeline 应用 → 测试 → 构建 → 切换
        ↓
Eval 定量评估进化效果
        ↓
Reflection / MetaLearning 记录学习
        ↓
下一轮进化更优
```

在这个闭环里，每个 crate 各司其职：

- **Observability 负责“看见”**：全链路采集，不让任何异常溜走。
- **Supervisor 负责“活下去”**：发现故障立即恢复，保证系统持续可用。
- **Reflection 负责“想明白”**：把分散的运行结果结构化，识别重复模式，决定“要不要改”。
- **Orchestrator 负责“做决定”**：基于 reflection 的学习输出和 observability 的实时信号，决定是重试、扩容、告警，还是触发代码进化。
- **Collaboration 负责“生成改动”**：多 Agent 协作生成标准 `.patch`。
- **Eval 负责“验证效果”**：没有 eval 的进化是盲目的，A/B 对比 + 统计显著性检验确保改动真的更好。

#### 安全与回滚

所有高危操作都在云原生沙盒内完成（根据环境自适应选择 K3s 轻量分支或 K8s 生产分支）：`git apply --check` 预检、`cargo test` 验证、`cargo build --release` 编译、二进制原子切换、健康检查。任何阶段失败都会自动回滚，系统永远不会因为一次失败的“自我改进”而崩溃。

一句话：**元启动让系统站起来，真自治让系统自己跑，全进化让系统持续变强。**


---

## 🛡️ 安全设计：把权力关在笼子里

Cogneva 追求的真自治和全进化，意味着 Agent 必须拥有修改代码、自我编译甚至获得 Root 权限的能力。这与宿主机的绝对安全之间存在天然矛盾。我们的安全哲学不是“限制 AI 不能做什么”，而是：**给它足够的自由去完成进化，同时把 Agent 掌握的权力严格圈定在可销毁的边界内**。


### 🧱 纵深防御三层

```text
┌─────────────────────────────────────────┐
│  应用层：输入净化 / 指令与数据隔离 / 输出过滤  │  ← 防止注入成功
├─────────────────────────────────────────┤
│  架构层：凭证隔离 / 权限最小化 / 安全网关代理   │  ← 防止拿到敏感数据
├─────────────────────────────────────────┤
│  物理层：微虚拟机 + Seccomp + 资源配额 + 阅后即焚 │  ← 即使成功，破坏也仅限沙盒
└─────────────────────────────────────────┘
```

- **应用层**防御提示词注入：扫描输入、隔离系统指令与外部数据、约束输出格式并审查敏感内容。
- **架构层**隔离权限与凭证：Agent 默认只有完成进化所需的最小权限，所有外部访问由网关代理。
- **物理层**兜底：微虚拟机提供硬件级隔离，Cgroups 限制 CPU/内存，任务结束立即销毁。

### ✂️ 计算与存储解耦

微虚拟机“阅后即焚”与自我进化需要持久化记忆并不冲突。我们把**计算**和**状态**分离：

- 微虚拟机本身无状态，只负责运行代码、测试、编译；
- 代码库、编译产物、进化日志存放在 Kubernetes Persistent Volume 中；
- 每次进化由全新微虚拟机从持久卷拉取代码，完成后再推回，最后销毁。

这样既保证了安全隔离，又保证了进化成果不会随沙盒销毁而丢失。



---

## 🔥 它解决什么痛点

### 🧑‍💼 痛点 1：现在的 AI 都是"助手"，不是"员工"

现在的 AI 产品定位都是**个人助理**：
- 你问一句，它答一句
- 每次对话从零开始，不会积累
- 复杂任务需要人一步一步教、一步一步盯
- 人睡觉，它就停工

**Cogneva 的解法**：**Agent 不是助手，是数字员工。**
- 你给任务，它自己组团队、明分工、强协同
- 24×7 自己干完，不需要人盯着
- 经验自动积累，越用越聪明
- 人睡觉，它继续干

### 💔 痛点 2：Agent 有状态，无法持久化工作

现在的 Agent：
- 记忆存在本地文件（如 MEMORY.md），无并发控制、无版本管理
- 多 Agent 同时读写同一个文件，冲突覆盖不可避免
- Agent 崩溃后，任务进度丢失，需要从头再来
- 无法跨机器迁移，扩容即丢状态

**Cogneva 的解法**：**Agent 无状态化 + 系统有状态机**
- **状态外置**：任务状态、记忆、技能、配置全部外置到数据库（PostgreSQL/Redis/Qdrant），Agent 本身不保存任何状态
- **状态机管理**：系统层通过消息队列串行化状态变更，天然并发安全，Agent 随时可重启、可扩缩容
- **断点续传**：Agent 崩溃后，新实例从状态机恢复上下文，任务不中断
- **弹性伸缩**：无状态 Agent 可随意水平扩容，状态由系统层统一管理

### 🔁 痛点 3：Agent 只能做"一次性"任务，不会积累

现在的 Agent 框架：
- 每次运行从零开始
- 上次成功的经验不会自动变成可复用的技能
- 同一个错误会反复犯

**Cogneva 的解法**：内置 **L0-L3 四层自我进化系统**
- L0：单次运行自审（即时质量检查）
- L1：跨会话学习（把成功经验提炼成 Skill）
- L2：代码进化（自动改进系统代码，cargo check 验证）
- L3：元学习（发现新模式、自主探索能力）

### 🔀 痛点 4：多 Agent 协作 = 人工编排 DAG

现在的多 Agent 方案：
- 人先用可视化画布/模版或代码定义好 DAG
- Agent 按预设路径执行
- 遇到意外情况就卡住
- 编排更偏向“Agent 角色”导向，而不是“任务完成”导向

**Cogneva 的解法**：**Event + Hook + Skill 动态编排**
- 没有预设的 DAG 画布/模版
- Agent 通过事件总线自发协作
- Hook 系统在运行时动态响应事件、合成新行为
- 拓扑随任务复杂度自适应调整
- 元任务（Meta-Task）将整体任务拆解并生成Action Plan，分配给 Squad 完成

### 📈 痛点 5：系统越搭越复杂，迁移成本越来越高

现在的 AI 平台：
- 第一天用 SQLite，数据多了迁移到 Postgres
- 本地跑通了，上云要重写配置
- 向量检索从本地换到云端，接口全变

**Cogneva 的解法**：**第一天即最终存储架构**
- 直接对接标准协议存储（PostgreSQL/Qdrant/Meilisearch/S3/NATS）
- 没有 Adapter/Factory 切换层
- 本地开发和生产部署用同一套配置

### 🙈 痛点 6：AI 系统的"黑盒"问题

现在的 AI 系统：
- 出了问题不知道哪一步错了
- 无法回溯 Agent 的决策过程
- 审计和合规无从谈起

**Cogneva 的解法**：**Protobuf Envelope + JSON Payload 原始数据流**
- 格式：`<varint length><protobuf envelope><JSON payload>`，zstd 压缩
- Protobuf 只负责 envelope（meta + context + stream name），业务数据全走 JSON
- 热层(0-7天) → 温层(7-90天) → 冷层(90天+)
- 可回放、可审计、可解释；加新事件类型无需改 proto、无需重新编译

---

## ✨ 核心优势

### 🎯 优势 1：动态编排

传统：人画 DAG → Agent 按图执行
Cogneva：Agent 产生 Event → Hook 系统动态响应 → Skill 自适应加载

**这是人机双友好的范式**：
- 传统 UI 为人类设计，Agent 难以调用
- Cogneva 的 UI is the reply of the Event Stream（UI 是事件流的回应）
- 人类通过 UI 消费事件流；Agent 直接订阅事件流，无需解析界面
- 同一套 Event Stream，对人是对话界面，对 Agent 是原生 API

### 🎛️ 优势 2：Agent 无状态化 + 系统有状态机

**核心原则：Agent 只负责"思考"和"调用工具"，所有状态管理外置到系统层**

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   Agent     │────→│  消息队列    │────→│  Redis/PostgreSQL   │
│ (只处理任务) │     │ (cog-orchestrator)  │     │   (状态存储)         │
└─────────────┘     └─────────────┘     └─────────────────────┘
      ↑                                                          │
      └────────────────── 不操作文件系统，无并发冲突 ──────────────┘
```

- **任务状态**：外置到消息队列，Agent 不保存本地进度
- **记忆**：外置到向量数据库，Agent 通过检索注入上下文
- **技能**：外置到 Registry，Agent 运行时动态加载
- **配置**：外置到配置中心，热重载无需重启 Agent
- **通知**：Event Stream 外置推送，不占用 Agent 上下文窗口

**效果**：Agent 无状态化，随时可重启、可扩缩容，无并发冲突，上下文窗口只用于"思考"不用于"记账"。系统层通过状态机统一管理全量状态。

### 🧩 优势 3：Capability-based 插件化架构

**cog-core 提供基础设施抽象，业务 Crate 通过 Capability 声明能力，cogneva 负责运行时自动发现与组装**：

```
┌─────────────────────────────────────────┐
│           cogneva（运行时组装器）         │
│  ┌─────────────────────────────────┐   │
│  │      cog-core（基础设施抽象层）   │   │
│  │  Backend Trait · 插件注册系统     │   │
│  │  自动发现 · 依赖验证 · 拓扑排序   │   │
│  └─────────────────────────────────┘   │
│         ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑          │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐  │
│  │llm │ │agent│ │memory│ │wiki │ │...│  │
│  │Crate│ │Crate│ │Crate │ │Crate│ │Crate│  │
│  └────┘ └────┘ └────┘ └────┘ └────┘  │
└─────────────────────────────────────────┘
```

- **Capability-based 插件注册**：每个 Crate 声明能力（如 `ProvidesLLM`、`ProvidesMemory`），运行时自动匹配
- **依赖拓扑排序**：`cogneva/src/bootstrap.rs` 自动解析 Crate 依赖图，按正确顺序初始化
- **零配置热插拔**：新增 Crate 只需放入 `crates/` 目录，重新编译即可自动识别，无需改主程序
- **故障隔离**：单个 Crate panic 不会拖垮整个系统，Supervisor 自动重启

### 🧠 优势 4：永久记忆 — 三层递进架构

不是简单的对话历史拼接，而是 **Raw → Schema → Summary 三层递进** 的永久记忆：

```
┌─────────────────────────────────────────┐
│  Layer 2 — Summary（语义摘要层）         │
│  文本摘要 + dense/sparse 双向量          │
│  Hybrid search + Cross-encoder rerank   │
├─────────────────────────────────────────┤
│  Layer 1 — Schema（结构化事实层）        │
│  实体、关系、事件，支持图遍历查询         │
├─────────────────────────────────────────┤
│  Layer 0 — Raw（原始数据层）             │
│  不可变追加，对象存储持久化               │
└─────────────────────────────────────────┘
```

**与文件级记忆的本质区别**：
- 文件级记忆（如 MEMORY.md）是平面文本，容量受限，检索靠关键词匹配
- Cogneva 的记忆是**结构化 + 向量化 + 图关系**，支持语义检索、时间范围过滤、关系链查询
- LLM 自动提取重要性评分（1-10 分），低价值记忆自动衰减，高价值记忆持续强化
- 多租户 Namespace 隔离，企业级数据安全

### 🔭 优势 5：全栈可观测性

自研指标/日志/链路追踪/原始数据流：
- Prometheus + Grafana（指标）
- Loki + Promtail（日志）
- Jaeger（链路追踪）
- Protobuf Envelope + JSON Payload 原始数据流（审计回溯）

### 🔍 优势 6：可评估、可回放、可审计

**Snapshot + Replay**：Agent 执行过程完整记录（Trace），支持确定性回放
- `TraceCollector` 收集执行轨迹 → `TraceStore` 持久化（内存/Redis/S3/文件）
- `ReplayEngine` 按时间线回放，定位问题根因
- 冷热分层：热层(内存/Redis) → 温层(文件) → 冷层(S3/zstd)

**cog-eval 评估框架**：没有 eval 的进化是盲目的
- Dataset / Metric / Runner / Comparator / Report / Harness 六组件
- 6 种内置指标：ExactMatch、SemanticSimilarity、ToolCallAccuracy、LatencyP50/P99、TokenEfficiency、CostPerTask
- A/B 对比 + 统计显著性检验（z-test / t-test）
- CI 集成：cargo test 时自动跑回归

### 🚗 优势 7：Supervisor 监控-驱动闭环（类自动驾驶）

不是被动告警，而是**主动感知 → 决策 → 执行**的自治闭环：

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   感知层    │───→│   决策层    │───→│   执行层    │
│  Hook事件   │    │ 规则引擎    │    │ 任务重平衡  │
│  心跳监控   │    │ 负载预测    │    │ 自动恢复    │
│  健康检查   │    │ 故障分类    │    │ 配额执法    │
└─────────────┘    └─────────────┘    └─────────────┘
       ↑                                    │
       └──────────── 效果反馈 ──────────────┘
```

- **感知**：`cog-supervisor` 通过 Hook 收集 Agent/LLM 事件，聚合健康状态
- **决策**：`SchedulerGate` 控制任务流，`TaskRebalancer` 动态调度，`Respawner` 故障恢复
- **执行**：Agent Kill/Restart/Checkpoint、DLQ Retry、Gate Pause/Resume
- **API 全暴露**：50+ 端点覆盖状态查询、控制干预、审计回放

### 🐳 优势 8：云原生第一天就绪

- FHS 标准目录（`/opt/cogneva`、`/var/lib/cogneva-data`、`/etc/cogneva`）
- systemd/Docker/Kubernetes 三种部署方式
- 配置热重载（不停机更新日志级别、网关配置、LLM Provider）
- Graceful Shutdown（零停机更新）

### 🧪 优势 9：对 Harness Engineering 的原生支持

Harness Engineering = 给 Agent 提供完整的执行环境（工具、上下文、评估、反馈）。Cogneva 为此设计了原生支持：

| Harness 要素 | Cogneva 实现 |
|-------------|-------------|
| **工具集（Toolset）** | `cog-skill` 动态发现 + `cog-extension` WASM/Rhai 扩展 |
| **上下文（Context）** | `cog-prompt` 模板引擎 + `cog-memory` 三层递进记忆 + `cog-wiki` 知识库 |
| **评估（Eval）** | `cog-eval` 六组件框架：Dataset / Metric / Runner / Comparator / Report / Harness |
| **反馈（Feedback）** | `cog-reflection` L0-L3 自我进化：SelfReview → ReflectionEngine → EvolutionEngine |
| **沙箱（Sandbox）** | `cog-agent` 运行时隔离 + `cog-supervisor` 监控-驱动闭环 |

**Context Engineering 子集**：
- Prompt 模板热重载 + A/B 测试
- 三层记忆自动注入上下文（Raw 事实 → Schema 关系 → Summary 语义）
- 知识库全文检索 + 向量语义检索 Hybrid 融合
- 上下文窗口自适应压缩（TokenBudgetManager）

### 🛠️ 优势 10：对 Agentic Engineering 的原生支持

Agentic Engineering = 让 Agent 具备自主规划、执行、协作、进化的能力。Cogneva 为此设计了完整栈：

| Agentic 要素 | Cogneva 实现 |
|-------------|-------------|
| **规划（Planning）** | `cog-orchestrator` DAG 编排 + ActionPlanner + 技能缺口自动补齐 |
| **执行（Execution）** | `cog-agent` ReAct 内核 + `cog-llm` 多模型统一调用 + 流式响应 |
| **协作（Collaboration）** | `cog-orchestrator` Process 编排 + `cog-collaboration` Squad 执行 + Ralph Loop 硬重置 |
| **进化（Evolution）** | `cog-reflection` L0-L3：SelfReview → Reflection → Evolution → MetaLearning |
| **监控（Observability）** | `cog-supervisor` 感知-决策-执行闭环 + `cog-observability` 全链路追踪 |

**Squad + Process 协作模型**：
- **Process**（`cog-orchestrator`）：把目标编排成可执行的 DAG 流程，负责任务状态机、依赖触发、超时检测、死信队列
- **Squad**（`cog-collaboration`）：原子任务执行单元，内部多 Agent 按 P→G→E 流水线或圆桌争论协作，Ralph Loop 防腐败硬重置
- **Agent**（`cog-agent`）：具体推理个体，来自 `GlobalAgentManager` worker pool

**三层质量循环（每个 Squad 内部）**：
- **Ralph Loop（外层）**：防腐败 —— 检测失败，全局硬重置，防止系统退化
- **P→G→E 流水线（中层）**：防糊涂 —— Planner/Generator/Evaluator 专业化分工，避免角色混乱
- **Self-Review Loop（内层）**：防自欺 —— 每个 Agent 干完自己检查，Observe→Critique→Compare→Decide→Revise→Log

### 🧫 优势 11：自我进化 — L0-L3 四层深度

| 层级 | 能力 |
|------|------|
| L0 单次自审 | 每次 Agent 运行后自动质量检查 |
| L1 跨会话学习 | 从成功/失败中提取 Skill，写入 Registry |
| L2 代码进化 | **自动生成 Rust 代码补丁，cargo check 验证通过** |
| L3 元学习 | 自主发现新模式、探索新能力 |

**L2 代码进化的具体流程**：
1. 检测重复错误模式
2. LLM 生成改进的 Rust 代码
3. `cargo check` 编译验证（最多 3 次迭代）
4. 通过质量门后写入 `evolution-patches/`
5. 通过 `hook_sink`/`tool_sink` channel 实时注册到运行系统

---

## 🏗️ 架构哲学：契约与组装

Cogneva 有 20+ 业务 crate，如果让它们直接互相依赖，很快会变成一团拆不开的“意大利面条”。我们的解法是 **“契约层 + 组装层”双核拆分**：

- **`cog-core` 是系统的“契约宪法”**：只定义跨 crate 的 trait、类型、配置、错误和插件契约，不实现任何业务逻辑，零外部 IO。所有业务 crate 都只依赖 `cog-core`，彼此零直接依赖。
- **`cogneva` 是系统的“运行时政府”**：把所有独立的业务 crate，按照正确的顺序、正确的方式、正确的生命周期，连接成一个可运行的整体。具体包括加载配置、自动发现插件、拓扑排序初始化、配置热重载、优雅关闭，不写任何业务逻辑。

这种拆分背后是 **DIP（依赖倒置原则）** 与 **高内聚低耦合**：高层模块不依赖低层模块，双方都依赖 `cog-core` 的抽象接口。`cog-agent` 不直接依赖 `cog-llm` 的具体实现，而是依赖 `cog-core::contract::llm::LlmClient`；`cog-orchestrator` 不直接依赖 `cog-stream` 的 NATS 实现，而是依赖 `cog-core::contract::stream::MessageBackend`。

新增一个业务 crate 只需：

1. 在 `cogneva/Cargo.toml` 加依赖；
2. 在该 crate 的 `plugin.rs` 暴露 `pub const DESCRIPTOR`；
3. 重新编译。

`cogneva/build.rs` 会自动扫描并生成插件注册表，`PluginRunner` 会按依赖关系拓扑排序初始化。系统因此具备**零配置热插拔**能力。

> **`cog-core` 让 Cogneva 能拆得开；`cogneva` 让 Cogneva 能跑起来。**
>
> 没有 `cog-core`，系统是铁板一块；没有 `cogneva`，系统是一盘散沙。


---

## 🗺️ 架构概览

### 📚 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 6   cogneva          主程序入口、组装、生命周期管理     │
├─────────────────────────────────────────────────────────────┤
│  Layer 5   cog-gateway      HTTP + WebSocket 统一网关        │
├─────────────────────────────────────────────────────────────┤
│  Layer 4   cog-auth / cog-quota / cog-wiki / cog-memory     │
│            认证、配额、知识库、永久记忆（横向能力层）          │
├─────────────────────────────────────────────────────────────┤
│  Layer 3   cog-orchestrator DAG编排、消息队列、任务调度       │
├─────────────────────────────────────────────────────────────┤
│  Layer 2   cog-collaboration Squad 多Agent协作              │
├─────────────────────────────────────────────────────────────┤
│  Layer 1.5 cog-agent        Agent ReAct 执行引擎            │
│  Layer 1   cog-llm          多模型统一调用                  │
├─────────────────────────────────────────────────────────────┤
│  Layer 0.5 cog-storage      数据存储与适配                  │
├─────────────────────────────────────────────────────────────┤
│  Layer 0   cog-core         基础设施抽象                    │
├─────────────────────────────────────────────────────────────┤
│  纵向贯穿   cog-supervisor / cog-observability /            │
│            cog-reflection / cog-protocol                    │
│            监控、可观测性、自我进化、协议适配                │
└─────────────────────────────────────────────────────────────┘
```

### 📡 通信机制（Backend Trait 热插拔）

所有通信层通过 `cog-core` 的 Backend Trait 抽象，**不绑定具体实现**，可随时替换：

| 场景 | Trait | 当前实现 | 可替换为 |
|------|-------|---------|---------|
| 同机进程间 | — | Unix Domain Socket | — |
| 跨机通信 | — | gRPC / UDP | — |
| 事件总线 | `MessageBackend` | NATS JetStream | Redis Streams / Kafka / 内存通道 |
| 外部 API | — | HTTP / WebSocket | — |

`MessageBackend` Trait 定义（`cog-core/src/stream/message_bus.rs`）：
- `publish` / `subscribe` / `create_consumer_group` / `ack` / `dlq`
- 实现方可针对 Redis pipeline、NATS 并发发布、内存单锁等优化 batch 和延迟发布

### 🗄️ 存储机制（Backend Trait 热插拔）

所有存储层同样通过 `cog-core` 的 Backend Trait 抽象，**不绑定具体数据库**：

| 数据类型 | Trait | 当前实现 | 可替换为 |
|---------|-------|---------|---------|
| 关系型数据 | — | PostgreSQL | TiDB / Neon / MySQL（同协议扩展） |
| 向量数据 | `VectorBackend` | Qdrant | Milvus / LanceDB / Memory（测试） |
| 状态/Checkpoint | `StateBackend` | PostgreSQL | Redis / 任意 KV 存储 |
| 对象存储 | `ObjectBackend` | SeaweedFS / S3 | MinIO / Azure Blob |
| 消息队列 | `MessageBackend` | NATS JetStream | Redis Streams / Kafka |
| 全文搜索 | `SearchBackend` | Meilisearch | Elasticsearch |
| 指标 | `MetricsBackend` | Prometheus | — |

`VectorBackend` Trait 示例（`cog-core/src/storage/vector.rs`）：
- `create_collection` / `insert` / `insert_sparse` / `search` / `search_sparse` / `search_hybrid`
- `search_hybrid` 默认实现：仅调用 `search`（稠密向量检索），不依赖 `search_sparse`；支持 sparse + dense hybrid 的向量数据库可覆盖为原生实现

---

## 🚀 快速开始

Cogneva 支持**元启动（Meta-bootstrap）**：从一台空白机器（Linux / macOS / Windows）出发，执行一行命令即可完成环境探测、依赖自修复、K3s/K8s 自适应部署、安全网关配置与沙盒初始化，最终把控制权交给 WebUI。

入口脚本会**自动判断国内/国外网络环境**（探测 rustup 分发域），国内环境自动把全部依赖切到国内镜像（Gitee 源码 / TUNA rustup 与 apt / rsproxy crates / DaoCloud 容器镜像 / npmmirror npm），无需手工选择。也可用 `COGNEVA_CN_MIRROR=1`（或 `0`）强制。

### 🐧 Linux

```bash
(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh
```

裸机直接引导。

### 🍎 macOS

```bash
(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh
```

**同一条命令**。K3s 需要 Linux 内核，脚本会自动安装 [Lima](https://lima-vm.io)（经 Homebrew，国内走 TUNA 镜像）并创建 Ubuntu 虚拟机，然后在 VM 内执行完全相同的一键命令。所有依赖都装在 VM 内，宿主只多一个 `limactl`。完成后 WebUI 经端口转发到 <http://localhost:8080>。管理 VM：`limactl shell cogneva` / `limactl stop cogneva` / `limactl delete cogneva`。

### 🪟 Windows

```powershell
# 管理员 PowerShell
iwr -useb https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.ps1 | iex
```

脚本自动安装 WSL2 + Ubuntu（如需重启会提示，重启后重跑本脚本即可，幂等），然后在 WSL 内执行同一条一键命令。WSL2 默认开启 localhostForwarding，WebUI 直接在浏览器访问 <http://localhost:8080>。强制国内镜像：先下载脚本再传参，如 `& ([scriptblock]::Create((iwr -useb <地址>).Content)) -CnMirror 1`。

> 第一个地址是 GitHub 官方 raw 地址；如果访问不通（比如国内受限网络），命令会自动切换到 Gitee 镜像下载，无需手动选择。脚本内部拉取源码时同样会自动回退到 Gitee 仓库。

引导器会自动：

1. 探测 CPU / 内存 / 节点 / 架构；
2. 配置并验证 LLM API Key（或本地模型）；
3. 自动选择 K3s 轻量分支或 K8s 生产分支；
4. 安装容器运行时、K3s/buildah；
5. 部署安全网关、NetworkPolicy、沙盒环境；
6. 启动 Cogneva 并打印 WebUI 地址；
7. 引导器退出，内存中 API Key 清零。

### ☸️ 已有集群

```bash
# 已有 K3s 集群
kubectl apply -f deploy/k3s/

# 已有标准 K8s 生产集群
kubectl apply -f deploy/k8s/
```

### 🔧 传统手动部署

手动编译：`cargo build --release`。容器镜像见 `Dockerfile`，K3s/K8s 部署清单见 `deploy/`。

---

## 📊 项目状态

**当前阶段**：Alpha → Beta 过渡期

- 核心框架已搭建完成，26 个 Crate 均有实质性实现
- 编译通过，`cargo clippy --workspace --all-targets -- -D warnings` 零告警，`cargo fmt --check` 通过（2026-07-21）
- 107 个测试套件全部通过（`cargo test --workspace`，2026-07-21）
- **待完善**：生产验证、生态扩展、社区建设

---

## ⚖️ 许可证

[Modified MIT License](LICENSE) — 在标准 MIT 基础上增加大规模商业使用归属展示要求（MAU ≥ 500 万 / 年收入 ≥ 500 万 CNY / 员工 ≥ 300 人时需展示 "Powered by Cogneva"）。
