<div align="center">

# 🌌 Cogneva

**Cognition emerges like a nova, awakening the taste of AI.**
*认知如新星般涌现，赋予 AI 以觉醒的品味。*

[![License: Modified MIT](https://img.shields.io/badge/License-Modified%20MIT-blue.svg)](LICENSE)
[![Rust 1.85+](https://img.shields.io/badge/Rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/hcipengm/cogneva/actions/workflows/ci.yml/badge.svg)](https://github.com/hcipengm/cogneva/actions/workflows/ci.yml)
[![Crates](https://img.shields.io/badge/Crates-26-5e6ad2.svg)](Cargo.toml)

**Meta-bootstrapping · Real-autonomous · Omni-evolution**
元启动 · 真自治 · 全进化

**English** | [简体中文](README.zh-CN.md)

</div>

---

Cogneva (pronounced /kɒɡˈneɪvə/), from **Cognition** + **Emerge** + **Nova**, is a **distributed AI multi-agent autonomous system** written in Rust.

These three are not isolated features, but three consecutive stages of the same cognitive infrastructure:

- Meta-bootstrapping provides the **foundation of existence**;
- Real-autonomy provides the **ability to operate**;
- Omni-evolution provides the **ability to grow**.

## 🔱 The Three Pillars

Cogneva is not a static AI tool, but a digital lifeform that can stand up on its own, run on its own, and grow on its own. Its core philosophy is condensed into three words:

### 🌱 Meta-bootstrapping

Meta-bootstrapping solves the **problem of system origin**: an AI system that claims to run and evolve itself must first be able to summon itself out of nothing. If booting still requires humans to manually configure dependencies, install clusters, and wire up networks step by step, then it is not a truly autonomous lifeform — just a collection of scripts that needs a human babysitter.

Cogneva's meta-bootstrapping starts from a blank Linux machine. Running a single shell command, `bootstrap.sh`, the system automatically performs environment probing, dependency self-repair, K3s/K8s adaptive deployment, security gateway configuration, and sandbox initialization, finally handing control over to the WebUI. The whole process is driven by two core ideas:

- **Bootstrapping architecture**: a minimal Rust bootstrapper acts as the "seed of the system". It checks itself, repairs itself, then brings up networking, storage, message queues, the Agent runtime, monitoring, and the gateway layer by layer, until the entire AI infrastructure blossoms.
- **Management as Code**: all deployment logic, configuration templates, and resource manifests are codified in the repository — no dependence on an operator's mouse clicks, no dependence on stale documents. Environment is code; changes are auditable, rollback-able, and reproducible.

Meta-bootstrapping also has built-in **environment adaptivity**: the `cog-bootstrap` bootstrapper probes machine resources, kernel capabilities, and network conditions to make deployment decisions; `cog-supervisor` provides continuous health checks and automatic recovery; `cog-gateway` configures the security gateway and network policies; `cog-agent` initializes the sandbox runtime. The system automatically chooses between the lightweight K3s branch and the production K8s branch, and can self-repair missing tools or permission problems in sandbox mode. It is not a "one-click install script" — it gives the system the **ability to survive from nothing**.

### 🤖 Real-autonomous

Real-autonomy solves the **problem of system will**: once the system is standing, can it keep moving toward a goal without step-by-step human instructions? Traditional AI is a "you ask, I answer" tool; a truly autonomous Cogneva is a "you set the goal, I find the path" lifeform. Humans are demoted from operators to goal-setters; the system evolves from executor to protagonist.

This autonomy is not a single-point capability, but a **layered closed loop**:

- **Perception layer**: `cog-observability` collects metrics, logs, events, and traces across the whole stack; `cog-supervisor` subscribes to Agent state changes via Hooks, continuously perceiving system health.
- **Decision layer**: `cog-orchestrator` decomposes goals into tasks; `MetaLearning` recommends strategies based on historical success rates; `SchedulerGate` dynamically controls task flow.
- **Execution layer**: `cog-orchestrator`'s **Process** (DAG orchestration) drives `cog-collaboration`'s **Squad** self-organized teaming; `cog-agent`'s ReAct kernel reasons autonomously among tools, memory, and knowledge bases; `Respawner` and `BinarySwitcher` automatically recover or roll back on failure.

The key is **there is no preset DAG**. Agents do not mechanically execute a flowchart drawn in advance by humans; they collaborate in real time through the event bus: one Agent emits an event, other Agents or Hooks respond dynamically, and the topology unfolds adaptively with task complexity. When the unexpected happens, the system adjusts itself; when failures happen, the system heals itself.

The runtime data produced by real-autonomy is the fuel for omni-evolution; the Patches and policies produced by omni-evolution in turn improve the decision quality of real-autonomy.

**In one sentence**: real-autonomy turns Cogneva from "a program that was started" into "an autonomous system with goals, collaboration, self-healing, and continuous learning".

So the essence of real-autonomy is: **turning "humans watching every step" into "humans setting the direction", and turning "executing scripts" into "surviving by goals"**.

### 🧬 Omni-evolution

Omni-evolution is Cogneva's **cognitive closure capability** — it lets the system not only repair itself at runtime, but also **understand itself, improve itself, and surpass itself** while running. This is not simply "AI writing code", but a complete negative-feedback grand loop: the system observes its own behavior, distills experience from successes and failures, turns experience into verifiable code changes, safely deploys them back into itself, and uses quantitative evaluation to confirm that the change actually made the system better.

#### Two kinds of evolution inputs

Evolution does not only wait for human commands. It simultaneously receives:

- **External intent / goal input**: humans or other AIs inject requirements, problems, and goals through WebUI, API, GitHub issue/PR/comment, and other channels; `cog-github` collects this external feedback and feeds it into the omni-evolution feedback loop, making it an external feedback source driving system evolution. In future A2A scenarios, any AI agent can submit evolution intents directly.
- **System self-discovery**: `cog-observability` continuously collects metrics, logs, and traces; `cog-supervisor` performs health checks and automatic recovery; `cog-reflection` distills the runtime results of Agents, Tools, Squads, and Patches into `Learning` / `ErrorEntry` / `SkillOutcome`. When a class of errors recurs, a skill's effectiveness degrades, or a pattern matures, Reflection proactively generates evolution tasks.

#### Dual-track evolution

Cogneva's omni-evolution is not a single channel, but **source-level evolution + artifact-level evolution** running in parallel:

- **Source-level evolution**: acts on the open-source engine framework (MIT), modifying `.rs` files via Patches — changing algorithms, frameworks, and data structures.
- **Artifact-level evolution**: acts on private policy artifacts (Protobuf meta-policies, thresholds, rules, experience bases), hot-swapped at runtime — changing behavior without changing source code.

#### The negative-feedback loop

```text
Observability collects system runtime data
        ↓
Supervisor health checks and auto-recovery
        ↓
Reflection distills errors/experience and triggers evolution tasks
        ↓
Orchestrator unified decision-making and routing
        ↓
Collaboration generates .patch
        ↓
PatchPipeline apply → test → build → switch
        ↓
Eval quantitatively evaluates evolution effect
        ↓
Reflection / MetaLearning records learning
        ↓
Next round of evolution is better
```

In this loop, every crate does its own job:

- **Observability "sees"**: full-stack collection — no anomaly slips away.
- **Supervisor "stays alive"**: recovers immediately upon failure, keeping the system continuously available.
- **Reflection "figures it out"**: structures scattered runtime results, identifies repeated patterns, decides "whether to change".
- **Orchestrator "decides"**: based on reflection's learning output and observability's real-time signals, decides whether to retry, scale, alert, or trigger code evolution.
- **Collaboration "generates the change"**: multi-Agent collaboration produces a standard `.patch`.
- **Eval "verifies the effect"**: evolution without eval is blind; A/B comparison + statistical significance testing ensures changes are genuinely better.

#### Safety and rollback

All high-risk operations happen inside a cloud-native sandbox (adaptively choosing the lightweight K3s branch or the production K8s branch by environment): `git apply --check` pre-validation, `cargo test` verification, `cargo build --release` compilation, atomic binary switching, and health checks. Failure at any stage triggers automatic rollback — the system never crashes because of a failed "self-improvement".

In one sentence: **meta-bootstrapping lets the system stand up, real-autonomy lets the system run itself, omni-evolution lets the system keep getting stronger.**

---

## 🛡️ Security Design: Cage the Power

Cogneva's pursuit of real-autonomy and omni-evolution means Agents must have the ability to modify code, compile themselves, and even obtain Root privileges. This naturally conflicts with the absolute security of the host machine. Our security philosophy is not "restrict what the AI cannot do", but: **give it enough freedom to complete evolution, while strictly confining the power an Agent holds within destructible boundaries**.

### 🧱 Three layers of defense in depth

```text
┌──────────────────────────────────────────────────┐
│  Application: input sanitization / instruction-   │  ← prevent injection from succeeding
│  data isolation / output filtering                │
├──────────────────────────────────────────────────┤
│  Architecture: credential isolation / least       │  ← prevent access to sensitive data
│  privilege / security gateway proxy               │
├──────────────────────────────────────────────────┤
│  Physical: microVM + Seccomp + resource quotas    │  ← even if successful, damage stays
│  + burn-after-reading                             │    inside the sandbox
└──────────────────────────────────────────────────┘
```

- The **application layer** defends against prompt injection: scan inputs, isolate system instructions from external data, constrain output formats, and review sensitive content.
- The **architecture layer** isolates privileges and credentials: Agents have by default only the minimum privileges needed to complete evolution, and all external access is proxied by the gateway.
- The **physical layer** is the backstop: microVMs provide hardware-level isolation, Cgroups limit CPU/memory, and tasks are destroyed immediately upon completion.

### ✂️ Compute/storage decoupling

"Burn-after-reading" microVMs do not conflict with self-evolution's need for persistent memory. We separate **compute** from **state**:

- The microVM itself is stateless, responsible only for running code, tests, and compilation;
- The code repository, build artifacts, and evolution logs live in Kubernetes Persistent Volumes;
- Each evolution run is performed by a brand-new microVM that pulls code from the persistent volume, pushes results back when done, and is then destroyed.

This guarantees both security isolation and that evolution outcomes are not lost when the sandbox is destroyed.

---

## 🔥 What Pain Points It Solves

### 🧑‍💼 Pain 1: Today's AIs are "assistants", not "employees"

Today's AI products are positioned as **personal assistants**:
- You ask, it answers
- Every conversation starts from zero — nothing accumulates
- Complex tasks need humans to teach and watch step by step
- When you sleep, it stops working

**Cogneva's answer**: **Agents are not assistants — they are digital employees.**
- Give it a task; it organizes its own team, divides labor, and collaborates
- Works 24×7 until done, no supervision needed
- Experience accumulates automatically — the more you use it, the smarter it gets
- When you sleep, it keeps working

### 💔 Pain 2: Stateful Agents cannot persist work

Today's Agents:
- Memory lives in local files (e.g. MEMORY.md) — no concurrency control, no versioning
- Multiple Agents reading/writing the same file inevitably conflict and overwrite
- When an Agent crashes, task progress is lost and must restart from scratch
- Cannot migrate across machines — scaling means losing state

**Cogneva's answer**: **stateless Agents + system-level state machine**
- **State externalization**: task state, memory, skills, and configuration are all externalized to databases (PostgreSQL/Redis/Qdrant); Agents themselves hold no state
- **State machine management**: the system layer serializes state changes through message queues — naturally concurrency-safe; Agents can be restarted and scaled at any time
- **Resumable execution**: after an Agent crashes, a new instance restores context from the state machine — tasks never interrupt
- **Elastic scaling**: stateless Agents scale horizontally at will; state is managed uniformly by the system layer

### 🔁 Pain 3: Agents only do "one-shot" tasks — nothing accumulates

Today's Agent frameworks:
- Every run starts from zero
- Yesterday's successful experience does not automatically become a reusable skill
- The same mistakes are made again and again

**Cogneva's answer**: built-in **L0–L3 four-layer self-evolution system**
- L0: per-run self-review (instant quality check)
- L1: cross-session learning (distilling successful experience into Skills)
- L2: code evolution (automatically improving system code, verified by cargo check)
- L3: meta-learning (discovering new patterns, autonomously exploring capabilities)

### 🔀 Pain 4: Multi-Agent collaboration = hand-orchestrated DAGs

Today's multi-Agent solutions:
- Humans first define a DAG on a visual canvas/template or in code
- Agents execute along the preset path
- Anything unexpected jams the pipeline
- Orchestration tends to be "Agent role"-oriented rather than "task completion"-oriented

**Cogneva's answer**: **Event + Hook + Skill dynamic orchestration**
- No preset DAG canvas/templates
- Agents collaborate spontaneously through the event bus
- The Hook system responds to events at runtime, synthesizing new behaviors
- Topology adapts to task complexity
- Meta-Tasks decompose the overall task and generate an Action Plan, assigned to Squads for completion

### 📈 Pain 5: Systems get more complex; migration costs keep rising

Today's AI platforms:
- Day one on SQLite; migrate to Postgres when data grows
- Works locally; rewriting configs for the cloud
- Switching vector search from local to cloud — the whole API changes

**Cogneva's answer**: **final storage architecture from day one**
- Directly targets standard-protocol storage (PostgreSQL/Qdrant/Meilisearch/S3/NATS)
- No Adapter/Factory switching layer
- Local development and production deployment share the same configuration

### 🙈 Pain 6: The "black box" problem of AI systems

Today's AI systems:
- When something breaks, you don't know which step went wrong
- No way to trace an Agent's decision process
- Auditing and compliance are out of the question

**Cogneva's answer**: **Protobuf Envelope + JSON Payload raw data stream**
- Format: `<varint length><protobuf envelope><JSON payload>`, zstd-compressed
- Protobuf only handles the envelope (meta + context + stream name); business data all goes through JSON
- Hot tier (0–7 days) → warm tier (7–90 days) → cold tier (90 days+)
- Replayable, auditable, explainable; adding new event types requires no proto changes, no recompilation

---

## ✨ Core Advantages

### 🎯 Advantage 1: Dynamic orchestration

Traditional: humans draw the DAG → Agents execute by the chart
Cogneva: Agents emit Events → the Hook system responds dynamically → Skills load adaptively

**This is a human-and-machine-friendly paradigm**:
- Traditional UIs are designed for humans; Agents can hardly use them
- Cogneva's UI is the reply of the Event Stream
- Humans consume the event stream through the UI; Agents subscribe to the event stream directly, no UI parsing needed
- The same Event Stream is a chat interface for humans and a native API for Agents

### 🎛️ Advantage 2: Stateless Agents + system-level state machine

**Core principle: Agents only "think" and "call tools"; all state management is externalized to the system layer**

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   Agent     │────→│ Message     │────→│  Redis/PostgreSQL   │
│ (tasks only)│     │ Queue       │     │   (state storage)   │
└─────────────┘     │ (cog-orchestrator)│ └─────────────────────┘
      ↑             └─────────────┘              │
      └────────── no filesystem ops, no concurrency conflicts ──┘
```

- **Task state**: externalized to the message queue; Agents hold no local progress
- **Memory**: externalized to the vector database; Agents inject context via retrieval
- **Skills**: externalized to the Registry; Agents load them dynamically at runtime
- **Configuration**: externalized to the config center; hot reload without restarting Agents
- **Notifications**: pushed via the Event Stream, not occupying the Agent's context window

**Effect**: stateless Agents can restart and scale at any time with no concurrency conflicts; the context window is used only for "thinking", not "bookkeeping". The system layer manages all state through a state machine.

### 🧩 Advantage 3: Capability-based plugin architecture

**cog-core provides infrastructure abstractions; business Crates declare capabilities via Capabilities; cogneva handles runtime auto-discovery and assembly**:

```
┌─────────────────────────────────────────┐
│      cogneva (runtime assembler)        │
│  ┌─────────────────────────────────┐   │
│  │   cog-core (infrastructure      │   │
│  │   abstraction layer)            │   │
│  │  Backend Traits · plugin        │   │
│  │  registration system            │   │
│  │  auto-discovery · dependency    │   │
│  │  validation · topological sort  │   │
│  └─────────────────────────────────┘   │
│         ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑ ↑          │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐  │
│  │llm │ │agent│ │memory│ │wiki │ │...│  │
│  │Crate│ │Crate│ │Crate │ │Crate│ │Crate│  │
│  └────┘ └────┘ └────┘ └────┘ └────┘  │
└─────────────────────────────────────────┘
```

- **Capability-based plugin registration**: each Crate declares capabilities (e.g. `ProvidesLLM`, `ProvidesMemory`), matched automatically at runtime
- **Dependency topological sorting**: `cogneva/src/bootstrap.rs` automatically resolves the Crate dependency graph and initializes in the correct order
- **Zero-config hot-plugging**: adding a Crate only requires placing it in `crates/` and recompiling — no main-program changes needed
- **Fault isolation**: a single Crate panic does not drag down the whole system; Supervisor restarts it automatically

### 🧠 Advantage 4: Permanent memory — three-layer progressive architecture

Not simple conversation-history concatenation, but a **Raw → Schema → Summary three-layer progressive** permanent memory:

```
┌─────────────────────────────────────────┐
│  Layer 2 — Summary (semantic layer)     │
│  text summaries + dense/sparse vectors  │
│  Hybrid search + Cross-encoder rerank   │
├─────────────────────────────────────────┤
│  Layer 1 — Schema (structured facts)    │
│  entities, relations, events;           │
│  graph-traversal queries                │
├─────────────────────────────────────────┤
│  Layer 0 — Raw (raw data layer)         │
│  immutable append-only, object storage  │
└─────────────────────────────────────────┘
```

**Essential differences from file-level memory**:
- File-level memory (e.g. MEMORY.md) is flat text, capacity-limited, keyword-matched retrieval
- Cogneva's memory is **structured + vectorized + graph-relational**, supporting semantic retrieval, time-range filtering, and relation-chain queries
- The LLM automatically assigns importance scores (1–10); low-value memories decay automatically, high-value memories are continuously reinforced
- Multi-tenant Namespace isolation for enterprise-grade data security

### 🔭 Advantage 5: Full-stack observability

Self-built metrics/logs/tracing/raw data stream:
- Prometheus + Grafana (metrics)
- Loki + Promtail (logs)
- Jaeger (tracing)
- Protobuf Envelope + JSON Payload raw data stream (audit backtracking)

### 🔍 Advantage 6: Evaluable, replayable, auditable

**Snapshot + Replay**: complete recording of Agent execution (Trace), deterministic replay
- `TraceCollector` collects execution traces → `TraceStore` persists them (memory/Redis/S3/file)
- `ReplayEngine` replays along the timeline to locate root causes
- Hot/cold tiering: hot (memory/Redis) → warm (file) → cold (S3/zstd)

**cog-eval evaluation framework**: evolution without eval is blind
- Six components: Dataset / Metric / Runner / Comparator / Report / Harness
- 6 built-in metrics: ExactMatch, SemanticSimilarity, ToolCallAccuracy, LatencyP50/P99, TokenEfficiency, CostPerTask
- A/B comparison + statistical significance testing (z-test / t-test)
- CI integration: regression runs automatically with cargo test

### 🚗 Advantage 7: Supervisor sense-decide-act loop (autonomous-driving style)

Not passive alerting, but an autonomous loop of **active perception → decision → execution**:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Perception  │───→│  Decision   │───→│  Execution  │
│ Hook events │    │ rule engine │    │ task        │
│ heartbeats  │    │ load        │    │ rebalancing │
│ health      │    │ prediction  │    │ auto-       │
│ checks      │    │ fault       │    │ recovery    │
│             │    │ classification│  │ quota       │
│             │    │             │    │ enforcement │
└─────────────┘    └─────────────┘    └─────────────┘
       ↑                                    │
       └────────── effect feedback ─────────┘
```

- **Perception**: `cog-supervisor` collects Agent/LLM events via Hooks, aggregating health status
- **Decision**: `SchedulerGate` controls task flow, `TaskRebalancer` schedules dynamically, `Respawner` recovers from faults
- **Execution**: Agent Kill/Restart/Checkpoint, DLQ Retry, Gate Pause/Resume
- **Full API exposure**: 50+ endpoints covering status queries, control interventions, and audit replay

### 🐳 Advantage 8: Cloud-native from day one

- FHS standard directories (`/opt/cogneva`, `/var/lib/cogneva-data`, `/etc/cogneva`)
- Three deployment modes: systemd / Docker / Kubernetes
- Configuration hot reload (update log levels, gateway config, LLM providers with zero downtime)
- Graceful Shutdown (zero-downtime updates)

### 🧪 Advantage 9: Native support for Harness Engineering

Harness Engineering = providing Agents with a complete execution environment (tools, context, evaluation, feedback). Cogneva is natively designed for it:

| Harness element | Cogneva implementation |
|-----------------|------------------------|
| **Toolset** | `cog-skill` dynamic discovery + `cog-extension` WASM/Rhai extensions |
| **Context** | `cog-prompt` template engine + `cog-memory` three-layer progressive memory + `cog-wiki` knowledge base |
| **Eval** | `cog-eval` six-component framework: Dataset / Metric / Runner / Comparator / Report / Harness |
| **Feedback** | `cog-reflection` L0–L3 self-evolution: SelfReview → ReflectionEngine → EvolutionEngine |
| **Sandbox** | `cog-agent` runtime isolation + `cog-supervisor` sense-decide-act loop |

**Context Engineering subset**:
- Prompt template hot reload + A/B testing
- Three-layer memory auto-injected into context (Raw facts → Schema relations → Summary semantics)
- Knowledge base full-text search + vector semantic search Hybrid fusion
- Adaptive context-window compression (TokenBudgetManager)

### 🛠️ Advantage 10: Native support for Agentic Engineering

Agentic Engineering = giving Agents the ability to autonomously plan, execute, collaborate, and evolve. Cogneva provides the complete stack:

| Agentic element | Cogneva implementation |
|-----------------|------------------------|
| **Planning** | `cog-orchestrator` DAG orchestration + ActionPlanner + automatic skill-gap filling |
| **Execution** | `cog-agent` ReAct kernel + `cog-llm` unified multi-model calls + streaming responses |
| **Collaboration** | `cog-orchestrator` Process orchestration + `cog-collaboration` Squad execution + Ralph Loop hard reset |
| **Evolution** | `cog-reflection` L0–L3: SelfReview → Reflection → Evolution → MetaLearning |
| **Observability** | `cog-supervisor` sense-decide-act loop + `cog-observability` full-stack tracing |

**Squad + Process collaboration model**:
- **Process** (`cog-orchestrator`): orchestrates goals into executable DAG flows; responsible for task state machines, dependency triggering, timeout detection, and dead-letter queues
- **Squad** (`cog-collaboration`): atomic task execution unit; multiple Agents inside collaborate via the P→G→E pipeline or roundtable debates; Ralph Loop anti-corruption hard reset
- **Agent** (`cog-agent`): concrete reasoning individuals from the `GlobalAgentManager` worker pool

**Three-layer quality loops (inside each Squad)**:
- **Ralph Loop (outer)**: anti-corruption — detects failure, globally hard-resets, prevents system degradation
- **P→G→E pipeline (middle)**: anti-confusion — Planner/Generator/Evaluator specialization, avoiding role chaos
- **Self-Review Loop (inner)**: anti-self-deception — every Agent checks its own work: Observe→Critique→Compare→Decide→Revise→Log

### 🧫 Advantage 11: Self-evolution — L0–L3 four-layer depth

| Layer | Capability |
|-------|-----------|
| L0 per-run self-review | automatic quality check after every Agent run |
| L1 cross-session learning | distill Skills from successes/failures, write into Registry |
| L2 code evolution | **automatically generate Rust code patches, verified by cargo check** |
| L3 meta-learning | autonomously discover new patterns, explore new capabilities |

**The concrete L2 code-evolution flow**:
1. Detect repeated error patterns
2. LLM generates improved Rust code
3. `cargo check` compilation verification (up to 3 iterations)
4. After passing the quality gate, written to `evolution-patches/`
5. Registered into the running system in real time via `hook_sink`/`tool_sink` channels

---

## 🏗️ Architectural Philosophy: Contracts and Assembly

Cogneva has 20+ business crates; if they depended on each other directly, they would quickly become an inseparable plate of "spaghetti". Our answer is the **"contract layer + assembly layer" dual-core split**:

- **`cog-core` is the system's "contract constitution"**: it only defines cross-crate traits, types, configurations, errors, and plugin contracts — no business logic, zero external IO. All business crates depend only on `cog-core`, with zero direct dependencies on each other.
- **`cogneva` is the system's "runtime government"**: it connects all independent business crates into a runnable whole in the correct order, in the correct way, with the correct lifecycle — loading configuration, auto-discovering plugins, topologically-sorted initialization, configuration hot reload, and graceful shutdown. It contains no business logic.

Behind this split are **DIP (Dependency Inversion Principle)** and **high cohesion, low coupling**: high-level modules do not depend on low-level modules; both depend on `cog-core`'s abstract interfaces. `cog-agent` does not directly depend on `cog-llm`'s concrete implementation — it depends on `cog_core::contract::llm::LlmClient`; `cog-orchestrator` does not directly depend on `cog-stream`'s NATS implementation — it depends on `cog-core::contract::stream::MessageBackend`.

Adding a new business crate only requires:

1. Adding the dependency in `cogneva/Cargo.toml`;
2. Exposing `pub const DESCRIPTOR` in the crate's `plugin.rs`;
3. Recompiling.

`cogneva/build.rs` automatically scans and generates the plugin registry; `PluginRunner` initializes in dependency-topological order. The system thus gains **zero-config hot-plugging**.

> **`cog-core` lets Cogneva come apart; `cogneva` lets Cogneva run.**
>
> Without `cog-core`, the system is a monolith; without `cogneva`, the system is scattered sand.

---

## 🗺️ Architecture Overview

### 📚 Layered architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 6   cogneva          main entry, assembly, lifecycle │
├─────────────────────────────────────────────────────────────┤
│  Layer 5   cog-gateway      unified HTTP + WebSocket gateway│
├─────────────────────────────────────────────────────────────┤
│  Layer 4   cog-auth / cog-quota / cog-wiki / cog-memory     │
│            auth, quota, knowledge base, permanent memory    │
│            (horizontal capability layer)                    │
├─────────────────────────────────────────────────────────────┤
│  Layer 3   cog-orchestrator DAG orchestration, message      │
│            queue, task scheduling                           │
├─────────────────────────────────────────────────────────────┤
│  Layer 2   cog-collaboration Squad multi-Agent collaboration│
├─────────────────────────────────────────────────────────────┤
│  Layer 1.5 cog-agent        Agent ReAct execution engine    │
│  Layer 1   cog-llm          unified multi-model calls       │
├─────────────────────────────────────────────────────────────┤
│  Layer 0.5 cog-storage      data storage & adaptation       │
├─────────────────────────────────────────────────────────────┤
│  Layer 0   cog-core         infrastructure abstraction      │
├─────────────────────────────────────────────────────────────┤
│  Vertical  cog-supervisor / cog-observability /             │
│            cog-reflection / cog-protocol                    │
│            monitoring, observability, self-evolution,       │
│            protocol adaptation                              │
└─────────────────────────────────────────────────────────────┘
```

### 📡 Communication (hot-swappable Backend Traits)

All communication layers are abstracted through `cog-core` Backend Traits — **not bound to concrete implementations**, replaceable at any time:

| Scenario | Trait | Current implementation | Replaceable with |
|----------|-------|------------------------|------------------|
| Same-host IPC | — | Unix Domain Socket | — |
| Cross-host | — | gRPC / UDP | — |
| Event bus | `MessageBackend` | NATS JetStream | Redis Streams / Kafka / in-memory channel |
| External API | — | HTTP / WebSocket | — |

`MessageBackend` Trait definition (`cog-core/src/stream/message_bus.rs`):
- `publish` / `subscribe` / `create_consumer_group` / `ack` / `dlq`
- Implementations can optimize batching and delayed delivery for Redis pipelines, NATS concurrent publishing, in-memory single-lock, etc.

### 🗄️ Storage (hot-swappable Backend Traits)

All storage layers are likewise abstracted through `cog-core` Backend Traits — **not bound to any concrete database**:

| Data type | Trait | Current implementation | Replaceable with |
|-----------|-------|------------------------|------------------|
| Relational data | — | PostgreSQL | TiDB / Neon / MySQL (same-protocol extension) |
| Vector data | `VectorBackend` | Qdrant | Milvus / LanceDB / Memory (testing) |
| State/Checkpoint | `StateBackend` | PostgreSQL | Redis / any KV store |
| Object storage | `ObjectBackend` | SeaweedFS / S3 | MinIO / Azure Blob |
| Message queue | `MessageBackend` | NATS JetStream | Redis Streams / Kafka |
| Full-text search | `SearchBackend` | Meilisearch | Elasticsearch |
| Metrics | `MetricsBackend` | Prometheus | — |

`VectorBackend` Trait example (`cog-core/src/storage/vector.rs`):
- `create_collection` / `insert` / `insert_sparse` / `search` / `search_sparse` / `search_hybrid`
- `search_hybrid` default implementation: only calls `search` (dense vector retrieval), does not depend on `search_sparse`; vector databases supporting sparse + dense hybrid can override with a native implementation

---

## 🚀 Quick Start

Cogneva supports **Meta-bootstrap**: starting from a blank machine (Linux, macOS, or Windows), a single command completes environment probing, dependency self-repair, K3s/K8s adaptive deployment, security gateway configuration, and sandbox initialization, finally handing control over to the WebUI.

The entry script automatically detects whether your network is in mainland China (by probing the rustup distribution domain) and switches every dependency — source code, Rust toolchain, crates, container images, OS packages — to domestic mirrors (Gitee / TUNA / rsproxy / DaoCloud) when needed. No manual selection. Force with `COGNEVA_CN_MIRROR=1` (or `0`).

### 🐧 Linux

```bash
(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh
```

Runs directly on the bare metal.

### 🍎 macOS

```bash
(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh || curl -fsSL -m 15 https://gitee.com/hcipengm/cogneva/raw/main/bootstrap.sh) | sh
```

The **same command**. K3s needs a Linux kernel, so the script automatically installs [Lima](https://lima-vm.io) (via Homebrew) and creates an Ubuntu VM, then runs the exact same one-liner inside it. All dependencies live inside the VM; the host only gets `limactl`. When finished, the WebUI is forwarded to <http://localhost:8080>. Manage the VM with `limactl shell cogneva` / `limactl stop cogneva` / `limactl delete cogneva`.

### 🪟 Windows

```powershell
# Administrator PowerShell
iwr -useb https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.ps1 | iex
```

The script installs WSL2 + Ubuntu (prompts for a reboot if required — just re-run it afterwards, it is idempotent), then runs the same one-liner inside WSL. WSL2's default localhostForwarding exposes the WebUI at <http://localhost:8080>. Force China mirrors with `-CnMirror 1`: download the script first and pipe it, e.g. `& ([scriptblock]::Create((iwr -useb <url>).Content)) -CnMirror 1`.

> The first URL is GitHub's official raw endpoint; if it is unreachable (e.g. restricted networks), the command automatically falls back to the Gitee mirror. The bootstrap script itself also falls back to the Gitee repo when fetching source code.

The bootstrapper automatically:

1. Probes CPU / memory / nodes / architecture;
2. Configures and verifies the LLM API Key (or a local model);
3. Automatically selects the lightweight K3s branch or the production K8s branch;
4. Installs the container runtime, K3s/buildah;
5. Deploys the security gateway, NetworkPolicy, and sandbox environment;
6. Starts Cogneva and prints the WebUI address;
7. Exits, zeroing the API Key from memory.

### ☸️ Existing cluster

```bash
# Existing K3s cluster
kubectl apply -f deploy/k3s/

# Existing standard K8s production cluster
kubectl apply -f deploy/k8s/
```

### 🔧 Traditional manual deployment

For manual compilation: `cargo build --release`. Container image: see `Dockerfile`. K3s/K8s manifests: see `deploy/`.

---

## 📊 Project Status

**Current stage**: Alpha → Beta transition

- Core framework complete; all 26 Crates have substantive implementations
- Compiles cleanly; `cargo clippy --workspace --all-targets -- -D warnings` zero warnings; `cargo fmt --check` passes (2026-07-21)
- All 107 test suites pass (`cargo test --workspace`, 2026-07-21)
- **To be improved**: production validation, ecosystem expansion, community building

---

## ⚖️ License

[Modified MIT License](LICENSE) — based on the standard MIT license with an added attribution-display requirement for large-scale commercial use (display "Powered by Cogneva" when MAU ≥ 5 million / annual revenue ≥ 5 million CNY / headcount ≥ 300).
