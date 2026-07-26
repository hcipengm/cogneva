//! 性能基准（审计 Phase 1 任务 1.5）：Agent 推理管线的可复现
//! 延迟 / 吞吐 / 内存占用测量。
//!
//! 使用 MockAgent 排除网络波动，测量 PGE 协作推理周期
//! （Planner → Generator → Evaluator）本身的框架开销：
//! - `planner_latency`：单次 Planner 推理延迟；
//! - `pge_cycle_throughput`：完整 PGE 周期吞吐；
//! - `pge_cycle_memory`：经计数分配器测量每周期堆分配字节数。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cog_collaboration::actors::{EvaluatorActor, GeneratorActor, PlannerActor, PreviousAttempt};
use cog_core::{Agent, AgentState, Task, TaskType};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ── 计数分配器：测量内存占用 ──────────────────────────────────────────

struct CountingAlloc;

static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

// ── Mock Agent：固定 JSON 响应，排除 LLM 网络因素 ─────────────────────

struct MockAgent {
    response: serde_json::Value,
}

#[async_trait]
impl Agent for MockAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn start(&self) {}
    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        Ok(cog_core::AgentCheckpoint {
            checkpoint_id: String::new(),
            task_id: String::new(),
            agent_state: serde_json::Value::Null,
            context_window: Vec::new(),
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        })
    }
    async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn state(&self) -> cog_core::SFResult<AgentState> {
        Ok(AgentState::Idle)
    }
    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }
    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }
    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }
    async fn write_board(
        &self,
        _task_id: &str,
        _field: &str,
        _value: &str,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
    async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn review_and_revise(
        &self,
        output: &str,
        _config: &cog_core::SelfReviewConfig,
    ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
        Ok((
            output.to_string(),
            cog_core::SelfReviewResult::Pass {
                score: 1.0,
                summary: "mock".into(),
            },
        ))
    }
}

fn task() -> Task {
    Task::new(
        "bench-task",
        TaskType::Custom("benchmark".into()),
        serde_json::json!({"goal": "measure pge cycle overhead"}),
    )
}

fn planner_agent() -> Arc<dyn Agent> {
    Arc::new(MockAgent {
        response: serde_json::json!({
            "summary": "bench plan",
            "plan": {"approach": "measure"},
            "sub_tasks": []
        }),
    })
}

fn generator_agent() -> Arc<dyn Agent> {
    Arc::new(MockAgent {
        response: serde_json::json!({
            "content": {"code": "fn bench() {}"},
            "artifacts": []
        }),
    })
}

fn evaluator_agent() -> Arc<dyn Agent> {
    Arc::new(MockAgent {
        response: serde_json::json!({
            "verdict": "pass",
            "feedback": "looks good",
            "score": 90,
            "criteria": []
        }),
    })
}

async fn run_pge_cycle() {
    let t = task();
    let planner = PlannerActor::new(planner_agent());
    let generator = GeneratorActor::new(generator_agent());
    let evaluator = EvaluatorActor::new(evaluator_agent());

    let plan = planner.plan(&t, 1, None, None, None, None).await;
    let generation = generator
        .generate(
            &t,
            &serde_json::json!(plan.plan),
            1,
            PreviousAttempt::default(),
            None,
        )
        .await;
    let evaluation = evaluator
        .evaluate(
            &t,
            &serde_json::json!(plan.plan),
            &generation.content,
            &[],
            &["correctness"],
            None,
        )
        .await;
    criterion::black_box(evaluation);
}

fn bench_planner_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("planner_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let t = task();
            let planner = PlannerActor::new(planner_agent());
            let out = planner.plan(&t, 1, None, None, None, None).await;
            criterion::black_box(out);
        });
    });
}

fn bench_pge_cycle_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pge_cycle_throughput");
    for cycles in [1u64, 10, 50] {
        group.throughput(Throughput::Elements(cycles));
        group.bench_with_input(
            BenchmarkId::from_parameter(cycles),
            &cycles,
            |b, &cycles| {
                b.to_async(&rt).iter(|| async move {
                    for _ in 0..cycles {
                        run_pge_cycle().await;
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_pge_cycle_memory(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("pge_cycle_memory", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            // 预热，排除惰性初始化分配。
            rt.block_on(run_pge_cycle());
            ALLOC_BYTES.store(0, Ordering::Relaxed);
            ALLOC_CALLS.store(0, Ordering::Relaxed);
            for _ in 0..iters {
                rt.block_on(run_pge_cycle());
            }
            let bytes = ALLOC_BYTES.load(Ordering::Relaxed);
            let calls = ALLOC_CALLS.load(Ordering::Relaxed);
            eprintln!(
                "pge_cycle_memory: {} bytes/iter, {} alloc calls/iter",
                bytes / iters.max(1) as usize,
                calls / iters.max(1) as usize
            );
            start.elapsed()
        });
    });
}

criterion_group!(
    benches,
    bench_planner_latency,
    bench_pge_cycle_throughput,
    bench_pge_cycle_memory
);
criterion_main!(benches);
