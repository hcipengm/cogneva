//! 标准 Benchmark 适配器（docs/2026-06-29_16-00 cog-eval 增强方案 §2.2 模块 1）。
//! 将 AgentBench / GAIA / SWE-bench 官方数据格式转换为 cog-eval 的 EvalDataset。

pub mod agentbench_loader;
pub mod gaia_runner;
pub mod swebench_runner;

pub use agentbench_loader::AgentBenchLoader;
pub use gaia_runner::GaiaRunner;
pub use swebench_runner::SweBenchRunner;
