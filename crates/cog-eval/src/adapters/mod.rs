//! 标准 Benchmark 适配器。
//! 将 AgentBench / GAIA / SWE-bench 官方数据格式转换为 cog-eval 的 EvalDataset。
//!
//! Standard benchmark adapters: each turns one release's own file format into an
//! [`crate::EvalDataset`]. HumanEval is the one the evaluation gate reads, and it
//! is read straight out of the repository (`eval_datasets/`), because a fixed
//! suite that does not ship with the code under test is chosen by the side being
//! measured.

pub mod agentbench_loader;
pub mod gaia_runner;
pub mod humaneval_loader;
pub mod swebench_runner;

pub use agentbench_loader::AgentBenchLoader;
pub use gaia_runner::GaiaRunner;
pub use humaneval_loader::HumanEvalLoader;
pub use swebench_runner::SweBenchRunner;
