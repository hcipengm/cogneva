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
pub mod hle;
pub mod humaneval_loader;
pub mod swe_pro;
pub mod swebench_runner;
pub mod toolathlon;

pub use agentbench_loader::AgentBenchLoader;
pub use gaia_runner::GaiaRunner;
pub use hle::{
    hle_benchmark, HleCaseSource, HleJudge, HleToolkit, HLE_JSONL, HLE_JUDGE_TEMPLATE_VERSION,
    HLE_TOOL_PYTHON, HLE_TOOL_WEB_SEARCH,
};
pub use humaneval_loader::HumanEvalLoader;
pub use swe_pro::{
    parse_test_list, swe_pro_benchmark, SweProBackend, SweProCaseSource, SweProEnvProvider,
    SweProJudge, SweProTestReport, SweProToolkit, SWE_PRO_JSONL, SWE_PRO_TOOL_BASH,
    SWE_PRO_TOOL_FILE_EDIT,
};
pub use swebench_runner::SweBenchRunner;
pub use toolathlon::{
    toolathlon_benchmark, ToolathlonBackend, ToolathlonCaseSource, ToolathlonCheck,
    ToolathlonEnvProvider, ToolathlonJudge, ToolathlonReport, ToolathlonToolkit,
    TOOLATHLON_TASKS_DIR,
};
