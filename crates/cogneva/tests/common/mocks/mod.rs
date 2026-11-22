// 这个模块被好几个测试目标共用（各自 `#[path]` 引进来）：某个目标没用到的替身，
// 在它那里就是「未使用」，而在别的目标里正是它在用。这不是缺陷，是共用的形状。
#![allow(unused_imports)]

pub mod mock_llm;
pub mod mock_metrics_backend;
pub mod mock_object_backend;
pub mod mock_raw_source;
pub mod mock_registration;
pub mod mock_state_backend;

pub use mock_llm::MockLLMProvider;
pub use mock_metrics_backend::MockMetricsBackend;
pub use mock_object_backend::MockObjectBackend;
#[allow(unused_imports)]
pub use mock_raw_source::{raw, source_ref};
#[allow(unused_imports)]
pub use mock_registration::registration;
pub use mock_state_backend::MockStateBackend;
