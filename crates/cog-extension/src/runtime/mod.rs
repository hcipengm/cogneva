//! Execution runtimes — WASM (wasmtime) and Rhai scripting.

pub mod loader;
#[cfg(feature = "rhai")]
pub mod script;
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "rhai")]
pub use script::RhaiRuntime;
#[cfg(feature = "wasm")]
pub use wasm::WasmRuntime;
