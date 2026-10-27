//! 本二进制的构建来源：哪一次提交、离发布有多远。
//!
//! 两个值由 `build.rs` 经 `cargo:rustc-env` 注入，源码里此前有六处各自写一遍
//! `env!("COGNEVA_GIT_REVISION")`。名字一样不代表读的是同一个东西：谁改了一处
//! 的字面量（或者哪天 build 脚本换了注入名），那一处读到的是**编译期没有定义的
//! 变量**——`env!` 在编译期就会红，看起来安全；真正会漂的是 build 脚本自己那个
//! 名字，它读的是运行期 env，读不到就退回 `unknown`，而 `unknown` 与「这一版确实
//! 没带来源」在读数上同形。名字收在这里，注入端与读取端各自只有一处。

/// 本二进制编译时的提交号；没有 `.git` 且没人注入时是 `unknown`。
pub const GIT_REVISION: &str = env!("COGNEVA_GIT_REVISION");

/// 距最近一次发布的代码标识（`v<版本>-<n>-g<rev>` 形状）；读不出来时明确说不出来。
pub const VERSION_ID: &str = env!("COGNEVA_VERSION_ID");
