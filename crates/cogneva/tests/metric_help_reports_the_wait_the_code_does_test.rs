//! 一条读数的说明必须描述这份代码正在做的事。
//!
//! `cogneva_mainline_advance_deferrals` 的说明曾写着「advance 是唯一一个等零秒的调用方，
//! 所以饿死从来不以一次被拒的形式出现在任何计数里」。而部署器其实会等满 `ADVANCE_BUILD_WAIT`
//! 才退回下一轮，它的被拒也确实落在平台那一格里：
//! `cogneva_build_gate_refused_total{asker="mainline_advance",reason="waited_out"}` 读得到非零。
//! 说明与读数互相矛盾，而读的人只有先信了说明，才会认为那一格永远是 0、不去读它。
//!
//! 判据不能是「说明里出现没出现某个词」（改天换个说法，同一条判据就红得没有理由），而是
//! **说明里那句关于等待时长的话与代码里的等待时长是不是同一件事**。所以这里把两边都读出来比：
//! 一边是 `mainline_deployer.rs` 里那个常数，一边是这条序列的说明。

#[path = "common/root.rs"]
mod root;

use std::fs;

const DEPLOYER: &str = "crates/cog-reflection/src/mainline_deployer.rs";
const WAIT_NEEDLE: &str = "const ADVANCE_BUILD_WAIT: Duration = Duration::from_secs(";

/// 读 `const ADVANCE_BUILD_WAIT: Duration = Duration::from_secs(N);` 的那个 N。
///
/// 读不到就 panic，不返回 `Option`：「没找到这个常数」与「这个常数是 0」在断言上长得一样，
/// 而 0 恰好是「等零秒」那一句会要求的值——把前者当后者，判据会在改名那天静默变成绿的。
fn advance_build_wait_secs(src: &str) -> u64 {
    let start = src.find(WAIT_NEEDLE).unwrap_or_else(|| {
        panic!("部署器源码里找不到 `{WAIT_NEEDLE}`：这条判据的题目没了，改判据而不是删常数")
    }) + WAIT_NEEDLE.len();
    let rest = &src[start..];
    let end = rest
        .find(')')
        .unwrap_or_else(|| panic!("`{WAIT_NEEDLE}` 后面没有右括号，读不出等待时长"));
    rest[..end]
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("`{WAIT_NEEDLE}{}` 不是数字：{e}", &rest[..end]))
}

#[test]
fn the_advance_help_does_not_claim_a_wait_the_deployer_does_not_do() {
    let root = root::repo_root();
    let src = fs::read_to_string(root.join(DEPLOYER)).expect("deployer source is readable");
    let wait = advance_build_wait_secs(&src);
    let help = cog_core::metric_help_text(
        cog_core::MetricType::Gauge,
        cog_core::metric_names::MAINLINE_ADVANCE_DEFERRALS.as_str(),
    );

    assert!(
        wait > 0,
        "`ADVANCE_BUILD_WAIT` 现在是 0 秒：说明里那句「等零秒」重新成立了，这条判据也就没有题目了"
    );
    assert!(
        !help.contains("waits zero seconds"),
        "说明还写着 advance 等零秒，而代码里等 {wait} 秒；平台的 \
         `cogneva_build_gate_refused_total{{asker=\"mainline_advance\",reason=\"waited_out\"}}` \
         正是这 {wait} 秒等满一次才落一格的计数。说明原文：{help}"
    );
    assert!(
        help.contains("waited_out"),
        "说明没有说出「被拒真的落在 waited_out 那一格」，于是读的人会以为那一格永远是 0、不去读它；\
         说明原文：{help}"
    );
}
