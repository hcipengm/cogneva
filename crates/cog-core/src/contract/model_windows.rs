//! 平台配着的那几只模型，各自公布多大的上下文窗口。
//!
//! 这个数**不在代码里**：它跟着 LLM 配置走。操作者在设置页挑一只型号时，那一项
//! 预设就带着厂商公布的窗口（`web/src/overlay/LlmSetupOverlay.tsx` 的 `PRESETS`），
//! 保存时随上游一起落进 `llm-upstreams`，网关的状态读数把它报给主应用，主应用用
//! [`replace`] 把这一份映射换成新的。
//!
//! 为什么不是一张内置表：内置表会在配置换掉之后继续说着上一只模型的窗口，而窗口
//! 正是循环裁剪的依据——表里那句旧话会把本来装得下的历史裁掉，或者让第一次调用
//! 顶在上游收不下的尺寸上。配置换了，映射跟着换；配置没说的型号，这里就是
//! `None`（「没人说过」），由实测补上（收下过多大的 prompt、拒收时自述多大的界）。
//!
//! 进程级而不是按对话：一个进程里所有员工打的是同一个上游池，窗口是池的属性，
//! 不是某一件活的属性。

use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

static CONFIGURED: OnceLock<RwLock<BTreeMap<String, u64>>> = OnceLock::new();

fn cell() -> &'static RwLock<BTreeMap<String, u64>> {
    CONFIGURED.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// 用配置面刚读到的那一份映射替换掉现有的。
///
/// 替换而不是并入：池里摘掉一家、换成另一只型号之后，旧的那一行不能再说话。
/// 同一个型号出现多次（多家上游跑同一只模型）时取**最小**的那个窗口——任何一发
/// 都可能落到那家最小的上游上，与拒收时取 `limit_min` 是同一条理由。
pub fn replace<I>(windows: I)
where
    I: IntoIterator<Item = (String, u64)>,
{
    let mut fresh: BTreeMap<String, u64> = BTreeMap::new();
    for (model, window) in windows {
        let model = model.trim();
        if model.is_empty() || window == 0 {
            continue;
        }
        fresh
            .entry(model.to_string())
            .and_modify(|held| *held = (*held).min(window))
            .or_insert(window);
    }
    *cell()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = fresh;
}

/// 一只模型在配置面上公布的窗口；`None` = 配置没说过，不是 0。
///
/// 先按原样比，再按忽略大小写比：上游把型号回报成 `MiniMax-M3` 还是
/// `minimax-m3` 不该决定查不查得到。
pub fn configured(model: &str) -> Option<u64> {
    let model = model.trim();
    if model.is_empty() {
        return None;
    }
    let held = cell()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(window) = held.get(model) {
        return Some(*window);
    }
    held.iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(model))
        .map(|(_, window)| *window)
}

/// 现在这一份映射（读数面与测试用）。
pub fn all() -> Vec<(String, u64)> {
    cell()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .map(|(model, window)| (model.clone(), *window))
        .collect()
}

/// 清空。配置面读不到、或者测试之间互相作证之前，用它把上一份撤掉。
pub fn clear() {
    cell()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 这一组用例共用同一个进程级表，测试线程默认并行：不排队的话，甲的
    /// `clear()` 会落在乙的断言与 `replace` 之间，红得看调度。
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn exclusive() -> std::sync::MutexGuard<'static, ()> {
        SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 一个进程级的全局表：测试之间会互相看见，所以每个用例自己收拾。
    fn reset(pairs: &[(&str, u64)]) {
        clear();
        replace(pairs.iter().map(|(m, w)| ((*m).to_string(), *w)));
    }

    #[test]
    fn the_map_is_what_the_config_last_said() {
        let _serial = exclusive();
        reset(&[("claude-opus-5-5", 1_000_000), ("k3", 1_048_576)]);
        assert_eq!(configured("claude-opus-5-5"), Some(1_000_000));
        // 上游回报的大小写不同，仍然是同一只模型。
        assert_eq!(configured("MiniMax-M3"), None);
        reset(&[("MiniMax-M3", 1_048_576)]);
        assert_eq!(configured("minimax-m3"), Some(1_048_576));
        // 配置没说过 —— 不是 0。
        assert_eq!(configured("via-security-gateway"), None);
        assert_eq!(configured(""), None);
    }

    /// 替换而不是并入：摘掉一家之后，旧的那一行不能再说话。
    #[test]
    fn a_new_configuration_replaces_the_old_one() {
        let _serial = exclusive();
        reset(&[("deepseek-flash", 1_048_576)]);
        replace([("kimi-k3".to_string(), 1_048_576)]);
        assert_eq!(configured("deepseek-flash"), None);
        assert_eq!(configured("kimi-k3"), Some(1_048_576));
    }

    /// 同一只模型在两家上游上、窗口不一样：可用的那个是最小的，因为任何一发都
    /// 可能落到小的那家。
    #[test]
    fn the_smallest_window_for_a_model_binds() {
        let _serial = exclusive();
        reset(&[("k3", 1_048_576), ("k3", 262_144), ("", 999), ("zero", 0)]);
        assert_eq!(configured("k3"), Some(262_144));
        assert_eq!(configured("zero"), None, "0 不是窗口，是没人说过");
        assert_eq!(all().len(), 1, "空名字与 0 不进表");
    }
}
