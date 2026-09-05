//! 自动晋级运行时开关（一键暂停）。
//!
//! 配置文件给持久默认值（`promotion.enabled`），本开关是 admin API 的
//! 运行时覆盖：暂停立即生效——排队中的晋级全部转人工，已生效变更不
//! 受影响；进程重启后回落到配置文件值。共享给 [`crate::AutoPromoter`]
//! （降级判定最先查）与 [`crate::EvolutionAdminService`]（admin 端点）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use cog_core::PromotionSwitchInfo;

/// 运行时暂停开关。无 IO、无锁竞争（读路径是原子加载）。
#[derive(Default)]
pub struct PromotionSwitch {
    paused: AtomicBool,
    updated_at: Mutex<Option<DateTime<Utc>>>,
    note: Mutex<String>,
}

impl PromotionSwitch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub fn set_paused(&self, paused: bool, note: &str) {
        self.paused.store(paused, Ordering::Relaxed);
        *self.updated_at.lock().unwrap() = Some(Utc::now());
        *self.note.lock().unwrap() = note.to_string();
    }

    /// 合成对外快照；`config_enabled` 由持有配置的一方提供。
    pub fn snapshot(&self, config_enabled: bool) -> PromotionSwitchInfo {
        let paused = self.is_paused();
        PromotionSwitchInfo {
            config_enabled,
            paused,
            effective_enabled: config_enabled && !paused,
            updated_at: *self.updated_at.lock().unwrap(),
            note: self.note.lock().unwrap().clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_and_resume_flip_effective_state() {
        let sw = PromotionSwitch::new();
        let snap = sw.snapshot(true);
        assert!(snap.effective_enabled);
        assert!(snap.updated_at.is_none());

        sw.set_paused(true, "人工介入");
        let snap = sw.snapshot(true);
        assert!(snap.paused);
        assert!(!snap.effective_enabled);
        assert_eq!(snap.note, "人工介入");
        assert!(snap.updated_at.is_some());

        // 配置关闭时运行时恢复不能让晋级生效（相与语义）。
        sw.set_paused(false, "恢复");
        assert!(!sw.snapshot(false).effective_enabled);
        assert!(sw.snapshot(true).effective_enabled);
    }
}
