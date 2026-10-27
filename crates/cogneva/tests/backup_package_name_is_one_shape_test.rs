//! principle: feedback_no_hardcoding_config_surface
//! 出包那侧写下包名，网关那侧按名字列举、下载、删除。两边各写一份前后缀时，
//! 改一处不会报错：网关认不出的名字不会被列出来，而「一个包也没有」正是第一次
//! 备份还没跑完时的读数——保留策略读着它什么也不删，看着像闲、不像坏。
//!
//! 前缀与后缀现在只声明一次（`cog_core::BACKUP_PACKAGE_PREFIX` /
//! `BACKUP_PACKAGE_SUFFIX`），所以它们不可能再各自漂移。这条判据守的是另一半：
//! 两个函数对「什么是一个包名」的判定要一致。写侧多拼一层目录、或时间戳里落进
//! 分隔符，网关就会安静地不认自己写下的包。

use chrono::{TimeZone, Utc};
use cog_gateway::backup_admin::is_package_name;
use cogneva::backup::package_name;

fn at(y: i32, m: u32, d: u32, hour: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hour, 17, 3).unwrap()
}

#[test]
fn the_name_the_writer_writes_is_one_the_gateway_lists() {
    for instant in [at(2026, 1, 1, 0), at(2026, 10, 27, 3), at(2027, 3, 9, 23)] {
        let name = package_name(instant);
        assert!(
            is_package_name(&name),
            "写侧写下 {name:?}，网关却不肯列它：页面会报「一个包也没有」，\
             而那个读数与「还没备份过」分不开"
        );
    }
}

#[test]
fn a_name_the_writer_could_not_have_written_is_refused() {
    // 写侧只会拼出一层文件名，且前缀后缀之间一定有内容（时间戳）。
    assert!(!is_package_name(
        "../cogneva-backup-20261027T031703Z.tar.zst"
    ));
    assert!(!is_package_name(
        "d/cogneva-backup-20261027T031703Z.tar.zst"
    ));
    assert!(!is_package_name("cogneva-backup-.tar.zst"));
    assert!(!is_package_name("cogneva-backup-20261027T031703Z"));
    // 出包目录里别的文件也不是包。
    assert!(!is_package_name("cogneva.json"));
}
