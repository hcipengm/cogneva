//! principle: feedback_a_retention_window_counted_in_runs_has_no_calendar_length
//!
//! Retention keeps the newest `keep` packages. It is a count of runs, not a
//! calendar window: a package survives because it is among the newest ones, and
//! is deleted even when it is only hours old, once `keep` newer packages exist.
//! Both halves need a witness, because "old packages stay" and "young packages
//! are protected" are the two ways a count gets silently turned into a window.

use chrono::{Duration, TimeZone, Utc};
use cogneva::backup::{enforce_retention, package_name};
use std::path::Path;

fn write_package(dir: &Path, at: chrono::DateTime<Utc>) -> String {
    let name = package_name(at);
    std::fs::write(dir.join(&name), b"package").unwrap();
    name
}

#[test]
fn a_package_old_enough_to_fail_any_window_is_kept_when_it_is_among_the_newest() {
    let dir = tempfile::tempdir().unwrap();
    // Five packages, all far older than any retention window somebody would
    // plausibly write as days or weeks.
    let names: Vec<String> = (1..=5)
        .map(|day| {
            write_package(
                dir.path(),
                Utc.with_ymd_and_hms(2024, 1, day, 3, 17, 0).unwrap(),
            )
        })
        .collect();

    let deleted = enforce_retention(dir.path(), 2);

    assert_eq!(deleted.len(), 3, "5 份留 2 份，删 3 份");
    for name in &names[3..] {
        assert!(
            dir.path().join(name).exists(),
            "{name} 是「最新的两份」之一，它的年龄不构成删除理由"
        );
    }
}

#[test]
fn a_package_hours_old_is_not_protected_by_its_youth() {
    let dir = tempfile::tempdir().unwrap();
    let now = Utc::now();
    let older = write_package(dir.path(), now - Duration::hours(3));
    let middle = write_package(dir.path(), now - Duration::hours(2));
    let newest = write_package(dir.path(), now - Duration::hours(1));

    let deleted = enforce_retention(dir.path(), 1);

    assert_eq!(deleted.len(), 2, "留 1 份就必须删 2 份，哪怕它们只有几小时");
    assert!(dir.path().join(newest).exists(), "最新那份必须还在");
    for name in [&older, &middle] {
        assert!(
            !dir.path().join(name).exists(),
            "{name} 很年轻，但「年轻」不是保留资格"
        );
    }
}

#[test]
fn files_that_are_not_packages_neither_count_nor_get_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let older = write_package(
        dir.path(),
        Utc.with_ymd_and_hms(2026, 3, 1, 3, 17, 0).unwrap(),
    );
    let newer = write_package(
        dir.path(),
        Utc.with_ymd_and_hms(2026, 3, 2, 3, 17, 0).unwrap(),
    );
    std::fs::write(dir.path().join("notes.txt"), b"keep me").unwrap();

    let deleted = enforce_retention(dir.path(), 1);

    assert_eq!(deleted.len(), 1, "只有包计入份数");
    assert!(!dir.path().join(&older).exists(), "{older} 应被删");
    assert!(dir.path().join(&newer).exists(), "{newer} 应保留");
    assert!(dir.path().join("notes.txt").exists(), "非包文件不许动");
}

#[test]
fn keep_zero_deletes_every_package_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    write_package(
        dir.path(),
        Utc.with_ymd_and_hms(2026, 3, 1, 3, 17, 0).unwrap(),
    );
    write_package(
        dir.path(),
        Utc.with_ymd_and_hms(2026, 3, 2, 3, 17, 0).unwrap(),
    );
    std::fs::write(dir.path().join("notes.txt"), b"keep me").unwrap();

    let deleted = enforce_retention(dir.path(), 0);

    assert_eq!(deleted.len(), 2, "留 0 份就要删光");
    assert!(dir.path().join("notes.txt").exists(), "非包文件不许动");
}
