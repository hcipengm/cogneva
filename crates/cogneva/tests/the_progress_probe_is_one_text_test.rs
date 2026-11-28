//! 进度探针只有一份文本：`cog_core::progress::PROGRESS_PROBE_COMMAND`。
//!
//! 探针读的是容器自己写下的窗口。文件里写 `unbounded`（这个作业类还没有一条实测）
//! 时，它只报读数、不判停——拿一个挑出来的数去杀掉一件没人量过的事，正是要拆的钟。
//! 少了那一支，`[ "$age" -le "$win" ]` 对着一个非数字比较，探针每次都以失败收尾。
//!
//! 2026-11-27 现场：chart 里手抄的那份停在旧版（没有 `unbounded` 分支），演练容器
//! 在 120s 初始延迟 + 3×30s 之后被 kubelet SIGKILL（exit 137，连续三次都是同一秒
//! 数），而进程本身一直在干活。手抄一处、判词一处，两份文本之间没有门——这条测试
//! 就是那道门：部署里的每一份探针文本都必须与常量逐字一致。

use crate::common::routes::repo_root;

/// 折叠空白：chart 的折叠标量（`>-`）把同一条命令拆成多行，判的是词，不是换行。
fn collapsed(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn every_progress_probe_is_the_canonical_text() {
    let root = repo_root();
    let canonical = collapsed(cog_core::progress::PROGRESS_PROBE_COMMAND);
    assert!(
        canonical.contains(r#"if [ "$win" = "unbounded" ]; then exit 0; fi"#),
        "常量自己丢了 unbounded 那一支：{}",
        canonical
    );

    // chart 两处（备份与演练各一条 Job），静态 k3s 清单各一处。
    let expected = [
        ("deploy/helm/cogneva/templates/backup.yaml", 2usize),
        ("deploy/k3s/backup-cronjob.yaml", 1),
        ("deploy/k3s/backup-drill-cronjob.yaml", 1),
    ];
    for (path, want) in expected {
        let text =
            std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"));
        let flat = collapsed(&text);
        let found = flat.matches(&canonical).count();
        assert_eq!(
            found, want,
            "{path}: 应有 {want} 处与 PROGRESS_PROBE_COMMAND 逐字一致的探针，找到 {found} 处。\
             手抄的那一份漂了——漂掉的正是 `unbounded` 那一支，它会让探针把一个还没有\
             实测窗口的作业在几分钟后杀掉"
        );
    }
}
