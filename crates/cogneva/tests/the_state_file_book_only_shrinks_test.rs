//! 盘上「每个 id 一份记录」的写法，只许减。
//!
//! 一处把状态写进 `format!("{id}.json")` 这类拼出来的文件名，就有第二份正本：分叉时
//! 没人知道哪份对，也没人读得出这份是什么时候写的、谁写的、能不能重建。普查器面六把它
//! 数出来；这一条给它一本只许减的账。
//!
//! 判的是**写法**不是文件名：同一行里 `format!(` 之后先出现 `{`、再出现 `.json"` 才算。
//! 一个固定的 `config.json` 不是「每个 id 一份」，按 `.json` 一刀切会把它误收进来
//! （2026-11-23 由 `bounds-census-shapes.test.sh` 钉出过这一格）。夹具不计：样例里出现
//! 这种写法是它们在扮演盘上那份记录。
//!
//! 减的办法：让它成为某条流上的事实（投影带水位与重建），或删掉这份盘上副本。
//!
//! 没有这次改动这条会红：盘上记录面没有任何防回潮的东西。

use crate::common::routes::repo_root;
use crate::common::rust_production::{production_lines_keeping_comments, production_texts};

/// 这一行是不是在拼一个 `….json` 文件名。
///
/// `replace(".json", format!(…))` 拼出来的是**同一份配置的另一个环境层**
/// （`cogneva.<env>.json`），不是一个 id 的记录：动态的那一段是环境名，读过就丢，
/// 没有第二份正本。这一格由 2026-11-26 的 `config_loader` / `config_watcher`
/// 两处误收钉出来——判据收紧到「拼接结果的动态段是一个 id」，配置层不算。
fn dynamic_record_name(line: &str) -> bool {
    if line.contains("replace(\".json\"") {
        return false;
    }
    // 临时目录里的按 id 文件是这一趟的中间件，不是盘上留下来的记录：进程退出、
    // 机器重启之后没有任何东西指望它还在。
    if line.contains("temp_dir()") {
        return false;
    }
    let Some(at) = line.find("format!(") else {
        return false;
    };
    let rest = &line[at..];
    let Some(open) = rest.find('{') else {
        return false;
    };
    // 占位符之后**紧接** `.json"` 才算拼一个按 id 的文件名。`format!("{} missing
    // manifest.json")` 是一句错误文本，`format!("{DIR}/output.json")` 是容器里一个
    // 固定名字的产物：两者都曾有 `{` 与 `.json"`，却都不是盘上每个 id 一份的记录。
    let probe = &rest[open..];
    probe.contains("}.json\"") || probe.contains("}.meta.json\"")
}

/// 行上写下的处置：`// state-file: <kind> — <为什么它不是第二份正本>`。
///
/// 与 `cap:`／`keep:` 同一套纪律：判词写在它判的那一行上，不另立一张名单。
/// 理由必须写出「这不是第二条正本」或「由什么兜住」；一句 `state-file: ok`
/// 不算处置。能被处置的只有中间件——缓存、跨进程交接队列、一次性工件；
/// 一份被读者当成平台事实的按 id 记录，没有任何理由能让它留下来。
fn disposed_state_file(line: &str) -> bool {
    let Some(at) = line.find("state-file:") else {
        return false;
    };
    let reason = line[at + "state-file:".len()..].trim();
    let (_, why) = match reason.split_once('—') {
        Some(pair) => pair,
        None => return false,
    };
    why.trim().chars().count() >= 12
        && (why.contains("不是") || why.contains("由") || why.contains("只活"))
}

#[test]
fn the_state_file_book_only_shrinks() {
    let root = repo_root();
    let baseline_path = root.join("deploy/scripts/state-files.baseline");
    let baseline_text = std::fs::read_to_string(&baseline_path)
        .expect("盘上记录的底账读不到（deploy/scripts/state-files.baseline）");
    let mut baseline: std::collections::BTreeMap<String, usize> = Default::default();
    for line in baseline_text.lines() {
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let (path, count) = line.split_once('\t').expect("底账是一行两格");
        baseline.insert(
            path.to_string(),
            count.trim().parse().expect("条数是一个数"),
        );
    }

    let mut seen = 0usize;
    let mut offenders = Vec::new();
    for (path, text) in production_texts(&root) {
        let relative = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        if relative.contains("/tests/") {
            continue;
        }
        // 只数**生产**那一半：整份文件里 `#[cfg(test)]` 的夹具用这种写法是在扮演
        // 盘上那份记录，不是又写了一份。此前按整份文件数，一条夹具就把某个文件的
        // 存量顶上去，红的是门自己，不是树。
        // 注释留着：处置写在那条构造的上一行。数的时候跳过注释行本身，
        // 注释里举例子的 `format!` 不是在写一份记录。
        let mut here = 0usize;
        let mut marker = "";
        for line in production_lines_keeping_comments(&text) {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                if trimmed.contains("state-file:") {
                    marker = trimmed;
                }
                continue;
            }
            if !trimmed.is_empty() {
                if dynamic_record_name(line)
                    && !disposed_state_file(marker)
                    && !disposed_state_file(line)
                {
                    here += 1;
                }
                marker = "";
            }
        }
        seen += here;
        match baseline.get(&relative) {
            Some(allowed) if here > *allowed => {
                offenders.push(format!("{relative}: {here} 条，底账 {allowed} 条"))
            }
            Some(allowed) if here < *allowed => {
                // 棘轮的另一半：答掉一条就把底账改小或删掉那一行，否则底账成了一块
                // 只涨不落的免检牌，下一个人从它上面读到的存量比树上多。
                offenders.push(format!(
                    "{relative}: 只剩 {here} 条，底账 {allowed} 条——把这一行改小或删掉"
                ))
            }
            Some(_) => {}
            None if here > 0 => {
                offenders.push(format!("{relative}: 新出现 {here} 条，底账里没有这个文件"))
            }
            None => {}
        }
    }
    for path in baseline.keys() {
        if !root.join(path).exists() {
            offenders.push(format!("{path}: 底账点名的文件不在树里（底账过期了）"));
        }
    }
    assert!(
        offenders.is_empty(),
        "盘上记录的存量涨了或有新文件带着它进来：\n  {}\n（生产代码合计 {seen} 条）",
        offenders.join("\n  ")
    );
}
