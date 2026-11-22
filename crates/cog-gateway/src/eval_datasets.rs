//! 评测数据集的列举。
//!
//! 读面 `/api/v1/eval/datasets` 列的是 `{data_dir}/eval/datasets` 下的 jsonl / yaml
//! 文件，而**没有任何一处写这个目录**：平台自己的数据集走对象层，这个目录等着部署
//! 往里放，chart 也没有供给它。于是这条读面在默认部署上恒为空，而空目录与不存在的
//! 目录在过去回到调用方的是同一个形状——一个空的 `datasets` 数组。界面上那句「还没有
//! 数据集」既是真话（没人放），也是假话（读的那个目录根本不在），看的人分不出自己
//! 该去放文件还是该去改部署。
//!
//! 所以这里把两件事分开答：目录是哪个、它在不在。空列表只在目录**在**的时候才代表
//! 「没有数据集」。

use std::path::{Path, PathBuf};

use cog_core::{EvalDatasetInfo, SFError, SFResult};

/// 评测产物在数据根下的那一级：数据集与报告都挂在它下面。
///
/// 两个读面各写一遍 `join("eval")` 时，改一处只会让其中一条从此读另一个目录，
/// 而那边返回的空列表与「这个部署没有评测产物」同形。
pub const EVAL_SUBDIR: &str = "eval";

/// 数据集目录：`{data_dir}/eval/datasets`。
pub fn datasets_dir(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(EVAL_SUBDIR).join("datasets")
}

/// 一次列举的结果：目录在不在，以及目录里认得出的数据集。
#[derive(Debug)]
pub struct ListedDatasets {
    pub datasets: Vec<EvalDatasetInfo>,
    pub present: bool,
}

/// 文件系统说「这个路径上不可能有数据集」：东西不在，或者某一级是普通文件。
/// 这是确定的「没有」。别的错——符号链接成环、目录搜不了——是「看不到」，
/// 折成「没有」就是把一次没量成报成 0。
fn path_is_absent(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

/// 列举目录里认得出的数据集。
///
/// 认的是扩展名（jsonl / yaml / yml），用例数只在 jsonl 上数——别的形状没有一条
/// 「一行一个用例」的约定，硬数出来的数字会是一个看着像读数、其实没有读者的数。
///
/// 目录看不到、条目读不全时返回错误：「目录里没有数据集」只有真的把目录看完了
/// 才配这么说。
pub fn list_datasets(dir: &Path) -> SFResult<ListedDatasets> {
    let absent = ListedDatasets {
        datasets: Vec::new(),
        present: false,
    };
    match std::fs::metadata(dir) {
        Ok(md) if md.is_dir() => {}
        Ok(_) => return Ok(absent),
        Err(e) if path_is_absent(&e) => return Ok(absent),
        Err(e) => return Err(SFError::IO(format!("looking at {}: {}", dir.display(), e))),
    }
    let entries = std::fs::read_dir(dir)
        .map_err(|e| SFError::IO(format!("reading {}: {}", dir.display(), e)))?;
    let mut datasets = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| SFError::IO(format!("reading {}: {}", dir.display(), e)))?;
        let path = entry.path();
        let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
            continue;
        };
        if !matches!(ext, "jsonl" | "yaml" | "yml") {
            continue;
        }
        let name = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        // 数不出来就说数不出来：yaml 没有一行一个用例的约定，读不出内容的
        // 文件更没有。写 0 会让「这个形状不数」与「这个数据集是空的」在屏上
        // 同形——前者该换个格式读，后者该往里写用例。
        let case_count = if ext == "jsonl" {
            std::fs::read_to_string(&path)
                .ok()
                .map(|c| c.lines().filter(|l| !l.trim().is_empty()).count())
        } else {
            None
        };
        datasets.push(EvalDatasetInfo {
            name,
            path: path.display().to_string(),
            case_count,
            tags: vec![ext.into()],
        });
    }
    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ListedDatasets {
        datasets,
        present: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 不存在的目录与空目录必须答得不一样：这条判据就是这条读面存在的理由。
    #[test]
    fn a_directory_that_is_not_there_is_not_an_empty_directory() {
        let scratch =
            std::env::temp_dir().join(format!("cog-eval-datasets-absent-{}", uuid::Uuid::new_v4()));
        let missing = list_datasets(&scratch).expect("the directory can be looked at");
        assert!(!missing.present, "目录不在时 present 必须是假");

        std::fs::create_dir_all(&scratch).unwrap();
        let empty = list_datasets(&scratch).expect("the directory can be looked at");
        assert!(empty.present, "目录在、只是没有文件，present 必须为真");
        assert!(empty.datasets.is_empty());
        std::fs::remove_dir_all(&scratch).ok();
    }

    #[test]
    fn only_the_three_shapes_are_listed_and_jsonl_lines_are_counted() {
        let scratch =
            std::env::temp_dir().join(format!("cog-eval-datasets-list-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&scratch).unwrap();
        std::fs::write(scratch.join("humaneval.jsonl"), "{\"a\":1}\n\n{\"a\":2}\n").unwrap();
        std::fs::write(scratch.join("suite.yaml"), "cases: []\n").unwrap();
        std::fs::write(scratch.join("notes.md"), "不是数据集\n").unwrap();

        let listed = list_datasets(&scratch).expect("the directory can be looked at");
        assert!(listed.present);
        let names: Vec<&str> = listed.datasets.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["humaneval", "suite"],
            "只认三种扩展名，且按名字排序"
        );
        assert_eq!(listed.datasets[0].case_count, Some(2), "空行不算用例");
        assert_eq!(
            listed.datasets[1].case_count, None,
            "yaml 不数行：这里是「没数」，不是「0 个用例」"
        );
        std::fs::remove_dir_all(&scratch).ok();
    }

    /// 读不出内容的 jsonl 也不许报成 0 个用例：那与一个空文件在屏上同形。
    ///
    /// 「读不出来」用同名目录造：`x.jsonl/` 名字符合约定、`read_to_string` 必失败
    /// （EISDIR），而这件事在 root 下也一样成立——权限位造出来的现场在 root 上
    /// 不成立，那样的测试只有跑到非 root 身份时才真的在测。
    #[test]
    fn a_jsonl_this_process_cannot_read_is_unmeasured_not_empty() {
        let scratch = std::env::temp_dir().join(format!(
            "cog-eval-datasets-unreadable-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(scratch.join("secret.jsonl")).unwrap();
        std::fs::write(scratch.join("empty.jsonl"), "").unwrap();

        let listed = list_datasets(&scratch).expect("the directory can be looked at");
        let unreadable = listed
            .datasets
            .iter()
            .find(|d| d.name == "secret")
            .expect("名字符合约定，就算读不出来也该在名单里");
        assert_eq!(unreadable.case_count, None, "读不出来不是 0 个用例");
        let empty = listed
            .datasets
            .iter()
            .find(|d| d.name == "empty")
            .expect("空文件是一个真的空数据集");
        assert_eq!(
            empty.case_count,
            Some(0),
            "读得出来、里面没有行，这才是量到的 0"
        );

        std::fs::remove_dir_all(&scratch).ok();
    }

    /// 目录「看不到」与「不在」必须答得不一样：前者一个数据集都没列出来，
    /// 但那是没量成，不是量到 0。用自指符号链接造现场——`metadata` 追它
    /// 得到 ELOOP，与权限位不同，这在 root 下一样成立。
    #[test]
    fn a_directory_that_cannot_be_looked_at_is_not_an_empty_one() {
        let scratch = std::env::temp_dir().join(format!(
            "cog-eval-datasets-unlookable-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&scratch).unwrap();
        std::os::unix::fs::symlink("loop", scratch.join("loop")).unwrap();

        let err = list_datasets(&scratch.join("loop"))
            .expect_err("看不到的目录不许答成「目录里没有数据集」");
        assert!(
            err.to_string().contains("loop"),
            "错误要点名看的是哪个路径，got: {err}"
        );

        std::fs::remove_dir_all(&scratch).ok();
    }
}
