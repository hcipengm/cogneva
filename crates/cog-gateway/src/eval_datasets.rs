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

use cog_core::EvalDatasetInfo;

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
pub struct ListedDatasets {
    pub datasets: Vec<EvalDatasetInfo>,
    pub present: bool,
}

/// 列举目录里认得出的数据集。
///
/// 认的是扩展名（jsonl / yaml / yml），用例数只在 jsonl 上数——别的形状没有一条
/// 「一行一个用例」的约定，硬数出来的数字会是一个看着像读数、其实没有读者的数。
pub fn list_datasets(dir: &Path) -> ListedDatasets {
    if !dir.is_dir() {
        return ListedDatasets {
            datasets: Vec::new(),
            present: false,
        };
    }
    let mut datasets = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
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
            let case_count = if ext == "jsonl" {
                std::fs::read_to_string(&path)
                    .map(|c| c.lines().filter(|l| !l.trim().is_empty()).count())
                    .unwrap_or(0)
            } else {
                0
            };
            datasets.push(EvalDatasetInfo {
                name,
                path: path.display().to_string(),
                case_count,
                tags: vec![ext.into()],
            });
        }
    }
    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    ListedDatasets {
        datasets,
        present: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 不存在的目录与空目录必须答得不一样：这条判据就是这条读面存在的理由。
    #[test]
    fn a_directory_that_is_not_there_is_not_an_empty_directory() {
        let scratch =
            std::env::temp_dir().join(format!("cog-eval-datasets-absent-{}", uuid::Uuid::new_v4()));
        let missing = list_datasets(&scratch);
        assert!(!missing.present, "目录不在时 present 必须是假");

        std::fs::create_dir_all(&scratch).unwrap();
        let empty = list_datasets(&scratch);
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

        let listed = list_datasets(&scratch);
        assert!(listed.present);
        let names: Vec<&str> = listed.datasets.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["humaneval", "suite"],
            "只认三种扩展名，且按名字排序"
        );
        assert_eq!(listed.datasets[0].case_count, 2, "空行不算用例");
        assert_eq!(listed.datasets[1].case_count, 0, "yaml 不数行");
        std::fs::remove_dir_all(&scratch).ok();
    }
}
