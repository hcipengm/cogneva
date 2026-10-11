//! 固定种子子集：快速迭代跑的那一份题，以及「为什么是这一份」。
//!
//! 全量三基准合计 3,734 道题（SWE-Pro 731 / Toolathlon 503 / HLE 2,500），跑一轮的成本
//! 决定了还能不能天天迭代。所以迭代用一份抽出来的子集，全量留给要报的那张表。
//!
//! **抽取只发生一次**：抽完把 id 写进存档，之后每次跑都读存档、不重抽。现抽的坏处不是
//! 「慢」，是同一个开关在不同时候跑的不是同一批题——题面数据换一版、抽取实现改一行，
//! 两次迭代的分差就不再可比，而这件事在命令行上一点看不出来。存档是这份子集的**身份**，
//! 所以它按文件哈希随表出去：钉到文件，不钉到路径。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Context;
use sha2::{Digest, Sha256};

use crate::dataset::EvalCase;

/// 存档是哪一份：路径与内容哈希。哈希随表写出去，读的人据此判断两次跑的是不是同一批题。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsetSource {
    pub path: PathBuf,
    pub sha256: String,
}

/// 每个基准跑哪些题。抽的那一次定下来，之后只读。
#[derive(Debug, Clone)]
pub struct CaseSubset {
    by_benchmark: BTreeMap<String, BTreeSet<String>>,
    source: SubsetSource,
}

impl CaseSubset {
    /// 读一份存档。存档是纯文本：`#` 开头是注记，其余每行一个 `基准 <TAB> 题 id`。
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("cannot read the subset archive {}", path.display()))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| anyhow::anyhow!("{} is not utf-8: {e}", path.display()))?;
        let by_benchmark = Self::parse(text)
            .with_context(|| format!("in the subset archive {}", path.display()))?;
        Ok(Self {
            by_benchmark,
            source: SubsetSource {
                path: path.to_path_buf(),
                sha256: hex::encode(Sha256::digest(&bytes)),
            },
        })
    }

    /// 解析存档的正文。逐行的形状错误都要报出来：一份读得下去但少了几行的存档，
    /// 会让这一列的分母悄悄变小，而表里的分照样是绿的。
    pub fn parse(text: &str) -> anyhow::Result<BTreeMap<String, BTreeSet<String>>> {
        let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (n, raw) in text.lines().enumerate() {
            let line = raw.trim_end_matches('\r');
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let at = n + 1;
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != 2 {
                anyhow::bail!(
                    "line {at}: expected `benchmark<TAB>case id`, found {} field(s) in {line:?}",
                    fields.len()
                );
            }
            let (benchmark, id) = (fields[0], fields[1]);
            if benchmark.is_empty() || id.is_empty() {
                anyhow::bail!("line {at}: an empty benchmark or case id in {line:?}");
            }
            if !out
                .entry(benchmark.to_string())
                .or_default()
                .insert(id.to_string())
            {
                anyhow::bail!(
                    "line {at}: `{id}` is listed twice for `{benchmark}` -- one case, two lines, \
                     so the archived count would not be the count that runs"
                );
            }
        }
        if out.is_empty() {
            anyhow::bail!("it names no cases");
        }
        Ok(out)
    }

    /// 存档覆盖的基准名，按字典序。
    pub fn benchmarks(&self) -> impl Iterator<Item = &str> {
        self.by_benchmark.keys().map(String::as_str)
    }

    /// 这个基准抽了多少道。`None` ＝存档里根本没有它。
    pub fn count(&self, benchmark: &str) -> Option<usize> {
        self.by_benchmark.get(benchmark).map(BTreeSet::len)
    }

    pub fn source(&self) -> &SubsetSource {
        &self.source
    }

    /// 从数据里挑出存档点名的那几道题，按数据自己的顺序交回。
    ///
    /// 三类错都要当场报，不许静默缩小范围：存档没覆盖这个基准（那它就成了一次全量跑，
    /// 与旁边几列并排的不是同一件事）、存档点名的题这份数据里没有（id 是上一版留下的，
    /// 该重抽或该换数据，而不是少跑一道）、以及数据把同一个 id 发两次（一个 id 选不出
    /// 到底是哪道题）。
    pub fn select(&self, benchmark: &str, cases: Vec<EvalCase>) -> anyhow::Result<Vec<EvalCase>> {
        let Some(wanted) = self.by_benchmark.get(benchmark) else {
            anyhow::bail!(
                "the subset archive names no cases for `{benchmark}` (it covers: {}); a benchmark \
                 missing from the archive would silently run in full beside columns that run a \
                 subset",
                self.benchmarks().collect::<Vec<_>>().join(", ")
            );
        };

        let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
        for case in &cases {
            if let Some(first) = seen.insert(case.id.as_str(), case.name.as_str()) {
                anyhow::bail!(
                    "the case source hands out the id `{}` twice ({first:?} and {:?}): a subset \
                     selected by id cannot say which of the two it meant",
                    case.id,
                    case.name
                );
            }
        }
        let mut missing: Vec<String> = wanted
            .iter()
            .filter(|id| !seen.contains_key(id.as_str()))
            .cloned()
            .collect();
        if !missing.is_empty() {
            missing.sort();
            anyhow::bail!(
                "the subset archive names {} case(s) this data root does not have: {}. The ids \
                 were drawn against another version of the data -- redraw the subset, or point \
                 --data-root at the data the archive belongs to",
                missing.len(),
                missing
                    .iter()
                    .map(|id| format!("`{id}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        drop(seen);

        let picked: Vec<EvalCase> = cases
            .into_iter()
            .filter(|c| wanted.contains(&c.id))
            .collect();
        // `wanted` 非空且逐个都在数据里，所以这里不会是空的；留一道闸是为了让「选了却
        // 一道都没选中」永远不变成一次分母为 0 的跑。
        if picked.is_empty() {
            anyhow::bail!(
                "the subset archive selects no case of `{benchmark}`, and a column with no case \
                 reads as a score of zero"
            );
        }
        Ok(picked)
    }
}

/// 按固定种子抽 `size` 道题。
///
/// 抽法要能被任何人在任何机器上复算：键取 `种子 <TAB> 基准 <TAB> 题 id` 的 sha256，取键
/// 最小的 `size` 个。不用随机数生成器，是因为它把「抽到哪些」押在实现的内部状态上；也不
/// 用「按文件顺序取前 N 个」，那样题面数据一重排，同一份「固定种子子集」就换了题。
pub fn draw(
    cases: &[EvalCase],
    benchmark: &str,
    size: usize,
    seed: u64,
) -> anyhow::Result<Vec<String>> {
    if cases.is_empty() {
        anyhow::bail!(
            "`{benchmark}` has no cases at this data root, so there is nothing to draw from"
        );
    }
    if size == 0 {
        anyhow::bail!(
            "a subset of 0 cases reads as `not one case passed` rather than `nothing ran`; \
             there is no run to make out of it"
        );
    }
    if size >= cases.len() {
        anyhow::bail!(
            "cannot draw {size} case(s) out of {}: that subset would be the whole benchmark, and \
             running it through the subset switch would make the table's denominator look like a \
             subset while it is the full set",
            cases.len()
        );
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for case in cases {
        if !seen.insert(case.id.as_str()) {
            anyhow::bail!(
                "`{benchmark}` hands out the id `{}` twice, so a draw off it cannot be a set of \
                 cases",
                case.id
            );
        }
    }

    let mut keyed: Vec<([u8; 32], &str)> = cases
        .iter()
        .map(|case| (draw_key(seed, benchmark, &case.id), case.id.as_str()))
        .collect();
    // 键相等时按 id 收尾：排序键不许留「相等就听天由命」的余地，否则同一份种子在两次跑
    // 里可以抽出不同的题（哈希相等虽然近乎不可能，但规则要写成全序）。
    keyed.sort();
    let mut ids: Vec<String> = keyed
        .into_iter()
        .take(size)
        .map(|(_, id)| id.to_string())
        .collect();
    ids.sort();
    Ok(ids)
}

fn draw_key(seed: u64, benchmark: &str, id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_be_bytes());
    hasher.update(b"\t");
    hasher.update(benchmark.as_bytes());
    hasher.update(b"\t");
    hasher.update(id.as_bytes());
    hasher.finalize().into()
}

/// 把抽好的 id 渲染成存档。
///
/// 存的是抽取这一步的输入（种子、每个基准抽多少）：题 id 本身读得出来，但「按哪个种子抽的」
/// 写不进正文，少了它，别人复算不出同一份子集。
pub fn render(selections: &BTreeMap<String, Vec<String>>, seed: u64, size: usize) -> String {
    let mut out = String::from(
        "# cogneva eval case subset -- drawn once and archived; a run reads this file, it never redraws\n",
    );
    out.push_str(&format!("# seed\t{seed}\n"));
    out.push_str(&format!("# size\t{size}\n"));
    out.push_str("# one line per case: benchmark<TAB>case id\n");
    for (benchmark, ids) in selections {
        for id in ids {
            out.push_str(&format!("{benchmark}\t{id}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(id: &str) -> EvalCase {
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!("question?"),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata: Default::default(),
        }
    }

    fn cases(n: usize) -> Vec<EvalCase> {
        (0..n).map(|i| case(&format!("c{i:03}"))).collect()
    }

    fn subset(entries: &[(&str, &[&str])]) -> CaseSubset {
        let mut text = String::from("# drawn by hand for this test\n");
        for (benchmark, ids) in entries {
            for id in *ids {
                text.push_str(&format!("{benchmark}\t{id}\n"));
            }
        }
        let by_benchmark = CaseSubset::parse(&text).unwrap();
        CaseSubset {
            by_benchmark,
            source: SubsetSource {
                path: PathBuf::from("/tmp/not-read"),
                sha256: "0".repeat(64),
            },
        }
    }

    #[test]
    fn a_fixed_seed_draws_the_same_cases_every_time() {
        let all = cases(200);
        let first = draw(&all, "hle", 20, 20261011).unwrap();
        assert_eq!(first.len(), 20);
        assert_eq!(first, draw(&all, "hle", 20, 20261011).unwrap());
        // 换种子换一份题——不换的话，「固定种子」这个说法就没有内容。
        assert_ne!(first, draw(&all, "hle", 20, 7).unwrap());
        // 换基准也换一份：同一个种子下两个基准各抽各的，不是同一批 id 分给两列。
        assert_ne!(first, draw(&all, "toolathlon", 20, 20261011).unwrap());
    }

    #[test]
    fn the_draw_does_not_depend_on_the_order_the_cases_arrive_in() {
        // 题面数据重排一次，同一份种子必须抽出同一批题：抽取键取在 id 上，不取在位置上。
        let all = cases(200);
        let mut reversed = all.clone();
        reversed.reverse();
        assert_eq!(
            draw(&all, "hle", 20, 20261011).unwrap(),
            draw(&reversed, "hle", 20, 20261011).unwrap()
        );
    }

    #[test]
    fn a_subset_is_strictly_smaller_than_the_benchmark() {
        let all = cases(10);
        assert!(draw(&all, "hle", 10, 1)
            .unwrap_err()
            .to_string()
            .contains("whole benchmark"));
        assert!(draw(&all, "hle", 0, 1)
            .unwrap_err()
            .to_string()
            .contains("0 cases"));
        assert!(draw(&[], "hle", 1, 1)
            .unwrap_err()
            .to_string()
            .contains("nothing to draw"));
    }

    #[test]
    fn selecting_keeps_the_data_order_and_drops_the_rest() {
        let all = cases(10);
        let picked = subset(&[("hle", &["c003", "c007"])])
            .select("hle", all.clone())
            .unwrap();
        let ids: Vec<&str> = picked.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["c003", "c007"]);
    }

    #[test]
    fn a_benchmark_the_archive_does_not_cover_is_an_error_not_a_full_run() {
        let err = subset(&[("hle", &["c003"])])
            .select("toolathlon", cases(5))
            .unwrap_err()
            .to_string();
        assert!(err.contains("toolathlon"), "{err}");
        assert!(err.contains("hle"), "要点出存档里有什么：{err}");
    }

    #[test]
    fn an_archived_id_this_data_root_lacks_is_an_error_that_names_it() {
        let err = subset(&[("hle", &["c003", "gone"])])
            .select("hle", cases(5))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`gone`"), "{err}");
        assert!(!err.contains("`c003`"), "存在的 id 不许一起报：{err}");
    }

    #[test]
    fn a_source_that_hands_out_one_id_twice_is_an_error() {
        let mut all = cases(5);
        all.push(case("c003"));
        let err = subset(&[("hle", &["c003"])])
            .select("hle", all)
            .unwrap_err()
            .to_string();
        assert!(err.contains("c003"), "{err}");
    }

    #[test]
    fn a_repeated_id_in_the_archive_is_an_error() {
        let err = CaseSubset::parse("hle\tc1\nhle\tc1\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("twice"), "{err}");

        // 空存档与坏形状都不许读成「一份很小的子集」。
        assert!(CaseSubset::parse("# nothing\n\n")
            .unwrap_err()
            .to_string()
            .contains("no cases"));
        assert!(CaseSubset::parse("hle\n")
            .unwrap_err()
            .to_string()
            .contains("field"));
        assert!(CaseSubset::parse("hle\t\n")
            .unwrap_err()
            .to_string()
            .contains("empty"));
    }

    #[test]
    fn the_archive_round_trips_through_its_own_rendering() {
        let all = cases(50);
        let hle = draw(&all, "hle", 5, 3).unwrap();
        let toolathlon = draw(&all, "toolathlon", 4, 3).unwrap();
        let mut selections = BTreeMap::new();
        selections.insert("hle".to_string(), hle.clone());
        selections.insert("toolathlon".to_string(), toolathlon.clone());

        let text = render(&selections, 3, 5);
        assert!(text.contains("# seed\t3\n"), "{text}");
        let parsed = CaseSubset::parse(&text).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed
                .get("hle")
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            hle
        );
        assert_eq!(parsed.get("toolathlon").unwrap().len(), 4);
    }

    #[test]
    fn loading_records_the_files_own_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seed3.tsv");
        std::fs::write(
            &path,
            render(&BTreeMap::from([("hle".into(), vec!["c1".into()])]), 3, 1),
        )
        .unwrap();
        let loaded = CaseSubset::load(&path).unwrap();
        assert_eq!(loaded.count("hle"), Some(1));
        assert_eq!(loaded.count("toolathlon"), None);
        assert_eq!(loaded.source().path, path);
        assert_eq!(loaded.source().sha256.len(), 64);

        // 两份内容不同的存档不能算出同一个哈希：表头那一行是这条的读者。
        let other = dir.path().join("seed4.tsv");
        std::fs::write(
            &other,
            render(&BTreeMap::from([("hle".into(), vec!["c2".into()])]), 4, 1),
        )
        .unwrap();
        assert_ne!(
            CaseSubset::load(&other).unwrap().source().sha256,
            loaded.source().sha256
        );
    }
}
