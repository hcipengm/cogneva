//! 画一份固定种子子集并存档：`cogneva eval-case-subset`。
//!
//! 为什么是独立子命令，而不是 `eval-table` 的一个开关：这条命令只读题面数据、只写一份
//! id 清单，一个模型、一个容器都不碰。挂到 `eval-table` 上，就得为「哪些参数在哪种模式下
//! 必填」另写一份说明，而那份说明每加一个开关都要改一次。分出来之后，两个命令各自收
//! 自己那套参数，读的人不用先判断自己在哪个模式里。

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use cog_eval::{draw_case_subset, render_case_subset, RunConfig};

use crate::eval_table::{build_benchmark, operands, ToolArm, Wiring, BENCHMARKS};

const USAGE: &str = "\
usage: cogneva eval-case-subset --size <n> --seed <n> --out <file> [--data-root <dir>]
                                [--benchmarks hle,swe-bench-pro,toolathlon]

  Draws a fixed-seed subset of cases and archives it. Runs read that archive; they never
  redraw, so the same subset switch keeps running the same cases after the data or this
  command changes.

  --size       how many cases to draw from each benchmark (one number for all of them)
  --seed       the draw seed. The drawn ids are a pure function of the seed and the case ids,
               so the archive can be recomputed -- but it is still read from the file, never
               redrawn at run time.
  --out        where to write the archive (a text file of `benchmark<TAB>case id` lines)
  --data-root  the fetch script's --dest; falls back to COG_EVAL_DATA_ROOT
  --benchmarks comma-separated; default all three, in the canonical order";

/// `eval-case-subset` 的参数。
#[derive(Debug, Clone)]
pub struct CaseSubsetArgs {
    pub data_root: Option<PathBuf>,
    pub benchmarks: Vec<&'static str>,
    pub size: usize,
    pub seed: u64,
    pub out: PathBuf,
}

fn next_arg<'a>(args: &'a [String], at: usize, name: &str) -> Result<&'a str, String> {
    args.get(at + 1)
        .map(String::as_str)
        .ok_or_else(|| format!("{name} needs a value"))
}

impl CaseSubsetArgs {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let mut data_root = None;
        let mut size = None;
        let mut seed = None;
        let mut out = None;
        let mut benchmarks: Option<Vec<&'static str>> = None;

        let mut i = 0;
        while i < args.len() {
            let flag = args[i].as_str();
            match flag {
                "--data-root" => {
                    data_root = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--out" => {
                    out = Some(PathBuf::from(next_arg(args, i, flag)?));
                    i += 2;
                }
                "--size" => {
                    let raw = next_arg(args, i, flag)?;
                    let n = raw
                        .parse::<usize>()
                        .map_err(|e| format!("--size: `{raw}` is not a count: {e}"))?;
                    if n == 0 {
                        return Err(
                            "--size 0 draws nothing, and an empty archive is not a subset".into(),
                        );
                    }
                    size = Some(n);
                    i += 2;
                }
                "--seed" => {
                    let raw = next_arg(args, i, flag)?;
                    seed = Some(
                        raw.parse::<u64>()
                            .map_err(|e| format!("--seed: `{raw}` is not a seed: {e}"))?,
                    );
                    i += 2;
                }
                "--benchmarks" => {
                    let raw = next_arg(args, i, flag)?;
                    let mut names: Vec<&str> = Vec::new();
                    for item in raw.split(',') {
                        let item = item.trim();
                        if item.is_empty() {
                            return Err(format!("empty item in list `{raw}`"));
                        }
                        if !BENCHMARKS.contains(&item) {
                            return Err(format!(
                                "unknown benchmark `{item}` (have: {})",
                                BENCHMARKS.join(", ")
                            ));
                        }
                        if names.contains(&item) {
                            return Err(format!("`{item}` is listed twice"));
                        }
                        names.push(item);
                    }
                    // 抽的顺序不影响抽到哪些（键取在 id 上），但仍按规范顺序写：两份参数
                    // 只是写得先后不同，产出的存档要逐字节相同。
                    benchmarks = Some(
                        BENCHMARKS
                            .iter()
                            .copied()
                            .filter(|k| names.contains(k))
                            .collect(),
                    );
                    i += 2;
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        Ok(Self {
            data_root,
            benchmarks: benchmarks.unwrap_or_else(|| BENCHMARKS.to_vec()),
            size: size
                .ok_or("--size <n> is required: how many cases to draw from each benchmark")?,
            seed: seed.ok_or("--seed <n> is required: an unseeded draw cannot be recomputed")?,
            out: out
                .ok_or("--out <file> is required: the ids are archived, not printed and lost")?,
        })
    }
}

/// 子命令主入口。
pub fn run_from_args() -> Result<(), Box<dyn std::error::Error>> {
    draw_and_archive()?;
    Ok(())
}

fn draw_and_archive() -> Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    let args =
        CaseSubsetArgs::parse(&operands(&argv)).map_err(|e| anyhow::anyhow!("{e}\n\n{USAGE}"))?;

    let data_root = match &args.data_root {
        Some(root) => root.clone(),
        None => RunConfig::from_env()?.data_root,
    };
    if !data_root.is_dir() {
        bail!("the data root {} is not a directory", data_root.display());
    }

    // 一个基准一个基准地读、抽、记。名单与取数适配器都取自跑表用的那套构造函数：
    // 「这个基准叫什么、题从哪儿读」只有一份权威，画子集不再抄一份名单。
    let mut selections: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();
    for name in &args.benchmarks {
        let bench = build_benchmark(name, None, &Wiring::default(), ToolArm::None)?;
        let cases = bench
            .cases
            .cases(&data_root)
            .with_context(|| format!("cannot read the cases of `{}`", bench.name))?;
        totals.insert(bench.name.clone(), cases.len());
        let ids = draw_case_subset(&cases, &bench.name, args.size, args.seed)?;
        selections.insert(bench.name.clone(), ids);
    }

    let text = render_case_subset(&selections, args.seed, args.size);
    std::fs::write(&args.out, &text)
        .with_context(|| format!("cannot write the subset archive {}", args.out.display()))?;

    // 画的时候就把「从多少里抽了多少」写出来：单题成本要靠它去估全量，而题数只在这里
    // 是已知的。
    println!("archive\t{}", args.out.display());
    println!("seed\t{}", args.seed);
    println!("size\t{}", args.size);
    for (benchmark, ids) in &selections {
        println!(
            "drawn\t{benchmark}\t{}\tof\t{}",
            ids.len(),
            totals.get(benchmark).copied().unwrap_or_default()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(raw: &[&str]) -> Result<CaseSubsetArgs, String> {
        parse_args(raw.iter().map(|s| s.to_string()).collect())
    }

    fn parse_args(raw: Vec<String>) -> Result<CaseSubsetArgs, String> {
        CaseSubsetArgs::parse(&raw)
    }

    fn minimal() -> Vec<String> {
        [
            "--size",
            "40",
            "--seed",
            "20261011",
            "--out",
            "/tmp/subset.tsv",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn the_three_required_arguments_are_required() {
        assert!(args(&[]).unwrap_err().contains("--size"));
        assert!(args(&["--size", "40", "--seed", "1"])
            .unwrap_err()
            .contains("--out"));
        assert!(args(&["--size", "40", "--out", "/tmp/x"])
            .unwrap_err()
            .contains("--seed"));
        // 抽 0 道不是一个比 40 道小的子集，是一份空存档。
        assert!(args(&["--size", "0", "--seed", "1", "--out", "/tmp/x"])
            .unwrap_err()
            .contains("--size 0"));
        assert!(args(&["--size", "forty", "--seed", "1", "--out", "/tmp/x"])
            .unwrap_err()
            .contains("not a count"));
    }

    #[test]
    fn the_default_is_every_benchmark_and_the_order_is_canonical() {
        let parsed = parse_args(minimal()).unwrap();
        assert_eq!(parsed.benchmarks, BENCHMARKS.to_vec());

        // 命令行写的先后不同，选出来的顺序要一样：存档会被 diff，两份参数不该产出两种字节。
        let mut other = minimal();
        other.extend(
            ["--benchmarks", "toolathlon,hle"]
                .iter()
                .map(|s| s.to_string()),
        );
        let mut reversed = vec!["--benchmarks".to_string(), "hle,toolathlon".to_string()];
        reversed.extend(minimal());
        assert_eq!(
            parse_args(other).unwrap().benchmarks,
            vec!["hle", "toolathlon"]
        );
        assert_eq!(
            parse_args(reversed).unwrap().benchmarks,
            vec!["hle", "toolathlon"]
        );
        assert!(args(&[
            "--size",
            "1",
            "--seed",
            "1",
            "--out",
            "/tmp/x",
            "--benchmarks",
            "gaia"
        ])
        .unwrap_err()
        .contains("gaia"));
    }
}
