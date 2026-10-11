//! SWE-bench Pro 的四件适配器：数据、环境、工具包、判分。
//!
//! 这一列与 HLE 的关键分野：它**有环境**——每个 instance 一个容器镜像，外壳在那棵树里
//! 改代码；工具面是「bash + file-edit」；判分是**确定性脚本**（跑 `fail_to_pass` /
//! `pass_to_pass`），不依赖任何生成式上游。
//!
//! 所以另两列在裁判缺席时报错，这一列不会；它的阻塞在**容器**上。没有容器后端时这一列
//! 同样报错、不是判错——「没跑成」与「跑错了」在表里都是 0，却是两件事，运行器要把前者
//! 记成 `errored`。判分规则的实现只要一个「哪些测试通过」的集合，那个集合怎么来的（起
//! 容器、跑官方测试命令、解析测试框架输出）由后端提供，评测台声明这个后端、不实现它。

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::Tool;
use serde::Deserialize;

use crate::bench::{CaseJudge, CaseSource, EnvProvider, ToolFactory, Toolkit, Verdict};
use crate::dataset::EvalCase;
use crate::scaffold::{AgentOutput, CaseEnv, ToolSet};

/// 取数脚本给 SWE-bench Pro 产出的文本形式在数据根里的相对路径（parquet 同名换扩展名）。
pub const SWE_PRO_JSONL: &str = "swe-bench-pro/test-00000-of-00001.jsonl";

/// SWE-bench Pro 工具名的钉死值。工具集由**基准**定、不由方法定。
pub const SWE_PRO_TOOL_BASH: &str = "bash";
pub const SWE_PRO_TOOL_FILE_EDIT: &str = "file_edit";

/// 数据适配器：读文本形式的 parquet 行。
///
/// 给外壳的只有**任务陈述**（`problem_statement` / `requirements` / `interface` 与仓库、
/// 基线 commit）：金标 `patch` 和测试清单是**判分方的**，放进输入就是把答案提前交出去。
/// 金标 `patch` 落在 `expected_output` 上，供「参考解全过」那一侧的双向验收读。
pub struct SweProCaseSource;

#[derive(Debug, Deserialize)]
struct SweProRow {
    instance_id: String,
    repo: String,
    base_commit: String,
    /// 金标补丁（unified diff）。外部字段名，勿改。
    patch: String,
    problem_statement: String,
    #[serde(default)]
    requirements: String,
    #[serde(default)]
    interface: String,
    #[serde(default)]
    repo_language: String,
    /// Python 字面量列表的**字符串**（不是 JSON 数组），见 [`parse_test_list`]。
    fail_to_pass: String,
    pass_to_pass: String,
    #[serde(default)]
    test_patch: String,
    #[serde(default)]
    before_repo_set_cmd: String,
    #[serde(default)]
    selected_test_files_to_run: String,
    #[serde(default)]
    dockerhub_tag: String,
}

impl SweProCaseSource {
    fn row_to_case(row: SweProRow) -> EvalCase {
        let mut metadata = HashMap::new();
        metadata.insert("instance_id".into(), row.instance_id.clone());
        metadata.insert("repo".into(), row.repo.clone());
        metadata.insert("base_commit".into(), row.base_commit.clone());
        metadata.insert("repo_language".into(), row.repo_language.clone());
        if !row.dockerhub_tag.is_empty() {
            metadata.insert("dockerhub_tag".into(), row.dockerhub_tag.clone());
        }
        if !row.before_repo_set_cmd.is_empty() {
            metadata.insert(
                "before_repo_set_cmd".into(),
                row.before_repo_set_cmd.clone(),
            );
        }
        if !row.selected_test_files_to_run.is_empty() {
            metadata.insert(
                "selected_test_files_to_run".into(),
                row.selected_test_files_to_run.clone(),
            );
        }
        if !row.test_patch.is_empty() {
            metadata.insert("test_patch".into(), row.test_patch.clone());
        }
        // 判分清单**原样**带走，由判分器在那一刻解析：这里替它解析一遍，等于让「解析失败」
        // 在数据适配这一步就变成一条读不进来的 case，而不是判分时的一条判不了。
        metadata.insert("fail_to_pass".into(), row.fail_to_pass);
        metadata.insert("pass_to_pass".into(), row.pass_to_pass);

        let mut tags = vec!["swe-bench-pro".to_string(), row.repo.clone()];
        if !row.repo_language.is_empty() {
            tags.push(format!("language:{}", row.repo_language));
        }
        EvalCase {
            id: format!("swe-pro-{}", row.instance_id),
            name: format!("{} @ {}", row.repo, row.instance_id),
            input: serde_json::json!({
                "repo": row.repo,
                "base_commit": row.base_commit,
                "repo_language": row.repo_language,
                "problem_statement": row.problem_statement,
                "requirements": row.requirements,
                "interface": row.interface,
            }),
            expected_output: Some(serde_json::Value::String(row.patch)),
            expected_tools: None,
            tags,
            // 判分是「测试全过」的 0/1，不是字符串相等，这里不挂一个会误导的指标名。
            metrics: vec![],
            metadata,
        }
    }
}

#[async_trait]
impl CaseSource for SweProCaseSource {
    fn cases(&self, root: &Path) -> anyhow::Result<Vec<EvalCase>> {
        let path = root.join(SWE_PRO_JSONL);
        let content = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "cannot read {}: {e} (SWE-bench Pro ships as a parquet; run the fetch script with \
                 --convert against the same --dest to write the text form this reads)",
                path.display()
            )
        })?;
        let mut cases = Vec::new();
        for (lineno, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let row: SweProRow = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("{} line {}: {e}", path.display(), lineno + 1))?;
            cases.push(Self::row_to_case(row));
        }
        Ok(cases)
    }
}

/// 把 `fail_to_pass` / `pass_to_pass` 的值解析成测试名列表。
///
/// 这两个字段是**Python 字面量列表的字符串**（`['a', 'b']`），不是 JSON——实测 731 行里
/// 只有 9 行碰巧是合法 JSON，用 JSON 解析器读会静默得到空列表，于是「没有测试要跑」把
/// 这一列整列判成通过。字符串里两种引号可以混用、单引号可被反斜杠转义，所以这里自己走
/// 一遍字面量：认出 `[ ]`、两种引号、以及 `\'` `\"` `\\` `\n` `\t` `\r`（未知转义按
/// Python 的做法原样保留反斜杠）。**整串必须被消费掉**——多出来的字符是错误，不是忽略。
pub fn parse_test_list(raw: &str) -> anyhow::Result<Vec<String>> {
    ListParser::new(raw).parse()
}

struct ListParser<'a> {
    raw: &'a str,
    b: &'a [u8],
    i: usize,
}

impl<'a> ListParser<'a> {
    fn new(raw: &'a str) -> Self {
        Self {
            raw,
            b: raw.as_bytes(),
            i: 0,
        }
    }

    fn parse(mut self) -> anyhow::Result<Vec<String>> {
        self.ws();
        self.expect(b'[')?;
        let mut out = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
        } else {
            loop {
                self.ws();
                out.push(self.string()?);
                self.ws();
                match self.next() {
                    Some(b',') => continue,
                    Some(b']') => break,
                    other => anyhow::bail!("{}: expected `,` or `]`, got {other:?}", self.ctx()),
                }
            }
        }
        self.ws();
        if self.i != self.b.len() {
            anyhow::bail!("{}: trailing characters after the list", self.ctx());
        }
        Ok(out)
    }

    fn ctx(&self) -> String {
        let head: String = self.raw.chars().take(64).collect();
        format!("not a test-name list at byte {}: {head:?}", self.i)
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.i += 1;
        }
        c
    }

    fn ws(&mut self) {
        while self.b.get(self.i).is_some_and(u8::is_ascii_whitespace) {
            self.i += 1;
        }
    }

    fn expect(&mut self, want: u8) -> anyhow::Result<()> {
        match self.next() {
            Some(c) if c == want => Ok(()),
            other => anyhow::bail!("{}: expected `{}`, got {other:?}", self.ctx(), want as char),
        }
    }

    /// 读一个引号字符串。字节续进、最后整体转 UTF-8，非 ASCII 的测试名不会被打断。
    fn string(&mut self) -> anyhow::Result<String> {
        let quote = match self.next() {
            Some(c @ (b'\'' | b'"')) => c,
            other => anyhow::bail!("{}: expected a quoted test name, got {other:?}", self.ctx()),
        };
        let mut out: Vec<u8> = Vec::new();
        loop {
            let c = self
                .next()
                .ok_or_else(|| anyhow::anyhow!("{}: unterminated quoted test name", self.ctx()))?;
            if c == quote {
                return String::from_utf8(out).map_err(|e| anyhow::anyhow!("{}: {e}", self.ctx()));
            }
            if c == b'\\' {
                let e = self
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{}: dangling escape", self.ctx()))?;
                match e {
                    b'\'' => out.push(b'\''),
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'n' => out.push(b'\n'),
                    b't' => out.push(b'\t'),
                    b'r' => out.push(b'\r'),
                    other => {
                        // Python 对未知转义保留反斜杠本身，这里照做。
                        out.push(b'\\');
                        out.push(other);
                    }
                }
            } else {
                out.push(c);
            }
        }
    }
}

/// SWE-bench Pro 的容器后端。评测台**声明它、不实现它**：起容器（镜像来自该行的
/// `dockerhub_tag`）、在容器里跑官方测试是平台侧的事，`cog-eval` 只定义它必须提供什么。
///
/// 判分只需要「哪些测试通过了」这个集合，不解析测试框架的输出——那套逐仓库的解析归后端。
#[async_trait]
pub trait SweProBackend: Send + Sync {
    /// 按 instance 起容器，跑该行的 `before_repo_set_cmd` 把仓库 reset 到外壳动手前，
    /// 返回环境句柄。外壳随后在这棵树里改。
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>>;

    /// 在外壳改过的树上跑该 instance 的官方测试，回报通过的测试名集合。
    async fn run_tests(
        &self,
        env: &Arc<dyn CaseEnv>,
        case: &EvalCase,
    ) -> anyhow::Result<SweProTestReport>;
}

/// 一次判分跑的测试结果。
#[derive(Debug, Clone, Default)]
pub struct SweProTestReport {
    /// 通过的测试名（`fail_to_pass` / `pass_to_pass` 用的那套字符串）。
    pub passed: BTreeSet<String>,
    /// 测试运行器的原始输出：进 `Verdict.readings`，低分要能归因到哪条测试红了。
    pub raw: String,
}

/// 环境适配器：把接容器这件事交给后端，自己只保证「没有后端时报错、不是判错」。
pub struct SweProEnvProvider {
    backend: Option<Arc<dyn SweProBackend>>,
}

impl SweProEnvProvider {
    pub fn new(backend: Option<Arc<dyn SweProBackend>>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl EnvProvider for SweProEnvProvider {
    async fn acquire(&self, case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
        let Some(backend) = &self.backend else {
            anyhow::bail!(
                "SWE-bench Pro runs each instance in its own container and no container backend is \
                 wired (case {}); a case scored without one would read as the model failing the task",
                case.id
            );
        };
        backend.acquire(case).await
    }
}

/// SWE-bench Pro 的工具包。
///
/// 协议**恰好** bash + file-edit（与公开的 code agent 配置逐类比照）。实现从外面注入：
/// 这一层只管名字与件数，缺实现时构造就失败，而不是给出一束空工具让分数悄悄掉下去。
///
/// 工具是**按题现造**的：bash 与 file-edit 要落在这道题自己的容器里，容器到
/// [`Toolkit::toolset`] 拿到 `env` 时才存在，所以清单不能在构造期定死。
pub struct SweProToolkit {
    /// `None` ＝ 关工具的那一次跑。
    factory: Option<Arc<ToolFactory>>,
}

impl SweProToolkit {
    /// 工具开的那一套，清单按题现造（绑到这道题的容器上）。
    pub fn with_factory(factory: Arc<ToolFactory>) -> Self {
        Self {
            factory: Some(factory),
        }
    }

    /// 清单不随题变的那一套。构造时先核一遍，早失败。
    pub fn with_tools(tools: Vec<Tool>) -> anyhow::Result<Self> {
        check_face(&tools)?;
        Ok(Self::with_factory(Arc::new(move |_, _| Ok(tools.clone()))))
    }

    /// 关工具的那一套（同一 harness 只关工具的那一次跑）。
    pub fn without_tools() -> Self {
        Self { factory: None }
    }
}

/// SWE-bench Pro 的工具面**恰好**是这两件。每次造出来的清单都要过这一关：工厂能按题变，
/// 判据因此要落在**真正交出去的**那一束上。
fn check_face(tools: &[Tool]) -> anyhow::Result<()> {
    let mut names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    names.sort_unstable();
    let mut wanted = [SWE_PRO_TOOL_BASH, SWE_PRO_TOOL_FILE_EDIT];
    wanted.sort_unstable();
    if names != wanted {
        anyhow::bail!(
            "SWE-bench Pro's tool face is exactly [{SWE_PRO_TOOL_BASH}, \
             {SWE_PRO_TOOL_FILE_EDIT}]; got {names:?}"
        );
    }
    Ok(())
}

impl Toolkit for SweProToolkit {
    fn toolset(&self, case: &EvalCase, env: &Arc<dyn CaseEnv>) -> anyhow::Result<ToolSet> {
        match &self.factory {
            Some(factory) => {
                let tools = factory(case, env)?;
                check_face(&tools)?;
                ToolSet::new(tools)
            }
            None => Ok(ToolSet::empty()),
        }
    }
}

/// 判分适配器：跑 `fail_to_pass` / `pass_to_pass`，不叫任何模型。
pub struct SweProJudge {
    backend: Option<Arc<dyn SweProBackend>>,
}

impl SweProJudge {
    pub fn new(backend: Option<Arc<dyn SweProBackend>>) -> Self {
        Self { backend }
    }
}

/// 判分规则本身（纯函数，可单独验）：`fail_to_pass` 全过 **且** `pass_to_pass` 一条不破。
///
/// `fail_to_pass` 为空是**这一行没得判**（判不了），不是失败——报错交回运行器记 `errored`。
/// `pass_to_pass` 为空是合法的（公开集里有这种行），那时只要求 `fail_to_pass` 全过。
fn grade(
    fail_to_pass: &[String],
    pass_to_pass: &[String],
    passed: &BTreeSet<String>,
) -> anyhow::Result<Verdict> {
    if fail_to_pass.is_empty() {
        anyhow::bail!("no fail_to_pass tests to run: this instance cannot be judged");
    }
    let missing: Vec<&str> = fail_to_pass
        .iter()
        .filter(|t| !passed.contains(t.as_str()))
        .map(String::as_str)
        .collect();
    let broken: Vec<&str> = pass_to_pass
        .iter()
        .filter(|t| !passed.contains(t.as_str()))
        .map(String::as_str)
        .collect();

    let f2p_passed = fail_to_pass.len() - missing.len();
    let p2p_passed = pass_to_pass.len() - broken.len();
    let mut verdict = if missing.is_empty() && broken.is_empty() {
        Verdict::pass(format!(
            "fail_to_pass {f2p_passed}/{}; pass_to_pass {p2p_passed}/{}",
            fail_to_pass.len(),
            pass_to_pass.len()
        ))
    } else {
        Verdict::fail(format!(
            "fail_to_pass {f2p_passed}/{} (still failing: {}); pass_to_pass {p2p_passed}/{} \
             (broken: {})",
            fail_to_pass.len(),
            sample(&missing),
            pass_to_pass.len(),
            sample(&broken),
        ))
    };
    verdict
        .readings
        .insert("fail_to_pass_total".into(), fail_to_pass.len().to_string());
    verdict
        .readings
        .insert("fail_to_pass_passed".into(), f2p_passed.to_string());
    verdict
        .readings
        .insert("pass_to_pass_total".into(), pass_to_pass.len().to_string());
    verdict
        .readings
        .insert("pass_to_pass_passed".into(), p2p_passed.to_string());
    Ok(verdict)
}

/// 判词里最多点几条测试名——全列出来会把判词撑成一整份测试报告。
fn sample(names: &[&str]) -> String {
    const MAX: usize = 5;
    if names.is_empty() {
        return "none".into();
    }
    let head: Vec<&str> = names.iter().take(MAX).copied().collect();
    if names.len() > MAX {
        format!("{} (+{} more)", head.join(" | "), names.len() - MAX)
    } else {
        head.join(" | ")
    }
}

#[async_trait]
impl CaseJudge for SweProJudge {
    async fn judge(
        &self,
        case: &EvalCase,
        env: &Arc<dyn CaseEnv>,
        _output: &AgentOutput,
    ) -> anyhow::Result<Verdict> {
        // 判分清单在哪一行读不进来，就在那一行报错——不要吞成一个空的清单。
        let fail_to_pass = parse_test_list(
            case.metadata
                .get("fail_to_pass")
                .map(String::as_str)
                .unwrap_or("[]"),
        )?;
        let pass_to_pass = parse_test_list(
            case.metadata
                .get("pass_to_pass")
                .map(String::as_str)
                .unwrap_or("[]"),
        )?;

        let Some(backend) = &self.backend else {
            anyhow::bail!(
                "SWE-bench Pro judges by running the instance's tests and no container backend is \
                 wired (case {}); a case judged without one would be scored as wrong when it was \
                 never run",
                case.id
            );
        };
        let report = backend.run_tests(env, case).await?;

        let mut verdict = grade(&fail_to_pass, &pass_to_pass, &report.passed)?;
        verdict
            .readings
            .insert("tests_passed".into(), report.passed.len().to_string());
        // 原始输出可能很长：只在 readings 里留一段，判词里不留。
        let raw: String = report.raw.chars().take(2000).collect();
        verdict.readings.insert("report".into(), raw);
        Ok(verdict)
    }
}

/// 把四件拼成一个 SWE-bench Pro 基准，交给运行器。
///
/// `backend` 是容器后端：可达时给一个实现，缺席时给 `None`——那时起环境与判分都报错，
/// 这一列读起来是「没跑成」而不是「分数低」。`tools` 决定带工具还是关工具的那一次。
pub fn swe_pro_benchmark(
    backend: Option<Arc<dyn SweProBackend>>,
    tools: SweProToolkit,
) -> crate::bench::Benchmark {
    crate::bench::Benchmark {
        name: "swe-bench-pro".into(),
        metric: "resolved@1".into(),
        cases: Arc::new(SweProCaseSource),
        env: Arc::new(SweProEnvProvider::new(backend.clone())),
        tools: Arc::new(tools),
        judge: Arc::new(SweProJudge::new(backend)),
        budget: crate::scaffold::Budget::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::{Budget, FinishReason, NoEnv};
    use cog_core::{ToolImplementation, Usage};

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "swe-pro-test-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(d.join("swe-bench-pro")).unwrap();
        d
    }

    fn write_jsonl(root: &Path, body: &str) {
        std::fs::write(root.join(SWE_PRO_JSONL), body).unwrap();
    }

    /// 一行的样子：`fail_to_pass` 是 Python 字面量（单引号 + 转义的撇号），不是 JSON。
    fn row(instance: &str, f2p: &str, p2p: &str) -> String {
        serde_json::json!({
            "instance_id": instance,
            "repo": "NodeBB/NodeBB",
            "base_commit": "abc123",
            "patch": "diff --git a/x b/x\n",
            "test_patch": "diff --git a/t b/t\n",
            "problem_statement": "the bug",
            "requirements": "- fix it",
            "interface": "Type: Method",
            "repo_language": "js",
            "fail_to_pass": f2p,
            "pass_to_pass": p2p,
            "before_repo_set_cmd": "git reset --hard abc123",
            "selected_test_files_to_run": "[\"test/x.js\"]",
            "dockerhub_tag": "nodebb.nodebb-NodeBB-x",
        })
        .to_string()
    }

    #[test]
    fn rows_become_cases_with_the_contract_carried() {
        let root = tempdir();
        write_jsonl(
            &root,
            &format!(
                "{}\n",
                row(
                    "instance_a",
                    r#"['test/x.js | does not fail', 'test/y.js | holds']"#,
                    "[]"
                )
            ),
        );
        let cases = SweProCaseSource.cases(&root).unwrap();
        assert_eq!(cases.len(), 1);
        let c = &cases[0];
        assert_eq!(c.id, "swe-pro-instance_a");
        assert_eq!(c.metadata["instance_id"], "instance_a");
        assert_eq!(c.metadata["dockerhub_tag"], "nodebb.nodebb-NodeBB-x");
        assert!(c.tags.contains(&"language:js".to_string()));
        // 金标补丁在 expected_output，不在外壳的输入里；判分清单也不进输入。
        assert_eq!(
            c.expected_output.as_ref().unwrap().as_str().unwrap(),
            "diff --git a/x b/x\n"
        );
        assert_eq!(c.input["problem_statement"], "the bug");
        assert!(c.input.get("patch").is_none());
        assert!(c.input.get("fail_to_pass").is_none());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_test_lists_are_python_literals_not_json() {
        // 单引号列表：JSON 解析器读不了，这正是这一列最容易踩的坑。
        assert!(serde_json::from_str::<Vec<String>>("['a', 'b']").is_err());
        assert_eq!(parse_test_list("['a', 'b']").unwrap(), vec!["a", "b"]);
        // 两种引号混用、撇号被反斜杠转义。
        assert_eq!(
            parse_test_list(r#"["it doesn\'t hold", 'b']"#).unwrap(),
            vec!["it doesn't hold", "b"]
        );
        // 空清单合法；test == ["say \"hi\""] 这种也要能读。
        assert!(parse_test_list("[]").unwrap().is_empty());
        assert_eq!(
            parse_test_list(r#"['say "hi"']"#).unwrap(),
            vec![r#"say "hi""#]
        );
        assert_eq!(parse_test_list(r#"['a\\b']"#).unwrap(), vec!["a\\b"]);
        // 整串必须被消费掉，多出来的字符是错误。
        assert!(parse_test_list("['a'] junk").is_err());
        assert!(parse_test_list("'a'").is_err());
        assert!(parse_test_list("['a'").is_err());
    }

    #[test]
    fn a_missing_text_form_names_what_to_run() {
        let root = tempdir();
        let err = SweProCaseSource.cases(&root).unwrap_err().to_string();
        assert!(err.contains("--convert"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    fn tool(name: &str) -> Tool {
        Tool {
            name: name.into(),
            description: name.into(),
            parameters: serde_json::json!({"type": "object"}),
            implementation: ToolImplementation::Native(Arc::new(|_| {
                Box::pin(async move { Ok(serde_json::json!(null)) })
            })),
        }
    }

    #[test]
    fn the_tool_face_is_exactly_two_pinned_tools() {
        assert!(SweProToolkit::with_tools(vec![
            tool(SWE_PRO_TOOL_FILE_EDIT),
            tool(SWE_PRO_TOOL_BASH)
        ])
        .is_ok());
        assert!(SweProToolkit::with_tools(vec![tool(SWE_PRO_TOOL_BASH)]).is_err());
        assert!(SweProToolkit::with_tools(vec![
            tool(SWE_PRO_TOOL_BASH),
            tool(SWE_PRO_TOOL_FILE_EDIT),
            tool("python")
        ])
        .is_err());
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let any = case("swe-pro-x", "['t']", "[]");
        assert!(SweProToolkit::without_tools()
            .toolset(&any, &env)
            .unwrap()
            .is_empty());
    }

    /// 工厂拿得到 `case`（与 `env`）：bash 与 file-edit 才能绑到**这道题的**容器上。
    #[test]
    fn a_factory_sees_the_case_it_is_building_tools_for() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        let factory: Arc<ToolFactory> = Arc::new(|c, _| {
            if c.id == "swe-pro-per-case" {
                Ok(vec![tool(SWE_PRO_TOOL_BASH), tool(SWE_PRO_TOOL_FILE_EDIT)])
            } else {
                anyhow::bail!("no container for {}", c.id)
            }
        });
        let kit = SweProToolkit::with_factory(factory);
        assert_eq!(
            kit.toolset(&case("swe-pro-per-case", "['t']", "[]"), &env)
                .unwrap()
                .definitions()
                .len(),
            2
        );
        assert!(kit
            .toolset(&case("swe-pro-other", "['t']", "[]"), &env)
            .is_err());
    }

    fn case(id: &str, f2p: &str, p2p: &str) -> EvalCase {
        let mut metadata = HashMap::new();
        metadata.insert("fail_to_pass".into(), f2p.into());
        metadata.insert("pass_to_pass".into(), p2p.into());
        EvalCase {
            id: id.into(),
            name: id.into(),
            input: serde_json::json!({"problem_statement": "x"}),
            expected_output: None,
            expected_tools: None,
            tags: vec![],
            metrics: vec![],
            metadata,
        }
    }

    fn output() -> AgentOutput {
        AgentOutput {
            final_answer: String::new(),
            trace: vec![],
            tokens: Usage::default(),
            finish: FinishReason::Answered,
        }
    }

    /// 一个按预置集合回报测试结果的后端，不真起容器。
    struct ScriptedBackend {
        passed: Vec<&'static str>,
        raw: &'static str,
    }

    #[async_trait]
    impl SweProBackend for ScriptedBackend {
        async fn acquire(&self, _case: &EvalCase) -> anyhow::Result<Arc<dyn CaseEnv>> {
            Ok(Arc::new(NoEnv))
        }
        async fn run_tests(
            &self,
            _env: &Arc<dyn CaseEnv>,
            _case: &EvalCase,
        ) -> anyhow::Result<SweProTestReport> {
            Ok(SweProTestReport {
                passed: self.passed.iter().map(|s| s.to_string()).collect(),
                raw: self.raw.into(),
            })
        }
    }

    fn backend(passed: Vec<&'static str>) -> Arc<dyn SweProBackend> {
        Arc::new(ScriptedBackend {
            passed,
            raw: "1 passed; 0 failed",
        })
    }

    #[tokio::test]
    async fn the_judge_applies_the_fail_to_pass_and_pass_to_pass_rule() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);

        // 全过 ⇒ 通过。
        let judge = SweProJudge::new(Some(backend(vec!["t1", "t2", "p1"])));
        let v = judge
            .judge(&case("c1", "['t1', 't2']", "['p1']"), &env, &output())
            .await
            .unwrap();
        assert!(v.resolved, "{}", v.detail);
        assert_eq!(v.readings["fail_to_pass_passed"], "2");
        assert_eq!(v.readings["pass_to_pass_passed"], "1");

        // fail_to_pass 有一条没过 ⇒ 失败，且判词点名那条。
        let judge = SweProJudge::new(Some(backend(vec!["t1", "p1"])));
        let v = judge
            .judge(&case("c2", "['t1', 't2']", "['p1']"), &env, &output())
            .await
            .unwrap();
        assert!(!v.resolved);
        assert!(v.detail.contains("t2"), "{}", v.detail);

        // pass_to_pass 被打破 ⇒ 失败，即便 fail_to_pass 全过。
        let judge = SweProJudge::new(Some(backend(vec!["t1", "t2"])));
        let v = judge
            .judge(&case("c3", "['t1', 't2']", "['p1']"), &env, &output())
            .await
            .unwrap();
        assert!(!v.resolved);
        assert!(v.detail.contains("p1"), "{}", v.detail);

        // pass_to_pass 为空的行（公开集里有）只要求 fail_to_pass 全过。
        let judge = SweProJudge::new(Some(backend(vec!["t1"])));
        let v = judge
            .judge(&case("c4", "['t1']", "[]"), &env, &output())
            .await
            .unwrap();
        assert!(v.resolved, "{}", v.detail);
    }

    #[tokio::test]
    async fn a_case_that_cannot_be_judged_errors_rather_than_failing() {
        let env: Arc<dyn CaseEnv> = Arc::new(NoEnv);
        // 没有后端：判不了，不是判错。
        let judge = SweProJudge::new(None);
        let err = judge
            .judge(&case("c", "['t1']", "[]"), &env, &output())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("never run"), "{err}");

        // fail_to_pass 为空的行没得判 ⇒ 报错，不是判失败。
        let judge = SweProJudge::new(Some(backend(vec![])));
        let err = judge
            .judge(&case("c2", "[]", "['p1']"), &env, &output())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be judged"), "{err}");
    }

    #[tokio::test]
    async fn the_four_adapters_compose_into_a_benchmark() {
        let root = tempdir();
        write_jsonl(&root, &format!("{}\n", row("instance_z", "['t']", "['p']")));
        let bench = swe_pro_benchmark(None, SweProToolkit::without_tools());
        assert_eq!(bench.name, "swe-bench-pro");
        assert_eq!(bench.metric, "resolved@1");
        assert_eq!(bench.budget, Budget::default());
        let cases = bench.cases.cases(&root).unwrap();
        assert_eq!(cases.len(), 1);
        // 没有容器后端 ⇒ 起环境就报错，不是给一个空环境让分数掉下去。
        assert!(bench.env.acquire(&cases[0]).await.is_err());
        std::fs::remove_dir_all(&root).ok();
    }
}
