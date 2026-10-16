//! 容器后端的真机验收：这东西在这台宿主上跑得通，而且判词**会动**。
//!
//! 这些用例默认不跑（`#[ignore]`）：它们要一台能起容器的机器、要从 registry 拉一份近
//! 1 GB 的题镜像，还要在容器里 `npm install`。跑法：
//!
//!   cargo test -p cogneva --test swe_pro_container -- --ignored --nocapture --test-threads=1
//!
//! 两条用例验的是同一个判分器的两面，缺一面这个后端就不算验过：
//!
//! - 参考解全过：把金标补丁打上去，`fail_to_pass` 必须**全过**；
//! - 空补丁必红：什么都不打，`fail_to_pass` 必须**一条都不过**，而 `pass_to_pass` 一条不破。
//!
//! 只有第一条时，「判分器永远说通过了」也能过；只有第二条时，「判分器永远说没过」也能过。
//! 两条一起，判词才被证明是随着树变的。这也是「形状未证不出分」里那个「证」。

use std::sync::Arc;

use cog_eval::{parse_test_list, CaseEnv, CaseSource, EvalCase, SweProBackend, SweProCaseSource};
use serde_json::json;

/// 本机取数脚本的落点。
const DEFAULT_DATA_ROOT: &str = "/srv/cogneva/benchmarks";
/// 这两条用例都跑这一题：它的金标补丁小、镜像小、测试快，而且 `fail_to_pass` 恰好三条。
const CASE: &str = "instance_NodeBB__NodeBB-04998908ba6721d64eba79ae3b65a351dcfbc5b5-vnan";

fn data_root() -> std::path::PathBuf {
    std::env::var("COG_EVAL_DATA_ROOT")
        .unwrap_or_else(|_| DEFAULT_DATA_ROOT.to_string())
        .into()
}

fn the_case(root: &std::path::Path) -> EvalCase {
    let wanted = std::env::var("COGNEVA_EVAL_CONTAINER_CASE").unwrap_or_else(|_| CASE.to_string());
    SweProCaseSource
        .cases(root)
        .expect("the SWE-bench Pro data is provisioned under this root")
        .into_iter()
        .find(|c| c.metadata.get("instance_id").map(String::as_str) == Some(wanted.as_str()))
        .unwrap_or_else(|| panic!("{wanted} is not in the dataset"))
}

fn list_of(case: &EvalCase, key: &str) -> Vec<String> {
    parse_test_list(case.metadata.get(key).map(String::as_str).unwrap_or("[]"))
        .expect("the dataset's test lists parse")
}

async fn backend() -> Arc<cogneva::eval_container::SweProContainerBackend> {
    cogneva::eval_container::swe_pro_container_backend(&data_root())
        .await
        .unwrap_or_else(|why| panic!("the container backend is not wired: {why}"))
}

/// 在容器里跑一条 shell。走的是**模型那条路**（`bash` 工具），不是另开一个执行端口——
/// 验的就是这条路上真的能改到那棵树。
async fn sh(env: &Arc<dyn CaseEnv>, command: &str) -> serde_json::Value {
    let executor = env
        .executor()
        .expect("this column's environment offers an execution face");
    executor
        .execute("bash", json!({ "command": command }))
        .await
        .expect("bash runs in the instance's container")
}

fn exit_code(out: &serde_json::Value) -> i64 {
    out.get("exit_code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(-1)
}

/// 参考解一侧：金标补丁打上去，`fail_to_pass` 全过。
#[tokio::test]
#[ignore = "needs a container machine, a ~1 GB instance image and an npm install in it"]
async fn the_reference_patch_turns_every_fail_to_pass_green() {
    let root = data_root();
    let case = the_case(&root);
    let backend = backend().await;

    let env = backend
        .acquire(&case)
        .await
        .expect("a container for this instance");
    let patch = case
        .expected_output
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .expect("the gold patch rides on expected_output");

    // 补丁里有反引号与 `$`，走 heredoc 更省心：定界符被引起来时里面一个字符都不展开。
    let applied = sh(
        &env,
        &format!(
            "cat > /tmp/gold.diff <<'COGNEVA_GOLD'\n{patch}\nCOGNEVA_GOLD\n\
             git apply -v /tmp/gold.diff && git diff --stat | tail -3"
        ),
    )
    .await;
    assert_eq!(
        exit_code(&applied),
        0,
        "gold patch did not apply: {applied}"
    );

    let report = backend
        .run_tests(&env, &case)
        .await
        .expect("the instance's official tests run");
    env.teardown().await;

    let f2p = list_of(&case, "fail_to_pass");
    assert!(
        !f2p.is_empty(),
        "this instance has nothing to fail into passing"
    );
    let missed: Vec<&String> = f2p
        .iter()
        .filter(|t| !report.passed.contains(t.as_str()))
        .collect();
    assert!(
        missed.is_empty(),
        "the reference patch left {} of {} fail_to_pass red: {missed:?}\n{}",
        missed.len(),
        f2p.len(),
        report.raw
    );
    let p2p = list_of(&case, "pass_to_pass");
    let broken: Vec<&String> = p2p
        .iter()
        .filter(|t| !report.passed.contains(t.as_str()))
        .collect();
    assert!(
        broken.is_empty(),
        "the reference patch broke {} of {} pass_to_pass: {broken:?}\n{}",
        broken.len(),
        p2p.len(),
        report.raw
    );
}

/// 空补丁一侧：不碰那棵树，`fail_to_pass` 必须一条都不过，而 `pass_to_pass` 一条不破。
///
/// 少了这一条，一个「判分器永远报通过」的后端也能过上面那条。而且它同时证明了另一件事：
/// 这台容器起出来的时候，那三条测试**确实是红的**——判词的红不是被环境弄红的。
#[tokio::test]
#[ignore = "needs a container machine, a ~1 GB instance image and an npm install in it"]
async fn an_untouched_tree_leaves_every_fail_to_pass_red() {
    let root = data_root();
    let case = the_case(&root);
    let backend = backend().await;

    let env = backend
        .acquire(&case)
        .await
        .expect("a container for this instance");
    let report = backend
        .run_tests(&env, &case)
        .await
        .expect("the instance's official tests run");
    env.teardown().await;

    let f2p = list_of(&case, "fail_to_pass");
    let green: Vec<&String> = f2p
        .iter()
        .filter(|t| report.passed.contains(t.as_str()))
        .collect();
    assert!(
        green.is_empty(),
        "an untouched tree passed {} of {} fail_to_pass, so this run does not discriminate: \
         {green:?}\n{}",
        green.len(),
        f2p.len(),
        report.raw
    );
    let p2p = list_of(&case, "pass_to_pass");
    let broken: Vec<&String> = p2p
        .iter()
        .filter(|t| !report.passed.contains(t.as_str()))
        .collect();
    assert!(
        broken.is_empty(),
        "the base tree alone already breaks {} of {} pass_to_pass, so pass_to_pass cannot tell a \
         regression from the starting point: {broken:?}",
        broken.len(),
        p2p.len()
    );
    // 那三条红必须**有名字**：只有「没解析出测试」时这一列读作「没得出判词」，那是另一件事。
    assert!(
        report.raw.contains("failed\t3"),
        "the three red tests are not accounted for in the reading: {}",
        report.raw
    );
}

/// 判分清单不上外壳那一侧：题面里不许出现那三条测试的名字，金标补丁只落在判分器读得到的地方。
///
/// 它**不需要**容器，所以在普通 `cargo test` 里就跑——而它守的正是上面两条用例的前提：
/// 那两条把金标补丁打上去再判，要是补丁本来就随题面交给了外壳，这一列量的是开卷考试。
#[tokio::test]
async fn the_task_statement_does_not_hand_over_the_answers() {
    let root = data_root();
    if !root
        .join("swe-bench-pro/test-00000-of-00001.jsonl")
        .is_file()
    {
        eprintln!("no SWE-bench Pro data under {}; skipping", root.display());
        return;
    }
    let case = the_case(&root);
    let f2p = list_of(&case, "fail_to_pass");
    assert!(
        !f2p.is_empty(),
        "this instance names no fail_to_pass, so nothing could turn green"
    );
    let shown = case.input.to_string();
    for test in &f2p {
        assert!(
            !shown.contains(test.as_str()),
            "the task statement hands the model a fail_to_pass name: {test}"
        );
    }
    assert!(
        case.expected_output.is_some(),
        "the gold patch has to ride somewhere the judge can read it"
    );
}
