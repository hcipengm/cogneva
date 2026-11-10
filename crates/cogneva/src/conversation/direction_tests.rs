//! A22 的门：外循环的方向盘。
//!
//! 与 `conversation.rs` 的其余测试分开放，理由只有一个：这一组要替
//! `ChangeLedgerStore` 造一本会说话的假账，而那份契约里有几个写入口
//! （`apply_measurement` 之类）。装配层有一道门按文本普查 `conversation.rs`
//! ——「读账的这一层不许出现写入口」——假账写得再正当也会被它读到。那不是门判错，
//! 是这几个字不该出现在被它读的那一页上。测试仍是 `conversation` 的子模块，
//! 私有的工具与 handler 直接调得到。

use super::tests::{call, Exhausted, RecordingAudit};
use super::*;

/// 这一组门只走 `get` 与 `record_readings`，别的门一律说没接。
fn not_here(what: &str) -> cog_core::SFError {
    cog_core::SFError::NotImplemented(format!(
        "档位这一组门只走 get 与 record_readings，{what} 没有被接到"
    ))
}

fn blank_row(identity: &cog_core::ChangeIdentity) -> cog_core::ChangeLedgerRow {
    cog_core::ChangeLedgerRow {
        identity: identity.clone(),
        state: cog_core::ChangeState::Pending,
        title: None,
        assertion: None,
        sources: Vec::new(),
        first_seen_at: chrono::Utc::now(),
        measured_at: None,
        measured_rev: None,
        delivered_rev: None,
        landed_at: None,
        unsettled_reason: None,
        retire_reason: None,
        retired_at: None,
    }
}

fn ledger_row(
    kind: &str,
    scope: &str,
    literal: &str,
    state: cog_core::ChangeState,
    sources: &[(&str, cog_core::ChangePriority)],
) -> cog_core::ChangeLedgerRow {
    let identity = cog_core::ChangeIdentity::new(kind, scope, literal);
    let mut row = blank_row(&identity);
    row.state = state;
    row.title = Some(format!("关于 {literal} 的那条变更"));
    row.assertion = Some(format!("把 {literal} 做出来"));
    row.sources = sources
        .iter()
        .map(|(doc, priority)| cog_core::ChangeSource {
            doc: (*doc).to_string(),
            verdict: None,
            priority: *priority,
            block: cog_core::ChangeBlock::NONE,
        })
        .collect();
    row
}

/// 一本会说话的账：`get` 回那一行，`record_readings` 照真账的规矩改写引用。
///
/// 引用的规矩是这里要证的那一半：同一个文档只说一格（再写是它自己那一格
/// 被改写，不是多出一条），一条变更的档是它所有引用里最高的那一条。写成
/// 追加而不是替换，重复定档就会在账上留下两句互相矛盾的话，而读面只会看到
/// 最后一条——这正是工具要替属主避免的事。其余的门说没接，这组门不该走
/// 到那里。
#[derive(Default)]
struct BandLedger {
    rows: std::sync::Mutex<std::collections::HashMap<String, cog_core::ChangeLedgerRow>>,
    written: std::sync::Mutex<Vec<cog_core::ChangeReading>>,
}

impl BandLedger {
    fn holding(rows: Vec<cog_core::ChangeLedgerRow>) -> Arc<Self> {
        Arc::new(Self {
            rows: std::sync::Mutex::new(
                rows.into_iter()
                    .map(|row| (row.identity.as_key(), row))
                    .collect(),
            ),
            written: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn row(&self, identity: &cog_core::ChangeIdentity) -> Option<cog_core::ChangeLedgerRow> {
        self.rows.lock().unwrap().get(&identity.as_key()).cloned()
    }

    fn written(&self) -> Vec<cog_core::ChangeReading> {
        self.written.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl cog_core::ChangeLedgerStore for BandLedger {
    async fn record_readings(
        &self,
        readings: &[cog_core::ChangeReading],
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        self.written
            .lock()
            .unwrap()
            .extend(readings.iter().cloned());
        let mut rows = self.rows.lock().unwrap();
        for reading in readings {
            let row = rows
                .entry(reading.identity.as_key())
                .or_insert_with(|| blank_row(&reading.identity));
            // 标题与正文是 COALESCE：这次没说，不擦掉账上已有的那一句。
            if reading.title.is_some() {
                row.title = reading.title.clone();
            }
            if reading.assertion.is_some() {
                row.assertion = reading.assertion.clone();
            }
            match row
                .sources
                .iter_mut()
                .find(|source| source.doc == reading.source.doc)
            {
                Some(source) => *source = reading.source.clone(),
                None => row.sources.push(reading.source.clone()),
            }
        }
        Ok(readings.len())
    }

    async fn declare(&self, _readings: &[cog_core::ChangeReading]) -> SFResult<usize> {
        Err(not_here("declare"))
    }

    async fn fold_declarations(
        &self,
        _limit: usize,
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        Err(not_here("fold_declarations"))
    }

    async fn fold_retirements(&self, _limit: usize) -> SFResult<usize> {
        Err(not_here("fold_retirements"))
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        Err(not_here("pending_declarations"))
    }

    async fn pending_retirements(&self) -> SFResult<u64> {
        Err(not_here("pending_retirements"))
    }

    async fn claimed_revisions(
        &self,
        _revisions: &[String],
    ) -> SFResult<std::collections::HashSet<String>> {
        Err(not_here("claimed_revisions"))
    }

    async fn apply_measurement(&self, _measurement: &cog_core::ChangeMeasurement) -> SFResult<()> {
        Err(not_here("apply_measurement"))
    }

    async fn outstanding(&self, _limit: usize) -> SFResult<Vec<cog_core::ChangeLedgerRow>> {
        Err(not_here("outstanding"))
    }

    async fn events(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::ChangeLedgerEvent>> {
        Err(not_here("events"))
    }

    async fn owed_by_band(&self) -> SFResult<Vec<(cog_core::ChangePriority, usize)>> {
        Err(not_here("owed_by_band"))
    }

    async fn landed_due(&self, _limit: usize) -> SFResult<Vec<cog_core::ChangeLedgerRow>> {
        Err(not_here("landed_due"))
    }

    async fn landed_at_revision(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<(cog_core::ChangeIdentity, Option<String>)>> {
        Err(not_here("landed_at_revision"))
    }

    async fn landed_touching(
        &self,
        _paths: &[String],
        _limit: usize,
    ) -> SFResult<Vec<cog_core::ChangeLedgerRow>> {
        Err(not_here("landed_touching"))
    }

    async fn landed_awaiting_parent_observation(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<(cog_core::ChangeIdentity, String)>> {
        Err(not_here("landed_awaiting_parent_observation"))
    }

    async fn record_parent_observation(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _observation: &str,
    ) -> SFResult<bool> {
        Err(not_here("record_parent_observation"))
    }

    async fn record_reopen(&self, _reopen: &cog_core::ChangeReopen) -> SFResult<()> {
        Err(not_here("record_reopen"))
    }

    async fn retired_surface_moved(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::ChangeLedgerRow>> {
        Err(not_here("retired_surface_moved"))
    }

    async fn retired_answered_elsewhere(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::ChangeLedgerRow>> {
        Err(not_here("retired_answered_elsewhere"))
    }

    async fn request_remeasurement(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _source: &str,
    ) -> SFResult<()> {
        Err(not_here("request_remeasurement"))
    }

    async fn pending_remeasurements(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<cog_core::ChangeIdentity>> {
        Err(not_here("pending_remeasurements"))
    }

    async fn mark_remeasured(&self, _identity: &cog_core::ChangeIdentity) -> SFResult<()> {
        Err(not_here("mark_remeasured"))
    }

    async fn remeasurement_backlog(&self) -> SFResult<cog_core::RemeasureBacklog> {
        Err(not_here("remeasurement_backlog"))
    }

    async fn record_delivery(
        &self,
        _identity: &cog_core::ChangeIdentity,
        _rev: &str,
    ) -> SFResult<()> {
        Err(not_here("record_delivery"))
    }

    async fn stamp_pool_block(&self, _unblock_condition: &str) -> SFResult<u64> {
        Err(not_here("stamp_pool_block"))
    }

    async fn clear_pool_block(&self) -> SFResult<u64> {
        Err(not_here("clear_pool_block"))
    }

    async fn pool_wait(&self) -> SFResult<cog_core::PoolWait> {
        Err(not_here("pool_wait"))
    }

    async fn get(
        &self,
        identity: &cog_core::ChangeIdentity,
    ) -> SFResult<Option<cog_core::ChangeLedgerRow>> {
        Ok(self.row(identity))
    }

    async fn census(&self) -> SFResult<Vec<(cog_core::ChangeState, u64)>> {
        Err(not_here("census"))
    }

    async fn funnel(&self, _days: u32) -> SFResult<Vec<cog_core::ChangeFunnelDay>> {
        Err(not_here("funnel"))
    }

    async fn record_dispatch_state(&self, _state: &cog_core::DispatchState) -> SFResult<()> {
        Err(not_here("record_dispatch_state"))
    }
}

/// 一张会记账的图：交进来的目标与行都留着，同一个 id 只在第一次算新增。
///
/// 「交过一次」与「刚交的」是两件事，而工具报的 `status` 正是要分开它们：
/// 图已经持有的 id 再交一次是幂等空操作，报成「刚交」就是告诉属主有一份活
/// 从没被接走。
#[derive(Default)]
struct HandoverGraph {
    tasks: std::sync::Mutex<std::collections::HashMap<String, cog_core::Task>>,
    handed: std::sync::Mutex<Vec<(String, Vec<cog_core::Task>)>>,
    /// 这个控制面认不认「按变更派工」。不认时它回 `None`，工具必须照实
    /// 说「这里不按变更派工」，而不是编一个永远不存在的 id。
    dispatches: bool,
}

impl HandoverGraph {
    fn dispatching() -> Arc<Self> {
        Arc::new(Self {
            dispatches: true,
            ..Default::default()
        })
    }

    fn hold(&self, id: &str) {
        self.tasks.lock().unwrap().insert(
            id.to_string(),
            cog_core::Task::new(
                id,
                cog_core::TaskType::Custom("held".into()),
                serde_json::json!({}),
            ),
        );
    }

    fn handed(&self) -> Vec<(String, Vec<cog_core::Task>)> {
        self.handed.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl cog_core::OrchestratorControl for HandoverGraph {
    async fn submit_goal(&self, _goal: &str, _tasks: Vec<cog_core::Task>) -> SFResult<()> {
        Ok(())
    }

    async fn submit_goal_auto(
        &self,
        goal: &str,
        tasks: Vec<cog_core::Task>,
    ) -> SFResult<Vec<String>> {
        self.handed
            .lock()
            .unwrap()
            .push((goal.to_string(), tasks.clone()));
        let mut held = self.tasks.lock().unwrap();
        let mut added = Vec::new();
        for task in tasks {
            if !held.contains_key(&task.id) {
                added.push(task.id.clone());
                held.insert(task.id.clone(), task);
            }
        }
        Ok(added)
    }

    async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn add_task(&self, task: cog_core::Task) -> SFResult<()> {
        self.tasks.lock().unwrap().insert(task.id.clone(), task);
        Ok(())
    }

    async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
        false
    }

    async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
        0
    }

    async fn get_ready_tasks(&self) -> Vec<cog_core::Task> {
        Vec::new()
    }

    async fn get_all_tasks(&self) -> Vec<cog_core::Task> {
        self.tasks.lock().unwrap().values().cloned().collect()
    }

    async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
        Ok(false)
    }

    async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn dlq_len(&self) -> SFResult<usize> {
        Ok(0)
    }

    async fn start_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn complete_task(
        &self,
        _task_id: &str,
        _result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn fail_task(
        &self,
        _task_id: &str,
        _error: String,
        _cause: Option<cog_core::UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        Ok((false, Vec::new(), false))
    }

    async fn fail_task_after(
        &self,
        _task_id: &str,
        _error: String,
        _cause: Option<cog_core::UpstreamFailure>,
        _retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        Ok((false, Vec::new(), false))
    }

    async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn get_task(&self, task_id: &str) -> Option<cog_core::Task> {
        self.tasks.lock().unwrap().get(task_id).cloned()
    }

    async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        Vec::new()
    }

    async fn get_dependents(&self, _task_id: &str) -> Option<Vec<cog_core::Task>> {
        None
    }

    async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<cog_core::Task>> {
        None
    }

    async fn get_graph(&self) -> (Vec<cog_core::Task>, Vec<(String, String)>) {
        (Vec::new(), Vec::new())
    }

    async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn all_completed(&self) -> bool {
        false
    }

    async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
        Ok(false)
    }

    fn change_dispatch_ids(&self, identity: &cog_core::ChangeIdentity) -> Option<Vec<String>> {
        self.dispatches
            .then(|| cog_orchestrator::change_ledger::dispatch_task_ids(identity))
    }
}

/// 一个会记账的进化控制面：放行与回滚各记一笔，拒绝也记得下是谁被拒了。
#[derive(Default)]
struct DecidingAdmin {
    approvals: std::sync::Mutex<Vec<String>>,
    rollbacks: std::sync::Mutex<usize>,
    refuse: bool,
}

impl DecidingAdmin {
    fn allowing() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn refusing() -> Arc<Self> {
        Arc::new(Self {
            refuse: true,
            ..Default::default()
        })
    }

    fn approvals(&self) -> Vec<String> {
        self.approvals.lock().unwrap().clone()
    }

    fn rollbacks(&self) -> usize {
        *self.rollbacks.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl cog_core::EvolutionAdmin for DecidingAdmin {
    async fn list_changes(&self) -> SFResult<Vec<cog_core::EvolutionChangeInfo>> {
        Ok(Vec::new())
    }

    async fn apply_change(&self, _change_id: &str) -> SFResult<cog_core::EvolutionApplyResponse> {
        Err(not_here("apply_change"))
    }

    async fn deploy_change(&self, _change_id: &str) -> SFResult<cog_core::EvolutionDeployResponse> {
        Err(not_here("deploy_change"))
    }

    async fn approve_change(&self, change_id: &str) -> SFResult<cog_core::EvolutionDeployResponse> {
        self.approvals.lock().unwrap().push(change_id.to_string());
        if self.refuse {
            return Err(cog_core::SFError::Agent("这一版没有停在等审批".into()));
        }
        Ok(cog_core::EvolutionDeployResponse {
            change_id: change_id.to_string(),
            commit_hash: "cafe1234".into(),
            staged_binary_path: "/tmp/staged".into(),
            switched: true,
        })
    }

    async fn rollback(&self) -> SFResult<cog_core::EvolutionRollbackResponse> {
        *self.rollbacks.lock().unwrap() += 1;
        if self.refuse {
            return Err(cog_core::SFError::Agent("没有可退的上一版".into()));
        }
        Ok(cog_core::EvolutionRollbackResponse {
            rolled_back: true,
            message: "back on the previous image".into(),
        })
    }
}

fn direction_tools(
    ledger: Option<Arc<BandLedger>>,
    graph: &Arc<HandoverGraph>,
    admin: Option<Arc<DecidingAdmin>>,
    owner: Option<&str>,
) -> Vec<Tool> {
    change_direction_tools(
        ledger.map(|ledger| ledger as Arc<dyn cog_core::ChangeLedgerStore>),
        graph.clone(),
        admin.map(|admin| admin as Arc<dyn cog_core::EvolutionAdmin>),
        None,
        None,
        owner.map(str::to_string),
        "luna-1822ab0e".into(),
    )
}

fn change_args(kind: &str, scope: &str, literal: &str) -> serde_json::Value {
    serde_json::json!({ "kind": kind, "scope": scope, "literal": literal })
}

fn band_args(kind: &str, scope: &str, literal: &str, band: &str) -> serde_json::Value {
    serde_json::json!({ "kind": kind, "scope": scope, "literal": literal, "band": band })
}

/// 属主的一句话落成一格引用，账上那一行按它排队。
///
/// 写的是引用而不是一个档位列：一条变更的档是它所有引用里最高的那一条，
/// 改写属主自己那一格不会动别人说的。这条门证的就是这一步——别人说的还在，
/// 属主说的也在，读出来的档是两者高的那一个。
#[tokio::test]
async fn an_owners_band_lands_as_a_citation_and_the_row_takes_the_highest_one() {
    let identity = cog_core::ChangeIdentity::new("grep", "crates/", "fn a()");
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[("spec-x", cog_core::ChangePriority::Unstated)],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, Some("ferris"));

    let answer = call(
        &tools,
        "set_change_priority",
        band_args("grep", "crates/", "fn a()", "p1"),
    )
    .await;

    assert_eq!(answer["available"], true, "{answer}");
    assert_eq!(answer["asked"], "p1", "{answer}");
    assert_eq!(answer["effective"], "p1", "{answer}");
    let row = ledger.row(&identity).expect("账上没有这一行");
    assert_eq!(row.priority(), cog_core::ChangePriority::P1);

    let written = ledger.written();
    assert_eq!(written.len(), 1, "一次定档写了不止一条引用");
    assert_eq!(
        written[0].source.doc, "owner:ferris",
        "写进去的引用没有说到是谁说的：两个属主对同一条说了不同的话就读不出是谁那句"
    );
    assert_eq!(written[0].title, None, "只改档，不该替别的文档改标题");
    assert_eq!(written[0].assertion, None, "只改档，不该替别的文档改正文");
    assert_eq!(
        written[0].source.verdict, None,
        "定档不是判决：做完没有不在这一格里"
    );
    assert_eq!(row.sources.len(), 2, "属主的那一句把账上原来那条文档挤掉了");
}

/// 同一个属主再说一次，是他自己那一格被改写，不是多出一条别人说的。
#[tokio::test]
async fn saying_it_again_rewrites_the_owners_own_citation_instead_of_leaving_two() {
    let identity = cog_core::ChangeIdentity::new("grep", "crates/", "fn a()");
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[("spec-x", cog_core::ChangePriority::Unstated)],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, Some("ferris"));

    for band in ["p1", "p3"] {
        let answer = call(
            &tools,
            "set_change_priority",
            band_args("grep", "crates/", "fn a()", band),
        )
        .await;
        assert_eq!(answer["effective"], band, "{answer}");
    }

    let row = ledger.row(&identity).expect("账上没有这一行");
    assert_eq!(
        row.sources.len(),
        2,
        "说两次留下了两条属主的引用：账上会有两句互相矛盾的话"
    );
    let owner = row
        .sources
        .iter()
        .find(|source| source.doc == "owner:ferris")
        .expect("属主的那一格没了");
    assert_eq!(owner.priority, cog_core::ChangePriority::P3);
    assert_eq!(
        row.priority(),
        cog_core::ChangePriority::P3,
        "另一条文档是 unstated，属主降到 p3 之后这一条就该按 p3 排队"
    );
}

/// 另一条文档要求得更急时，属主说「降下来」不能替他宣布降成了。
///
/// 返回里的 `effective` 是账算出来的那一个，不是他要的那一个；这一句要
/// 说给属主听——否则他以为已经不急了，而派工顺序一点没变。
#[tokio::test]
async fn a_band_the_owner_lowers_cannot_beat_a_document_that_asks_more_urgently() {
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[("spec-p0", cog_core::ChangePriority::P0)],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, Some("ferris"));

    let answer = call(
        &tools,
        "set_change_priority",
        band_args("grep", "crates/", "fn a()", "p2"),
    )
    .await;

    assert_eq!(answer["asked"], "p2", "{answer}");
    assert_eq!(
        answer["effective"], "p0",
        "别家文档还在要 p0，账上这一条没有降到 p2：{answer}"
    );
    assert!(
        answer["note"]
            .as_str()
            .unwrap_or_default()
            .contains("最高的那一条"),
        "没有说清为什么降不下来：{}",
        answer["note"]
    );
}

/// 档位是闭集：闭集外的一个字当场拒绝，而且一个字都不写进账。
#[tokio::test]
async fn a_band_outside_the_closed_set_is_refused_before_anything_is_written() {
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, Some("ferris"));

    let answer = call(
        &tools,
        "set_change_priority",
        band_args("grep", "crates/", "fn a()", "p9"),
    )
    .await;

    assert_eq!(answer["refused"], "unknown_band", "{answer}");
    let note = answer["note"].as_str().unwrap_or_default();
    for band in ["p0", "p1", "p2", "p3", "unstated"] {
        assert!(
            note.contains(band),
            "没说清闭集里有哪些：{}",
            answer["note"]
        );
    }
    assert!(ledger.written().is_empty(), "被拒的档位还是写进了账");
}

/// 账上没有这条身份时，定档不替它新开一行：三段里差一个字就是另一条变更。
#[tokio::test]
async fn a_band_on_a_change_the_account_does_not_hold_does_not_open_a_row() {
    let identity = cog_core::ChangeIdentity::new("grep", "crates/", "fn b()");
    let ledger = BandLedger::holding(Vec::new());
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, Some("ferris"));

    let answer = call(
        &tools,
        "set_change_priority",
        band_args("grep", "crates/", "fn b()", "p0"),
    )
    .await;

    assert_eq!(answer["refused"], "unknown_change", "{answer}");
    assert!(
        ledger.row(&identity).is_none(),
        "被拒的定档在账上新开了一行"
    );
    assert!(ledger.written().is_empty(), "被拒的定档还是写进了账");
}

/// 交出去的那份活，判据取账上那一行，不是执行者自己的回话。
///
/// 这一条同时钉住三样：id 是从身份派生的那一个（读的人据此从账回到产物）、
/// 行挂在项目锚上（否则每条变更都成只有一个任务的项目）、验收点名账上的
/// landed/retired（否则「做完了」由执行者自己说）。
#[tokio::test]
async fn handing_a_change_over_here_carries_the_accounts_own_judgement() {
    let identity = cog_core::ChangeIdentity::new("grep", "crates/", "fn a()");
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[("spec-x", cog_core::ChangePriority::Unstated)],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, None);

    let answer = call(
        &tools,
        "dispatch_change",
        change_args("grep", "crates/", "fn a()"),
    )
    .await;

    assert_eq!(answer["status"], "submitted", "{answer}");
    assert_eq!(answer["generation"], 0, "{answer}");
    let ids = cog_orchestrator::change_ledger::dispatch_task_ids(&identity);
    assert_eq!(answer["task_id"], ids[0], "{answer}");

    let handed = graph.handed();
    assert_eq!(handed.len(), 1, "一次请求交了不止一份活");
    let (goal, rows) = &handed[0];
    assert!(goal.contains("fn a()"), "目标里没有这条变更：{goal}");

    let task = rows
        .iter()
        .find(|task| task.id == ids[0])
        .expect("交出去的行里没有那条任务");
    let named = &task.input[cog_core::INPUT_CHANGE_IDENTITY];
    assert_eq!(named["kind"], "grep", "{named}");
    assert_eq!(named["scope"], "crates/", "{named}");
    assert_eq!(named["literal"], "fn a()", "{named}");

    let parent = task.parent_task_id.as_deref().expect("没有挂在项目锚上");
    let anchor = rows
        .iter()
        .find(|row| row.id == parent)
        .expect("父任务不在交出去的行里");
    assert!(!anchor.is_executable, "项目锚是可执行的：队列会去跑它");
    assert_eq!(anchor.goal_id, task.goal_id, "锚与变更不在同一个项目里");

    let acceptance = task.acceptance.join(" ");
    assert!(
        acceptance.contains("landed 或 retired") && acceptance.contains("账"),
        "判据没有点账上那一行：{acceptance}"
    );
}

/// 已了结的行不派；图上已经有这一行时，再说一次不会多派一份活。
///
/// 「交过了」与「刚交的」是两件事：图收下的 id 再交一次是幂等空操作，
/// 报成「刚交」等于告诉属主一份活正在跑，而它可能早就死在那代上了。这里也
/// **不**换一代再交——开新的一代是派工环按「失败是环境的」做的判断，一段
/// 对话看不到那个分类，按一下开一代就是同一条变更烧好几份钱。
#[tokio::test]
async fn a_settled_row_is_not_handed_over_and_a_row_already_on_the_graph_is_named() {
    let identity = cog_core::ChangeIdentity::new("grep", "crates/", "fn a()");
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Landed,
        &[],
    )]);
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(Some(ledger.clone()), &graph, None, None);

    let answer = call(
        &tools,
        "dispatch_change",
        change_args("grep", "crates/", "fn a()"),
    )
    .await;
    assert_eq!(answer["refused"], "already_settled", "{answer}");
    assert!(graph.handed().is_empty(), "了结的行还是被派出去了");

    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[],
    )]);
    let graph = HandoverGraph::dispatching();
    let ids = cog_orchestrator::change_ledger::dispatch_task_ids(&identity);
    graph.hold(&ids[0]);
    let tools = direction_tools(Some(ledger), &graph, None, None);

    let answer = call(
        &tools,
        "dispatch_change",
        change_args("grep", "crates/", "fn a()"),
    )
    .await;
    assert_eq!(answer["status"], "already_held", "{answer}");
    assert_eq!(answer["task_id"], ids[0], "没有点名图上那一行：{answer}");
    assert_eq!(answer["generation"], 0, "{answer}");
    assert!(graph.handed().is_empty(), "图上已经有的那一行又被交了一份");
    assert!(
        answer["note"]
            .as_str()
            .unwrap_or_default()
            .contains("retry_task"),
        "没有说清想重跑那一次该用哪一只：{}",
        answer["note"]
    );
}

/// 交一条变更也是一次派活：额度用尽时它交不出去，而且与提交面写同一格。
///
/// 少这一道，同一个人在提交面上已经派不了活，在聊天框里点一下「把这条交出去」
/// 却还能派出——两条路要的是同一份钱，准入上却看不见，拒绝也没落账。
#[tokio::test]
async fn a_hand_over_over_the_day_s_quota_is_refused_and_booked_like_the_submit_face() {
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[],
    )]);
    let graph = HandoverGraph::dispatching();
    let audit = Arc::new(RecordingAudit::default());
    let admission: Arc<dyn HierarchyManager> = Arc::new(Exhausted);
    let stream: Arc<dyn AuditStream> = audit.clone();
    let tools = change_direction_tools(
        Some(ledger as Arc<dyn cog_core::ChangeLedgerStore>),
        graph.clone(),
        None,
        Some(admission),
        Some(stream),
        Some("ferris".into()),
        "luna-1822ab0e".into(),
    );

    let answer = call(
        &tools,
        "dispatch_change",
        change_args("grep", "crates/", "fn a()"),
    )
    .await;

    assert_eq!(answer["status"], "refused", "{answer}");
    assert!(
        answer["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("额度"),
        "{}",
        answer["reason"]
    );
    assert!(graph.handed().is_empty(), "被额度挡下还是派出去了");
    assert_eq!(
        audit.actions(),
        vec!["quota.dispatch_rejected".to_string()],
        "这一次拒绝没有写进审计链"
    );
    assert_eq!(
        audit.actors(),
        vec!["luna-1822ab0e".to_string()],
        "拒绝记在「用这个界面的人」头上，不是这个员工"
    );
}

/// 不按变更派工的控制面要它自己说，工具不许替它编一个 id。
#[tokio::test]
async fn a_control_plane_that_does_not_dispatch_by_change_says_so() {
    let ledger = BandLedger::holding(vec![ledger_row(
        "grep",
        "crates/",
        "fn a()",
        cog_core::ChangeState::Pending,
        &[],
    )]);
    let graph = Arc::new(HandoverGraph::default());
    let tools = direction_tools(Some(ledger), &graph, None, None);

    let answer = call(
        &tools,
        "dispatch_change",
        change_args("grep", "crates/", "fn a()"),
    )
    .await;

    assert_eq!(answer["refused"], "not_dispatched_here", "{answer}");
    assert!(
        graph.handed().is_empty(),
        "这个控制面根本没接，却交了一份活"
    );
}

/// 属主的三只开关只在**这一段连接带登录身份**时注册。
///
/// 账上的一行引用要写「这是谁说的」，一条没有人的连接答不出这一问；给它记
/// 一行，账上就多一条没有人的决定。交出去与派活同级，任何一段对话都能做。
#[test]
fn the_owners_switches_exist_only_when_a_person_is_on_the_connection() {
    let graph = HandoverGraph::dispatching();
    let names =
        |tools: Vec<Tool>| -> Vec<String> { tools.into_iter().map(|tool| tool.name).collect() };

    let anonymous = names(direction_tools(None, &graph, None, None));
    assert_eq!(anonymous, vec!["dispatch_change".to_string()]);

    let signed_in = names(direction_tools(None, &graph, None, Some("ferris")));
    assert_eq!(
        signed_in,
        vec![
            "dispatch_change".to_string(),
            "set_change_priority".to_string(),
            "approve_change".to_string(),
            "rollback_change".to_string(),
        ],
        "方向盘是闭集：加一只就要在这里说清它改的是什么"
    );
}

/// 没有装配进化控制面时说「这里做不了」，不能说成「没有待批的变更」。
///
/// 两个读数要做的事相反：一个是去找谁没装，一个是去批。把前者说成后者，
/// 属主会以为队列是空的，而其实这个进程根本没有那张面。
#[tokio::test]
async fn an_absent_evolution_plane_says_it_cannot_rather_than_nothing_to_approve() {
    let graph = HandoverGraph::dispatching();
    let tools = direction_tools(None, &graph, None, Some("ferris"));

    for (name, args) in [
        ("approve_change", serde_json::json!({ "change_id": "c1" })),
        ("rollback_change", serde_json::json!({ "why": "刚才那笔" })),
    ] {
        let answer = call(&tools, name, args).await;
        assert_eq!(answer["available"], false, "{name}: {answer}");
        assert!(
            answer["note"]
                .as_str()
                .unwrap_or_default()
                .contains("做不了"),
            "{name} 把缺席说成了别的什么：{}",
            answer["note"]
        );
    }
}

/// 放行失败照实说不等于已放行，也不重试同一次。
#[tokio::test]
async fn an_approval_that_did_not_happen_is_reported_as_not_done_and_tried_once() {
    let graph = HandoverGraph::dispatching();
    let admin = DecidingAdmin::refusing();
    let tools = direction_tools(None, &graph, Some(admin.clone()), Some("ferris"));

    let answer = call(
        &tools,
        "approve_change",
        serde_json::json!({ "change_id": "c1" }),
    )
    .await;

    assert_eq!(answer["approved"], false, "{answer}");
    assert!(
        answer["note"]
            .as_str()
            .unwrap_or_default()
            .contains("没有放行"),
        "{}",
        answer["note"]
    );
    assert_eq!(
        admin.approvals(),
        vec!["c1".to_string()],
        "同一次放行被重试了"
    );
}

/// 属主放行一条等审批的变更、把线退回上一版：两条都落到控制面上。
#[tokio::test]
async fn an_owner_can_let_a_held_change_through_and_can_take_the_line_back() {
    let graph = HandoverGraph::dispatching();
    let admin = DecidingAdmin::allowing();
    let tools = direction_tools(None, &graph, Some(admin.clone()), Some("ferris"));

    let answer = call(
        &tools,
        "approve_change",
        serde_json::json!({ "change_id": "c1" }),
    )
    .await;
    assert_eq!(answer["approved"], true, "{answer}");
    assert_eq!(answer["change_id"], "c1", "{answer}");
    assert_eq!(answer["commit"], "cafe1234", "{answer}");
    assert_eq!(answer["switched"], true, "{answer}");
    assert_eq!(admin.approvals(), vec!["c1".to_string()]);

    let answer = call(
        &tools,
        "rollback_change",
        serde_json::json!({ "why": "刚才那次构建红了" }),
    )
    .await;
    assert_eq!(answer["rolled_back"], true, "{answer}");
    assert_eq!(answer["message"], "back on the previous image", "{answer}");
    assert_eq!(admin.rollbacks(), 1);
}
