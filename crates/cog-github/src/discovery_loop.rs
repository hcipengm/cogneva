//! GitHub discovery loop — the autonomous sensor loop from the design doc.
//!
//! Each round: scan issues → rebuild/refresh conversations → triage →
//! either submit a `github_issue_fix` task to the orchestrator, post a
//! clarification question, escalate, or skip. Then poll tracked PRs and
//! record outcomes into the reflection engine.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::GitHubIntegrationConfig;
use cog_core::{
    ActionPlannerMeta, ActionPlannerSource, OrchestratorControl, Task, TaskStatus, TaskType,
};

use crate::conversation::{ConversationState, IssueConversation};
use crate::discovery::IssueDiscovery;
use crate::error::Result;
use crate::outcome_recorder::OutcomeRecorder;
use crate::provider::{CiFailureEvent, CodePlatformProvider, PlatformIssue};
use crate::triage::{IssueTriage, TriageDecision};

/// DAG-side timeout for a single assess task (must exceed the wait budget
/// below so the poller gives up around the same time the task would be killed).
const ASSESS_TASK_TIMEOUT_SECS: u64 = 120;
/// How long the discovery loop blocks waiting for an assess verdict before it
/// gives up and falls back to the local heuristic this round.
const ASSESS_WAIT_TIMEOUT_SECS: u64 = 100;
/// Poll interval while waiting for the assess task to complete.
const ASSESS_POLL_INTERVAL_MS: u64 = 500;

/// The autonomous GitHub sensor loop.
pub struct GitHubDiscoveryLoop {
    provider: Arc<dyn CodePlatformProvider>,
    triage: IssueTriage,
    config: GitHubIntegrationConfig,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    reflection: Option<Arc<dyn cog_core::ReflectionEngine>>,
    discovery: IssueDiscovery,
    /// Conversation state keyed by `"<kind>:<number>"` (e.g. `"issue:7"`,
    /// `"pr:12"`). The kind prefix keeps issue and PR number spaces apart
    /// (on Gitee they are independent sequences).
    conversations: HashMap<String, IssueConversation>,
    recorder: OutcomeRecorder,
    /// Intent guard keys (`"<kind>:<number>"`) that already produced a task
    /// this process lifetime — covers both issues and PRs.
    submitted: std::collections::HashSet<String>,
    /// Intent guard keys with a bot clarification question already posted and
    /// no substantive user reply seen yet. In-memory guard so concurrent
    /// triggers (webhook + polling, or the bot's own comment event arriving
    /// before the just-posted comment is re-read) cannot post a second
    /// question.
    awaiting_clarification: std::collections::HashSet<String>,
    /// CI run ids that already produced a fix task this process lifetime.
    ci_submitted: std::collections::HashSet<u64>,
    /// CI run ids seen by the polling fallback. `None` until the first poll,
    /// which adopts all currently failed runs without submitting (so a pod
    /// restart does not resubmit old failures).
    ci_seen: Option<std::collections::HashSet<u64>>,
}

/// Which external-intent surface a conversation belongs to. Issues and PRs
/// are equal intent entry points and share the same actionability logic; the
/// kind only selects the comment endpoints and the fix-task flavor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntentKind {
    Issue,
    Pr,
}

impl IntentKind {
    fn as_str(self) -> &'static str {
        match self {
            IntentKind::Issue => "issue",
            IntentKind::Pr => "pr",
        }
    }

    /// Guard-map key, namespaced so issue and PR number spaces never collide.
    fn key(self, number: u64) -> String {
        format!("{}:{number}", self.as_str())
    }
}

/// Borrowed identifying + descriptive fields of one external intent (issue or
/// PR) that needs an actionability verdict. Bundled so the judge/assess path
/// takes a single argument instead of a long parameter list.
struct IntentContext<'a> {
    kind: IntentKind,
    number: u64,
    title: &'a str,
    body: &'a str,
    labels: &'a [String],
    author: &'a str,
}

impl IntentContext<'_> {
    /// Materialize the cross-crate issue view the local rules/heuristic read.
    /// PRs are judged as issues with a `[PR]` title prefix, so the local path
    /// reuses the same shape.
    fn to_platform_issue(&self) -> PlatformIssue {
        PlatformIssue {
            number: self.number,
            title: self.title.to_string(),
            body: self.body.to_string(),
            state: "open".to_string(),
            labels: self.labels.to_vec(),
            author: self.author.to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

impl GitHubDiscoveryLoop {
    /// Create a loop. `orchestrator` / `reflection` are optional: without
    /// them the loop still scans and clarifies but cannot submit tasks or
    /// record outcomes.
    pub fn new(
        provider: Arc<dyn CodePlatformProvider>,
        triage: IssueTriage,
        config: GitHubIntegrationConfig,
        orchestrator: Option<Arc<dyn OrchestratorControl>>,
        reflection: Option<Arc<dyn cog_core::ReflectionEngine>>,
    ) -> Self {
        Self {
            provider,
            triage,
            config,
            orchestrator,
            reflection,
            discovery: IssueDiscovery::new(),
            conversations: HashMap::new(),
            recorder: OutcomeRecorder::new(),
            submitted: std::collections::HashSet::new(),
            ci_submitted: std::collections::HashSet::new(),
            awaiting_clarification: std::collections::HashSet::new(),
            ci_seen: None,
        }
    }

    /// Register a PR created for a change so its outcome is recorded.
    pub fn track_pr(&mut self, pr_number: u64, change_id: impl Into<String>) {
        self.recorder.track(pr_number, change_id);
    }

    /// Run one discovery round. Returns the number of issues scanned.
    pub async fn run_once(&mut self) -> Result<usize> {
        let issues = self
            .discovery
            .scan(self.provider.as_ref(), &self.config)
            .await?;
        let scanned = issues.len();

        for issue in issues {
            if let Err(e) = self.process_issue(&issue).await {
                tracing::warn!(
                    issue = issue.number,
                    error = %e,
                    "Failed to process discovered issue"
                );
            }
        }

        // PR 与 issue 同为外部意图入口：PR 未必带解法，可能只是需求描述。
        match self.provider.list_open_pull_requests().await {
            Ok(prs) => {
                for pr in prs {
                    if let Err(e) = self.process_pr(&pr).await {
                        tracing::warn!(
                            pr = pr.number,
                            error = %e,
                            "Failed to process discovered pull request"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list open pull requests");
            }
        }

        if let Some(ref reflection) = self.reflection {
            if let Err(e) = self
                .recorder
                .poll_once(self.provider.as_ref(), reflection.as_ref())
                .await
            {
                tracing::warn!(error = %e, "Outcome polling failed");
            }
        }

        self.poll_ci_failures().await;

        Ok(scanned)
    }

    /// Polling fallback for CI failure detection when the webhook endpoint is
    /// not publicly reachable. The first poll after boot only records the
    /// currently failed runs; later polls submit fix tasks for new failures.
    async fn poll_ci_failures(&mut self) {
        let events = self
            .provider
            .list_recent_ci_failures(20)
            .await
            .unwrap_or_default();

        match self.ci_seen {
            None => {
                self.ci_seen = Some(events.iter().map(|e| e.run_id).collect());
                tracing::debug!(
                    adopted = self.ci_seen.as_ref().map_or(0, |s| s.len()),
                    "CI polling adopted pre-existing failed runs"
                );
            }
            Some(ref mut seen) => {
                let fresh: Vec<_> = events
                    .into_iter()
                    .filter(|e| seen.insert(e.run_id))
                    .collect();
                for event in fresh {
                    if let Err(e) = self.process_ci_failure(event).await {
                        tracing::warn!(error = %e, "CI failure processing failed");
                    }
                }
            }
        }
    }

    /// Run forever with the configured poll interval.
    pub async fn run(&mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let interval = std::time::Duration::from_secs(self.config.poll_interval_secs.max(30));
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::info!("GitHub discovery loop shutting down");
                    return;
                }
                _ = tokio::time::sleep(interval) => {}
            }
            if let Err(e) = self.run_once().await {
                tracing::warn!(error = %e, "GitHub discovery round failed");
            }
        }
    }

    /// 处理单个 issue 事件（webhook 驱动）：拉取 issue 并走完整处理管线。
    /// 返回是否实际处理了该 issue（找不到时返回 false）。
    pub async fn process_issue_event(&mut self, issue_number: u64) -> Result<bool> {
        match self.provider.get_issue(issue_number).await {
            Ok(issue) => {
                self.process_issue(&issue).await?;
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(issue = issue_number, error = %e, "Webhook event: issue not fetchable");
                Ok(false)
            }
        }
    }

    /// Whether a webhook event is the bot's OWN comment on an issue. Such
    /// events must not retrigger issue processing — the bot answering itself
    /// is what caused duplicate clarification in production.
    pub fn is_self_comment_event(
        &self,
        is_gitee: bool,
        event: &str,
        action: &str,
        payload: &serde_json::Value,
    ) -> bool {
        let (body, author) = if is_gitee {
            // Gitee Note Hook on an issue: payload.note.body / .user.login.
            let ev = event.trim_end_matches(" Hook").to_ascii_lowercase();
            let is_note = ev == "note" && payload["noteable_type"].as_str() == Some("Issue");
            if !is_note {
                return false;
            }
            let note = payload
                .get("note")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (
                note["body"].as_str().unwrap_or_default().to_string(),
                note["user"]["login"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        } else {
            if !(event == "issue_comment" && action == "created") {
                return false;
            }
            (
                payload["comment"]["body"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                payload["comment"]["user"]["login"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        };

        if body.is_empty() && author.is_empty() {
            return false;
        }
        // 机器人自己发的评论：要么正文带机器人签名（token 属于人类账号时
        // 作者是人类用户名），要么作者就是配置的行动账号。
        let sig = &self.config.conversation.bot_signature;
        let signed = !sig.is_empty() && body.trim_end().ends_with(sig.as_str());
        let actor = self.config.primary_account().ok().map(|a| a.username());
        let self_actor = actor.is_some_and(|u| !u.is_empty() && u == author)
            || (!self.config.bot_identity.username.is_empty()
                && self.config.bot_identity.username == author);
        signed || self_actor
    }

    /// 处理 CI 失败事件（webhook 驱动）：拉取失败 job 日志尾部并提交
    /// `github_ci_fix` 修复任务。重复 run 或无法提交时返回 false。
    pub async fn process_ci_failure(&mut self, event: CiFailureEvent) -> Result<bool> {
        if self.ci_submitted.contains(&event.run_id) {
            tracing::debug!(
                run_id = event.run_id,
                "CI failure already submitted; skipping"
            );
            return Ok(false);
        }

        let Some(ref orchestrator) = self.orchestrator else {
            tracing::warn!(
                run_id = event.run_id,
                "Orchestrator not available; cannot submit github_ci_fix task"
            );
            return Ok(false);
        };

        let logs = self
            .provider
            .fetch_ci_failure_logs(event.run_id)
            .await
            .unwrap_or_default();

        let log_section = logs
            .iter()
            .map(|l| {
                format!(
                    "### Job: {} (id {})\n```\n{}\n```",
                    l.job_name, l.job_id, l.log_tail
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        let log_section = if log_section.is_empty() {
            "(logs unavailable)".to_string()
        } else {
            log_section
        };

        let goal = format!(
            "Fix CI failure in workflow \"{}\" run {} (branch {}, commit {})\n\nFailed job logs:\n{}",
            event.workflow_name, event.run_id, event.head_branch, event.head_sha, log_section
        );

        let task = Task::new(
            format!("github-ci-run-{}", event.run_id),
            TaskType::Custom("github_ci_fix".into()),
            serde_json::json!({
                "goal": goal,
                "run_id": event.run_id,
                "workflow_name": event.workflow_name,
                "head_sha": event.head_sha,
                "head_branch": event.head_branch,
                "run_url": event.html_url,
                "failed_jobs": logs
                    .iter()
                    .map(|l| serde_json::json!({
                        "job_id": l.job_id,
                        "job_name": l.job_name,
                        "log_tail": l.log_tail,
                    }))
                    .collect::<Vec<_>>(),
                "evolution_mode": "generate_change",
            }),
        );

        let task_ids = orchestrator
            .submit_goal_auto(&goal, vec![task])
            .await
            .map_err(|e| crate::error::CogGitHubError::Provider(e.to_string()))?;
        tracing::info!(
            run_id = event.run_id,
            workflow = %event.workflow_name,
            tasks = ?task_ids,
            "Submitted github_ci_fix task to orchestrator"
        );
        self.ci_submitted.insert(event.run_id);
        Ok(true)
    }

    async fn process_issue(&mut self, issue: &PlatformIssue) -> Result<()> {
        let kind = IntentKind::Issue;
        let key = kind.key(issue.number);

        // Rebuild conversation state from platform comments and run the shared
        // state-machine guards (stale / awaiting / already submitted). `None`
        // means this round needs no action.
        let Some(mut conversation) = self.prepare_conversation(kind, issue.number).await? else {
            return Ok(());
        };

        // Judge actionability semantically from the whole thread plus any
        // attached screenshots/recordings (fed as real media blocks via the
        // multimodal assess task; falls back to the local rules heuristic when
        // no orchestrator/LLM is available).
        let intent = IntentContext {
            kind,
            number: issue.number,
            title: &issue.title,
            body: &issue.body,
            labels: &issue.labels,
            author: &issue.author,
        };
        let decision = self.judge_intent(&intent, &conversation).await;

        let is_fix = self
            .act_on_decision(kind, issue.number, &key, decision, &mut conversation)
            .await?;
        if is_fix && self.config.auto_create_pr {
            self.submit_fix_task(issue, &conversation).await?;
            self.submitted.insert(key.clone());
        }

        self.conversations.insert(key, conversation);
        Ok(())
    }

    /// Pull the comment thread for the intent and run the shared conversation
    /// state-machine guards. Returns the live conversation when actionability
    /// should be judged this round, or `None` when the intent is stale, already
    /// awaiting a reply, or already acted on (in which cases state is persisted
    /// and no further work is needed).
    async fn prepare_conversation(
        &mut self,
        kind: IntentKind,
        number: u64,
    ) -> Result<Option<IssueConversation>> {
        let comments = match kind {
            IntentKind::Issue => self.provider.list_issue_comments(number).await,
            IntentKind::Pr => self.provider.list_pull_comments(number).await,
        }
        .unwrap_or_default();
        let key = kind.key(number);
        let conversation = IssueConversation::from_comments(
            number,
            &comments,
            &self.config.bot_identity.username,
            &self.config.conversation,
        );

        match conversation.state {
            ConversationState::Stale => {
                tracing::info!(%key, "Conversation stale; skipping");
                self.conversations.insert(key, conversation);
                return Ok(None);
            }
            ConversationState::AwaitingClarification => {
                // Still waiting for a substantive reply; arm the in-memory guard
                // so a concurrent trigger (webhook + polling, or the bot's own
                // comment event arriving before the just-posted comment is
                // re-read) cannot post a second question.
                self.awaiting_clarification.insert(key.clone());
                self.conversations.insert(key, conversation);
                return Ok(None);
            }
            ConversationState::UserReplied => {
                // The reporter sent a new turn (text, screenshot, or link).
                // Release the guard so the judge can re-decide actionability
                // from the full thread; a further question (if still unclear)
                // is allowed subject to the round budget.
                self.awaiting_clarification.remove(&key);
            }
            _ => {}
        }

        // Already acted on this intent and the reporter only replied afterwards:
        // don't re-submit.
        if conversation.state == ConversationState::UserReplied && self.submitted.contains(&key) {
            self.conversations.insert(key, conversation);
            return Ok(None);
        }

        Ok(Some(conversation))
    }

    /// Decide actionability for one external intent. With an orchestrator wired,
    /// a lightweight multimodal `platform_intent_assess` task reads the body,
    /// the whole comment thread, and any fetched media blocks and returns a
    /// semantic verdict (fix / clarify / skip / escalate). Without an
    /// orchestrator, or when the assess task fails, this degrades to the local
    /// model-free rules heuristic so the loop stays autonomous.
    async fn judge_intent(
        &self,
        intent: &IntentContext<'_>,
        conversation: &IssueConversation,
    ) -> TriageDecision {
        let key = intent.kind.key(intent.number);
        let issue = intent.to_platform_issue();

        // Deterministic label rules short-circuit before any model call: a
        // forbidden/human-required label is certain, must not spend an assess
        // round-trip, and must not depend on the model honoring it.
        if let Some(decision) = IssueTriage::rules_decision(&issue, &self.config) {
            return decision;
        }

        let reply_thread = conversation.triage_context();
        let media = self.fetch_media_blocks(conversation).await;

        if let Some(orchestrator) = &self.orchestrator {
            match self
                .run_assess_task(orchestrator, intent, &reply_thread, &media)
                .await
            {
                Ok(decision) => {
                    tracing::debug!(%key, "actionability verdict from multimodal assess task");
                    return decision;
                }
                Err(e) => {
                    tracing::warn!(
                        %key,
                        error = %e,
                        "assess task failed; falling back to local rules heuristic"
                    );
                }
            }
        } else {
            tracing::info!(%key, "no orchestrator; judging actionability with local rules heuristic");
        }

        // No-LLM fallback: rules plus a readable-text heuristic. The label
        // rules already short-circuited above, so re-running them here only
        // repeats a cheap, side-effect-free check. Media cannot be read on this
        // path, so a screenshot-only thread still asks the reporter for text.
        self.triage
            .evaluate(&issue, &self.config, &reply_thread)
            .await
    }

    /// Download the intent's media attachments through the zero-credential
    /// gateway proxy and encode them as base64 content blocks for the assessor.
    /// Failures are logged and skipped — a missing attachment never blocks the
    /// text-only judgement.
    async fn fetch_media_blocks(&self, conversation: &IssueConversation) -> Vec<serde_json::Value> {
        use base64::Engine as _;
        let mut blocks = Vec::new();
        for url in conversation.media_urls() {
            match self.provider.fetch_attachment(&url).await {
                Ok(att) => {
                    let data_base64 = base64::engine::general_purpose::STANDARD.encode(&att.bytes);
                    blocks.push(serde_json::json!({
                        "data_base64": data_base64,
                        "mime_type": att.mime_type,
                        "url": url,
                    }));
                }
                Err(e) => {
                    tracing::warn!(
                        url = %url,
                        error = %e,
                        "Failed to fetch attachment; judging text-only"
                    );
                }
            }
        }
        blocks
    }

    /// Submit the one-shot multimodal assess task and block (poll the DAG) until
    /// its JSON verdict rides back on `task.result`, then map it to a
    /// [`TriageDecision`]. The task is injected `verified` so it bypasses
    /// ActionPlanner decomposition and routes straight to the single-agent
    /// assessor.
    async fn run_assess_task(
        &self,
        orchestrator: &Arc<dyn OrchestratorControl>,
        intent: &IntentContext<'_>,
        reply_thread: &str,
        media: &[serde_json::Value],
    ) -> Result<TriageDecision> {
        let kind = intent.kind;
        let number = intent.number;
        // Unique per submission so a re-judgement after a user reply does not
        // collide with an already-completed task id.
        let task_id = format!(
            "intent-assess-{}-{}-{}",
            kind.as_str(),
            number,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        );
        let goal = format!(
            "Assess actionability of {} #{}: {}",
            kind.as_str(),
            number,
            intent.title
        );
        let input = serde_json::json!({
            "kind": kind.as_str(),
            "number": number,
            "title": intent.title,
            "body": intent.body,
            "labels": intent.labels,
            "author": intent.author,
            "reply_thread": reply_thread,
            "media": media,
        });
        let mut task = Task::new(
            task_id.clone(),
            TaskType::Custom("platform_intent_assess".into()),
            input,
        );
        task.action_planner_meta = Some(ActionPlannerMeta {
            verified: true,
            version: Some("1.0.0".into()),
            note: Some("Multimodal actionability verdict; route to single-agent assessor".into()),
            source: Some(ActionPlannerSource::UserProvided),
            confidence: None,
            timestamp: Some(chrono::Utc::now()),
        });
        task.timeout_seconds = ASSESS_TASK_TIMEOUT_SECS;

        let ids = orchestrator
            .submit_goal_auto(&goal, vec![task])
            .await
            .map_err(|e| crate::error::CogGitHubError::Provider(e.to_string()))?;
        let id = ids.into_iter().next().unwrap_or(task_id);

        let deadline = Instant::now() + Duration::from_secs(ASSESS_WAIT_TIMEOUT_SECS);
        loop {
            if let Some(t) = orchestrator.get_task(&id).await {
                match t.status {
                    TaskStatus::Completed => {
                        let verdict = t.result.unwrap_or_default();
                        return Ok(Self::verdict_to_decision(&verdict));
                    }
                    TaskStatus::Failed => {
                        return Err(crate::error::CogGitHubError::Provider(format!(
                            "assess task {id} failed: {}",
                            t.error.unwrap_or_default()
                        )));
                    }
                    TaskStatus::Cancelled => {
                        return Err(crate::error::CogGitHubError::Provider(format!(
                            "assess task {id} cancelled"
                        )));
                    }
                    _ => {}
                }
            }
            if Instant::now() >= deadline {
                return Err(crate::error::CogGitHubError::Provider(format!(
                    "assess task {id} timed out after {ASSESS_WAIT_TIMEOUT_SECS}s"
                )));
            }
            tokio::time::sleep(Duration::from_millis(ASSESS_POLL_INTERVAL_MS)).await;
        }
    }

    /// Map the assessor's JSON verdict (`{decision, question, priority, reason}`)
    /// onto the internal triage decision. Unknown/absent decisions degrade to
    /// Skip so a malformed verdict never forces an unwanted fix.
    fn verdict_to_decision(v: &serde_json::Value) -> TriageDecision {
        let reason = v
            .get("reason")
            .and_then(|r| r.as_str())
            .unwrap_or("")
            .to_string();
        let question = v
            .get("question")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .to_string();
        let priority = v.get("priority").and_then(|p| p.as_u64()).unwrap_or(3) as u8;
        match v.get("decision").and_then(|d| d.as_str()).unwrap_or("") {
            "fix" => TriageDecision::Fix {
                priority,
                rationale: reason,
            },
            "clarify" => TriageDecision::AskForClarification {
                question: if question.trim().is_empty() {
                    "Could you share the expected vs actual behavior and the steps to reproduce?"
                        .into()
                } else {
                    question
                },
            },
            "escalate" => TriageDecision::EscalateHuman { reason },
            other => TriageDecision::Skip {
                reason: if reason.trim().is_empty() {
                    format!("assessor decision: {other:?}")
                } else {
                    reason
                },
            },
        }
    }

    /// Apply the side effects of a triage decision: post exactly one
    /// clarification (guarded against duplicates and bounded by the round
    /// budget), log skip/escalate, and clear the awaiting guard on a fix.
    /// Returns `true` when the caller should submit the fix/intent task; the
    /// caller sets the `submitted` guard only after the submit succeeds.
    async fn act_on_decision(
        &mut self,
        kind: IntentKind,
        number: u64,
        key: &str,
        decision: TriageDecision,
        conversation: &mut IssueConversation,
    ) -> Result<bool> {
        match decision {
            TriageDecision::Fix {
                priority,
                rationale,
            } => {
                tracing::info!(
                    %key,
                    priority,
                    rationale = %rationale,
                    "Actionability judge: act now"
                );
                self.awaiting_clarification.remove(key);
                Ok(true)
            }
            TriageDecision::AskForClarification { question } => {
                // Hard in-memory guard: a question is already outstanding and no
                // new user turn has arrived — never post a duplicate, even if
                // the comment re-read raced or a webhook+poll fired together.
                // A further question after a real reply is bounded by
                // max_clarification_rounds (conversation.ask).
                if self.awaiting_clarification.contains(key) {
                    tracing::info!(%key, "Clarification already outstanding; not re-asking");
                    return Ok(false);
                }
                if let Some(body) = conversation.ask(&question, &self.config.conversation) {
                    let posted = self.post_clarification(kind, number, &body).await?;
                    if posted {
                        self.awaiting_clarification.insert(key.to_string());
                    }
                    tracing::info!(%key, posted, "Posted clarification question");
                }
                Ok(false)
            }
            TriageDecision::EscalateHuman { reason } => {
                tracing::warn!(%key, reason = %reason, "Escalated to human");
                Ok(false)
            }
            TriageDecision::Skip { reason } => {
                tracing::debug!(%key, reason = %reason, "Skipped");
                Ok(false)
            }
        }
    }

    /// Post a clarification comment to the right surface: issues use the issue
    /// endpoint, PRs use the pull-request endpoint (Gitee serves these on
    /// different paths; GitHub treats a PR as an issue).
    async fn post_clarification(&self, kind: IntentKind, number: u64, body: &str) -> Result<bool> {
        if !self.config.conversation.auto_reply {
            tracing::info!(
                kind = kind.as_str(),
                number,
                "auto_reply disabled; clarification question not posted"
            );
            return Ok(false);
        }
        match kind {
            IntentKind::Issue => {
                self.provider
                    .comment_on_issue(number, body.to_string())
                    .await?
            }
            IntentKind::Pr => {
                self.provider
                    .comment_on_pull(number, body.to_string())
                    .await?
            }
        }
        Ok(true)
    }

    async fn submit_fix_task(
        &self,
        issue: &PlatformIssue,
        conversation: &IssueConversation,
    ) -> Result<()> {
        let Some(ref orchestrator) = self.orchestrator else {
            tracing::warn!(
                issue = issue.number,
                "Orchestrator not available; cannot submit github_issue_fix task"
            );
            return Ok(());
        };

        let history: Vec<serde_json::Value> = conversation
            .turns
            .iter()
            .map(|t| {
                serde_json::json!({
                    "role": match t.role {
                        crate::conversation::ConversationRole::User => "user",
                        crate::conversation::ConversationRole::Bot => "bot",
                    },
                    "body": t.body,
                    "created_at": t.created_at,
                })
            })
            .collect();

        let goal = format!(
            "Fix GitHub issue #{}: {}\n\n{}",
            issue.number, issue.title, issue.body
        );

        let task = Task::new(
            format!("github-issue-{}", issue.number),
            TaskType::Custom("github_issue_fix".into()),
            serde_json::json!({
                "goal": goal,
                "issue_number": issue.number,
                "issue_title": issue.title,
                "issue_body": issue.body,
                "issue_labels": issue.labels,
                "issue_url": format!("https://github.com/{}/issues/{}", self.config.repo, issue.number),
                "conversation": history,
                "evolution_mode": "generate_change",
            }),
        );

        let task_ids = orchestrator
            .submit_goal_auto(&goal, vec![task])
            .await
            .map_err(|e| crate::error::CogGitHubError::Provider(e.to_string()))?;
        tracing::info!(
            issue = issue.number,
            tasks = ?task_ids,
            "Submitted github_issue_fix task to orchestrator"
        );
        Ok(())
    }

    /// PR 意图处理：与 issue 同走 triage → orchestrator 主流程，但先做
    /// 自循环防护——自家进化流水线产的 PR（`cogneva/` 分支前缀或机器人
    /// 署名）绝不能回流成新意图，否则无限自我增殖。
    async fn process_pr(&mut self, pr: &crate::provider::PlatformPullRequest) -> Result<()> {
        if pr.head_branch.starts_with("cogneva/") || pr.author == self.config.bot_identity.username
        {
            tracing::debug!(pr = pr.number, "Skipping self-produced PR");
            return Ok(());
        }
        let kind = IntentKind::Pr;
        let key = kind.key(pr.number);
        if self.submitted.contains(&key) {
            return Ok(());
        }
        if pr
            .labels
            .iter()
            .any(|l| self.config.forbidden_labels.contains(l))
        {
            tracing::info!(pr = pr.number, "PR carries forbidden label; skipping");
            self.submitted.insert(key);
            return Ok(());
        }

        // PRs and issues are equal external-intent entry points: same comment
        // thread rebuild, same state-machine guards, same semantic actionability
        // judge (multimodal assess task with local heuristic fallback). PRs can
        // also receive a clarification question when the intent is unclear.
        let Some(mut conversation) = self.prepare_conversation(kind, pr.number).await? else {
            return Ok(());
        };

        let title = format!("[PR] {}", pr.title);
        let intent = IntentContext {
            kind,
            number: pr.number,
            title: &title,
            body: &pr.body,
            labels: &pr.labels,
            author: &pr.author,
        };
        let decision = self.judge_intent(&intent, &conversation).await;

        let is_fix = self
            .act_on_decision(kind, pr.number, &key, decision, &mut conversation)
            .await?;
        if is_fix && self.config.auto_create_pr {
            self.submit_pr_intent_task(pr).await?;
            self.submitted.insert(key.clone());
        }

        self.conversations.insert(key, conversation);
        Ok(())
    }

    async fn submit_pr_intent_task(&self, pr: &crate::provider::PlatformPullRequest) -> Result<()> {
        let Some(ref orchestrator) = self.orchestrator else {
            tracing::warn!(
                pr = pr.number,
                "Orchestrator not available; cannot submit platform_pr_intent task"
            );
            return Ok(());
        };

        let goal = format!(
            "Evaluate and realize the intent of pull request #{}: {}\n\n{}\n\n\
             The PR may or may not contain a solution. Assess the intent, \
             decide the right end state, and implement it.",
            pr.number, pr.title, pr.body
        );

        let task = Task::new(
            format!("platform-pr-{}", pr.number),
            TaskType::Custom("platform_pr_intent".into()),
            serde_json::json!({
                "goal": goal,
                "pr_number": pr.number,
                "pr_title": pr.title,
                "pr_body": pr.body,
                "pr_labels": pr.labels,
                "pr_url": pr.url,
                "evolution_mode": "generate_change",
            }),
        );

        let task_ids = orchestrator
            .submit_goal_auto(&goal, vec![task])
            .await
            .map_err(|e| crate::error::CogGitHubError::Provider(e.to_string()))?;
        tracing::info!(
            pr = pr.number,
            tasks = ?task_ids,
            "Submitted platform_pr_intent task to orchestrator"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{CiJobLog, CreatePullRequest, PlatformPullRequest, PullRequestDetail};
    use chrono::Utc;
    use std::sync::Mutex;

    struct MockProvider {
        issues: Vec<PlatformIssue>,
        comments: Mutex<Vec<(u64, String)>>,
        ci_logs: Vec<CiJobLog>,
        ci_runs: Mutex<Vec<CiFailureEvent>>,
    }

    #[async_trait::async_trait]
    impl CodePlatformProvider for MockProvider {
        async fn list_open_issues(&self) -> Result<Vec<PlatformIssue>> {
            Ok(self.issues.clone())
        }
        async fn create_pull_request(
            &self,
            _req: CreatePullRequest,
        ) -> Result<PlatformPullRequest> {
            unimplemented!()
        }
        async fn comment_on_issue(&self, issue_number: u64, body: String) -> Result<()> {
            self.comments.lock().unwrap().push((issue_number, body));
            Ok(())
        }
        async fn merge_pull_request(&self, _pr: u64, _sha: String) -> Result<()> {
            unimplemented!()
        }
        async fn get_pull_request(&self, _pr: u64) -> Result<PullRequestDetail> {
            unimplemented!()
        }
        async fn fetch_ci_failure_logs(&self, _run_id: u64) -> Result<Vec<CiJobLog>> {
            Ok(self.ci_logs.clone())
        }
        async fn list_recent_ci_failures(&self, _max: usize) -> Result<Vec<CiFailureEvent>> {
            Ok(self.ci_runs.lock().unwrap().clone())
        }
    }

    struct MockOrchestrator {
        goals: Mutex<Vec<String>>,
        task_types: Mutex<Vec<String>>,
        /// Verdict returned for a `platform_intent_assess` task. `None` reports
        /// the task as Failed so the fallback path can be exercised without
        /// waiting out the real poll timeout.
        assess_verdict: Mutex<Option<serde_json::Value>>,
    }

    impl MockOrchestrator {
        fn new() -> Self {
            Self {
                goals: Mutex::new(vec![]),
                task_types: Mutex::new(vec![]),
                assess_verdict: Mutex::new(Some(serde_json::json!({
                    "decision": "fix",
                    "question": "",
                    "priority": 2,
                    "reason": "mock assessor: actionable",
                }))),
            }
        }

        fn with_verdict(self, verdict: serde_json::Value) -> Self {
            *self.assess_verdict.lock().unwrap() = Some(verdict);
            self
        }

        fn with_failed_assess(self) -> Self {
            *self.assess_verdict.lock().unwrap() = None;
            self
        }
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for MockOrchestrator {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(
            &self,
            goal: &str,
            tasks: Vec<Task>,
        ) -> cog_core::SFResult<Vec<String>> {
            self.goals.lock().unwrap().push(goal.to_string());
            let mut ids = Vec::with_capacity(tasks.len());
            for task in tasks {
                let type_name = match &task.task_type {
                    TaskType::Custom(name) => name.clone(),
                    other => format!("{other:?}"),
                };
                self.task_types.lock().unwrap().push(type_name);
                ids.push(task.id);
            }
            Ok(ids)
        }
        async fn assign_task(&self, _t: &str, _a: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn add_task(&self, _t: Task) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn crew_can_retry(&self, _ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn push_to_dlq(&self, _t: &str, _e: String) -> cog_core::SFResult<bool> {
            unimplemented!()
        }
        async fn retry_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn dlq_len(&self) -> cog_core::SFResult<usize> {
            unimplemented!()
        }
        async fn start_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn complete_task(
            &self,
            _t: &str,
            _r: serde_json::Value,
        ) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn fail_task(
            &self,
            _t: &str,
            _e: String,
        ) -> cog_core::SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn cancel_task(&self, _t: &str) -> cog_core::SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn get_task(&self, id: &str) -> Option<Task> {
            // Only the assess task is polled; report it finished immediately so
            // tests never wait out the real 100s poll timeout.
            if !id.contains("intent-assess") {
                return None;
            }
            let verdict = self.assess_verdict.lock().unwrap().clone();
            let mut task = Task::new(
                id,
                TaskType::Custom("platform_intent_assess".into()),
                serde_json::json!({}),
            );
            match verdict {
                Some(result) => {
                    task.status = TaskStatus::Completed;
                    task.result = Some(result);
                }
                None => {
                    task.status = TaskStatus::Failed;
                    task.error = Some("mock assess failure".into());
                }
            }
            Some(task)
        }
        async fn schedule_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            vec![]
        }
        async fn get_dependents(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _t: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (vec![], vec![])
        }
        async fn delete_task(&self, _t: &str) -> cog_core::SFResult<()> {
            unimplemented!()
        }
        async fn all_completed(&self) -> bool {
            true
        }
        async fn replay_dlq(&self, _t: &str) -> cog_core::SFResult<bool> {
            unimplemented!()
        }
    }

    fn issue(number: u64, body: &str) -> PlatformIssue {
        PlatformIssue {
            number,
            title: format!("issue {}", number),
            body: body.into(),
            state: "open".into(),
            labels: vec![],
            author: "alice".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn config() -> GitHubIntegrationConfig {
        GitHubIntegrationConfig {
            repo: "owner/repo".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn clear_issue_submits_task() {
        let provider = Arc::new(MockProvider {
            issues: vec![issue(
                1,
                "The /health endpoint is slow. Expected under 50ms, actual 500ms. Reproduce: curl it.",
            )],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );
        let scanned = loop_.run_once().await.unwrap();
        assert_eq!(scanned, 1);
        // With an orchestrator the loop first submits the multimodal assess
        // task; a `fix` verdict then submits the real github_issue_fix task.
        let types = orchestrator.task_types.lock().unwrap();
        assert!(
            types.iter().any(|t| t == "platform_intent_assess"),
            "assess task should be submitted before acting; got {types:?}"
        );
        assert!(
            types.iter().any(|t| t == "github_issue_fix"),
            "a fix verdict should submit the github_issue_fix task; got {types:?}"
        );
    }

    #[tokio::test]
    async fn assess_clarify_verdict_posts_question_and_no_fix() {
        // The semantic assessor (mocked) judges a terse issue as still unclear:
        // the loop posts the assessor's question once and must not submit a fix.
        let provider = Arc::new(MockProvider {
            issues: vec![issue(7, "it breaks")],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new().with_verdict(serde_json::json!({
            "decision": "clarify",
            "question": "Which release are you running, and what is the exact error?",
            "priority": 3,
            "reason": "no reproduction detail",
        })));

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider.clone(),
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );
        loop_.run_once().await.unwrap();

        let comments = provider.comments.lock().unwrap();
        assert_eq!(comments.len(), 1, "exactly one clarification comment");
        assert_eq!(comments[0].0, 7);
        assert!(
            comments[0].1.contains("Which release are you running"),
            "comment should carry the assessor's question; got: {}",
            comments[0].1
        );
        drop(comments);

        let types = orchestrator.task_types.lock().unwrap();
        assert!(types.iter().any(|t| t == "platform_intent_assess"));
        assert!(
            !types.iter().any(|t| t == "github_issue_fix"),
            "a clarify verdict must not submit a fix task; got {types:?}"
        );
    }

    #[tokio::test]
    async fn assess_failure_falls_back_to_heuristic_fix() {
        // When the assess task fails, the loop degrades to the local text
        // heuristic: a clear, reproducible body is still fixed autonomously.
        let provider = Arc::new(MockProvider {
            issues: vec![issue(
                8,
                "The /health endpoint is slow. Expected under 50ms, actual 500ms. Reproduce: curl it.",
            )],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new().with_failed_assess());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );
        loop_.run_once().await.unwrap();

        let types = orchestrator.task_types.lock().unwrap();
        assert!(types.iter().any(|t| t == "platform_intent_assess"));
        assert!(
            types.iter().any(|t| t == "github_issue_fix"),
            "heuristic fallback should still submit a fix for a clear issue; got {types:?}"
        );
    }

    #[tokio::test]
    async fn unclear_issue_posts_clarification() {
        let provider = Arc::new(MockProvider {
            issues: vec![issue(2, "too short")],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider.clone(),
            IssueTriage::rules_only(),
            config(),
            None,
            None,
        );
        loop_.run_once().await.unwrap();
        let comments = provider.comments.lock().unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].0, 2);
        assert!(comments[0].1.contains("— Cogneva Bot"));
    }

    #[tokio::test]
    async fn unclear_issue_asks_only_once_across_repeated_triggers() {
        // 同一 issue 被重复触发（webhook + 轮询，或自己发的评论事件在读到
        // 之前就到达——mock 默认不回传已发评论）时，只能问一次。
        let provider = Arc::new(MockProvider {
            issues: vec![issue(20, "too short")],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let mut loop_ = GitHubDiscoveryLoop::new(
            provider.clone(),
            IssueTriage::rules_only(),
            config(),
            None,
            None,
        );
        loop_.run_once().await.unwrap();
        // 第二次触发：没有任何新的实质性用户回复。
        loop_.run_once().await.unwrap();
        let comments = provider.comments.lock().unwrap();
        assert_eq!(
            comments.len(),
            1,
            "bot must ask only once, posted {comments:?}"
        );
        assert_eq!(comments[0].0, 20);
    }

    #[test]
    fn self_comment_webhook_event_is_ignored() {
        // 机器人用人类账号 token 发评论：作者是人类用户名，但正文带签名。
        let mut cfg = config();
        cfg.bot_identity.username = "cogneva-bot".into();
        cfg.accounts = vec![crate::config::GitHubAccount::Human(
            crate::config::HumanAccount {
                username: "hcipengm".into(),
                ..Default::default()
            },
        )];
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let loop_ = GitHubDiscoveryLoop::new(provider, IssueTriage::rules_only(), cfg, None, None);

        let payload = serde_json::json!({
            "action": "created",
            "comment": {
                "body": "Could you describe the problem?\n\n— Cogneva Bot",
                "user": { "login": "hcipengm" }
            },
            "issue": { "number": 5 }
        });
        assert!(loop_.is_self_comment_event(false, "issue_comment", "created", &payload));

        // 别人的真实回复：不应被当成自发事件。
        let human = serde_json::json!({
            "action": "created",
            "comment": {
                "body": "it crashes on startup, here are the steps",
                "user": { "login": "alice" }
            },
            "issue": { "number": 5 }
        });
        assert!(!loop_.is_self_comment_event(false, "issue_comment", "created", &human));

        // issue 本身的 opened 事件不是评论事件，不该判自发。
        let opened = serde_json::json!({
            "action": "opened",
            "issue": { "number": 5, "body": "hi" }
        });
        assert!(!loop_.is_self_comment_event(false, "issues", "opened", &opened));
    }

    #[tokio::test]
    async fn forbidden_label_is_skipped() {
        let mut forbidden = issue(3, "x".repeat(100).as_str());
        forbidden.labels = vec!["wontfix".into()];
        let provider = Arc::new(MockProvider {
            issues: vec![forbidden],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider.clone(),
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );
        loop_.run_once().await.unwrap();
        assert!(orchestrator.goals.lock().unwrap().is_empty());
        assert!(provider.comments.lock().unwrap().is_empty());
    }

    fn ci_event(run_id: u64) -> CiFailureEvent {
        CiFailureEvent {
            run_id,
            workflow_name: "CI".into(),
            head_sha: "abc123".into(),
            head_branch: "main".into(),
            html_url: format!("https://github.com/owner/repo/actions/runs/{run_id}"),
        }
    }

    #[tokio::test]
    async fn ci_failure_submits_fix_task_with_logs() {
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![CiJobLog {
                job_id: 42,
                job_name: "Clippy".into(),
                log_tail: "error: clippy::question_mark".into(),
            }],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );

        assert!(loop_.process_ci_failure(ci_event(1001)).await.unwrap());
        let goals = orchestrator.goals.lock().unwrap();
        assert_eq!(goals.len(), 1);
        assert!(goals[0].contains("run 1001"));
        assert!(goals[0].contains("Clippy"));
        assert!(goals[0].contains("clippy::question_mark"));
    }

    #[tokio::test]
    async fn ci_failure_dedupes_repeat_events() {
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );

        assert!(loop_.process_ci_failure(ci_event(2002)).await.unwrap());
        assert!(!loop_.process_ci_failure(ci_event(2002)).await.unwrap());
        assert_eq!(orchestrator.goals.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ci_failure_without_orchestrator_is_unsubmittable() {
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });

        let mut loop_ =
            GitHubDiscoveryLoop::new(provider, IssueTriage::rules_only(), config(), None, None);

        assert!(!loop_.process_ci_failure(ci_event(3003)).await.unwrap());
    }

    #[tokio::test]
    async fn ci_polling_adopts_existing_failures_on_first_round() {
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![ci_event(4001), ci_event(4002)]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );

        loop_.run_once().await.unwrap();
        assert!(orchestrator.goals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn ci_polling_submits_only_new_failures() {
        let provider = Arc::new(MockProvider {
            issues: vec![],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![ci_event(5001)]),
        });
        let orchestrator = Arc::new(MockOrchestrator::new());

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider.clone(),
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );

        // First round adopts run 5001 without submitting.
        loop_.run_once().await.unwrap();
        assert!(orchestrator.goals.lock().unwrap().is_empty());

        // A new failure appears; the next round submits exactly one task.
        provider.ci_runs.lock().unwrap().push(ci_event(5002));
        loop_.run_once().await.unwrap();
        assert_eq!(orchestrator.goals.lock().unwrap().len(), 1);
        assert!(orchestrator.goals.lock().unwrap()[0].contains("run 5002"));

        // Polling again with the same set submits nothing.
        loop_.run_once().await.unwrap();
        assert_eq!(orchestrator.goals.lock().unwrap().len(), 1);
    }
}
