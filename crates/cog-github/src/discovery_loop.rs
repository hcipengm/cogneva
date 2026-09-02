//! GitHub discovery loop — the autonomous sensor loop from the design doc.
//!
//! Each round: scan issues → rebuild/refresh conversations → triage →
//! either submit a `github_issue_fix` task to the orchestrator, post a
//! clarification question, escalate, or skip. Then poll tracked PRs and
//! record outcomes into the reflection engine.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::GitHubIntegrationConfig;
use cog_core::{OrchestratorControl, Task, TaskType};

use crate::conversation::{ConversationState, IssueConversation};
use crate::discovery::IssueDiscovery;
use crate::error::Result;
use crate::outcome_recorder::OutcomeRecorder;
use crate::provider::{CiFailureEvent, CodePlatformProvider, PlatformIssue};
use crate::triage::{IssueTriage, TriageDecision};

/// The autonomous GitHub sensor loop.
pub struct GitHubDiscoveryLoop {
    provider: Arc<dyn CodePlatformProvider>,
    triage: IssueTriage,
    config: GitHubIntegrationConfig,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
    reflection: Option<Arc<dyn cog_core::ReflectionEngine>>,
    discovery: IssueDiscovery,
    conversations: HashMap<u64, IssueConversation>,
    recorder: OutcomeRecorder,
    /// Issue numbers that already produced a task this process lifetime.
    submitted: std::collections::HashSet<u64>,
    /// PR numbers that already produced an intent task this process lifetime.
    submitted_prs: std::collections::HashSet<u64>,
    /// CI run ids that already produced a fix task this process lifetime.
    ci_submitted: std::collections::HashSet<u64>,
    /// CI run ids seen by the polling fallback. `None` until the first poll,
    /// which adopts all currently failed runs without submitting (so a pod
    /// restart does not resubmit old failures).
    ci_seen: Option<std::collections::HashSet<u64>>,
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
            submitted_prs: std::collections::HashSet::new(),
            ci_submitted: std::collections::HashSet::new(),
            ci_seen: None,
        }
    }

    /// Register a PR created for a patch so its outcome is recorded.
    pub fn track_pr(&mut self, pr_number: u64, patch_id: impl Into<String>) {
        self.recorder.track(pr_number, patch_id);
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
                "evolution_mode": "generate_patch",
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
        // Rebuild conversation state from platform comments each round so
        // user replies are picked up.
        let comments = self
            .provider
            .list_issue_comments(issue.number)
            .await
            .unwrap_or_default();
        let conversation = IssueConversation::from_comments(
            issue.number,
            &comments,
            &self.config.bot_identity.username,
            &self.config.conversation,
        );

        match conversation.state {
            ConversationState::Stale => {
                tracing::info!(issue = issue.number, "Conversation stale; skipping");
                self.conversations.insert(issue.number, conversation);
                return Ok(());
            }
            ConversationState::AwaitingClarification => {
                // Still waiting for the reporter; nothing to do this round.
                self.conversations.insert(issue.number, conversation);
                return Ok(());
            }
            _ => {}
        }

        if conversation.state == ConversationState::Clarified
            && self.submitted.contains(&issue.number)
        {
            self.conversations.insert(issue.number, conversation);
            return Ok(());
        }

        let decision = self.triage.evaluate(issue, &self.config).await;

        match decision {
            TriageDecision::Fix {
                priority,
                rationale,
            } => {
                tracing::info!(
                    issue = issue.number,
                    priority,
                    rationale = %rationale,
                    "Triage decided to fix issue"
                );
                if self.config.auto_create_pr {
                    self.submit_fix_task(issue, &conversation).await?;
                    self.submitted.insert(issue.number);
                }
            }
            TriageDecision::AskForClarification { question } => {
                let mut convo = conversation;
                if let Some(body) = convo.ask(&question, &self.config.conversation) {
                    let posted = convo
                        .post_reply(self.provider.as_ref(), &body, &self.config.conversation)
                        .await?;
                    tracing::info!(
                        issue = issue.number,
                        posted,
                        "Posted clarification question"
                    );
                }
                self.conversations.insert(issue.number, convo);
                return Ok(());
            }
            TriageDecision::EscalateHuman { reason } => {
                tracing::warn!(
                    issue = issue.number,
                    reason = %reason,
                    "Issue escalated to human"
                );
            }
            TriageDecision::Skip { reason } => {
                tracing::debug!(
                    issue = issue.number,
                    reason = %reason,
                    "Issue skipped"
                );
            }
        }

        self.conversations.insert(issue.number, conversation);
        Ok(())
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
                "evolution_mode": "generate_patch",
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
        if self.submitted_prs.contains(&pr.number) {
            return Ok(());
        }
        if pr
            .labels
            .iter()
            .any(|l| self.config.forbidden_labels.contains(l))
        {
            tracing::info!(pr = pr.number, "PR carries forbidden label; skipping");
            self.submitted_prs.insert(pr.number);
            return Ok(());
        }

        // 复用 issue triage 规则评估 PR 意图是否值得做：PR 与 issue 同为
        // 外部意图，门禁标准一致。
        let intent = PlatformIssue {
            number: pr.number,
            title: format!("[PR] {}", pr.title),
            body: pr.body.clone(),
            state: pr.state.clone(),
            labels: pr.labels.clone(),
            author: pr.author.clone(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let decision = self.triage.evaluate(&intent, &self.config).await;

        match decision {
            TriageDecision::Fix {
                priority,
                rationale,
            } => {
                tracing::info!(
                    pr = pr.number,
                    priority,
                    rationale = %rationale,
                    "Triage decided to pursue PR intent"
                );
                if self.config.auto_create_pr {
                    self.submit_pr_intent_task(pr).await?;
                    self.submitted_prs.insert(pr.number);
                }
            }
            other => {
                tracing::info!(pr = pr.number, decision = ?other, "PR intent not pursued");
            }
        }
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
                "evolution_mode": "generate_patch",
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
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for MockOrchestrator {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(
            &self,
            goal: &str,
            _tasks: Vec<Task>,
        ) -> cog_core::SFResult<Vec<String>> {
            self.goals.lock().unwrap().push(goal.to_string());
            Ok(vec!["task-1".into()])
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
        async fn get_task(&self, _t: &str) -> Option<Task> {
            None
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
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

        let mut loop_ = GitHubDiscoveryLoop::new(
            provider,
            IssueTriage::rules_only(),
            config(),
            Some(orchestrator.clone()),
            None,
        );
        let scanned = loop_.run_once().await.unwrap();
        assert_eq!(scanned, 1);
        assert_eq!(orchestrator.goals.lock().unwrap().len(), 1);
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
    async fn forbidden_label_is_skipped() {
        let mut forbidden = issue(3, "x".repeat(100).as_str());
        forbidden.labels = vec!["wontfix".into()];
        let provider = Arc::new(MockProvider {
            issues: vec![forbidden],
            comments: Mutex::new(vec![]),
            ci_logs: vec![],
            ci_runs: Mutex::new(vec![]),
        });
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

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
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

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
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

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
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

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
        let orchestrator = Arc::new(MockOrchestrator {
            goals: Mutex::new(vec![]),
        });

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
