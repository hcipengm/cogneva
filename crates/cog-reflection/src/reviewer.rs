//! Periodic review scheduler.
//! Runs background passes that inspect the learning corpus, promote what is
//! ready into the layer each learning was promoted for, and report on
//! high-priority items and emerging patterns.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::SFResult;
use tokio::time::interval;
use tracing::{info, warn};

use crate::matcher::LearningMatcher;
use crate::promoter::LearningPromoter;
use crate::recorder::LearningRecorder;
use crate::types::{LearningFilter, PromotionOutcome, PromotionResult, ReviewReport};
use cog_core::{LearningStatus, Priority};

/// Background task that periodically reviews the learning corpus.
#[derive(Clone)]
pub struct PeriodicReviewer {
    recorder: Arc<dyn LearningRecorder>,
    matcher: Arc<dyn LearningMatcher>,
    promoter: Arc<dyn LearningPromoter>,
    interval: Duration,
}

impl PeriodicReviewer {
    pub fn new(
        recorder: Arc<dyn LearningRecorder>,
        matcher: Arc<dyn LearningMatcher>,
        promoter: Arc<dyn LearningPromoter>,
        interval: Duration,
    ) -> Self {
        Self {
            recorder,
            matcher,
            promoter,
            interval,
        }
    }

    /// How often this reviewer is meant to run.
    ///
    /// A driver mounts the loop from outside and has to name a period: reading
    /// it from here keeps the cadence defined once. A second literal at the
    /// spawn site would be free to drift from this one, and the copy that
    /// drifts is always the one nobody edits.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Run the review loop indefinitely.
    /// Callers typically spawn this with `tokio::spawn`.
    pub async fn run(&self) {
        let mut ticker = interval(self.interval);
        loop {
            ticker.tick().await;
            match self.review_once().await {
                Ok(report) => {
                    // The tally is by the layer each promotion reached, not by
                    // how many were ready: a pass where every write fell back
                    // reads exactly like a pass where none did if the line
                    // stops at "ready".
                    let (mut promoted, mut degraded) = (0usize, 0usize);
                    for outcome in &report.promotions {
                        match outcome.result {
                            PromotionResult::Promoted { .. } => promoted += 1,
                            PromotionResult::Degraded { .. } => degraded += 1,
                            PromotionResult::NotReady { .. } => {}
                        }
                    }
                    info!(
                        "periodic review complete: {} pending, {} patterns, {} ready for promotion, {} reached their layer, {} fell back",
                        report.total_pending,
                        report.patterns_detected.len(),
                        report.promotions_ready.len(),
                        promoted,
                        degraded
                    );
                }
                Err(e) => {
                    warn!("periodic review failed: {}", e);
                }
            }
        }
    }

    /// Perform a single review pass: report on the corpus, then promote.
    ///
    /// The pass acts on the list it just built. It used to stop at building it:
    /// `promotions_ready` was collected into the report, the report was logged,
    /// and nothing ever called the promoter — so the layer this engine holds a
    /// handle to received nothing, and the only visible sign was a count in a
    /// log line that read the same whether the promotions happened or not.
    ///
    /// A learning is marked promoted only when it reached the layer it was
    /// promoted *for*. One that fell back stays promotable, which is what lets
    /// it reach its own layer on a later pass — the wiki handle resolves only
    /// once its publisher has initialised, and a pass that ran before that must
    /// not be the last word on the learning.
    pub async fn review_once(&self) -> SFResult<ReviewReport> {
        let all = self.recorder.list_learnings(None).await?;

        let total_pending = all
            .iter()
            .filter(|l| matches!(l.status, LearningStatus::Pending))
            .count();
        let total_in_progress = all
            .iter()
            .filter(|l| matches!(l.status, LearningStatus::InProgress))
            .count();

        let high_priority_pending = self
            .recorder
            .list_learnings(Some(LearningFilter {
                status: Some(LearningStatus::Pending),
                priority: Some(Priority::High),
                ..Default::default()
            }))
            .await?;

        let recently_resolved = self
            .recorder
            .list_learnings(Some(LearningFilter {
                status: Some(LearningStatus::Resolved),
                since: Some(Utc::now() - chrono::Duration::days(7)),
                ..Default::default()
            }))
            .await?;

        let patterns = self.matcher.detect_patterns().await?;

        // Identify learnings ready for promotion.
        let mut promotions_ready = Vec::new();
        for learning in &all {
            if self.promoter.should_promote(learning)
                && !matches!(learning.status, LearningStatus::Promoted)
            {
                promotions_ready.push(learning.clone());
            }
        }

        // Then promote them. The status is written back here rather than inside
        // the promoter because the promoter holds no recorder: it decides and
        // writes, this pass owns the corpus entry that says the decision is
        // spent.
        let mut promotions = Vec::with_capacity(promotions_ready.len());
        for learning in &promotions_ready {
            match self.promoter.promote_if_ready(learning).await {
                Ok(result) => {
                    if matches!(result, PromotionResult::Promoted { .. }) {
                        let mut promoted = learning.clone();
                        promoted.status = LearningStatus::Promoted;
                        if let Err(e) = self.recorder.record_learning(promoted).await {
                            // The write into the target layer already happened,
                            // so the learning stays promotable and the next
                            // pass promotes it again. Said out loud because the
                            // alternative reading of a repeated promotion is
                            // "the promoter is broken".
                            warn!(
                                "learning {} was promoted but its status could not be recorded; \
                                 the next pass will promote it again: {e}",
                                learning.id
                            );
                        }
                    }
                    promotions.push(PromotionOutcome {
                        learning_id: learning.id.clone(),
                        result,
                    });
                }
                Err(e) => warn!(
                    "promotion of learning {} failed outright (no target took it): {e}",
                    learning.id
                ),
            }
        }

        Ok(ReviewReport {
            total_pending,
            total_in_progress,
            high_priority_pending,
            recently_resolved,
            patterns_detected: patterns,
            promotions_ready,
            promotions,
            reviewed_at: Utc::now(),
        })
    }

    /// Quick health-check: return true if the reviewer can reach the recorder.
    pub async fn health_check(&self) -> bool {
        self.recorder.list_learnings(None).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::PeriodicReviewer;
    use crate::matcher::DefaultLearningMatcher;
    use crate::promoter::DefaultLearningPromoter;
    use crate::recorder::{InMemoryRecorder, LearningRecorder};
    use crate::types::PromotionResult;
    use async_trait::async_trait;
    use cog_core::{
        Area, Config, Learning, LearningCategory, LearningSource, LearningStatus, PluginContext,
        Priority, SFResult, SkillRegistry,
    };
    use std::sync::{Arc, Mutex};

    /// A wiki that keeps what it was asked to store, so the test can count how
    /// many documents a pass actually produced.
    struct RecordingWiki {
        written: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl cog_core::WikiBackend for RecordingWiki {
        async fn health_check(&self) -> bool {
            true
        }

        fn provider_name(&self) -> &str {
            "recording"
        }

        async fn ingest_document(&self, relative_path: &str, _content: &str) -> SFResult<()> {
            self.written.lock().unwrap().push(relative_path.to_owned());
            Ok(())
        }
    }

    fn promotable_insight() -> Learning {
        // Every threshold `should_promote` reads is set from the promoter's own
        // defaults, not from numbers copied here: a test that hard-codes them
        // keeps passing after the defaults move and stops exercising the
        // question it was written for.
        let mut learning = Learning::new(
            LearningCategory::Insight,
            Priority::Medium,
            Area::Backend,
            "the same read timeout bit us three times",
            "details",
            "raise the read timeout",
            LearningSource::SelfReview,
        );
        learning.recurrence_count = 3;
        learning.related_tasks = vec!["task-a".into(), "task-b".into()];
        learning.last_seen = learning.first_seen + chrono::Duration::minutes(5);
        learning
    }

    async fn reviewer_over(
        wiki: Arc<RecordingWiki>,
    ) -> (
        PeriodicReviewer,
        Arc<InMemoryRecorder>,
        Arc<cog_core::LateService<dyn cog_core::WikiBackend>>,
    ) {
        let recorder = Arc::new(InMemoryRecorder::new());
        let matcher = Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        // One context, so the handle resolves against the registry the wiki is
        // published into — the arrangement the plugin produces.
        let ctx = PluginContext::new(Config::default());
        let handle = Arc::new(cog_core::LateService::<dyn cog_core::WikiBackend>::new(
            ctx.clone(),
        ));
        let promoter =
            DefaultLearningPromoter::new(Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())))
                .with_wiki_late((*handle).clone());
        ctx.publish_service::<dyn cog_core::WikiBackend>(wiki);

        let reviewer = PeriodicReviewer::new(
            recorder.clone(),
            matcher,
            Arc::new(promoter),
            std::time::Duration::from_secs(3600),
        );
        (reviewer, recorder, handle)
    }

    /// A review pass has to promote, not only say what could be promoted.
    ///
    /// This is the whole defect in one assertion: with the promoter reachable
    /// but never called, every line of the report stays identical and the wiki
    /// layer the engine holds a handle to receives nothing.
    #[tokio::test]
    async fn a_review_pass_promotes_what_it_calls_ready() {
        let wiki = Arc::new(RecordingWiki {
            written: Mutex::new(Vec::new()),
        });
        let (reviewer, recorder, _handle) = reviewer_over(wiki.clone()).await;
        recorder
            .record_learning(promotable_insight())
            .await
            .unwrap();

        let report = reviewer.review_once().await.unwrap();

        assert_eq!(
            report.promotions_ready.len(),
            1,
            "the fixture has to be promotable for this test to ask anything"
        );
        assert_eq!(
            report.promotions.len(),
            1,
            "the pass acted on nothing it listed"
        );
        assert!(
            matches!(
                report.promotions[0].result,
                PromotionResult::Promoted { .. }
            ),
            "expected a promotion, got {:?}",
            report.promotions[0].result
        );
        assert_eq!(
            wiki.written.lock().unwrap().len(),
            1,
            "the promoted learning never reached the wiki layer"
        );
    }

    /// A promotion is a fact about the corpus, so the next pass has to see it.
    /// Without the write-back the hourly loop re-promotes the same learning for
    /// as long as the process lives, and the duplicate copies are the only sign.
    #[tokio::test]
    async fn a_learning_already_promoted_is_not_promoted_again() {
        let wiki = Arc::new(RecordingWiki {
            written: Mutex::new(Vec::new()),
        });
        let (reviewer, recorder, _handle) = reviewer_over(wiki.clone()).await;
        recorder
            .record_learning(promotable_insight())
            .await
            .unwrap();

        reviewer.review_once().await.unwrap();
        let second = reviewer.review_once().await.unwrap();

        assert!(
            second.promotions_ready.is_empty(),
            "the second pass still considers {} learning(s) promotable",
            second.promotions_ready.len()
        );
        assert!(
            second.promotions.is_empty(),
            "the second pass promoted {} learning(s) again",
            second.promotions.len()
        );
        assert_eq!(
            wiki.written.lock().unwrap().len(),
            1,
            "the same learning was written to the wiki twice"
        );
    }

    /// A pass whose target layer never takes anything must not report the same
    /// tally as one where everything landed: the count is what a reader has.
    #[tokio::test]
    async fn a_pass_that_falls_back_does_not_count_as_promoted() {
        let recorder = Arc::new(InMemoryRecorder::new());
        let matcher = Arc::new(DefaultLearningMatcher::new(recorder.clone(), None));
        // A handle whose context never had a wiki published into it.
        let ctx = PluginContext::new(Config::default());
        let handle = cog_core::LateService::<dyn cog_core::WikiBackend>::new(ctx);
        let promoter =
            DefaultLearningPromoter::new(Arc::new(tokio::sync::RwLock::new(SkillRegistry::new())))
                .with_wiki_late(handle);
        let reviewer = PeriodicReviewer::new(
            recorder.clone(),
            matcher,
            Arc::new(promoter),
            std::time::Duration::from_secs(3600),
        );
        recorder
            .record_learning(promotable_insight())
            .await
            .unwrap();

        let report = reviewer.review_once().await.unwrap();

        assert_eq!(report.promotions.len(), 1);
        assert!(
            matches!(
                report.promotions[0].result,
                PromotionResult::Degraded { .. }
            ),
            "a fallback is not a promotion: {:?}",
            report.promotions[0].result
        );
        // And it stays promotable, because the layer it was promoted for may
        // appear on a later pass.
        let still = reviewer.review_once().await.unwrap();
        assert_eq!(
            still.promotions.len(),
            1,
            "a learning that fell back must be retried against its own layer"
        );
    }

    /// `LearningStatus::Promoted` is what makes the write-back observable, so
    /// pin the corpus entry the pass leaves behind rather than only its report.
    #[tokio::test]
    async fn the_promoted_status_is_written_back_to_the_corpus() {
        let wiki = Arc::new(RecordingWiki {
            written: Mutex::new(Vec::new()),
        });
        let (reviewer, recorder, _handle) = reviewer_over(wiki).await;
        recorder
            .record_learning(promotable_insight())
            .await
            .unwrap();

        reviewer.review_once().await.unwrap();

        let stored = recorder.list_learnings(None).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert!(
            matches!(stored[0].status, LearningStatus::Promoted),
            "the corpus still reads {:?} after the promotion",
            stored[0].status
        );
    }
}
