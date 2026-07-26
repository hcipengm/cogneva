//! Periodic review scheduler.
//! Runs background passes that inspect the learning corpus and emit
//! reports (or trigger hooks) for high-priority items, ready promotions,
//! and emerging patterns.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cog_core::SFResult;
use tokio::time::interval;
use tracing::{info, warn};

use crate::matcher::LearningMatcher;
use crate::promoter::LearningPromoter;
use crate::recorder::LearningRecorder;
use crate::types::{LearningFilter, ReviewReport};
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

    /// Run the review loop indefinitely.
    /// Callers typically spawn this with `tokio::spawn`.
    pub async fn run(&self) {
        let mut ticker = interval(self.interval);
        loop {
            ticker.tick().await;
            match self.review_once().await {
                Ok(report) => {
                    info!(
                        "periodic review complete: {} pending, {} patterns, {} ready for promotion",
                        report.total_pending,
                        report.patterns_detected.len(),
                        report.promotions_ready.len()
                    );
                }
                Err(e) => {
                    warn!("periodic review failed: {}", e);
                }
            }
        }
    }

    /// Perform a single review pass and return the report.
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

        Ok(ReviewReport {
            total_pending,
            total_in_progress,
            high_priority_pending,
            recently_resolved,
            patterns_detected: patterns,
            promotions_ready,
            reviewed_at: Utc::now(),
        })
    }

    /// Quick health-check: return true if the reviewer can reach the recorder.
    pub async fn health_check(&self) -> bool {
        self.recorder.list_learnings(None).await.is_ok()
    }
}
