//! The runtime principle account, against a live PostgreSQL.
//!
//! What these tests check is what the database decides, not what a memory
//! implementation of the same trait would agree to: that a proposal is not in
//! force, that only a decision puts a rule in force, that a live rule's text
//! cannot be overwritten by filing the same slug again, that a rejected row can
//! be re-proposed, that retiring only ever applies to a rule in force, that
//! deciding the same thing twice moves nothing the second time (so the date a
//! rule became in force stays the date it became in force), and that the usage
//! counter counts what was said to it. Each of those is a fact about a `WHERE`
//! clause, a CHECK or an `ON CONFLICT` branch, and a memory store would answer
//! every one of them by the rules its author happened to write down.
//!
//! They are ignored by default and need `COGNEVA_TEST_DATABASE_URL` pointing at
//! a throwaway database:
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test runtime_principle_store -- --ignored
//! ```
//!
//! Every row this suite writes carries a per-run marker in its slug and is
//! deleted at the end, so the account is left as it was found.

mod common;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use cog_core::{RuntimePrinciple, RuntimePrincipleState, RuntimePrincipleStore, SFError};
use cog_storage::PostgresRuntimePrincipleStore;

use cog_test_support::database_url;

struct Probe {
    store: PostgresRuntimePrincipleStore,
    pool: PgPool,
    marker: String,
}

async fn store() -> Probe {
    let url = database_url();
    common::migrate_once(&url, 97_021).await;
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the database");
    Probe {
        store: PostgresRuntimePrincipleStore::new(pool.clone()),
        pool,
        marker: format!("probe-runtime-principle-{}", uuid::Uuid::new_v4()),
    }
}

impl Probe {
    /// A slug of this run's own, so parallel suites and leftover rows from an
    /// earlier run cannot be mistaken for this one's.
    fn slug(&self, suffix: &str) -> String {
        format!("{}/{suffix}", self.marker)
    }

    async fn propose(&self, suffix: &str, trigger: &str, criterion: &str, action: &str) -> String {
        let slug = self.slug(suffix);
        let row = RuntimePrinciple::proposed(
            slug.clone(),
            trigger,
            criterion,
            action,
            "E0412",
            Utc::now(),
        )
        .expect("a full triple is a rule");
        self.store.propose(&row).await.expect("file the proposal");
        slug
    }

    /// Whether this run's row is in the account's in-force set. The set holds
    /// every tenant's rules, so the question is about this slug, not about the
    /// set's size.
    async fn in_force(&self, slug: &str) -> Option<RuntimePrinciple> {
        self.store
            .in_force()
            .await
            .expect("read the rules in force")
            .into_iter()
            .find(|row| row.slug == slug)
    }

    async fn row(&self, slug: &str) -> RuntimePrinciple {
        self.store
            .list(None)
            .await
            .expect("list the account")
            .into_iter()
            .find(|row| row.slug == slug)
            .unwrap_or_else(|| panic!("{slug} is on the account"))
    }

    async fn cleanup(&self) {
        sqlx::query("DELETE FROM runtime_principle WHERE slug LIKE $1")
            .bind(format!("{}/%", self.marker))
            .execute(&self.pool)
            .await
            .expect("remove this run's rows");
    }
}

fn at(offset_seconds: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-11T09:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + chrono::Duration::seconds(offset_seconds)
}

/// 提了不算生效：生效集只认属主批过的那一行。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_proposal_is_not_in_force_until_it_is_approved() {
    let probe = store().await;
    let slug = probe
        .propose(
            "build-before-merge",
            "在合并之前",
            "构建是不是绿的",
            "红了就停下来修",
        )
        .await;

    assert!(
        probe.in_force(&slug).await.is_none(),
        "a proposal nobody approved is not a rule anyone works by"
    );
    assert_eq!(
        probe.row(&slug).await.state,
        RuntimePrincipleState::Proposed
    );

    assert!(
        probe
            .store
            .decide(&slug, RuntimePrincipleState::Approved, at(60))
            .await
            .expect("approve the proposal"),
        "approving a proposal moves it"
    );

    let row = probe
        .in_force(&slug)
        .await
        .expect("an approved rule is in force");
    assert_eq!(row.state, RuntimePrincipleState::Approved);
    assert_eq!(row.trigger, "在合并之前");
    assert_eq!(row.decided_at, Some(at(60)));

    probe.cleanup().await;
}

/// 撤回只作用于生效中的规则：撤了之后它就不在交给员工的集合里了，
/// 而一行还留着读得到它什么时候失效。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_retired_rule_leaves_the_in_force_set_and_stays_on_the_account() {
    let probe = store().await;
    let slug = probe
        .propose(
            "say-who-you-ask",
            "要给同事派活时",
            "对方是不是名册上的人",
            "用它的 id",
        )
        .await;
    probe
        .store
        .decide(&slug, RuntimePrincipleState::Approved, at(60))
        .await
        .expect("approve the proposal");
    assert!(probe.in_force(&slug).await.is_some());

    assert!(
        probe
            .store
            .decide(&slug, RuntimePrincipleState::Retired, at(120))
            .await
            .expect("retire the rule"),
        "retiring a rule in force moves it"
    );

    assert!(
        probe.in_force(&slug).await.is_none(),
        "a retired rule must not be handed to another run"
    );
    let row = probe.row(&slug).await;
    assert_eq!(row.state, RuntimePrincipleState::Retired);
    assert_eq!(row.decided_at, Some(at(120)));

    // 一条没批过的提案不能被「撤回」：那不是一次转换。
    let pending = probe
        .propose("never-approved", "任何时候", "有没有被批准", "什么都不做")
        .await;
    assert!(
        !probe
            .store
            .decide(&pending, RuntimePrincipleState::Retired, at(180))
            .await
            .expect("a refusal to move is not an error"),
        "a proposal nobody approved cannot be retired"
    );

    probe.cleanup().await;
}

/// 生效中的规则不接受就地改写：同一个 slug 再提一次被拒，而账上那句话还是
/// 被批准过的那一句。改一条原则是提一条新的、批了再撤旧的。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_live_rule_cannot_be_overwritten_by_filing_the_same_slug_again() {
    let probe = store().await;
    let slug = probe
        .propose("fixed-once", "第一版触发条件", "第一版判据", "第一版动作")
        .await;
    probe
        .store
        .decide(&slug, RuntimePrincipleState::Approved, at(60))
        .await
        .expect("approve the proposal");

    let second = RuntimePrinciple::proposed(
        slug.clone(),
        "第二版触发条件",
        "第二版判据",
        "第二版动作",
        "E0513",
        at(90),
    )
    .expect("a full triple is a rule");
    let refused = probe.store.propose(&second).await;
    assert!(
        matches!(refused, Err(SFError::Conflict(_))),
        "a live rule's text must not be overwritten by a second proposal: {refused:?}"
    );

    let row = probe.row(&slug).await;
    assert_eq!(row.trigger, "第一版触发条件", "the approved text stays");
    assert_eq!(row.state, RuntimePrincipleState::Approved);

    // 等拍板的提案也不能被另一个提案静默覆盖：两个人写了提案，不能有一个白写。
    let pending = probe
        .propose("waiting", "原触发条件", "原判据", "原动作")
        .await;
    let other = RuntimePrinciple::proposed(
        pending.clone(),
        "别人的触发条件",
        "别人的判据",
        "别人的动作",
        "E0513",
        at(100),
    )
    .expect("a full triple is a rule");
    assert!(
        probe.store.propose(&other).await.is_err(),
        "a pending proposal must not be silently replaced"
    );
    assert_eq!(probe.row(&pending).await.trigger, "原触发条件");

    probe.cleanup().await;
}

/// 被否决的行可以按新文本重新提案：否决不是终点，是「这条提过、当时没同意」
/// 的记录；它重新回到「提案」态等下一次拍板。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_rejected_rule_may_be_proposed_again_with_new_text() {
    let probe = store().await;
    let slug = probe
        .propose("try-again", "原触发条件", "原判据", "原动作")
        .await;
    probe
        .store
        .decide(&slug, RuntimePrincipleState::Rejected, at(60))
        .await
        .expect("reject the proposal");
    assert_eq!(
        probe.row(&slug).await.state,
        RuntimePrincipleState::Rejected
    );
    assert!(probe.in_force(&slug).await.is_none());

    let again = RuntimePrinciple::proposed(
        slug.clone(),
        "改过的触发条件",
        "改过的判据",
        "改过的动作",
        "E0412",
        at(120),
    )
    .expect("a full triple is a rule");
    probe
        .store
        .propose(&again)
        .await
        .expect("a rejected row may be proposed again");

    let row = probe.row(&slug).await;
    assert_eq!(row.state, RuntimePrincipleState::Proposed);
    assert_eq!(row.trigger, "改过的触发条件");
    assert_eq!(row.decided_at, None, "a fresh proposal has no decision yet");
    assert_eq!(row.proposed_at, at(120));

    probe.cleanup().await;
}

/// 同一件事拍两次：第二次答「没动」，而生效时刻还是第一次那个。否则一条
/// 规则的「哪天开始生效」会被最后一次重跑覆盖掉。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn deciding_the_same_thing_twice_does_not_move_the_date_it_became_in_force() {
    let probe = store().await;
    let slug = probe
        .propose("decided-once", "任何时候", "有没有被重复批准", "只算第一次")
        .await;

    assert!(probe
        .store
        .decide(&slug, RuntimePrincipleState::Approved, at(60))
        .await
        .expect("approve"));
    assert!(
        !probe
            .store
            .decide(&slug, RuntimePrincipleState::Approved, at(600))
            .await
            .expect("a repeat is not an error"),
        "the second approval moved nothing"
    );
    assert_eq!(
        probe.row(&slug).await.decided_at,
        Some(at(60)),
        "when a rule became in force is the first decision, not the last one"
    );

    probe.cleanup().await;
}

/// 引用次数只加不重算：交出去几轮就是几轮，未知的 slug 静默跳过。
#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn note_used_counts_the_runs_a_rule_was_handed_to() {
    let probe = store().await;
    let slug = probe
        .propose("counted", "任何时候", "有没有被交出去", "记一轮")
        .await;
    probe
        .store
        .decide(&slug, RuntimePrincipleState::Approved, at(60))
        .await
        .expect("approve");
    assert_eq!(probe.row(&slug).await.used, 0, "nothing was handed out yet");

    probe
        .store
        .note_used(std::slice::from_ref(&slug))
        .await
        .expect("count one run");
    probe
        .store
        .note_used(&[slug.clone(), probe.slug("not-on-the-account")])
        .await
        .expect("an unknown slug is skipped, not an error");

    assert_eq!(probe.row(&slug).await.used, 2);

    probe.cleanup().await;
}
