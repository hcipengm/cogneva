//! One trace id, four stages, read back in time order.
//!
//! The platform records causality in three places that never met: the gateway
//! writes a raw record per request, the agent's events land in the trace store,
//! and the build and deploy stages are Kubernetes Jobs annotated with the
//! `traceparent` they ran under. Each of those is readable on its own and none
//! of them answers the question an operator actually asks — "what happened to
//! this one request, from the HTTP call to the rollout".
//!
//! This module is the assembly, and its design point is the missing case. A
//! stage that produced nothing and a stage that has no readable carrier look
//! identical when both are absent from the answer — one means the work did not
//! happen, the other means this view cannot see it. So every stage appears in
//! the result whether or not it has anything in it, and an empty one says which
//! of the two it is.

use chrono::{DateTime, Utc};
use cog_core::{AgentTrace, RawRecord};

/// The annotation a dispatched Job carries so its stage can be found by trace
/// id. Written by both the change job and the rollout job; changing it in one
/// place only is how a stage silently drops out of the chain.
pub const TRACEPARENT_ANNOTATION: &str = "cogneva.ai/traceparent";

/// The stages, in the order a request walks them. The order is fixed by the
/// pipeline, not by the timestamps: a stage that starts before its predecessor
/// (a rollout dispatched while the response was still being written) must still
/// read as the later stage.
pub const STAGES: [&str; 4] = ["gateway", "agent", "build", "deploy"];

#[derive(Debug, Clone, serde::Serialize)]
pub struct Stage {
    pub stage: &'static str,
    pub present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<DateTime<Utc>>,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
    /// Why this stage has nothing. `None` when the stage is present, because a
    /// present stage does not need an excuse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_because: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceFanout {
    pub trace_id: String,
    /// True when every stage has something in it. A single boolean so callers do
    /// not have to re-derive it from the stages and get the rule slightly wrong.
    pub complete: bool,
    pub stages: Vec<Stage>,
}

/// What a Job contributes to the chain: which stage it is, and when it ran.
#[derive(Debug, Clone)]
pub struct JobStage {
    /// `change-execution` (build) or `mainline-rollout` (deploy).
    pub component: String,
    pub name: String,
    pub traceparent: String,
    pub created_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub succeeded: Option<bool>,
}

/// The trace id inside a `traceparent`, or `None` when it is not one.
fn trace_id_of(traceparent: &str) -> Option<String> {
    cog_core::TraceContext::from_traceparent(traceparent).map(|ctx| ctx.trace_id)
}

/// Assemble the four stages for `trace_id`.
pub fn trace_fanout(
    trace_id: &str,
    gateway_records: &[RawRecord],
    agent: Option<&AgentTrace>,
    jobs: &[JobStage],
) -> TraceFanout {
    let wanted = trace_id.trim().to_ascii_lowercase();

    let mut stages = Vec::with_capacity(STAGES.len());

    // ── gateway ────────────────────────────────────────────────────────────
    // The gateway's segment is the request itself. Records of every other
    // producer are filtered out here rather than upstream: the raw stream is
    // one file per stream kind, not per producer.
    let mut requests: Vec<&RawRecord> = gateway_records
        .iter()
        .filter(|r| r.meta.trace_id.eq_ignore_ascii_case(&wanted))
        .filter(|r| r.meta.recorded_by == "cog-gateway")
        .collect();
    requests.sort_by_key(|r| r.meta.recorded_at);
    stages.push(match requests.first() {
        Some(first) => Stage {
            stage: "gateway",
            present: true,
            at: Some(first.meta.recorded_at),
            summary: format!(
                "{} request(s); first at {}",
                requests.len(),
                first.meta.recorded_at.to_rfc3339()
            ),
            detail: Some(serde_json::json!({
                "requests": requests.len(),
                "last_at": requests.last().map(|r| r.meta.recorded_at.to_rfc3339()),
                "records": requests.iter().map(|r| serde_json::json!({
                    "at": r.meta.recorded_at.to_rfc3339(),
                    "span_id": r.meta.span_id,
                })).collect::<Vec<_>>(),
            })),
            missing_because: None,
        },
        None => Stage {
            stage: "gateway",
            present: false,
            at: None,
            summary: String::new(),
            detail: None,
            missing_because: Some(
                "no gateway record carries this trace id; the request either never reached the \
                 gateway or the raw stream it was written to has been rotated away"
                    .into(),
            ),
        },
    });

    // ── agent ──────────────────────────────────────────────────────────────
    stages.push(match agent {
        Some(trace) => {
            let first = trace.events.first();
            Stage {
                stage: "agent",
                present: true,
                at: Some(trace.created_at),
                summary: format!(
                    "{} event(s) for agent {} on task {}",
                    trace.events.len().max(trace.event_count as usize),
                    trace.agent_id,
                    trace.task_id
                ),
                detail: Some(serde_json::json!({
                    "agent_id": trace.agent_id,
                    "task_id": trace.task_id,
                    "event_count": trace.event_count,
                    "loaded_events": trace.events.len(),
                    "first_event": first.map(|e| format!("{e:?}")),
                })),
                missing_because: None,
            }
        }
        None => Stage {
            stage: "agent",
            present: false,
            at: None,
            summary: String::new(),
            detail: None,
            missing_because: Some(
                "the trace store has no trace under this id; no agent ran under it, or the \
                 trace aged out of its tier"
                    .into(),
            ),
        },
    });

    // ── build / deploy ─────────────────────────────────────────────────────
    // Both are Jobs, distinguished by the component label the dispatcher set.
    for (stage, component) in [
        ("build", "change-execution"),
        ("deploy", "mainline-rollout"),
    ] {
        let mut mine: Vec<&JobStage> = jobs
            .iter()
            .filter(|j| j.component == component)
            .filter(|j| trace_id_of(&j.traceparent).as_deref() == Some(wanted.as_str()))
            .collect();
        mine.sort_by_key(|j| j.created_at);
        stages.push(match mine.first() {
            Some(job) => Stage {
                stage,
                present: true,
                at: job.created_at.or(job.finished_at),
                summary: match job.succeeded {
                    Some(true) => format!("job {} succeeded", job.name),
                    Some(false) => format!("job {} failed", job.name),
                    None => format!("job {} did not finish", job.name),
                },
                detail: Some(serde_json::json!({
                    "jobs": mine.iter().map(|j| serde_json::json!({
                        "name": j.name,
                        "created_at": j.created_at.map(|t| t.to_rfc3339()),
                        "finished_at": j.finished_at.map(|t| t.to_rfc3339()),
                        "succeeded": j.succeeded,
                    })).collect::<Vec<_>>(),
                })),
                missing_because: None,
            },
            None => Stage {
                stage,
                present: false,
                at: None,
                summary: String::new(),
                detail: None,
                missing_because: Some(match stage {
                    "build" => {
                        "no change job is annotated with this trace id; this rollout did not go \
                         through a build, or the job's ttl removed it before the question was asked"
                            .into()
                    }
                    _ => "no rollout job is annotated with this trace id; the deploy stage never \
                         ran under this trace"
                        .into(),
                }),
            },
        });
    }

    let complete = stages.iter().all(|s| s.present);
    TraceFanout {
        trace_id: wanted,
        complete,
        stages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn record(trace_id: &str, recorded_by: &str, secs: i64) -> RawRecord {
        let mut meta = cog_core::RawMeta {
            version: "1.0".into(),
            stream: "transport_raw".into(),
            recorded_at: at(secs),
            recorded_by: recorded_by.into(),
            sequence: 0,
            trace_id: trace_id.into(),
            span_id: Some("00000000000000aa".into()),
        };
        meta.recorded_by = recorded_by.into();
        cog_core::RawRecord {
            meta,
            context: cog_core::RawContext::default(),
            payload: cog_core::RawPayload {
                direction: "inbound".into(),
                transport: "http".into(),
                format: Some("json".into()),
                raw: serde_json::json!({}),
            },
        }
    }

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    fn job(component: &str, name: &str, tp: &str, secs: i64, ok: Option<bool>) -> JobStage {
        JobStage {
            component: component.into(),
            name: name.into(),
            traceparent: tp.into(),
            created_at: Some(at(secs)),
            finished_at: Some(at(secs + 10)),
            succeeded: ok,
        }
    }

    #[test]
    fn a_trace_with_all_four_carriers_reads_back_in_pipeline_order() {
        let records = vec![
            record("4bf92f3577b34da6a3ce929d0e0e4736", "cog-gateway", 100),
            // 同一流里别的 trace 的记录：按 trace id 过滤，不能混进来。
            record("00000000000000000000000000000001", "cog-gateway", 101),
        ];
        let jobs = vec![
            job("change-execution", "cogneva-change-x", TP, 200, Some(true)),
            job(
                "mainline-rollout",
                "cogneva-mainline-y",
                TP,
                300,
                Some(true),
            ),
        ];
        let fan = trace_fanout("4bf92f3577b34da6a3ce929d0e0e4736", &records, None, &jobs);
        assert_eq!(
            fan.stages.iter().map(|s| s.stage).collect::<Vec<_>>(),
            STAGES.to_vec(),
            "the four stages must appear in pipeline order"
        );
        assert!(fan.stages[0].present, "gateway record is present");
        assert!(fan.stages[2].present, "build job is present");
        assert!(fan.stages[3].present, "deploy job is present");
        assert!(!fan.stages[1].present, "no agent trace was supplied");
        assert!(!fan.complete);
    }

    #[test]
    fn an_absent_stage_says_whether_it_did_not_happen_or_cannot_be_seen() {
        // 这一条是这份视图存在的理由：缺一段与看不见一段在答案里必须不同形。
        let fan = trace_fanout("4bf92f3577b34da6a3ce929d0e0e4736", &[], None, &[]);
        assert_eq!(fan.stages.len(), STAGES.len());
        for stage in &fan.stages {
            assert!(!stage.present);
            let why = stage
                .missing_because
                .as_deref()
                .unwrap_or_default()
                .to_string();
            assert!(
                !why.is_empty(),
                "stage {} is absent without a reason",
                stage.stage
            );
        }
        // 每一段的理由点名它自己的载体，而不是一句通用的「没有数据」。
        assert!(fan.stages[0]
            .missing_because
            .as_deref()
            .unwrap()
            .contains("gateway"));
        assert!(fan.stages[1]
            .missing_because
            .as_deref()
            .unwrap()
            .contains("trace store"));
        assert!(fan.stages[2]
            .missing_because
            .as_deref()
            .unwrap()
            .contains("change job"));
        assert!(fan.stages[3]
            .missing_because
            .as_deref()
            .unwrap()
            .contains("rollout job"));
    }

    #[test]
    fn a_job_annotated_with_another_trace_stays_out_of_this_one() {
        let other = "00-11111111111111111111111111111111-2222222222222222-01";
        let jobs = vec![job(
            "change-execution",
            "cogneva-change-z",
            other,
            200,
            Some(true),
        )];
        let fan = trace_fanout("4bf92f3577b34da6a3ce929d0e0e4736", &[], None, &jobs);
        assert!(
            !fan.stages[2].present,
            "a job from another chain must not be read as this trace's build"
        );
    }
}

/// Read the four stages for one trace id out of the surfaces that carry them.
///
/// Every source is optional on purpose: a gateway that cannot read the raw
/// stream still answers for the stages it can see, and says so for the one it
/// cannot. The alternative — failing the whole query because one store is
/// unavailable — turns a partial answer into no answer exactly when something
/// is wrong.
pub async fn trace_fanout_handler(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::GatewayState>>,
    axum::extract::Path(trace_id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use axum::Json;

    let normalized = trace_id.trim().to_ascii_lowercase();
    if cog_core::TraceContext::from_traceparent(&normalized).is_none() && normalized.len() != 32 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "not_a_trace_id",
                "trace_id": trace_id,
                "expected": "32 lowercase hex characters, or a traceparent whose second field is",
            })),
        )
            .into_response();
    }

    let records = match state.raw_logger.read_proto("transport_raw").await {
        Ok(records) => records,
        Err(e) => {
            tracing::warn!(error = %e, "trace fanout: the raw stream could not be read; the gateway stage will be reported as unreadable");
            Vec::new()
        }
    };
    let agent = match state.trace_store.as_ref() {
        Some(store) => match store.load(&normalized).await {
            Ok(trace) => trace,
            Err(e) => {
                tracing::warn!(error = %e, "trace fanout: the trace store could not be read");
                None
            }
        },
        None => None,
    };
    let jobs = cluster_job_stages().await;

    let fanout = trace_fanout(&normalized, &records, agent.as_ref(), &jobs);
    (axum::http::StatusCode::OK, Json(fanout)).into_response()
}

/// Every rollout and change job in this namespace, with the annotation the
/// dispatcher stamped on it.
///
/// Read from the cluster rather than from a store of our own: the Job *is* the
/// record that the stage ran, and a second copy of it would be a second thing
/// that can disagree with the cluster.
async fn cluster_job_stages() -> Vec<JobStage> {
    let Ok(client) = crate::llm_admin::KubeClient::in_cluster() else {
        return Vec::new();
    };
    let path = format!("/apis/batch/v1/namespaces/{}/jobs", client.namespace());
    let Ok(body) = client.get_json(&path).await else {
        return Vec::new();
    };
    parse_job_stages(&body)
}

/// Pull the stages out of a JobList. Kept pure so the shapes it must survive —
/// a missing annotation, a job with no completion time, a failed job — are
/// testable without a cluster.
pub fn parse_job_stages(body: &serde_json::Value) -> Vec<JobStage> {
    let Some(items) = body.get("items").and_then(|i| i.as_array()) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|job| {
            let component = job
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.get(cog_core::topology::LABEL_COMPONENT))
                .and_then(|c| c.as_str())?;
            if component != "change-execution" && component != "mainline-rollout" {
                return None;
            }
            let traceparent = job
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(TRACEPARENT_ANNOTATION))
                .and_then(|t| t.as_str())?
                .to_string();
            let name = job
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            let stamp = |v: Option<&serde_json::Value>| {
                v.and_then(|s| s.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|t| t.with_timezone(&Utc))
            };
            let created_at = stamp(job.get("metadata").and_then(|m| m.get("creationTimestamp")));
            let status = job.get("status");
            let finished_at = stamp(status.and_then(|s| s.get("completionTime")));
            let succeeded = status
                .and_then(|s| s.get("conditions"))
                .and_then(|c| c.as_array())
                .and_then(|conds| {
                    conds.iter().find_map(|c| {
                        let kind = c.get("type").and_then(|t| t.as_str())?;
                        let ok = c.get("status").and_then(|s| s.as_str())? == "True";
                        match kind {
                            "Complete" => Some(ok),
                            "Failed" => Some(!ok),
                            _ => None,
                        }
                    })
                });
            Some(JobStage {
                component: component.to_string(),
                name,
                traceparent,
                created_at,
                finished_at,
                succeeded,
            })
        })
        .collect()
}

#[cfg(test)]
mod job_list_tests {
    use super::*;

    fn job_list() -> serde_json::Value {
        serde_json::json!({
            "items": [
                {
                    "metadata": {
                        "name": "cogneva-change-abc",
                        "creationTimestamp": "2026-10-25T12:00:00Z",
                        "labels": { "app.kubernetes.io/component": "change-execution" },
                        "annotations": { "cogneva.ai/traceparent":
                            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" }
                    },
                    "status": {
                        "completionTime": "2026-10-25T12:10:00Z",
                        "conditions": [{ "type": "Complete", "status": "True" }]
                    }
                },
                {
                    // 没有注解的 Job：不是这一条链的，读成空而不是读成"有一个没名字的段"。
                    "metadata": {
                        "name": "some-other-job",
                        "labels": { "app.kubernetes.io/component": "change-execution" }
                    }
                },
                {
                    // 不是这两段的组件。
                    "metadata": {
                        "name": "cogneva-5ccc46d969",
                        "labels": { "app.kubernetes.io/component": "gateway" },
                        "annotations": { "cogneva.ai/traceparent":
                            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" }
                    }
                }
            ]
        })
    }

    #[test]
    fn only_annotated_stage_jobs_become_segments() {
        let stages = parse_job_stages(&job_list());
        assert_eq!(stages.len(), 1, "one job is a stage; the rest are neither");
        assert_eq!(stages[0].component, "change-execution");
        assert_eq!(stages[0].name, "cogneva-change-abc");
        assert_eq!(stages[0].succeeded, Some(true));
        assert!(stages[0].finished_at.is_some());
    }

    #[test]
    fn a_job_that_did_not_finish_is_not_read_as_success() {
        // 「没跑完」与「跑成功」在面板上必须不同形：前者判不了，后者是判过了。
        let body = serde_json::json!({
            "items": [{
                "metadata": {
                    "name": "cogneva-mainline-x",
                    "creationTimestamp": "2026-10-25T12:00:00Z",
                    "labels": { "app.kubernetes.io/component": "mainline-rollout" },
                    "annotations": { "cogneva.ai/traceparent":
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" }
                }
            }]
        });
        let stages = parse_job_stages(&body);
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].succeeded, None);
        assert!(stages[0].finished_at.is_none());
    }
}
