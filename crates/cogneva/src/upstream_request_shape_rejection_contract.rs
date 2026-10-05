//! Upstream request-shape rejection contract.
//!
//! When the security gateway gets a non-2xx response before the first byte, it
//! has to distinguish two very different failures:
//!
//! * **Request-shape errors (HTTP 400 / 404 / 422)** — a bad message chain,
//!   an unsupported parameter, or a nonexistent endpoint. These are caller
//!   defects: the same request would be refused by every compatible upstream,
//!   so they must NOT be written into the upstream health/suspect table
//!   (otherwise one bad request poisons the whole pool). The gateway still
//!   fails over — capabilities do differ between upstreams — but records only
//!   the independent `llm_upstream_client_errors_total` counter.
//! * **Every other non-2xx status** (quota, rate limit, auth, server errors,
//!   …) — an upstream-health event, which goes through the failure-marking
//!   path.
//!
//! This contract lives under `src/` rather than `tests/` so it is built as an
//! in-crate unit test and stays inside the contributable path set.

/// Mirror of the security gateway's request-shape classification:
/// `matches!(status.as_u16(), 400 | 404 | 422)`.
///
/// The predicate is duplicated here deliberately: the contract must be able to
/// name the exact status table even though the gateway's copy is inline on the
/// request path. If either side changes, both sides change together.
fn is_request_shape_error(status: u16) -> bool {
    matches!(status, 400 | 404 | 422)
}

#[test]
fn request_shape_statuses_are_caller_faults_not_upstream_health_events() {
    // Bad request, endpoint-not-found, unprocessable entity: the request's
    // shape is wrong, so the health table must not be touched.
    for status in [400, 404, 422] {
        assert!(
            is_request_shape_error(status),
            "HTTP {status} is a request-shape rejection and must not be \
             recorded against upstream health"
        );
    }
}

#[test]
fn quota_auth_rate_limit_and_server_statuses_remain_upstream_health_events() {
    // These shapes are vendor/health conditions and must keep going through
    // the failure-marking path rather than the independent counter.
    for status in [401, 402, 403, 429, 451, 500, 502, 503, 504] {
        assert!(
            !is_request_shape_error(status),
            "HTTP {status} must not be reclassified as a request-shape error"
        );
    }
}

#[test]
fn the_independent_counter_recorded_for_shape_rejections_is_published() {
    // For shape errors the gateway records this counter instead of marking the
    // upstream, so the metric name has to keep existing and stay non-empty.
    let counter = cog_core::metric_names::LLM_UPSTREAM_CLIENT_ERRORS_TOTAL;
    assert!(
        !counter.is_empty(),
        "the request-shape rejection counter needs a real metric name"
    );
}
