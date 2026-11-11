//! principle: feedback_no_hardcoding_config_surface
//! The trace id travels between processes as a header, and every hop has to
//! spell that header the same way. The gateway stamps it on the responses it
//! writes, the outbound client stamps it on the calls it makes, the trace store
//! reads it back off the wire. A hop that renames one copy writes a header
//! nobody reads -- and nothing errors: the next process starts a fresh trace,
//! so the operator sees a shorter chain, which is exactly what a request that
//! never left would look like. The names are declared once in
//! `cog_core::contract::observability`; this gate keeps every other crate
//! reading that declaration instead of typing the name again.
//!
//! 反证：声明之前，这道门在 `cog-gateway/src/lib.rs` 报 7 行、在
//! `cog-observability/src/traces.rs` 报 3 行——那 10 行正是当时树上各自写了一遍的头名。
//!
//! 读法、减法与过期判据只有一份，在 `common::declared_once`。

use crate::common;

use common::declared_once::{Declared, Wall};
use common::routes::repo_root;

/// The one file allowed to spell the header names.
const DECLARATION: &str = "crates/cog-core/src/contract/observability.rs";

const SUBJECT: &str = "追踪头名";

/// What a red means: the next hop reads a header nobody writes, starts a fresh
/// trace, and a short chain reads exactly like a request that never left.
const WHY: &str = "改了一处而没改另一处不会报错：那一跳写出去的头没人读，\
                   下一跳就另起一条追踪，读出来是「链路比以为的短」，\
                   而短链正是「请求根本没出去」的样子。";

/// The wall over the header names.
///
/// The hyphenated names cannot occur inside an identifier, so looking for the
/// bare text finds them wherever they are written -- including inside a
/// `format!` or a longer string, which is where a rename would otherwise hide.
/// `traceparent` is also a field name on half a dozen structs across the
/// workspace (`ctx.traceparent`, `"--traceparent"`), so only the standalone
/// literal counts as spelling the header.
fn wall() -> Wall<'static> {
    use cog_core::contract::observability as o;
    Wall::new(SUBJECT, WHY)
        .declared(vec![
            Declared::new("TRACEPARENT_HEADER", o::TRACEPARENT_HEADER).owned_by(DECLARATION),
            Declared::new("TRACE_ID_HEADER", o::TRACE_ID_HEADER)
                .spelled_as_text()
                .owned_by(DECLARATION),
            Declared::new("SPAN_ID_HEADER", o::SPAN_ID_HEADER)
                .spelled_as_text()
                .owned_by(DECLARATION),
            Declared::new("PARENT_SPAN_ID_HEADER", o::PARENT_SPAN_ID_HEADER)
                .spelled_as_text()
                .owned_by(DECLARATION),
        ])
        .floor(4)
        .ceiling(200)
}

#[test]
fn every_hop_spells_the_trace_headers_from_the_declaration() {
    wall().assert_clean(&repo_root());
}

/// The declared names have to be the ones the two halves of the round trip
/// actually use: a declaration nothing reads is the same second copy, moved.
#[test]
fn the_context_round_trips_through_the_declared_headers() {
    use cog_core::contract::observability as o;
    use cog_core::TraceContext;

    let mut ctx = TraceContext::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    ctx.parent_span_id = Some("b7ad6b7169203331".to_string());

    let headers = ctx.to_headers();
    for key in [
        o::TRACEPARENT_HEADER,
        o::TRACE_ID_HEADER,
        o::SPAN_ID_HEADER,
        o::PARENT_SPAN_ID_HEADER,
    ] {
        assert!(
            headers.contains_key(key),
            "这条上下文没有按声明写出 {key}：写出去的头没人读，下一跳就另起一条追踪。\
             实得 {:?}",
            headers.keys().collect::<Vec<_>>()
        );
    }

    let back = TraceContext::from_headers(&headers).expect("声明过的头必须能被自己读懂");
    assert_eq!(back.trace_id, ctx.trace_id);
    assert_eq!(back.span_id, ctx.span_id);
    assert_eq!(back.parent_span_id, ctx.parent_span_id);

    // 老一跳只认那一对自带 id 的头：没有 traceparent 时也得读出同一条追踪。
    let mut legacy = headers;
    legacy.remove(o::TRACEPARENT_HEADER);
    let back = TraceContext::from_headers(&legacy).expect("回落那一支也必须认得这些头");
    assert_eq!(back.trace_id, ctx.trace_id);
    assert_eq!(back.span_id, ctx.span_id);
}
