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

mod common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources};

/// The one file allowed to spell the header names.
const DECLARATION: &str = "crates/cog-core/src/contract/observability.rs";

/// A header name, and whether it has to be quoted to be recognised.
///
/// The hyphenated names cannot occur inside an identifier, so looking for the
/// bare text finds them wherever they are written -- including inside a
/// `format!` or a longer string, which is where a rename would otherwise hide.
/// `traceparent` is also a field name on half a dozen structs across the
/// workspace (`ctx.traceparent`, `"--traceparent"`), so only the standalone
/// literal counts as spelling the header.
fn headers() -> Vec<(&'static str, bool)> {
    use cog_core::contract::observability as o;
    vec![
        (o::TRACEPARENT_HEADER, true),
        (o::TRACE_ID_HEADER, false),
        (o::SPAN_ID_HEADER, false),
        (o::PARENT_SPAN_ID_HEADER, false),
    ]
}

fn offences(text: &str) -> Vec<String> {
    let mut hits = Vec::new();
    for line in production_lines(text) {
        let spelled = headers().into_iter().any(|(name, quoted)| {
            if quoted {
                line.contains(&format!("\"{name}\""))
            } else {
                line.contains(name)
            }
        });
        if spelled {
            hits.push(line.trim().to_string());
        }
    }
    hits
}

#[test]
fn every_hop_spells_the_trace_headers_from_the_declaration() {
    let sources = production_sources(&repo_root());

    let mut hits = Vec::new();
    for path in sources {
        let rel = path
            .strip_prefix(repo_root())
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if rel == DECLARATION {
            continue;
        }
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        hits.extend(
            offences(&text)
                .into_iter()
                .map(|hit| format!("{rel}: {hit}")),
        );
    }
    assert!(
        hits.is_empty(),
        "这些地方自己写了一遍追踪头名，没读 {DECLARATION} 里的声明。\
         改了一处而没改另一处不会报错：那一跳写出去的头没人读，下一跳就另起一条追踪，\
         读出来是「链路比以为的短」，而短链正是「请求根本没出去」的样子。\n{}",
        hits.join("\n")
    );
}

#[test]
fn the_census_reddens_on_code_and_ignores_comments_and_fixtures() {
    use cog_core::contract::observability as o;
    let text = format!(
        "/// 说明里提到 {} 不算。\n\
         pub fn stamp() -> String {{ format!(\"{}={{}}\", \"x\") }}\n\
         // {} 出现在注释里也不算\n\
         #[cfg(test)]\n\
         mod tests {{ const H: &str = \"{}\"; }}\n",
        o::TRACE_ID_HEADER,
        o::SPAN_ID_HEADER,
        o::PARENT_SPAN_ID_HEADER,
        o::TRACE_ID_HEADER
    );
    let hits = offences(&text);
    assert_eq!(hits.len(), 1, "应当只留下那行代码，实得 {hits:?}");
    assert!(
        hits[0].contains("pub fn stamp()"),
        "留下的应当是代码行，实得 {hits:?}"
    );

    // `traceparent` 只认独立字面量：同名的字段与 `--traceparent` 开关不是头名。
    let text = format!(
        "pub struct Wire {{ pub traceparent: Option<String> }}\n\
         let args = vec![\"--traceparent\".to_string()];\n\
         let h = \"{}\";\n",
        o::TRACEPARENT_HEADER
    );
    let hits = offences(&text);
    assert_eq!(
        hits.len(),
        1,
        "字段名与命令行开关不该被当成头名，实得 {hits:?}"
    );
    assert!(hits[0].contains("let h ="), "实得 {hits:?}");
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
