//! WebUI 接管台（2.5.6）：引导器自毁后，运维人员通过浏览器接管自进化系统。
//!
//! 页面为自包含静态 HTML（无构建依赖），调用 admin API：
//! change 列表/审批/应用/部署/回滚、D5 指标、进化事件、审计事件。
//! HTML 本身公开；所有 API 调用仍需 admin Bearer Token（页面中输入）。

use axum::response::{Html, IntoResponse, Response};

const TAKEOVER_HTML: &str = include_str!("takeover.html");

pub async fn takeover_handler() -> Response {
    // no-cache：页面 JS 随版本演进，禁止浏览器启发式缓存旧页面
    // （OAuth 向导等新逻辑必须立即生效）
    (
        [(axum::http::header::CACHE_CONTROL, "no-cache")],
        Html(TAKEOVER_HTML),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::TAKEOVER_HTML;

    /// 页面里定义的 i18n 词条：键 → 该词条的整段文本。
    ///
    /// 词条可以跨行（英文一行、中文一行），所以按花括号配平累积，不按行切。
    fn i18n_entries() -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut key: Option<String> = None;
        let mut body = String::new();
        let mut depth = 0i32;
        for line in TAKEOVER_HTML.lines() {
            if key.is_none() {
                // 词条形态：恰好两个空格缩进（顶层键）+ `键: {`。冒号后的对齐空格
                // 数量随键长变化，所以只能比较「去掉空白后以 { 起头」，不能匹配
                // 固定的 `: {`——那样只会认下短键，长键全部漏掉。
                if line.starts_with("  ") && !line.starts_with("   ") {
                    if let Some((k, rest)) = line.trim_start().split_once(':') {
                        if !k.is_empty()
                            && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                            && rest.trim_start().starts_with('{')
                        {
                            key = Some(k.to_string());
                            body.clear();
                            depth = 0;
                        }
                    }
                }
            }
            if let Some(k) = &key {
                body.push_str(line);
                body.push('\n');
                depth += line.matches('{').count() as i32;
                depth -= line.matches('}').count() as i32;
                if depth <= 0 {
                    out.push((k.clone(), body.clone()));
                    key = None;
                }
            }
        }
        out
    }

    /// 脚本里所有被引用的字面量键：`t('key')`、`t(a ? 'x' : 'y')`、`data-i18n`。
    ///
    /// 取 `t(` 到配平右括号之间的**全部**字符串字面量，而不是只认 `t('key')`
    /// 这一种写法——三元表达式里的键同样是字面量，漏掉就等于给它们开了后门。
    /// 解析不了的（括号不配平）当场 panic：静默跳过会让这个门禁变成摆设。
    fn referenced_keys() -> Vec<String> {
        let mut out = Vec::new();
        for lit in attribute_literals(TAKEOVER_HTML, "data-i18n") {
            out.push(lit);
        }
        for lit in attribute_literals(TAKEOVER_HTML, "data-i18n-ph") {
            out.push(lit);
        }
        let bytes = TAKEOVER_HTML.as_bytes();
        let mut i = 0;
        while let Some(pos) = TAKEOVER_HTML[i..].find("t(") {
            // `t(` 必须是一个独立调用：别的标识符的尾巴（`setTimeout(`）里也有
            // 这两个字符，把它的实参当键读就会凭空造出一个不存在的键。
            let abs = i + pos;
            if abs > 0 {
                let prev = bytes[abs - 1] as char;
                if prev.is_ascii_alphanumeric() || prev == '_' || prev == '$' {
                    i = abs + 1;
                    continue;
                }
            }
            let start = i + pos + 2;
            let mut depth = 1i32;
            let mut j = start;
            let mut strings = Vec::new();
            while j < bytes.len() {
                match bytes[j] as char {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    '\'' => {
                        if let Some(end) = TAKEOVER_HTML[j + 1..].find('\'') {
                            strings.push(TAKEOVER_HTML[j + 1..j + 1 + end].to_string());
                            j += end + 1;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            assert!(
                depth == 0,
                "takeover 页面里有一处 t( 的括号不配平，i18n 门禁看不见它的键"
            );
            for s in strings {
                if !s.is_empty() {
                    out.push(s);
                }
            }
            i = j.max(start) + 1;
        }
        out
    }

    /// 取某个属性（`data-i18n` / `data-i18n-ph`）的全部取值。
    fn attribute_literals(html: &str, attr: &str) -> Vec<String> {
        let needle = format!("{attr}=\"");
        let mut out = Vec::new();
        let mut i = 0;
        while let Some(pos) = html[i..].find(&needle) {
            let start = i + pos + needle.len();
            if let Some(end) = html[start..].find('"') {
                out.push(html[start..start + end].to_string());
                i = start + end;
            } else {
                break;
            }
        }
        out
    }

    #[test]
    fn every_translation_exists_in_both_languages() {
        let entries = i18n_entries();
        assert!(
            entries.len() > 100,
            "i18n 词条解析明显不完整: {}",
            entries.len()
        );
        for (key, body) in entries {
            for lang in ["en:", "zh:"] {
                assert!(body.contains(lang), "词条 {key} 缺 {lang} 文案");
            }
        }
    }

    #[test]
    fn every_referenced_translation_key_is_defined() {
        let defined: Vec<String> = i18n_entries().into_iter().map(|(k, _)| k).collect();
        for key in referenced_keys() {
            assert!(
                defined.iter().any(|d| d == &key),
                "页面引用了未定义的 i18n 键 {key}（显示出来就是键名本身）"
            );
        }
    }

    /// `$(...)` 调用的实参文本，以及调用结束后的下标。
    ///
    /// 按括号配平取整段实参，而不是截到下一个引号为止：页面里既有
    /// `$('m-events')`，也有 `$('rec' + platform)` 这种拼出来的 id，
    /// 后者截出来的只是半个名字，会变成假告警。
    fn call_span(html: &str, start: usize) -> Option<(String, usize)> {
        let bytes = html.as_bytes();
        let mut depth = 1i32;
        let mut j = start;
        while j < bytes.len() {
            match bytes[j] as char {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some((html[start..j].to_string(), j + 1));
                    }
                }
                _ => {}
            }
            j += 1;
        }
        None
    }

    /// 一段代码里的单引号字符串字面量。
    fn string_literals(span: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut i = 0;
        while let Some(pos) = span[i..].find('\'') {
            let start = i + pos + 1;
            let Some(end) = span[start..].find('\'') else {
                break;
            };
            out.push(span[start..start + end].to_string());
            i = start + end + 1;
        }
        out
    }

    #[test]
    fn every_element_the_script_touches_exists() {
        // 这个页面没有编译期检查：写错一个 id 就是运行时静默失效。
        // 纯字面量取到的 id 必须真实存在；拼接出来的 id 静态算不出全名，
        // 退一步要求每个片段都是某个真实 id 的子串（片段写错同样会被抓住），
        // 唯一完全跳过的是压根没有字面量的（如 dataset 里传进来的 id）。
        let ids = attribute_literals(TAKEOVER_HTML, "id");
        let mut i = 0;
        let mut seen = 0;
        while let Some(pos) = TAKEOVER_HTML[i..].find("$(") {
            let start = i + pos + 2;
            let Some((span, next)) = call_span(TAKEOVER_HTML, start) else {
                break;
            };
            let literals = string_literals(&span);
            if !literals.is_empty() {
                seen += 1;
                let whole = literals.len() == 1 && span.trim() == format!("'{}'", literals[0]);
                for lit in literals {
                    assert!(
                        if whole {
                            ids.iter().any(|id| id == &lit)
                        } else {
                            ids.iter().any(|id| id.contains(&lit))
                        },
                        "脚本操作了不存在的元素 #{lit}"
                    );
                }
            }
            i = next;
        }
        assert!(seen > 50, "元素引用解析明显不完整: {seen}");
    }

    #[test]
    fn embedded_page_references_admin_api() {
        let html = super::TAKEOVER_HTML;
        for needle in [
            "/api/v1/evolution/changes",
            "/api/v1/evolution/rollback",
            "/api/v1/evolution/metrics",
            "/api/v1/evolution/events",
            "/api/v1/evolution/promotion/switch",
            "/api/v1/evolution/promotions",
            "/api/v1/evolution/promotion/trend",
            "/api/v1/audit/events",
            "/approve",
        ] {
            assert!(html.contains(needle), "takeover page missing {needle}");
        }
    }
}
