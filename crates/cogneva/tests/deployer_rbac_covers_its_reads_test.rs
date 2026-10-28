//! The cluster rights the evolution ServiceAccount is granted, against the ones
//! its commands need.
//! principle: feedback_a_second_reader_of_a_fact_references_the_one_definition
//!
//! A missing grant is invisible from every other face. The process starts, the
//! loop runs, every other gate is green — the command that needed the right
//! simply fails, at the moment it is needed, with a `Forbidden` the loop has no
//! reason to raise above the log line it writes. Two live readings of that
//! shape, one per half of this file:
//!
//! * `kubectl get resourcequota -o json` answered `resourcequotas is forbidden`
//!   for `system:serviceaccount:cogneva:cogneva-evolution`, so the governance
//!   drift comparison had a declared side and no cluster side — the failure
//!   counter moved, the per-object reading never appeared;
//! * `kubectl rollout undo deployment/cogneva --dry-run=server` answered
//!   `failed to retrieve replica sets from deployment cogneva: replicasets.apps
//!   is forbidden`, so the change-deploy rollback path would have reached its
//!   one job — undoing a bad rollout — and stopped on a permission error.
//!
//! Neither is a manifest drift: `deploy/scripts/check-deploy-parity.sh` compares
//! the Role across all four carriers and they agree, because they agree on the
//! wrong list. What was missing is the comparison to the *user*.
//!
//! The rights are spent two ways, and this file owns one of them:
//!
//! 1. **here** — the resource words the deployer's own argv names (the `get`/
//!    `delete`/`patch`/`set image`/`rollout` calls in this crate, including the
//!    ones the rollout and change Jobs run). A command is not a document, so
//!    reading the manifests cannot see any of this;
//! 2. the API objects the release set delivers, because the rollout Job applies
//!    the bundle with `-f`. That face already has an owner inside this crate:
//!    `the_evolution_role_covers_every_document_the_loop_will_apply`, which
//!    walks every deliverable tree through the loop's own filter. It is named
//!    here only so a reader of one knows the other exists.
//!
//! Ceilings, stated rather than implied. The scan reads text: a command whose
//! resource word is computed is resolved through the local `let` that spells it
//! or through the table below, and anything else is a **failure**, not a skip —
//! the one direction a text scan can be trusted in. The file list is the
//! scan's own input, so a third check closes it: every source file in this crate
//! that contains a kubectl command must be named here, with the principal it
//! runs under. New commands under the same ServiceAccount therefore cannot
//! arrive unnoticed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::rust_production::{production_sources, production_text, relative_name};

use cog_reflection::{workload_kind_of_resource, WORKLOAD_KINDS};
use serde::Deserialize;

/// The Role the in-cluster deployer runs under. Both Job specs it writes name
/// this ServiceAccount, and the parity gate holds this file, the chart template
/// and the three rendered profiles equal, so reading one carrier is reading all
/// of them.
const RBAC: &str = "deploy/k3s/evolution-rbac.yaml";

const CRATE_SRC: &str = "crates/cog-reflection/src";

/// The ServiceAccount both Job specs and the deployer pod run under.
const SERVICE_ACCOUNT: &str = "cogneva-evolution";

/// Sources whose cluster commands run under that ServiceAccount. The rollout
/// Job (dispatched from `mainline_deployer.rs`, running the code in
/// `mainline_rollout.rs`), the change Job (dispatched from
/// `change_execution.rs`) and the in-Job rollout the change path runs
/// (`image_rollout.rs`) all name it; the deployer pod itself is the first file.
const SCANNED: &[&str] = &[
    "crates/cog-reflection/src/mainline_deployer.rs",
    "crates/cog-reflection/src/mainline_rollout.rs",
    "crates/cog-reflection/src/change_execution.rs",
    "crates/cog-reflection/src/image_rollout.rs",
    // Runs in the same process and under the same ServiceAccount as the
    // deployer: it reads a claim's sizes and patches its request, and the two
    // admission bounds that cap that request.
    "crates/cog-reflection/src/volume_capacity.rs",
];

/// Crate sources that do talk to the cluster under a **different** principal.
/// They are listed so the discovery check below can tell "a file nobody
/// classified" from "a file classified as someone else's".
const OTHER_PRINCIPALS: &[(&str, &str)] = &[
    (
        "crates/cog-reflection/src/gitops_puller.rs",
        "app pod: SA cogneva, Role cogneva-gitops-puller",
    ),
    (
        "crates/cog-reflection/src/gitops_publisher.rs",
        "app pod: SA cogneva, Roles cogneva-gitops-puller / llm-admin",
    ),
    (
        "crates/cog-reflection/src/live_revision.rs",
        "app pod: SA cogneva, Role cogneva-gitops-puller (same reads as gitops_publisher, \
         which is its only production caller today)",
    ),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// One file the `SCANNED` list names, read by path.
///
/// Not by position: the list is the set of sources that speak under this
/// ServiceAccount, and adding a member is a statement about that set. A test
/// that indexed into it would silently start reading whichever file the new
/// member pushed aside -- which is exactly what a judgement about *this* source
/// must not do.
fn scanned(relative: &str) -> String {
    assert!(
        SCANNED.contains(&relative),
        "{relative} is not in SCANNED, so nothing here judges it"
    );
    read(relative)
}

/// The shipped half of a file: a fixture that writes an argv is not a command,
/// and a test may name any resource it likes.
///
/// It used to end at the first test item. `mainline_deployer.rs` is on this
/// gate's list and carries a `#[cfg(test)]` helper at line 7,812 with 1,793
/// lines of production after it, so a read added down there would have needed a
/// grant that nothing here asked for -- the shape this whole file exists to
/// catch, sitting outside the search.
fn production_source(text: &str) -> String {
    production_text(text)
}

// ---------------------------------------------------------------------------
// The Role
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Role {
    /// `Role` 还是 `ClusterRole`。两者规则同形，差别只在作用域：一份清单同时
    /// 声明这两类时，只看 `Role` 会让 ClusterRole 的规则在判据里凭空消失。
    kind: String,
    name: String,
    rules: Vec<Rule>,
}

#[derive(Clone)]
struct Rule {
    groups: Vec<String>,
    resources: Vec<String>,
    verbs: Vec<String>,
    /// `resourceNames` 白名单：非空时这条规则**只**覆盖这几个对象。
    names: Vec<String>,
}

/// `text` 里的每一篇文档，或者一次带文件名的 panic。解析不出来的文档是**失败**，
/// 不是跳过：丢掉它就把它带的规则一起丢掉了，闸门会在一片它从没读过的输入上报绿。
/// （`serde_yaml` 在文档反序列化失败时不推进游标，所以这里把错误丢掉不只是有损。）
fn yaml_docs(text: &str, file: &str) -> Vec<serde_yaml::Value> {
    let mut out: Vec<serde_yaml::Value> = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(text) {
        match serde_yaml::Value::deserialize(doc) {
            Ok(v) if v.is_null() => {}
            Ok(v) => out.push(v),
            Err(e) => panic!(
                "{file}: 有一篇文档解析不出来（{e}）：解析不了是判据的形态问题，不能当它不存在"
            ),
        }
    }
    out
}

fn strings(value: Option<&serde_yaml::Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// 一条绑定：把哪条 Role 授给谁。
///
/// 每份清单可以有多条绑定（namespace 的 RoleBinding 与集群作用域的
/// ClusterRoleBinding 常常写在一起），所以这里是一列，不是一个值。早先只留
/// 最后一个 roleRef 的写法在同一份文件里第二条绑定出现时会静默指错对象。
struct Binding {
    kind: String,
    role_name: String,
    subjects: Vec<String>,
}

/// The Roles a manifest file declares, plus its bindings.
struct RoleFile {
    roles: Vec<Role>,
    bindings: Vec<Binding>,
}

impl RoleFile {
    /// 授给 `subject` 的那几条角色，按名字从 `roles` 里取。
    ///
    /// 一份清单里同一个身份可以同时被 namespace 的 RoleBinding 与集群作用域的
    /// ClusterRoleBinding 绑定，API server 按并集判权，所以这里返回一列。
    fn held_by(&self, subject: &str) -> Vec<&Role> {
        self.bindings
            .iter()
            .filter(|b| b.subjects.iter().any(|s| s == subject))
            .flat_map(|b| self.roles.iter().filter(move |r| r.name == b.role_name))
            .collect()
    }
}

fn roles_in(relative: &str) -> RoleFile {
    let text = read(relative);
    let mut roles: Vec<Role> = Vec::new();
    let mut bindings: Vec<Binding> = Vec::new();

    for doc in yaml_docs(&text, relative) {
        let kind = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        match kind {
            // 两类都是「一张规则表」，只差作用域。丢下 ClusterRole 的规则会让
            // 自检的条数对不上——而它对不上时唯一的解释是「有一段规则没进判据」。
            "Role" | "ClusterRole" => {
                let name = doc
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or_default()
                    .to_string();
                let mut rules = Vec::new();
                for rule in doc
                    .get("rules")
                    .and_then(|r| r.as_sequence())
                    .map(|s| s.to_vec())
                    .unwrap_or_default()
                {
                    rules.push(Rule {
                        groups: strings(rule.get("apiGroups")),
                        resources: strings(rule.get("resources")),
                        verbs: strings(rule.get("verbs")),
                        names: strings(rule.get("resourceNames")),
                    });
                }
                roles.push(Role {
                    kind: kind.to_string(),
                    name,
                    rules,
                });
            }
            "RoleBinding" | "ClusterRoleBinding" => {
                let subjects: Vec<String> = doc
                    .get("subjects")
                    .and_then(|s| s.as_sequence())
                    .map(|s| s.to_vec())
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|subject| {
                        subject
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(str::to_string)
                    })
                    .collect();
                let role_name = doc
                    .get("roleRef")
                    .and_then(|r| r.get("name"))
                    .and_then(|n| n.as_str())
                    .map(str::to_string);
                assert!(
                    role_name.is_some(),
                    "{relative} 的 {kind} 没有 roleRef.name：绑定指向不明，判不了它授给了谁"
                );
                bindings.push(Binding {
                    kind: kind.to_string(),
                    role_name: role_name.unwrap_or_default(),
                    subjects,
                });
            }
            _ => {}
        }
    }

    let parsed: usize = roles.iter().map(|r| r.rules.len()).sum();
    assert_eq!(
        parsed,
        text.lines()
            .filter(|l| l.trim_start().starts_with("- apiGroups:"))
            .count(),
        "{relative} 的规则条数解析后对不上，比对结果不可信"
    );

    RoleFile { roles, bindings }
}

fn role() -> Role {
    let file = roles_in(RBAC);

    assert!(!file.roles.is_empty(), "{RBAC} 里没有解析出任何规则");
    assert!(
        !file.bindings.is_empty(),
        "{RBAC} 里没有任何绑定：授了权却没人拿到"
    );
    let bound: Vec<&Binding> = file
        .bindings
        .iter()
        .filter(|b| b.subjects.iter().any(|s| s == SERVICE_ACCOUNT))
        .collect();
    assert_eq!(
        bound.len(),
        1,
        "{RBAC} 里绑给 {SERVICE_ACCOUNT} 的绑定有 {} 条，这条判据只读得了唯一一条",
        bound.len()
    );
    // 部署器要的是**命名空间内**的权利：ClusterRoleBinding 会把同一张表铺到
    // 集群作用域，那不是这里判的东西，出现了要说出来而不是照读。
    assert_eq!(
        bound[0].kind, "RoleBinding",
        "部署器的绑定是 {}，这条判据读的是命名空间内的 RoleBinding",
        bound[0].kind
    );
    let held = file.held_by(SERVICE_ACCOUNT);
    assert_eq!(
        held.len(),
        1,
        "{RBAC} 的绑定指向 {:?}，但这份文件里叫这个名字的角色有 {} 个",
        bound[0].role_name,
        held.len()
    );
    assert_eq!(
        held[0].kind, "Role",
        "RoleBinding 指向的是 {} {}：命名空间绑定只能引用 Role",
        held[0].kind, held[0].name
    );

    held[0].clone()
}

impl Role {
    /// A right is held when *some* rule for that object holds it, because the
    /// API server answers from the union of every rule matching the request.
    /// The Role itself splits its ConfigMap rights across a read rule and a
    /// delete rule; asking one rule to carry both would report a correct Role
    /// as missing a grant, and a gate that fails on the live, working
    /// configuration is a gate everyone learns to ignore.
    fn grants(&self, group: &str, resource: &str, verbs: &[&str]) -> bool {
        let held: BTreeSet<&str> = self
            .rules
            .iter()
            .filter(|r| {
                r.groups.iter().any(|g| g == group) && r.resources.iter().any(|x| x == resource)
            })
            .flat_map(|r| r.verbs.iter().map(String::as_str))
            .collect();
        verbs.iter().all(|v| held.contains(v))
    }
}

impl Rule {
    /// Whether this rule covers the verb **for this object name**. A rule that
    /// names `resourceNames` covers only those objects; a rule that names none
    /// covers the whole collection. RBAC also refuses a collection-wide `list`
    /// for a name-scoped rule unless the request carries a matching
    /// `fieldSelector`, which is why the base read asks for each name
    /// separately -- and why checking `get` here is the right question: a
    /// name-scoped `list` would be a grant that cannot serve the request.
    fn grants_named(&self, group: &str, resource: &str, verb: &str, name: &str) -> bool {
        self.groups.iter().any(|g| g == group)
            && self.resources.iter().any(|x| x == resource)
            && self.verbs.iter().any(|v| v == verb)
            && (self.names.is_empty() || self.names.iter().any(|n| n == name))
    }
}

// ---------------------------------------------------------------------------
// The commands
// ---------------------------------------------------------------------------

/// The verbs kubectl takes a resource word for. `set` and `rollout` take a
/// subcommand first and their target after it, so they are handled apart.
const VERBS: &[&str] = &[
    "get", "list", "watch", "describe", "create", "apply", "delete", "patch", "scale", "exec",
    "logs", "set", "rollout",
];

/// The verbs this crate cannot be confused with another tool about: a
/// `git`/`buildah`/`cargo` subcommand is never spelled `get` or `rollout`, so a
/// word after one of these that this gate cannot resolve is a missing grant
/// rather than another tool's argument. The rest of [`VERBS`] does collide --
/// `git describe --tags` and `git worktree list --porcelain` both sit in these
/// sources -- so for those the command only counts when the word after it is a
/// resource this gate can name.
const CORE_VERBS: &[&str] = &[
    "get", "create", "delete", "patch", "apply", "set", "rollout",
];

struct Command {
    file: String,
    line: usize,
    verb: String,
    sub: Option<String>,
    slot: Option<String>,
}

/// A quoted string that is the whole element, else `None` (an expression).
fn literal(element: &str) -> Option<String> {
    let e = element.trim();
    if e.len() >= 2 && e.starts_with('"') && e.ends_with('"') {
        return Some(
            e[1..e.len() - 1]
                .replace("\\\"", "\"")
                .replace("\\\\", "\\"),
        );
    }
    None
}

/// The elements of every `&[ … ]` in the source, with the line it starts on.
///
/// Bracket depth is walked rather than skipped, so an argv carrying an array of
/// its own is still one list. A span that never closes is not a command and is
/// dropped; that is the single place this scan can see nothing, and it can only
/// under-see a slice expression, never a `&[` that ends.
fn argv_spans(src: &str) -> Vec<(usize, Vec<String>)> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 1 < chars.len() {
        if chars[i] != '&' || chars[i + 1] != '[' {
            i += 1;
            continue;
        }
        let mut depth = 0i32;
        let mut in_str = false;
        let mut esc = false;
        let mut j = i + 1;
        while j < chars.len() {
            let c = chars[j];
            if in_str {
                if esc {
                    esc = false;
                } else if c == '\\' {
                    esc = true;
                } else if c == '"' {
                    in_str = false;
                }
            } else if c == '"' && (j == 0 || chars[j - 1] != '\'') {
                in_str = true;
            } else if c == '[' {
                depth += 1;
            } else if c == ']' {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            j += 1;
        }
        if j < chars.len() && depth == 0 {
            let span: String = chars[i + 2..j].iter().collect();
            let line = chars[..i].iter().filter(|c| **c == '\n').count() + 1;
            out.push((line, elements(&span)));
            i = j + 1;
        } else {
            i += 1;
        }
    }
    out
}

/// Split a span into its elements at top-level commas, keeping strings intact.
fn elements(span: &str) -> Vec<String> {
    let chars: Vec<char> = span.chars().collect();
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut esc = false;
    let mut depth = 0i32;
    for (k, c) in chars.iter().enumerate() {
        if in_str {
            cur.push(*c);
            if esc {
                esc = false;
            } else if *c == '\\' {
                esc = true;
            } else if *c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' if k == 0 || chars[k - 1] != '\'' => {
                in_str = true;
                cur.push(*c);
            }
            '(' | '[' | '{' => {
                depth += 1;
                cur.push(*c);
            }
            ')' | ']' | '}' => {
                depth -= 1;
                cur.push(*c);
            }
            ',' if depth == 0 => {
                out.push(cur.trim().to_string());
                cur.clear();
            }
            _ => cur.push(*c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out.retain(|e| !e.is_empty());
    out
}

fn commands_in(file: &str, src: &str) -> Vec<Command> {
    let mut out = Vec::new();
    for (line, els) in argv_spans(src) {
        for (k, element) in els.iter().enumerate() {
            let Some(word) = literal(element) else {
                continue;
            };
            if !VERBS.contains(&word.as_str()) {
                continue;
            }
            if !CORE_VERBS.contains(&word.as_str()) {
                let named = els.get(k + 1).and_then(|e| literal(e));
                if named.is_none_or(|w| resolve_word(&w).is_err()) {
                    continue;
                }
            }
            let (sub, slot) = match word.as_str() {
                "set" | "rollout" => (
                    els.get(k + 1).and_then(|e| literal(e)),
                    els.get(k + 2).cloned(),
                ),
                _ => (None, els.get(k + 1).cloned()),
            };
            out.push(Command {
                file: file.to_string(),
                line,
                verb: word,
                sub,
                slot,
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Resolving a resource word
// ---------------------------------------------------------------------------

/// kubectl's own words, for the objects that are not workloads. The workload
/// half is read from the crate's table instead of being repeated here, so a new
/// workload kind reaches both faces at once.
const SHORTCUTS: &[(&str, &str, &str)] = &[
    ("configmap", "", "configmaps"),
    ("service", "", "services"),
    ("serviceaccount", "", "serviceaccounts"),
    ("pod", "", "pods"),
    ("pvc", "", "persistentvolumeclaims"),
    ("resourcequota", "", "resourcequotas"),
    ("limitrange", "", "limitranges"),
    ("event", "", "events"),
    ("secret", "", "secrets"),
    ("job", "batch", "jobs"),
    ("cronjob", "batch", "cronjobs"),
    ("replicaset", "apps", "replicasets"),
    ("networkpolicy", "networking.k8s.io", "networkpolicies"),
    ("ingress", "networking.k8s.io", "ingresses"),
];

fn resolve_word(word: &str) -> Result<(&'static str, &'static str), String> {
    let w = word.trim().to_lowercase();
    let singular = w.strip_suffix('s').unwrap_or(&w);
    for candidate in [w.as_str(), singular] {
        if let Some(row) = workload_kind_of_resource(candidate) {
            return Ok((row.rbac_group, row.rbac_resource));
        }
        if let Some((_, group, resource)) = SHORTCUTS.iter().find(|(word, _, _)| *word == candidate)
        {
            return Ok((group, resource));
        }
    }
    Err(format!(
        "kubectl 词表里没有 {word:?}：这个资源词落在判据之外，授权面判不了它"
    ))
}

/// A resource slot whose word the source computes rather than writes.
///
/// Each row carries the text that justifies its words, and that text is checked
/// in the same file: if the guard goes away the expression may hold something
/// else, and this row has to be re-read rather than trusted.
struct Computed {
    expr: &'static str,
    words: Words,
    guard: &'static str,
}

enum Words {
    /// The resource words of the crate's own workload table.
    WorkloadRows,
    /// Words the expression lowers to itself (a kind name made lowercase).
    Literal(&'static [&'static str]),
}

const COMPUTED: &[Computed] = &[
    Computed {
        expr: "entry.resource",
        words: Words::WorkloadRows,
        guard: "for entry in &WORKLOAD_KINDS",
    },
    Computed {
        expr: "&consumer.kind",
        words: Words::WorkloadRows,
        guard: "workload_kind_of_resource(&consumer.kind)",
    },
    Computed {
        expr: "w.kind",
        words: Words::WorkloadRows,
        guard: "workload_kind_of_resource(w.kind)",
    },
    Computed {
        expr: "resource",
        words: Words::WorkloadRows,
        guard: "workload_kind_of_resource(resource)",
    },
    Computed {
        expr: "kind",
        words: Words::WorkloadRows,
        guard: "for kind in SUPPORT_WORKLOAD_KINDS",
    },
    Computed {
        expr: "&workload.kind_arg()",
        words: Words::Literal(&["deployment", "statefulset", "daemonset", "job"]),
        guard: "kind_arg",
    },
];

/// The `TYPE/NAME` form kubectl takes: the resource word is the part before the
/// slash of a literal inside the expression (`format!("deployment/{name}")`).
fn type_name_in(expr: &str) -> Option<String> {
    let chars: Vec<char> = expr.chars().collect();
    let mut k = 0usize;
    while k < chars.len() {
        if chars[k] != '"' || (k > 0 && chars[k - 1] == '\'') {
            k += 1;
            continue;
        }
        let mut in_str = true;
        let mut esc = false;
        let mut body = String::new();
        k += 1;
        while k < chars.len() {
            let c = chars[k];
            if esc {
                body.push(c);
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
                break;
            } else {
                body.push(c);
            }
            k += 1;
        }
        if !in_str {
            if let Some((word, _)) = body.split_once('/') {
                if !word.is_empty() && word.chars().all(|c| c.is_ascii_alphabetic()) {
                    return Some(word.to_string());
                }
            }
        }
        k += 1;
    }
    None
}

/// The text `let <ident> = …;` binds in the same file, if it is bound once.
fn binding_expr(src: &str, ident: &str) -> Option<String> {
    let needle = format!("let {ident} = ");
    let start = src.find(&needle)? + needle.len();
    let rest = &src[start..];
    let end = rest.find(';').unwrap_or(rest.len());
    Some(rest[..end].trim().to_string())
}

fn bare_ident(slot: &str) -> Option<&str> {
    let t = slot.trim().trim_start_matches('&').trim();
    if !t.is_empty()
        && t.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Some(t);
    }
    None
}

/// The API objects a slot can address, or the reason this scan refuses to guess.
fn slot_words(
    file_src: &str,
    slot: &str,
    used: &mut BTreeSet<&'static str>,
) -> Result<Vec<(&'static str, &'static str)>, String> {
    if let Some(word) = literal(slot) {
        return Ok(vec![resolve_word(&word)?]);
    }
    if let Some(word) = type_name_in(slot) {
        return Ok(vec![resolve_word(&word)?]);
    }
    if let Some(ident) = bare_ident(slot) {
        if let Some(expr) = binding_expr(file_src, ident) {
            if let Some(word) = type_name_in(&expr) {
                return Ok(vec![resolve_word(&word)?]);
            }
        }
    }
    if let Some(row) = COMPUTED.iter().find(|c| c.expr == slot.trim()) {
        if !file_src.contains(row.guard) {
            return Err(format!(
                "槽位 {:?} 的来源判据 {:?} 在文件里找不到了：这几个字可能已经不是那张表给的",
                slot, row.guard
            ));
        }
        used.insert(row.expr);
        return match row.words {
            Words::WorkloadRows => Ok(WORKLOAD_KINDS
                .iter()
                .map(|k| (k.rbac_group, k.rbac_resource))
                .collect()),
            Words::Literal(words) => words
                .iter()
                .map(|w| resolve_word(w))
                .collect::<Result<Vec<_>, _>>(),
        };
    }
    Err(format!(
        "看不出一条命令的资源槽位 {:?} 指的是什么资源：让它是字面量，或者在这里登记它的来源与守卫",
        slot
    ))
}

/// One thing a command needs permission for: the API group, the resource, and
/// the verbs it will use against it.
type Obligation = (&'static str, &'static str, Vec<&'static str>);

/// What a command needs, beside the resource it names.
///
/// `rollout undo` is the one that is not a straight read or write of its
/// target: kubectl resolves the previous template through the deployment's
/// ReplicaSets before it patches anything, so the target's rule is not enough.
/// That read is what answered the live probe with `failed to retrieve replica
/// sets from deployment cogneva: replicasets.apps is forbidden`.
fn obligations(
    cmd: &Command,
    target: (&'static str, &'static str),
) -> Result<Vec<Obligation>, String> {
    let (group, resource) = target;
    let here = |verbs: Vec<&'static str>| vec![(group, resource, verbs)];
    match cmd.verb.as_str() {
        "get" | "list" | "watch" => {
            let verb: &'static str = match cmd.verb.as_str() {
                "get" => "get",
                "list" => "list",
                _ => "watch",
            };
            Ok(here(vec![verb]))
        }
        "describe" | "logs" => Ok(here(vec!["get"])),
        "create" => Ok(here(vec!["create"])),
        "delete" => Ok(here(vec!["delete"])),
        "patch" | "scale" => Ok(here(vec!["patch"])),
        "set" => match cmd.sub.as_deref() {
            Some("image") => Ok(here(vec!["patch"])),
            other => Err(format!("set {other:?} 的命令形态不在判据里，授权面判不了")),
        },
        "rollout" => match cmd.sub.as_deref() {
            Some("status") => Ok(here(vec!["get"])),
            Some("restart" | "pause" | "resume") => Ok(here(vec!["patch"])),
            Some("undo") => Ok(vec![
                (group, resource, vec!["patch"]),
                ("apps", "replicasets", vec!["get", "list"]),
            ]),
            other => Err(format!(
                "rollout {other:?} 的命令形态不在判据里，授权面判不了"
            )),
        },
        "apply" => Ok(Vec::new()),
        other => Err(format!("{other} 还没有登记它的授权含义")),
    }
}

fn needed_for(file: &'static str, used: &mut BTreeSet<&'static str>) -> Vec<Obligation> {
    let src = read(file);
    let body = production_source(&src);
    let mut needed = Vec::new();
    for cmd in commands_in(file, &body) {
        if cmd.verb == "apply" {
            assert_eq!(
                cmd.slot.as_deref().and_then(literal).as_deref(),
                Some("-f"),
                "{}:{} apply 的形态不是 -f：这条路的授权面由交付面那半边判，别的形态没人判",
                cmd.file,
                cmd.line
            );
            continue;
        }
        let Some(slot) = cmd.slot.as_deref() else {
            panic!("{}:{} 的 {} 没有资源槽位", cmd.file, cmd.line, cmd.verb);
        };
        // 动词后面直接跟参数：这一条命令的形态不是"动词 + 资源词"，扫描器不猜。
        if literal(slot).is_some_and(|w| w.starts_with('-')) {
            panic!(
                "{}:{} 的 {} 后面直接跟了参数 {slot:?}：这条命令的形态扫描器看不懂",
                cmd.file, cmd.line, cmd.verb
            );
        }
        let words = slot_words(&body, slot, used).unwrap_or_else(|e| {
            panic!(
                "{}:{} {} 的目标槽位 {slot:?}：{e}",
                cmd.file, cmd.line, cmd.verb
            )
        });
        for word in words {
            needed.extend(
                obligations(&cmd, word)
                    .unwrap_or_else(|e| panic!("{}:{} {}：{e}", cmd.file, cmd.line, cmd.verb)),
            );
        }
    }
    needed
}

// ---------------------------------------------------------------------------
// The gates
// ---------------------------------------------------------------------------

/// Every resource word the deployer's commands name is granted, with the verbs
/// the command uses.
#[test]
fn the_role_grants_every_resource_the_loop_addresses() {
    let role = role();
    let mut used = BTreeSet::new();
    let mut needed: BTreeMap<(&'static str, &'static str), BTreeSet<&'static str>> =
        BTreeMap::new();
    for file in SCANNED {
        for (group, resource, verbs) in needed_for(file, &mut used) {
            needed
                .entry((group, resource))
                .or_default()
                .extend(verbs.iter().copied());
        }
    }

    assert!(
        needed.len() >= 5,
        "扫出来的资源面只有 {} 个对象，这个门禁是空的",
        needed.len()
    );

    let mut missing = Vec::new();
    for ((group, resource), verbs) in &needed {
        let verbs: Vec<&str> = verbs.iter().copied().collect();
        if !role.grants(group, resource, &verbs) {
            missing.push(format!("{group}/{resource} 需要 {verbs:?}"));
        }
    }
    assert!(
        missing.is_empty(),
        "{} 没有覆盖部署器会走的路径，少了就是在真需要的那一刻报权限被拒: {missing:?}",
        role.name
    );

    // 表里每一行都得真的被用上：没人用的那一行是过期的判断，会替下一处
    // 计算出来的槽位背书。
    for row in COMPUTED {
        assert!(
            used.contains(row.expr),
            "计算型槽位表里的 {:?} 没有出现在任何一条命令里：这一行该删了",
            row.expr
        );
    }
}

/// The scan has to be able to see the commands it is judging.
#[test]
fn the_scan_sees_the_commands_it_is_supposed_to() {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut resolved: BTreeSet<(&'static str, &'static str)> = BTreeSet::new();
    let mut used = BTreeSet::new();
    for file in SCANNED {
        let src = read(file);
        let body = production_source(&src);
        for cmd in commands_in(file, &body) {
            *seen.entry(cmd.verb.clone()).or_default() += 1;
        }
        for (group, resource, _) in needed_for(file, &mut used) {
            resolved.insert((group, resource));
        }
    }
    assert!(
        seen.values().sum::<usize>() >= 12,
        "只扫到 {seen:?}：命令行读不出来了，绿的是空集"
    );
    for verb in ["get", "delete", "patch", "set", "rollout"] {
        assert!(seen.contains_key(verb), "扫不到 {verb} 这类命令：{seen:?}");
    }

    // 两个真出过事的资源词必须在解析结果里：一个真的少授过（resourcequotas
    // 报 Forbidden），一个是经 kubectl 自己那一跳读出去的（replicasets）。
    for word in [
        ("", "resourcequotas"),
        ("", "persistentvolumeclaims"),
        ("batch", "jobs"),
        ("apps", "replicasets"),
    ] {
        assert!(
            resolved.contains(&word),
            "解析出来的资源面里没有 {word:?}：别名表或槽位解析坏了"
        );
    }
}

/// A file that talks to the cluster must be classified, not merely absent.
#[test]
fn every_file_that_talks_to_the_cluster_is_named() {
    let mut named: BTreeSet<String> = SCANNED.iter().map(|f| f.to_string()).collect();
    for (file, _) in OTHER_PRINCIPALS {
        assert!(
            named.insert(file.to_string()),
            "{file} 同时出现在两个清单里"
        );
    }
    for file in &named {
        assert!(
            repo_root().join(file).is_file(),
            "清单里的 {file} 不存在（改名了？）"
        );
    }

    let root = repo_root();
    let prefix = format!("{CRATE_SRC}/");
    let mut found: BTreeSet<String> = BTreeSet::new();
    for path in production_sources(&root)
        .into_iter()
        .filter(|path| relative_name(&root, path).starts_with(&prefix))
    {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = relative_name(&root, &path);
        if !commands_in(&rel, &production_source(&text)).is_empty() {
            found.insert(rel);
        }
    }

    let unclassified: Vec<&String> = found.difference(&named).collect();
    assert!(
        unclassified.is_empty(),
        "这些源文件里有 kubectl 命令行却没有登记归属：{unclassified:?}——\
         要么加进 SCANNED（跑在进化 SA 下），要么加进 OTHER_PRINCIPALS（跑在别的身份下）"
    );
    assert!(!found.is_empty(), "一个带 kubectl 命令的文件都没扫到");
}

/// The Role is bound to the ServiceAccount the Job specs name, and the deployer
/// pod runs under that same one.
#[test]
fn the_job_specs_and_the_role_name_the_same_principal() {
    let deployer = scanned("crates/cog-reflection/src/mainline_deployer.rs");
    // The identity lives in one constant so the preflight's error message and the
    // Job's `serviceAccountName` cannot drift apart. Read that constant out and
    // compare it to the ServiceAccount the Role binds, then check the Job takes
    // its value from the constant rather than spelling the name a second time --
    // two literals would agree today and be free to disagree tomorrow, which is
    // the drift this test exists to forbid.
    let sa = const_str_value(&deployer, "EVOLUTION_SERVICE_ACCOUNT").unwrap_or_else(|| {
        panic!("部署器源码里没有 EVOLUTION_SERVICE_ACCOUNT：滚动 Job 的身份没有单一来源")
    });
    assert_eq!(
        sa, SERVICE_ACCOUNT,
        "滚动 Job 用的 SA 与授权面绑的那个不是同一个身份"
    );
    assert!(
        deployer.contains("\"serviceAccountName\": EVOLUTION_SERVICE_ACCOUNT"),
        "滚动 Job 的 serviceAccountName 不再取自那个常量：授权面绑的就不是它了"
    );
    let change = scanned("crates/cog-reflection/src/change_execution.rs");
    assert!(
        change.contains("serviceAccountName") && change.contains("facts.service_account"),
        "变更 Job 不再从父 Pod 继承 SA：它跑在哪个身份上没人知道"
    );
    // role() 自己就断言了绑定与 roleRef，这里只把两个身份写成一处事实。
    let role = role();
    assert_eq!(role.name, SERVICE_ACCOUNT);
}

/// Keeps the walk honest about what it read, not about where files happen to be.
#[test]
fn the_role_file_is_the_one_the_parity_gate_holds() {
    let guards = read("deploy/scripts/check-deploy-parity.sh");
    assert!(
        guards.contains("RoleBinding") && guards.contains("role_rules"),
        "跨载体 parity 不再比权限面：这条判据读的是 deploy/k3s 这一个载体"
    );
    assert!(
        Path::new(&repo_root().join(RBAC)).is_file(),
        "{RBAC} 不在仓库里"
    );
}

// ---------------------------------------------------------------------------
// The publisher's base read
// ---------------------------------------------------------------------------

/// `cog_core::topology::X` 的取值。
///
/// 名字现在声明在一处、由各 crate 引用，判据要读的仍是那个名字本身，只是顺着
/// 引用去那份声明里取——否则换一次写法，一条「授权面绑的是不是这个身份」的
/// 判据就会因为读不到字面量而静默变成空读数。
fn declared_name(reference: &str) -> Option<String> {
    let name = reference.trim().strip_prefix("cog_core::topology::")?;
    let topology = read("crates/cog-core/src/topology.rs");
    let needle = format!("pub const {name}: &str = \"");
    let start = topology.find(&needle)? + needle.len();
    let end = topology[start..].find('"')? + start;
    Some(topology[start..end].to_string())
}

/// `const NAME: &str = "value";` 里的那个字符串，或它所引用的那份声明里的值。
fn const_str_value(src: &str, name: &str) -> Option<String> {
    let needle = format!("const {name}: &str = \"");
    if let Some(at) = src.find(&needle) {
        let start = at + needle.len();
        let end = src[start..].find('"')? + start;
        return Some(src[start..end].to_string());
    }
    let path_needle = format!("const {name}: &str = ");
    let start = src.find(&path_needle)? + path_needle.len();
    let end = src[start..].find(';')? + start;
    declared_name(&src[start..end])
}

/// `const NAME: [&str; N] = ["a", "b", …];` 里的那些字符串，引用声明的按名字解析。
fn const_string_list(src: &str, name: &str) -> Vec<String> {
    let needle = format!("const {name}: ");
    let Some(start) = src.find(&needle) else {
        return Vec::new();
    };
    let rest = &src[start + needle.len()..];
    let Some(open) = rest.find("= [") else {
        return Vec::new();
    };
    let body = &rest[open + 3..];
    let Some(close) = body.find(']') else {
        return Vec::new();
    };
    body[..close]
        .split(',')
        .filter_map(|element| literal(element).or_else(|| declared_name(element)))
        .collect()
}

/// `- name: KEY` 之后、下一个 env 条目之前那段里的 `value: "…"`。
fn env_value(src: &str, key: &str) -> Option<String> {
    let needle = format!("- name: {key}\n");
    let start = src.find(&needle)? + needle.len();
    let rest = &src[start..];
    let end = rest.find("- name: ").unwrap_or(rest.len());
    quoted(&rest[..end], "value: \"")
}

/// ConfigMap `data:` 里的一行 `KEY: "…"`。
fn configmap_value(src: &str, key: &str) -> Option<String> {
    quoted(src, &format!("  {key}: \""))
}

fn quoted(src: &str, needle: &str) -> Option<String> {
    let start = src.find(needle)? + needle.len();
    let tail = &src[start..];
    let close = tail.find('"')?;
    Some(tail[..close].to_string())
}

/// 推送端要读金丝雀基底那四个工作负载当前在跑的不可变 tag，而这条读数的命令
/// 必须落在它自己身份的授权面内。这份判据把「谁会构造推送端」也跟着清单里的
/// 开关读，不跟着注释走：两个 Pod 的基础配置都打开 GitOps，所以两个身份的授权
/// 都要覆盖那四个名字。漏掉任何一个，读不到只会留一行 warn 后静默退回浮动 tag
/// ——「拿不到授权」与「集群里确实还是浮动 tag」在判决里同形。
#[test]
fn every_identity_that_can_publish_can_read_the_canary_base_names() {
    let revision = read("crates/cog-reflection/src/live_revision.rs");
    let names = const_string_list(&revision, "BASE_DEPLOYMENTS");
    assert_eq!(
        names.len(),
        4,
        "现役 rev 的四个主负载名单读出来是 {names:?}：这条判据靠它才算有内容"
    );
    assert!(
        revision.contains("\"deployment/{name}\""),
        "这份读法不再按名读：resourceNames 授权下集合级 list 必被拒，而读不到只静默退回浮动 tag"
    );

    // 名单只此一份：推送端引用它，自己不再留一份。两份会各自漂，而漂掉的那份不报错，
    // 它只会让某个读者少认一个名字——那时这条判据读的还是那份没人用的。
    let publisher = read("crates/cog-reflection/src/gitops_publisher.rs");
    assert!(
        publisher.contains("live_revision::read_base_deployments"),
        "推送端不再引用现役 rev 那一份读法：两份名单意味着有一个读者会少认一个名字"
    );
    assert!(
        !publisher.contains("const CANARY_BASE_DEPLOYMENTS"),
        "推送端又长出了自己那份名单"
    );

    let app = env_value(
        &read("deploy/k3s/deployment.yaml"),
        "COGNEVA_GITOPS_ENABLED",
    );
    assert_eq!(
        app.as_deref(),
        Some("true"),
        "app Pod 的 COGNEVA_GITOPS_ENABLED 读不出来：这条判据的前提就不成立了。\
         若它是有意关掉的，把这个身份从这里去掉，别让它继续假装被覆盖"
    );
    let evolution = configmap_value(
        &read("deploy/k3s/evolution-configmap.yaml"),
        "COGNEVA_GITOPS_ENABLED",
    );
    assert_eq!(
        evolution.as_deref(),
        Some("true"),
        "进化 Pod 的 COGNEVA_GITOPS_ENABLED 读不出来：同上"
    );

    for (who, files) in [
        (
            "app Pod（SA cogneva）",
            &[
                "deploy/k3s/gitops-puller-rbac.yaml",
                "deploy/k3s/llm-admin-rbac.yaml",
            ][..],
        ),
        (
            "进化 Pod（SA cogneva-evolution）",
            &["deploy/k3s/evolution-rbac.yaml"][..],
        ),
    ] {
        // 一个 SA 上可以挂多条角色，API server 按并集判权。并集里同时有
        // 命名空间内的 Role 与集群作用域的 ClusterRole：这两类在授权上等价，
        // 只是作用域不同，所以这里问的是「这个身份能不能读 deployment/<名>」，
        // 不是「哪一条规则给的权利」。
        let rules: Vec<Rule> = files
            .iter()
            .flat_map(|f| roles_in(f).roles)
            .flat_map(|r| r.rules)
            .collect();
        assert!(!rules.is_empty(), "{who} 一条规则都没读到");
        for name in &names {
            assert!(
                rules
                    .iter()
                    .any(|r| r.grants_named("apps", "deployments", "get", name)),
                "{who} 读不到 deployment/{name}：推送端会把它当成浮动 tag 的基底"
            );
        }
    }
}

/// 判据读不懂的形态要**响**：一篇解析不出来的文档被丢掉，它带的规则就一起消失，
/// 于是「读不到授权」被读成「没有这条规则」。走这个文件自己的 `yaml_docs`，而不是
/// 只断言那串文本是坏的。
#[test]
#[should_panic(expected = "解析不出来")]
fn a_document_that_does_not_parse_is_a_failure_not_a_skip() {
    // 未闭合的 flow sequence：一处真实的畸形形态（比如没渲染完的模板落到这里）。
    let _ = yaml_docs(
        "apiVersion: rbac.authorization.k8s.io/v1\nkind: Role\nrules: [\n",
        "torn.yaml",
    );
}
