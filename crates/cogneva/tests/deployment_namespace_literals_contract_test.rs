//! principle: feedback_no_hardcoding_config_surface
//! The namespace is the deployment's to declare: `deploy/helm/cogneva/values.yaml`
//! carries `global.namespace`, the chart renders every object into it, and
//! bootstrap reads that name once (`declared_namespace`) and hands it to every
//! `kubectl`/`helm` call it makes.
//!
//! A second copy in Rust is a copy no `--set` can move. It is also one that never
//! reports being wrong: point the deployment at another namespace and every
//! `kubectl -n <old name>` still exits 0 — `get` reads an empty list, `delete`
//! says NotFound after `--ignore-not-found`, `create namespace` quietly rebuilds
//! the old one. The install then reports success while the workloads it waits on
//! were never in the namespace it looked at.
//!
//! The predicate is the *shape* of a namespace argument, not the bare name:
//! `cogneva` is also the release name, the workload name and half the image
//! references, and a gate that reddened on those would be turned off before it
//! caught anything. The name itself is read from the chart, so moving
//! `global.namespace` moves this gate with it instead of leaving it scanning for
//! a name nobody uses.
//!
//! Two copies are legitimate and both are held to the chart: bootstrap reads
//! `global.namespace` at install time, and in-pod code carries one compiled
//! default in `cog_core::topology` because it cannot read the chart. Everything
//! else reads one of those two.
//!
//! Test modules are not scanned: a fixture may name whatever it likes.

mod common;

use common::routes::repo_root;

use common::rust_production::{production_sources, production_text};

/// The namespace the chart declares. Read, not written down here — that is the
/// whole point of the gate.
fn declared_namespace() -> String {
    let path = repo_root().join("deploy/helm/cogneva/values.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let parsed: serde_json::Value =
        serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    parsed
        .pointer("/global/namespace")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "{} 没有声明 global.namespace：这条判据无从判起，也不该猜一个名字",
                path.display()
            )
        })
        .to_string()
}

/// Every `*.rs` file under `crates/*/src`, relative to the repo root.
/// The shapes a namespace literal takes. `-n <name>` inside a shell string and
/// `"-n", "<name>"` in an argument list are the same fact written two ways, so
/// both are asked for; whitespace is stripped before the search, because the
/// argument pair is usually split across lines. The last two are the config
/// defaults (`namespace: "<name>".into()`), which are a second declaration in
/// exactly the same sense: a default the chart cannot move, and one that is
/// wrong the moment `global.namespace` is set to something else.
fn shapes(namespace: &str) -> Vec<String> {
    vec![
        format!("\"-n\",\"{namespace}\""),
        format!("\"--namespace\",\"{namespace}\""),
        format!("-n{namespace}"),
        format!("--namespace={namespace}"),
        format!("namespace={namespace}"),
        format!("\"namespace/{namespace}\""),
        format!("namespace:{namespace}"),
        format!("namespace{namespace}"),
        format!("namespace\":\"{namespace}\""),
        format!("namespace:\"{namespace}\""),
    ]
}

/// The one file allowed to carry the name: the in-cluster declaration. In-pod
/// code cannot read the chart, so it needs a compiled copy; the test below
/// holds that copy and `global.namespace` to the same value.
const DECLARATION: &str = "crates/cog-core/src/topology.rs";

/// One hit: where, and the shape that matched. Read only through `Debug` when
/// the assertion prints them, which is enough for a test-only type.
#[derive(Debug)]
#[allow(dead_code)]
struct Hit {
    file: String,
    shape: String,
    around: String,
}

fn census() -> Vec<Hit> {
    let namespace = declared_namespace();
    let wanted = shapes(&namespace);
    let mut hits = Vec::new();
    for path in production_sources(&repo_root()) {
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
        // Whitespace out: `"-n",` and the name it belongs to are usually on
        // different lines, and a line-at-a-time search would miss exactly the
        // call form `cargo fmt` produces.
        let flat: String = production_text(&text).split_whitespace().collect();
        for shape in &wanted {
            let Some(at) = flat.find(shape.as_str()) else {
                continue;
            };
            let start = at.saturating_sub(60);
            let end = (at + shape.len() + 60).min(flat.len());
            hits.push(Hit {
                file: rel.clone(),
                shape: shape.clone(),
                around: flat[start..end].to_string(),
            });
        }
    }
    hits
}

#[test]
fn no_source_writes_the_declared_namespace_into_a_command() {
    let hits = census();
    assert!(
        hits.is_empty(),
        "代码里把命名空间写进了命令。这个名字由 chart 声明、由 bootstrap 读一次\
         （crates/bootstrap 里的 declared_namespace），其他地方再抄一份就是第二份声明：\n{:#?}",
        hits
    );
}

/// A reading is only a reading if the scanner can see the shape it is looking
/// for. These are the three forms that were actually in the tree — an argument
/// pair split across lines, a shell pipeline inside a `format!`, and a resource
/// id — plus the two that are not a namespace argument at all.
#[test]
fn the_scanner_recognises_the_shapes_and_only_those() {
    let namespace = declared_namespace();
    let wanted = shapes(&namespace);
    let flat = |s: &str| -> String { s.split_whitespace().collect() };
    // An argument pair split across lines.
    let pair = flat(&format!("\"-n\",\n                \"{namespace}\","));
    assert!(wanted.iter().any(|s| pair.contains(s.as_str())), "{pair}");
    // A shell pipeline inside a string that is formatted at run time.
    let shell = flat(&format!(
        "format!(\"kubectl create namespace {namespace} --dry-run=client -o yaml | kubectl apply -f -\")"
    ));
    assert!(wanted.iter().any(|s| shell.contains(s.as_str())), "{shell}");
    // A resource id handed to `kubectl patch`.
    let id = flat(&format!("vec![\"namespace/{namespace}\".to_string()]"));
    assert!(wanted.iter().any(|s| id.contains(s.as_str())), "{id}");
    // Not shapes: the release name, the workload name, an image reference, and
    // the same name carried in a variable.
    for innocent in [
        format!("\"{namespace}\".into()"),
        format!("deployments/{namespace}"),
        format!("localhost:30500/{namespace}:local"),
        "-n\",\n            namespace,".to_string(),
    ] {
        let flat = flat(&innocent);
        assert!(
            !wanted.iter().any(|s| flat.contains(s.as_str())),
            "{innocent} 不是命名空间参数，判据不该在它上面变红"
        );
    }
}

/// The one manifest bootstrap renders at run time must leave the name to the
/// installer. A literal there would send the distributor into a namespace the
/// `-n <declared>` calls around it never look at.
#[test]
fn the_manifest_bootstrap_renders_leaves_the_namespace_to_the_installer() {
    let namespace = declared_namespace();
    let path = repo_root().join("deploy/k8s/image-distributor.yaml");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let literal = format!("namespace: {namespace}");
    let offenders: Vec<&str> = text
        .lines()
        .filter(|line| line.trim().starts_with(&literal))
        .collect();
    assert!(
        offenders.is_empty(),
        "{} 里写着 `{literal}`：这份模板由引导器渲染，命名空间要留占位符；实得 {offenders:?}",
        path.display()
    );
    assert!(
        text.contains("namespace: __NAMESPACE__"),
        "{} 没有 __NAMESPACE__ 占位符，引导器渲染时没有地方写它读到的名字",
        path.display()
    );
}

/// In-pod code cannot read the chart, so it carries one compiled copy of the
/// name. That is a second declaration of the same fact -- the chart's
/// `global.namespace` and `cog_core::topology::DEFAULT_NAMESPACE` -- and
/// nothing at build time compares them, so they are compared here.
#[test]
fn the_compiled_default_is_the_namespace_the_chart_declares() {
    assert_eq!(
        cog_core::topology::DEFAULT_NAMESPACE,
        declared_namespace(),
        "chart 声明的 global.namespace 与 cog_core::topology::DEFAULT_NAMESPACE 不一致：\
         装机时按前者把对象建在一个命名空间里，进程自己按后者去另一个命名空间找"
    );
}

/// The config default is the shape the command shapes would have missed: it is
/// not a `-n` argument, it is a field the deployers take when nothing overrides
/// it. It has to be visible to the scanner, or the defaults drift back.
#[test]
fn the_scanner_sees_the_config_default_shape() {
    let namespace = declared_namespace();
    let wanted = shapes(&namespace);
    let flat = |s: &str| -> String { s.split_whitespace().collect() };
    for form in [
        format!("namespace: \"{namespace}\".into(),"),
        format!("pub namespace: \"{namespace}\".to_string(),"),
    ] {
        let flat = flat(&form);
        assert!(
            wanted.iter().any(|s| flat.contains(s.as_str())),
            "判据没认出配置默认值这个形状：{form}"
        );
    }
}

// ---- 第三份声明：脚本里的命令行默认值 ----

/// 脚本里同一个名字的第三份声明。前两份（chart 的 `global.namespace`、进程内那一个
/// 编译默认值）由上面几条判据钉住；这一份是 `NS="${COGNEVA_NS:-cogneva}"` 这类默认值，
/// `--set global.namespace=...` 搬不动它。搬不动的后果不是报错，是**读空**：装到另一个
/// 命名空间之后，`kubectl -n <旧名字> get` 照样退 0 并读回空表，脚本接着按「什么都没有」
/// 往下走，装完报成功。
///
/// 人口是三个目录下的一层 `.sh`（`deploy/scripts`、`deploy/k3s`、`.github/scripts`）。
/// `deploy/k3s/observability/` 不在人口里：那套观测栈声明的是自己的命名空间
/// （chart values 里另有一行），拿平台的名字去判它，红的是它的正当声明。
const SCRIPT_DIRS: &[&str] = &["deploy/scripts", "deploy/k3s", ".github/scripts"];

/// 人口下限。少到这个数以下说明扫描本身指错了地方——那与「全都一致」在输出上分不开。
const SCRIPT_FLOOR: usize = 8;

/// 变量名里哪几种算「这个脚本的命名空间」：`NS`、`NAMESPACE`、以及 `*_NS`。
/// `VENDOR_DOMAINS`、`QUOTA_NAME` 这些也是大写、也以 NS/NAME 收尾，但它们不是命名空间，
/// 按名字放进来只会把门变成一处误报。
fn is_namespace_var(upper: &str) -> bool {
    upper == "NS" || upper == "NAMESPACE" || upper.ends_with("_NS")
}

/// 一个命名空间名的样子：DNS 标签，小写字母、数字、`-` 与 `.`。
///
/// 这一条挡的是另一类误报：脚本里嵌着的 Python 片段（`ns = doc["metadata"].get("namespace")`、
/// `os.environ.get('X') or 'cogneva'`）也以 `ns=`/`NAMESPACE=` 开头，但它们的右边不是一个
/// 名字，而是一段代码。名字写不成那样。
fn looks_like_a_namespace(candidate: &str) -> bool {
    !candidate.is_empty()
        && !candidate.starts_with(['$', '<', '{'])
        && candidate
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
}

/// `at` 之后的第一个词：引号里的取引号内，否则取到空白或分隔符为止。
fn literal_after(line: &str, at: usize) -> Option<String> {
    let rest = line[at..].trim_start_matches(|c: char| c == '=' || c.is_whitespace());
    let token: String = match rest.chars().next() {
        Some(quote @ ('"' | '\'')) => rest[1..].chars().take_while(|c| *c != quote).collect(),
        _ => rest
            .chars()
            .take_while(|c| !c.is_whitespace() && !"\"'|;&)".contains(*c))
            .collect(),
    };
    if token.chars().all(|c| c.is_ascii_digit()) || !looks_like_a_namespace(&token) {
        return None;
    }
    Some(token)
}

/// `${X:-cogneva}` / `${2-cogneva}` / `"cogneva"` 里的那个默认值。
fn default_of(value: &str) -> Option<String> {
    let unquoted = value.trim().trim_matches(['"', '\'']);
    let inner = if unquoted.starts_with("${") && unquoted.ends_with('}') {
        let body = &unquoted[2..unquoted.len() - 1];
        match body.split_once(":-").or_else(|| body.split_once('-')) {
            Some((_, fallback)) => fallback,
            None => return None,
        }
    } else {
        unquoted
    };
    let literal = inner.trim().trim_matches(['"', '\'']);
    if literal.chars().all(|c| c.is_ascii_digit()) || !looks_like_a_namespace(literal) {
        return None;
    }
    Some(literal.to_string())
}

/// 一行脚本里的命名空间字面量：`(写法, 字面量)`。
///
/// 三种写法都读：`--namespace X`（含 `=`）、`-n X`、以及 `NS=` 这类赋值默认值。
/// 刻意不收的：注释行（用法示例里常写着同样的命令，它不是在执行）；`-n` 出现在
/// `sudo -n`、`head -n`、`grep -n` 里（那是那个命令自己的选项）；值写成变量或占位符
/// （`$NS`、`<ns>`）——那不是又抄了一份，是读了那一份。
fn script_literals_in(line: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if line.trim_start().starts_with('#') {
        return out;
    }

    let mut from = 0;
    while let Some(found) = line[from..].find("--namespace") {
        let at = from + found + "--namespace".len();
        from = at;
        if let Some(literal) = literal_after(line, at) {
            out.push(("--namespace".to_string(), literal));
        }
    }

    if line.contains("kubectl") || line.contains("helm") {
        let bytes = line.as_bytes();
        let mut from = 0;
        while let Some(found) = line[from..].find("-n") {
            let at = from + found;
            from = at + 2;
            let before_ok = at > 0
                && !matches!(bytes[at - 1], b'-' | b'.' | b'_')
                && !bytes[at - 1].is_ascii_alphanumeric();
            if !before_ok {
                continue;
            }
            // 这个 `-n` 归谁：往上取紧挨着的那一串字母。用 `split_whitespace` 取不出来——
            // `${COGNEVA_KUBECTL:-sudo -n k3s kubectl}` 里紧挨着的是 `:-sudo`，
            // 「这是 sudo 自己的短选项」要按字母读，不按空格读。
            let owner: String = line[..at]
                .trim_end()
                .chars()
                .rev()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect::<Vec<char>>()
                .into_iter()
                .rev()
                .collect();
            if matches!(
                owner.as_str(),
                "sudo"
                    | "grep"
                    | "head"
                    | "tail"
                    | "sed"
                    | "awk"
                    | "curl"
                    | "ssh"
                    | "docker"
                    | "env"
            ) {
                continue;
            }
            if let Some(literal) = literal_after(line, at + 2) {
                out.push(("-n".to_string(), literal));
            }
        }
    }

    let trimmed = line.trim_start();
    if let Some((name, value)) = trimmed.split_once('=') {
        let name = name.trim();
        let looks_like_identifier =
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if looks_like_identifier && is_namespace_var(&name.to_ascii_uppercase()) {
            if let Some(literal) = default_of(value) {
                out.push((name.to_string(), literal));
            }
        }
    }

    out
}

/// 脚本里的每一个默认值都必须是 chart 声明的那一个名字。
#[test]
fn every_script_default_is_the_namespace_the_chart_declares() {
    let root = repo_root();
    let namespace = declared_namespace();
    let mut found = 0usize;
    let mut off = Vec::new();
    for dir in SCRIPT_DIRS {
        let path = root.join(dir);
        let entries = std::fs::read_dir(&path)
            .unwrap_or_else(|e| panic!("{} 读不出来：脚本人口无从数起（{e}）", path.display()));
        for entry in entries.flatten() {
            let file = entry.path();
            if file.extension().and_then(|e| e.to_str()) != Some("sh") {
                continue;
            }
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            let text = std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("{} 读不出来：{e}", file.display()));
            for (index, line) in text.lines().enumerate() {
                for (form, literal) in script_literals_in(line) {
                    found += 1;
                    if literal != namespace {
                        off.push(format!(
                            "{rel}:{}: {form} 默认成 {literal}，chart 声明的是 {namespace}",
                            index + 1
                        ));
                    }
                }
            }
        }
    }
    assert!(
        found >= SCRIPT_FLOOR,
        "只数出 {found} 处脚本里的命名空间默认值（下限 {SCRIPT_FLOOR}）：扫描塌了，\
         这条判据此刻什么都没证明"
    );
    assert!(
        off.is_empty(),
        "这些脚本的命名空间与 chart 的 global.namespace 不是同一个名字——\
         `--set global.namespace` 搬不动它们，读的时候退 0 而表是空的：\n{}",
        off.join("\n")
    );
}

/// 判据按形状读：三种写法都认，`sudo -n`、`head -n`、注释、变量值都不认。
#[test]
fn the_script_scanner_reads_the_defaults_and_only_those() {
    let shapes = |line: &str| -> Vec<String> {
        script_literals_in(line)
            .into_iter()
            .map(|(_, literal)| literal)
            .collect()
    };
    assert_eq!(shapes(r#"NS="${COGNEVA_NS:-cogneva}""#), vec!["cogneva"]);
    assert_eq!(shapes(r#"NS="${2:-cogneva}""#), vec!["cogneva"]);
    assert_eq!(shapes("NS=cogneva"), vec!["cogneva"]);
    assert_eq!(shapes(r#"namespace="cogneva""#), vec!["cogneva"]);
    assert_eq!(
        shapes("helm template cogneva deploy/helm/cogneva --namespace cogneva"),
        vec!["cogneva"]
    );
    assert_eq!(shapes("kubectl -n cogneva get pods"), vec!["cogneva"]);

    // 不是这一族的：别人的短选项、注释里的示例命令、变量、占位符。
    assert_eq!(
        shapes(r#"KUBECTL="${COGNEVA_KUBECTL:-sudo -n k3s kubectl}""#),
        Vec::<String>::new()
    );
    assert_eq!(
        shapes("kubectl get pods | head -n 5"),
        Vec::<String>::new(),
        "`head -n` 不是命名空间参数"
    );
    assert_eq!(
        shapes("# kubectl -n cogneva edit secret 后滚动"),
        Vec::<String>::new(),
        "注释里的示例命令不是在执行"
    );
    assert_eq!(shapes(r#"kubectl -n "$NS" get pods"#), Vec::<String>::new());
    assert_eq!(
        shapes(r#"kubectl -n <ns> get pods"#),
        Vec::<String>::new(),
        "占位符不是又抄了一份名字"
    );
    assert_eq!(
        shapes(r#"QUOTA_NAME="${COGNEVA_QUOTA:-cogneva-quota}""#),
        Vec::<String>::new(),
        "配额名不是命名空间"
    );
}
