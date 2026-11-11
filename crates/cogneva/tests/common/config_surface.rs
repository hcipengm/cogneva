//! 配置面的唯一读法：进程把哪张表读成哪些节、声明文档在哪里、每个节被类型读回后
//! 还产生哪些键。
//!
//! 两份判据共用这一份实现：
//!
//! * `config_section_reader_contract_test` 判「文档写的键有字段接着」；
//! * `every_config_knob_is_named_by_a_delivered_document_test` 判「类型产生的键
//!   至少在某一份交付文档里被写出来」——一个只活在代码默认值里的旋钮，运维在交付
//!   文档里看不见它，也就没有发现它的路径。
//!
//! 节的读回类型、声明文档的位置、以及「这个键背后有没有字段」的探针都只写在这里。
//! 分开抄两份时，两份对同一份文档会给出不同答案，而两份都能各自绿。
//!
//! 判据要走到嵌套节里去。`observability.loki.*`、`infra_watch.rules[]`、`providers.*`
//! 这些键在文档里是一层对象，在类型里是一层字段；只看顶层时，内层两边都是盲的。
//! 所以这里从读取器的类型名出发，读一遍该类型的字段声明，跟着字段类型往下走：
//! `Option` / `Vec` / `Box` / `Arc` 剥壳，本地结构体递归，`HashMap` / `Value` 是
//! 动态键（任何键都合法），枚举与外部叶子停。字段声明也给出 `skip` /
//! `skip_serializing_if` 的答案，于是「默认值被序列化省掉」不再让一个真实旋钮消失。
//!
//! 有一条不能从父类型推出来：`self_evolution.artifact_evolution` 在
//! `SelfEvolutionConfig` 里没有字段——那是 cog-reflection 按 JSON 指针自己读的节。
//! 它作为 [`Reader::with_nested`] 的一条例外登记，登记与指针的拼写由上面那道门读回
//! 源码来判。

// 每个集成测试二进制都编译这一整个 `common`，但只有用到配置面的那两个读它：其余
// 二进制里这些格子没人构造，而它们不是死代码，是同一份实现被别人编进来。
#![allow(dead_code)]

use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::lex::Dialect;
use super::routes::{matching_brace, read, walk};

/// One section's reader: the type that decides what the section may contain,
/// and where the code spells the section out.
pub struct Reader {
    pub section: &'static str,
    /// The Rust type that reads this section. The type graph starts here.
    pub type_name: &'static str,
    /// `(file, text)`: read back from disk, the file must contain the text.
    /// `None` for a section the process's own config struct reads — there the
    /// type is the join, and the name is checked against that struct's keys.
    pub join: Option<(&'static str, &'static str)>,
    /// Keys inside this section that a *different* reader takes out of the
    /// document by pointer. The section's own type has no field for them, so
    /// without writing them down here the gate would report a knob that is read
    /// as one that is not — and the declaration is joined to the source the
    /// same way a section's is.
    pub nested: Vec<Reader>,
    /// Read a section back through the type that reads it: `Ok` with what the
    /// type keeps, `Err` with why a value does not fit it at all. A value that
    /// does not fit is a defect of its own — the reader either fails or falls
    /// back to its defaults, and either way the document reads as applied.
    read_back: fn(&Value) -> Result<Value, String>,
}

impl Reader {
    pub fn of<T>(section: &'static str, join: Option<(&'static str, &'static str)>) -> Self
    where
        T: serde::de::DeserializeOwned + serde::Serialize,
    {
        Self {
            section,
            type_name: std::any::type_name::<T>(),
            join,
            nested: Vec::new(),
            read_back: |value| {
                let typed: T = serde_json::from_value(value.clone()).map_err(|e| {
                    format!(
                        "the value does not fit {}, the type that reads it: {e}",
                        std::any::type_name::<T>()
                    )
                })?;
                Ok(serde_json::to_value(typed).expect("a section read back serializes"))
            },
        }
    }

    /// Declare a key inside this section that another reader takes by pointer.
    pub fn with_nested(mut self, reader: Reader) -> Self {
        self.nested.push(reader);
        self
    }

    /// Read a section back through the type that reads it.
    pub fn read_back(&self, value: &Value) -> Result<Value, String> {
        (self.read_back)(value)
    }

    /// Whether the type demonstrably has a field at `key`, proved by handing it
    /// a value and reading it back.
    ///
    /// A section may write an optional key as `null`, and a null is dropped on
    /// the way back out exactly like a key no field stands behind — so the
    /// round trip alone cannot tell an unset knob from a typo'd one. A probe
    /// value can: a real field keeps it, an unknown key drops every one. A
    /// probe whose shape the type does not take is not evidence either way, so
    /// it is skipped rather than read as absence.
    ///
    /// The list carries non-empty containers as well as empty ones: a field
    /// skipped when it is empty (`skip_serializing_if = "Vec::is_empty"`) keeps
    /// the key for `["probe"]` and drops it for `[]`, and the empty probe alone
    /// would report a field that is read as one that does not exist.
    ///
    /// This stays for the readers the type graph cannot parse (a section that
    /// is itself a `HashMap` or `Value`); a named struct is judged from its
    /// declaration, which also sees the `skip_serializing_if` fields a round
    /// trip can never show.
    pub fn has_field(&self, section: &Map<String, Value>, key: &str) -> bool {
        let probes = [
            serde_json::json!("probe"),
            serde_json::json!(1),
            serde_json::json!(true),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!(["probe"]),
            serde_json::json!({"probe": 1}),
        ];
        probes.iter().any(|probe| {
            let mut probed = section.clone();
            probed.insert(key.to_string(), probe.clone());
            self.read_back(&Value::Object(probed))
                .map(|read_back| read_back.get(key).is_some())
                .unwrap_or(false)
        })
    }
}

/// Every classified section and the type that reads it.
///
/// The list is hand-written because no type can be named generically, but it is
/// not free to fall behind: the gate compares it against the section table in
/// both directions, so a section with no entry fails there, and the entry's join
/// is read back out of the source it names.
pub fn readers() -> Vec<Reader> {
    vec![
        // Carried by `cog_core::Config`, the struct the process loads the
        // document into and the plugins read (`AppConfig` flattens it), so the
        // field itself is the join and there is no second spelling to check.
        Reader::of::<cog_core::config::AppInfo>("app", None),
        Reader::of::<cog_core::config::AgentConfig>("agent", None),
        Reader::of::<cog_core::config::DagExecutorConfig>("dag_executor", None),
        Reader::of::<cog_core::config::GatewayConfig>("gateway", None),
        Reader::of::<cog_core::config::HookEngineConfig>("hook_engine", None),
        Reader::of::<cog_core::config::MetricsConfig>("metrics", None),
        Reader::of::<cog_core::config::MultiBackendConsumerConfig>("multi_backend_consumer", None),
        Reader::of::<cog_core::config::ProviderConfigs>("providers", None),
        Reader::of::<cog_core::contract::storage::RawLoggerConfig>("raw_logger", None),
        Reader::of::<cog_core::config::SelfEvolutionConfig>("self_evolution", None).with_nested(
            Reader::of::<cog_reflection::PolicyEvolutionConfig>(
                "artifact_evolution",
                Some((
                    "crates/cog-reflection/src/policy_evolution.rs",
                    "pointer(\"/self_evolution/artifact_evolution\")",
                )),
            ),
        ),
        Reader::of::<cog_core::config::SupervisorConfig>("supervisor", None),
        Reader::of::<cog_core::config::SystemConfig>("system", None),
        Reader::of::<cog_core::config::TierMigratorConfig>("tier_migrator", None),
        // The env supersede map: the loader merges each `PATH=dot.path` pair
        // over the document, so every value in it is moved as a string.
        Reader::of::<std::collections::HashMap<String, String>>("env", None),
        // Read by a crate's own loader, which spells the pointer as a literal.
        Reader::of::<cog_llm::config::LLMRoutingConfig>(
            "llm_routing",
            Some((
                "crates/cog-llm/src/config.rs",
                "load_section(\"/llm_routing\"",
            )),
        ),
        Reader::of::<cog_llm::config::TuningConfig>(
            "tuning",
            Some(("crates/cog-llm/src/config.rs", "load_section(\"/tuning\"")),
        ),
        Reader::of::<cog_agent::config::AgentLoopConfig>(
            "agent_loop",
            Some((
                "crates/cog-agent/src/config.rs",
                "load_section(\"/agent_loop\"",
            )),
        ),
        Reader::of::<cog_agent::config::AgentManagerConfig>(
            "agent_pool",
            Some((
                "crates/cog-agent/src/config.rs",
                "load_section(\"/agent_pool\"",
            )),
        ),
        Reader::of::<cog_collaboration::config::BoundaryConfig>(
            "boundary",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/boundary\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::PgeSettings>(
            "pge",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/pge\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::RalphSettings>(
            "ralph",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/ralph\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::SelfReviewSettings>(
            "self_review",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/self_review\")",
            )),
        ),
        // Read by seeking the value out of the document by JSON pointer.
        Reader::of::<cog_github::config::GitHubIntegrationConfig>(
            "github_integration",
            Some((
                "crates/cog-github/src/config.rs",
                "pointer(\"/github_integration\")",
            )),
        ),
        Reader::of::<cog_github::config::GiteeIntegrationConfig>(
            "gitee_integration",
            Some((
                "crates/cog-github/src/config.rs",
                "pointer(\"/gitee_integration\")",
            )),
        ),
        Reader::of::<cog_memory::config::MemoryConfig>(
            "memory",
            Some(("crates/cog-memory/src/config.rs", "pointer(\"/memory\")")),
        ),
        Reader::of::<cog_prompt::config::PromptConfig>(
            "prompts",
            Some(("crates/cog-prompt/src/config.rs", "pointer(\"/prompts\")")),
        ),
        Reader::of::<cog_observability::config::ObservabilityExportersConfig>(
            "observability",
            Some((
                "crates/cog-observability/src/config.rs",
                "pointer(\"/observability\")",
            )),
        ),
        // Not read by anything, by design: the delivery comparator drops every
        // key starting with `_comment` at any depth, which is what makes a
        // template self-describing without changing what a process reads.
        Reader::of::<Map<String, Value>>(
            "_comment",
            Some((
                "crates/cog-core/src/config_sections.rs",
                "!key.starts_with(\"_comment\")",
            )),
        ),
    ]
}

/// The sections `cog_core::Config` carries, read out of the type rather than
/// listed here a second time.
pub fn sections_the_process_config_carries() -> Vec<String> {
    let value =
        serde_json::to_value(cog_core::Config::default()).expect("the process config serializes");
    let mut names: Vec<String> = value
        .as_object()
        .expect("the process config is an object")
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

/// Every section the section table classifies.
pub fn classified_sections() -> Vec<String> {
    let mut names: Vec<String> = cog_core::config_sections::CONFIG_SECTIONS
        .iter()
        .map(|s| s.name.to_string())
        .collect();
    names.sort();
    names
}

/// Counts for one document: sections read, and keys the type produces but the
/// document does not write.
#[derive(Default)]
pub struct Readings {
    pub sections: usize,
    pub undocumented: usize,
}

/// What the document writes in one section, and what the type produces there:
/// the two sets a knob-level judgement is made from.
pub struct SectionShape {
    /// `system`, `observability.loki`, or `infra_watch.rules[]` for an array
    /// element.
    pub path: String,
    /// Keys the document writes (including a nested section's key).
    pub written: BTreeSet<String>,
    /// Keys the type produces when it reads this document's section.
    pub produced: BTreeSet<String>,
}

/// One field of a config type, with the document path it is written at.
///
/// This is the population a consumption census walks: what the type declares,
/// not what the document happens to write. A field only reachable through a
/// `#[serde(flatten)]` field keeps its parent's path, because that is the level
/// the document writes it at; a flatten field's own name is not a key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConfigField {
    /// The dotted path the document writes, `[]` marking a list element.
    pub section_path: String,
    /// The Rust identifier production code reads the field by.
    pub ident: String,
    /// The type that declares the field.
    pub owner_type: String,
    /// The crate the declaring type lives in.
    pub owner_crate: String,
}

/// A struct's field as written in its declaration: identifier and type text.
#[derive(Clone, PartialEq, Eq)]
pub struct TypeField {
    pub ident: String,
    pub ty: String,
}

/// A struct's declaration, reduced to what a reader of access chains needs.
#[derive(Clone)]
pub struct TypeShape {
    pub crate_name: String,
    pub name: String,
    pub fields: Vec<TypeField>,
}

impl TypeShape {
    pub fn field(&self, ident: &str) -> Option<&TypeField> {
        self.fields.iter().find(|field| field.ident == ident)
    }
}

/// The inner type of a wrapper the code reads through by field access.
///
/// `state.config.read().unwrap().notification_limit` walks through `Arc`,
/// `RwLock` and two calls; the type that owns `notification_limit` is the
/// innermost argument. References are stripped before matching, so
/// `&'a Arc<T>` unwraps to `T` here as well.
pub fn container_inner(ty: &str) -> Option<String> {
    let bare = strip_reference(ty);
    if let Some((name, inner)) = generic_first_arg(bare) {
        if matches!(
            name.as_str(),
            "Option"
                | "Arc"
                | "Rc"
                | "Box"
                | "RwLock"
                | "Mutex"
                | "RefCell"
                | "Cell"
                | "Cow"
                | "Pin"
                | "Vec"
                | "BTreeSet"
                | "HashSet"
                | "VecDeque"
        ) {
            return Some(inner);
        }
    }
    array_element(bare)
}

/// `path::Wrapper<first, rest>` → wrapper's last segment and first argument.
///
/// Unlike [`outer_arg`], the wrapper may be spelled with a full path
/// (`std::sync::Arc<T>`); the text before `<` is read as a path and only its
/// last segment decides whether the shape is a wrapper.
fn generic_first_arg(ty: &str) -> Option<(String, String)> {
    let bare = strip_reference(ty).trim();
    let open = bare.find('<')?;
    let head = bare[..open].trim();
    if head.is_empty() || !head.contains("::") && !head.chars().next()?.is_alphanumeric() {
        return None;
    }
    let rest = &bare[open + 1..];
    let mut depth = 1i64;
    let mut end = None;
    for (index, c) in rest.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    if !rest[end + 1..].trim().is_empty() {
        return None;
    }
    let first = split_top(&rest[..end], ',')
        .into_iter()
        .next()?
        .trim()
        .to_string();
    if first.is_empty() {
        return None;
    }
    Some((last_segment(head), first))
}

/// Check one section-shaped value against the reader that takes it, and every
/// key inside it — including a key a nested reader takes by pointer.
pub fn check_section(
    graph: &TypeGraph,
    file: &str,
    reader: &Reader,
    value: &Value,
    readings: &mut Readings,
) {
    let mut shapes = Vec::new();
    section_shapes(graph, file, reader, value, reader.section, &mut shapes);
    readings.sections += shapes.len();
    readings.undocumented += shapes
        .iter()
        .map(|shape| shape.produced.difference(&shape.written).count())
        .sum::<usize>();
}

/// The written and produced key sets of `value` and every nested section under
/// it, with `prefix` as the accumulated path.
///
/// A key the document writes must have a field behind it: serde passes an
/// unknown key over in silence, so it reads as applied while nothing uses it.
/// That assertion lives here, in the one walk both gates share. The walk
/// follows the declared field types, so the assertion reaches a key inside a
/// nested object or an array element as well.
pub fn section_shapes(
    graph: &TypeGraph,
    file: &str,
    reader: &Reader,
    value: &Value,
    prefix: &str,
    out: &mut Vec<SectionShape>,
) {
    if !value.is_object() {
        // Only the top-level `_comment` is not an object: it is one line of
        // prose the comparator drops, so its shape is its own business.
        assert_eq!(
            reader.section, "_comment",
            "{file} `{}` is not an object, and no reader can read it",
            reader.section
        );
        return;
    }
    let read_back = reader
        .read_back(value)
        .unwrap_or_else(|e| panic!("{file} `{}`: {e}", reader.section));
    let read_back = read_back
        .as_object()
        .expect("a section read back is an object");
    let info = graph.reader_type(reader.type_name);
    graph.walk(
        file,
        Some(reader),
        info.as_deref(),
        value,
        prefix,
        out,
        &mut Vec::new(),
        Some(read_back),
    );
}

/// A parsed field of a config type.
#[derive(Clone)]
struct FieldInfo {
    /// The Rust identifier the production code reads the field by. The
    /// canonical name is what the document writes; they differ wherever serde
    /// renames, and a census of readers has to look for the identifier.
    ident: String,
    /// The name the type writes back out.
    canonical: String,
    /// Every name the type accepts on the way in: the canonical one plus the
    /// `alias`es.
    accepted: Vec<String>,
    /// The field's type as written; resolved by [`TypeGraph::classify`].
    ty: String,
    /// `#[serde(skip)]`: neither read nor written. A document key for it is a
    /// key nothing reads.
    skip: bool,
    /// `#[serde(skip_deserializing)]`: accepted by the type's shape and then
    /// dropped; the document reads as applied while nothing takes the value.
    skip_deserializing: bool,
    flatten: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeKind {
    Struct,
    Enum,
}

/// One config type read out of its declaration.
struct TypeInfo {
    crate_name: String,
    path: PathBuf,
    name: String,
    kind: TypeKind,
    fields: Vec<FieldInfo>,
}

/// A field's type, after unwrapping the containers the walk follows.
enum Shape {
    Leaf,
    Struct(Rc<TypeInfo>),
    Seq(Box<Shape>),
    /// Dynamic keys: a map or `serde_json::Value`; every key is legal.
    Map,
}

struct Source {
    crate_name: String,
    path: PathBuf,
    text: String,
}

/// 树上某个结构体的名字：`(crate, 类型名)`。
type TypeName = (String, String);

/// 已解析的结构体，按名字记；`None` 是「读到过、但没有可解析的声明」，记下来是为了
/// 每份文档不必再读一次树。
type ParsedTypes = RefCell<BTreeMap<TypeName, Option<Rc<TypeInfo>>>>;

/// The config type graph, read from the declarations under `crates/*/src`.
///
/// The graph is built once per gate run: walking the same type for every
/// document and every section would re-read the tree dozens of times.
pub struct TypeGraph {
    root: PathBuf,
    sources: Vec<Source>,
    definitions: BTreeMap<TypeName, Vec<usize>>,
    parsed: ParsedTypes,
}

impl TypeGraph {
    pub fn new(root: &Path) -> Self {
        let mut paths = Vec::new();
        walk(&root.join("crates"), &["rs"], &mut paths);
        paths.sort();
        let mut sources = Vec::new();
        let mut definitions: BTreeMap<TypeName, Vec<usize>> = BTreeMap::new();
        for path in paths {
            let Some(crate_name) = crate_name_of(root, &path) else {
                continue;
            };
            let text = read(&path);
            let index = sources.len();
            for name in declared_types(&text) {
                definitions
                    .entry((crate_name.clone(), name))
                    .or_default()
                    .push(index);
            }
            sources.push(Source {
                crate_name,
                path,
                text,
            });
        }
        Self {
            root: root.to_path_buf(),
            sources,
            definitions,
            parsed: RefCell::new(BTreeMap::new()),
        }
    }

    /// Every field the reader types declare, with the document path it is
    /// written at and the type that declares it.
    ///
    /// The population is the declarations, not a delivered document: a field
    /// the chart omits is still a field the process reads (and the knob gate
    /// already judges whether some document writes it).
    pub fn config_fields(&self, readers: &[Reader]) -> Vec<ConfigField> {
        let mut out = Vec::new();
        for reader in readers {
            if matches!(reader.section, "_comment" | "env") {
                continue;
            }
            self.collect_reader_fields(reader, reader.section, &mut out);
        }
        out.sort();
        out.dedup();
        out
    }

    fn collect_reader_fields(&self, reader: &Reader, path: &str, out: &mut Vec<ConfigField>) {
        if let Some(info) = self.reader_type(reader.type_name) {
            let mut stack = Vec::new();
            self.collect_type_fields(&info, path, &mut stack, out);
        }
        for nested in &reader.nested {
            self.collect_reader_fields(nested, &format!("{path}.{}", nested.section), out);
        }
    }

    fn collect_type_fields(
        &self,
        info: &Rc<TypeInfo>,
        path: &str,
        stack: &mut Vec<(String, String)>,
        out: &mut Vec<ConfigField>,
    ) {
        let key = (info.crate_name.clone(), info.name.clone());
        if stack.contains(&key) {
            return;
        }
        stack.push(key);
        for field in &info.fields {
            if field.skip || field.skip_deserializing {
                continue;
            }
            if field.flatten {
                // A flattened field's own name is not a document key: its
                // subfields sit at the parent's level.
                if let Shape::Struct(sub) = self.classify(&field.ty, info) {
                    self.collect_type_fields(&sub, path, stack, out);
                }
                continue;
            }
            let field_path = format!("{path}.{}", field.canonical);
            out.push(ConfigField {
                section_path: field_path.clone(),
                ident: field.ident.clone(),
                owner_type: info.name.clone(),
                owner_crate: info.crate_name.clone(),
            });
            match self.classify(&field.ty, info) {
                Shape::Struct(sub) => {
                    self.collect_type_fields(&sub, &field_path, stack, out);
                }
                Shape::Seq(element) => {
                    if let Shape::Struct(sub) = *element {
                        self.collect_type_fields(&sub, &format!("{field_path}[]"), stack, out);
                    }
                }
                Shape::Map | Shape::Leaf => {}
            }
        }
        stack.pop();
    }

    /// 这个名字是否在多个 crate 里都有声明：裸名解析会撞车，读的一方必须补全。
    pub fn ambiguous_name(&self, name: &str) -> bool {
        self.definitions
            .keys()
            .filter(|(_, declared)| declared == name)
            .count()
            > 1
    }

    /// 声明索引里有没有这个 `(crate, 类型名)`。
    pub fn declared_in(&self, crate_name: &str, name: &str) -> bool {
        self.definitions
            .contains_key(&(crate_name.to_string(), name.to_string()))
    }

    /// A named struct's declaration, for resolving an access chain's receiver.
    ///
    /// `from_crate` is the crate the name was written in; a bare name is looked
    /// up there first, then across the tree if exactly one crate declares it.
    /// `from_path` lets the definition index prefer the module the crate's
    /// layout implies, the same way [`Self::classify`] does.
    pub fn shape_named(
        &self,
        ty: &str,
        from_crate: &str,
        from_path: Option<&Path>,
    ) -> Option<TypeShape> {
        let bare = strip_reference(ty).trim();
        if bare.is_empty() || container_inner(bare).is_some() {
            return None;
        }
        let prefer = from_path.unwrap_or_else(|| Path::new(""));
        let candidates: Vec<(String, String)> = if let Some(rest) = bare
            .strip_prefix("crate::")
            .or_else(|| bare.strip_prefix("self::"))
            .or_else(|| bare.strip_prefix("super::"))
        {
            vec![(from_crate.to_string(), last_segment(rest))]
        } else if bare.contains("::") {
            let first = bare.split("::").next().unwrap_or_default().to_string();
            if self
                .definitions
                .keys()
                .any(|(crate_name, _)| crate_name == &first)
            {
                vec![(first, last_segment(bare))]
            } else {
                // `use config::X` 这样的 uniform path：相对当前 crate 的模块路径。
                // 名字在别的 crate 里同样存在时，由 `type_info` 在下面判空。
                vec![(from_crate.to_string(), last_segment(bare))]
            }
        } else {
            let same = self
                .definitions
                .contains_key(&(from_crate.to_string(), bare.to_string()));
            if same {
                vec![(from_crate.to_string(), bare.to_string())]
            } else {
                let found: Vec<(String, String)> = self
                    .definitions
                    .keys()
                    .filter(|(_, name)| name == bare)
                    .cloned()
                    .collect();
                if found.len() == 1 {
                    found
                } else {
                    return None;
                }
            }
        };
        for (crate_name, name) in candidates {
            let preferred_path = if prefer.as_os_str().is_empty() {
                None
            } else {
                Some(prefer)
            };
            let Some(info) = self.type_info(&crate_name, &name, preferred_path) else {
                continue;
            };
            if info.kind != TypeKind::Struct {
                return None;
            }
            return Some(TypeShape {
                crate_name: info.crate_name.clone(),
                name: info.name.clone(),
                fields: info
                    .fields
                    .iter()
                    .map(|field| TypeField {
                        ident: field.ident.clone(),
                        ty: field.ty.clone(),
                    })
                    .collect(),
            });
        }
        None
    }

    /// The shape of `value` read as `T`, without a document or a reader: the
    /// declarations alone. Used by the self-proofs to show that a skipped
    /// field is still produced.
    pub fn shapes_of<T>(&self, file: &str, prefix: &str, value: &Value) -> Vec<SectionShape> {
        self.shapes_named(file, std::any::type_name::<T>(), prefix, value)
    }

    /// [`Self::shapes_of`] with the type named as a string, for a type the
    /// self-proof defines in a temporary tree.
    pub fn shapes_named(
        &self,
        file: &str,
        type_name: &str,
        prefix: &str,
        value: &Value,
    ) -> Vec<SectionShape> {
        let info = self.reader_type(type_name);
        let mut out = Vec::new();
        self.walk(
            file,
            None,
            info.as_deref(),
            value,
            prefix,
            &mut out,
            &mut Vec::new(),
            None,
        );
        out
    }

    /// The type that reads this section, if it is a struct the graph can parse.
    /// `None` is the dynamic shape (a map, a `Value`, or an external type): the
    /// walk then only checks what the document writes against the read-back,
    /// which is all a dynamic section admits.
    fn reader_type(&self, type_name: &str) -> Option<Rc<TypeInfo>> {
        if type_name.starts_with("std::")
            || type_name.starts_with("alloc::")
            || type_name.starts_with("core::")
            || type_name.starts_with("serde_json")
        {
            return None;
        }
        let parts: Vec<&str> = type_name.split("::").collect();
        let (crate_name, name) = (parts.first()?.to_string(), parts.last()?.to_string());
        let prefer = self.preferred_path(&crate_name, &parts[1..parts.len() - 1]);
        match self.type_info(&crate_name, &name, prefer.as_deref()) {
            Some(info) if info.kind == TypeKind::Struct => Some(info),
            Some(_) => None,
            None => panic!(
                "the reader type `{type_name}` is not a struct the type graph can find: \
                 either the declaration moved or the graph is reading the wrong tree"
            ),
        }
    }

    /// The file a qualified type name is spelled in, when it follows the
    /// crate's module layout. Re-exported types fall back to the definition
    /// index instead.
    fn preferred_path(&self, crate_name: &str, modules: &[&str]) -> Option<PathBuf> {
        let mut path = self
            .root
            .join("crates")
            .join(crate_name.replace('_', "-"))
            .join("src");
        if modules.is_empty() {
            return None;
        }
        for module in modules {
            path.push(module);
        }
        path.set_extension("rs");
        path.is_file().then_some(path)
    }

    fn type_info(
        &self,
        crate_name: &str,
        name: &str,
        prefer: Option<&Path>,
    ) -> Option<Rc<TypeInfo>> {
        let key = (crate_name.to_string(), name.to_string());
        if let Some(hit) = self.parsed.borrow().get(&key) {
            return hit.clone();
        }
        let indices = self.definitions.get(&key)?;
        let chosen = prefer
            .and_then(|wanted| {
                indices
                    .iter()
                    .find(|index| self.sources[**index].path == wanted)
            })
            .or_else(|| indices.first());
        let source = &self.sources[*chosen?];
        let info = self.parse_type(source, name).map(Rc::new);
        self.parsed.borrow_mut().insert(key, info.clone());
        info
    }

    fn parse_type(&self, source: &Source, name: &str) -> Option<TypeInfo> {
        let (offset, kind) = find_definition(&source.text, name)?;
        if kind == TypeKind::Enum {
            return Some(TypeInfo {
                crate_name: source.crate_name.clone(),
                path: source.path.clone(),
                name: name.to_string(),
                kind,
                fields: Vec::new(),
            });
        }
        let Some(body) = struct_body(&source.text, offset) else {
            // A unit or tuple struct: a document object has no fields to land
            // on, so it is a dynamic shape rather than a field set.
            return Some(TypeInfo {
                crate_name: source.crate_name.clone(),
                path: source.path.clone(),
                name: name.to_string(),
                kind,
                fields: Vec::new(),
            });
        };
        let attrs = attributes_before(&source.text, offset);
        let rename_all = string_attr(&attrs, "rename_all");
        let fields = parse_fields(&source.path, name, body, rename_all.as_deref());
        Some(TypeInfo {
            crate_name: source.crate_name.clone(),
            path: source.path.clone(),
            name: name.to_string(),
            kind,
            fields,
        })
    }

    /// Resolve a field's written type into the shape the walk follows.
    fn classify(&self, ty: &str, from: &TypeInfo) -> Shape {
        let ty = ty.trim();
        if ty.is_empty() {
            return Shape::Leaf;
        }
        if let Some(inner) = outer_arg(ty, "Option")
            .or_else(|| outer_arg(ty, "Box"))
            .or_else(|| outer_arg(ty, "Arc"))
            .or_else(|| outer_arg(ty, "Rc"))
        {
            return self.classify(&inner, from);
        }
        if let Some(inner) = outer_arg(ty, "Vec").or_else(|| array_element(ty)) {
            return Shape::Seq(Box::new(self.classify(&inner, from)));
        }
        if let Some(args) = outer_args(ty, "Cow") {
            if let Some(last) = args.last() {
                return self.classify(last, from);
            }
        }
        if outer_arg(ty, "HashMap").is_some()
            || outer_arg(ty, "BTreeMap").is_some()
            || outer_arg(ty, "IndexMap").is_some()
            || is_dynamic_name(ty)
        {
            return Shape::Map;
        }
        let bare = strip_reference(ty);
        let (crate_name, name) = if let Some(rest) = bare.strip_prefix("crate::") {
            (from.crate_name.clone(), last_segment(rest))
        } else if let Some(rest) = bare.strip_prefix("self::") {
            (from.crate_name.clone(), last_segment(rest))
        } else if let Some(rest) = bare.strip_prefix("super::") {
            (from.crate_name.clone(), last_segment(rest))
        } else if bare.contains("::") {
            let first = bare.split("::").next().unwrap_or_default();
            if self
                .definitions
                .keys()
                .any(|(crate_name, _)| crate_name == first)
            {
                (first.to_string(), last_segment(bare))
            } else {
                return Shape::Leaf;
            }
        } else {
            (from.crate_name.clone(), bare.to_string())
        };
        if is_leaf(&name) {
            return Shape::Leaf;
        }
        match self.type_info(&crate_name, &name, Some(&from.path)) {
            Some(info) if info.kind == TypeKind::Struct => Shape::Struct(info),
            Some(_) => Shape::Leaf,
            None => {
                if name.chars().next().is_some_and(|c| c.is_uppercase()) {
                    panic!(
                        "cannot resolve type `{ty}` read by `{}`: the type graph would \
                         silently treat an unknown local struct as a leaf",
                        from.name
                    );
                }
                Shape::Leaf
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        &self,
        file: &str,
        reader: Option<&Reader>,
        info: Option<&TypeInfo>,
        value: &Value,
        prefix: &str,
        out: &mut Vec<SectionShape>,
        stack: &mut Vec<(String, String)>,
        read_back: Option<&Map<String, Value>>,
    ) {
        let Value::Object(object) = value else {
            panic!(
                "{file} `{prefix}` is not an object: a section (or a struct-typed field) \
                 must be written as one"
            );
        };
        let mut written = BTreeSet::new();
        for key in object.keys() {
            written.insert(key.clone());
        }
        let Some(info) = info else {
            let produced = read_back
                .map(|map| map.keys().cloned().collect())
                .unwrap_or_default();
            out.push(SectionShape {
                path: prefix.to_string(),
                written,
                produced,
            });
            return;
        };

        let mut produced: BTreeSet<String> = info
            .fields
            .iter()
            .filter(|field| !field.skip && !field.skip_deserializing)
            .map(|field| field.canonical.clone())
            .collect();
        let mut accepted: BTreeMap<String, FieldInfo> = BTreeMap::new();
        let mut dynamic = false;
        for field in &info.fields {
            if field.flatten {
                match self.classify(&field.ty, info) {
                    Shape::Struct(sub) => {
                        for nested in &sub.fields {
                            if !nested.skip && !nested.skip_deserializing {
                                produced.insert(nested.canonical.clone());
                            }
                            if !nested.flatten {
                                for name in &nested.accepted {
                                    accepted.insert(name.clone(), nested.clone());
                                }
                            }
                        }
                    }
                    _ => dynamic = true,
                }
                continue;
            }
            for name in &field.accepted {
                accepted.insert(name.clone(), field.clone());
            }
        }
        if let Some(read_back) = read_back {
            produced.extend(read_back.keys().cloned());
        }

        for key in object.keys() {
            // `_comment` is prose the delivery comparator drops at any depth; it
            // is not a knob, so it is written by design rather than read by a
            // field.
            if key.starts_with("_comment") {
                continue;
            }
            if let Some(nested) = reader.and_then(|reader| {
                reader
                    .nested
                    .iter()
                    .find(|nested| nested.section == key.as_str())
            }) {
                section_shapes(
                    self,
                    file,
                    nested,
                    &object[key],
                    &format!("{prefix}.{key}"),
                    out,
                );
                continue;
            }
            let Some(field) = accepted.get(key.as_str()) else {
                if dynamic {
                    continue;
                }
                panic!(
                    "{file} `{prefix}.{key}` has no field behind it: serde passes it over \
                     in silence, so it reads as applied while nothing uses it"
                );
            };
            if field.skip {
                panic!(
                    "{file} `{prefix}.{key}` names a field marked `#[serde(skip)]`: the key \
                     is neither read nor written, so the container has no field behind it"
                );
            }
            if field.skip_deserializing {
                panic!(
                    "{file} `{prefix}.{key}` names a field marked \
                     `#[serde(skip_deserializing)]`: the value is accepted by the shape and \
                     then dropped, so the key reads as applied while nothing takes it"
                );
            }
            let nested_prefix = format!("{prefix}.{key}");
            match self.classify(&field.ty, info) {
                Shape::Struct(sub) => {
                    if object[key].is_null() {
                        // `Option<T>` written as null: serde keeps no object
                        // and the type's defaults decide; there is nothing to
                        // walk into.
                        continue;
                    }
                    if !object[key].is_object() {
                        panic!(
                            "{file} `{nested_prefix}` is not an object, and `{}` reads it \
                             as `{}`",
                            info.name, field.ty
                        );
                    }
                    let key_of = (sub.crate_name.clone(), sub.name.clone());
                    if stack.contains(&key_of) {
                        continue;
                    }
                    stack.push(key_of);
                    self.walk(
                        file,
                        None,
                        Some(&sub),
                        &object[key],
                        &nested_prefix,
                        out,
                        stack,
                        None,
                    );
                    stack.pop();
                }
                Shape::Seq(element) => {
                    let Shape::Struct(sub) = *element else {
                        continue;
                    };
                    let Value::Array(items) = &object[key] else {
                        if object[key].is_null() {
                            continue;
                        }
                        panic!(
                            "{file} `{nested_prefix}` is not an array, and `{}` reads it as `{}`",
                            info.name, field.ty
                        );
                    };
                    let key_of = (sub.crate_name.clone(), sub.name.clone());
                    if stack.contains(&key_of) {
                        continue;
                    }
                    stack.push(key_of);
                    let array_prefix = format!("{nested_prefix}[]");
                    if items.is_empty() {
                        self.walk(
                            file,
                            None,
                            Some(&sub),
                            &Value::Object(Map::new()),
                            &array_prefix,
                            out,
                            stack,
                            None,
                        );
                    }
                    for item in items {
                        if !item.is_object() {
                            panic!(
                                "{file} `{array_prefix}` has an element that is not an object, \
                                 and `{}` reads it as `{}`",
                                info.name, field.ty
                            );
                        }
                        self.walk(
                            file,
                            None,
                            Some(&sub),
                            item,
                            &array_prefix,
                            out,
                            stack,
                            None,
                        );
                    }
                    stack.pop();
                }
                Shape::Map | Shape::Leaf => {}
            }
        }

        out.push(SectionShape {
            path: prefix.to_string(),
            written,
            produced,
        });
    }
}

/// The crate a source file belongs to: the directory after `crates/`.
fn crate_name_of(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let parts: Vec<_> = relative.components().collect();
    let position = parts.iter().position(|part| part.as_os_str() == "crates")?;
    // Only a crate's `src/` is a declaration surface: `tests/`, `benches/`
    // and `examples/` hold fixture types with the same names, and indexing
    // them would let a fixture's struct stand in for a production one.
    if parts.get(position + 2)?.as_os_str() != "src" {
        return None;
    }
    let crate_dir = parts.get(position + 1)?.as_os_str().to_str()?;
    Some(crate_dir.replace('-', "_"))
}

/// Every `struct Name` / `enum Name` a source declares, by name.
fn declared_types(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = strip_line_comment(line).trim_start();
        let line = strip_visibility(line);
        for keyword in ["struct ", "enum "] {
            if let Some(rest) = line.strip_prefix(keyword) {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    out.push(name);
                }
            }
        }
    }
    out
}

fn strip_visibility(line: &str) -> &str {
    let Some(rest) = line.strip_prefix("pub") else {
        return line;
    };
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix('(') {
        if let Some(close) = rest.find(')') {
            return rest[close + 1..].trim_start();
        }
    }
    rest
}

fn strip_line_comment(line: &str) -> &str {
    let mut in_string = false;
    let mut escaped = false;
    let mut chars = line.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        if c == '"' {
            in_string = true;
        } else if c == '/' && chars.peek().is_some_and(|(_, next)| *next == '/') {
            return &line[..index];
        }
    }
    line
}

/// The byte offset of a definition and its kind.
fn find_definition(text: &str, name: &str) -> Option<(usize, TypeKind)> {
    let mut offset = 0;
    for line in text.lines() {
        let code = strip_line_comment(line).trim_start();
        let code = strip_visibility(code);
        for (keyword, kind) in [("struct ", TypeKind::Struct), ("enum ", TypeKind::Enum)] {
            if let Some(rest) = code.strip_prefix(keyword) {
                let found: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if found == name {
                    let guess = line.find(keyword).map(|i| offset + i).unwrap_or(offset);
                    return Some((guess, kind));
                }
            }
        }
        offset += line.len() + 1;
    }
    None
}

/// The body between a struct's braces, or `None` for a unit / tuple struct.
fn struct_body(text: &str, offset: usize) -> Option<&str> {
    let rest = &text[offset..];
    let brace = rest.find('{')?;
    if let Some(semicolon) = rest.find(';') {
        if semicolon < brace {
            return None;
        }
    }
    let end = matching_brace(text, offset + brace, Dialect::Rust)?;
    Some(&text[offset + brace + 1..end])
}

/// The attributes and doc lines immediately above a definition.
fn attributes_before(text: &str, offset: usize) -> String {
    let before = &text[..offset];
    let mut collected = Vec::new();
    for line in before.lines().rev() {
        let trimmed = line.trim();
        if trimmed.starts_with("#[") || trimmed.starts_with("///") || trimmed.starts_with("//!") {
            collected.push(trimmed.to_string());
            continue;
        }
        break;
    }
    collected.reverse();
    collected.join("\n")
}

/// Split the `serde(...)` item list out of an attribute block.
fn serde_items(attrs: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut search = attrs;
    while let Some(start) = search.find("serde(") {
        let after = &search[start + "serde(".len()..];
        let mut depth = 1i64;
        let mut end = None;
        for (index, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(index);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else {
            break;
        };
        for item in split_top(&after[..end], ',') {
            let item = item.trim();
            if !item.is_empty() {
                items.push(item.to_string());
            }
        }
        search = &after[end + 1..];
    }
    items
}

fn string_attr(attrs: &str, name: &str) -> Option<String> {
    serde_items(attrs).into_iter().find_map(|item| {
        let rest = item.strip_prefix(name)?;
        let rest = rest.trim_start().strip_prefix('=')?.trim();
        rest.strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .map(str::to_string)
    })
}

fn parse_fields(path: &Path, owner: &str, body: &str, rename_all: Option<&str>) -> Vec<FieldInfo> {
    let lines: Vec<&str> = body.lines().collect();
    let mut fields = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < lines.len() {
        let line = strip_line_comment(lines[i]).trim();
        if line.is_empty() {
            i += 1;
            continue;
        }
        if line.starts_with("#[") {
            // 属性可以写多行：`#[serde(\n default,\n ...\n)]`。
            let mut attr = line.to_string();
            while !attr.trim_end().ends_with(']') && i + 1 < lines.len() {
                i += 1;
                attr.push(' ');
                attr.push_str(strip_line_comment(lines[i]).trim());
            }
            pending.push(attr);
            i += 1;
            continue;
        }
        if line.starts_with("///") || line.starts_with("//!") {
            pending.push(line.to_string());
            i += 1;
            continue;
        }
        let Some((ident, mut ty)) = field_start(line) else {
            panic!(
                "{}: a line in the body of `{owner}` is not a field: `{line}`",
                path.display()
            );
        };
        // 类型可以写在冒号下一行：`pub hook:\n    Option<...>`。
        while (ty.is_empty() || !type_complete(&ty)) && i + 1 < lines.len() {
            i += 1;
            ty.push(' ');
            ty.push_str(strip_line_comment(lines[i]).trim());
        }
        let attrs = pending.join(" ");
        let items = serde_items(&attrs);
        let has = |wanted: &str| items.iter().any(|item| item == wanted);
        let rename = string_attr(&attrs, "rename");
        let aliases: Vec<String> = items
            .iter()
            .filter_map(|item| {
                let rest = item.strip_prefix("alias")?;
                let rest = rest.trim_start().strip_prefix('=')?.trim();
                rest.strip_prefix('"')
                    .and_then(|rest| rest.strip_suffix('"'))
                    .map(str::to_string)
            })
            .collect();
        let canonical = rename.unwrap_or_else(|| rename_case(&ident, rename_all));
        let mut accepted = vec![canonical.clone()];
        accepted.extend(aliases);
        fields.push(FieldInfo {
            ident,
            canonical,
            accepted,
            ty: ty.trim().trim_end_matches(',').trim().to_string(),
            skip: has("skip"),
            skip_deserializing: has("skip_deserializing"),
            flatten: has("flatten"),
        });
        pending.clear();
        i += 1;
    }
    fields
}

fn field_start(line: &str) -> Option<(String, String)> {
    let line = strip_visibility(line).trim_start();
    let colon = line.find(':')?;
    let ident = line[..colon].trim();
    if ident.is_empty() || !ident.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    let ty = line[colon + 1..].trim();
    Some((ident.to_string(), ty.to_string()))
}

fn type_complete(ty: &str) -> bool {
    let mut depth = 0i64;
    let mut in_string = false;
    let mut escaped = false;
    let mut chars = ty.chars().peekable();
    while let Some(c) = chars.next() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            // 返回类型里的 `->`：`>` 不是泛型的闭合，不能减深度。
            '-' if chars.peek() == Some(&'>') => {
                chars.next();
            }
            '<' | '(' | '[' => depth += 1,
            '>' | ')' | ']' => depth -= 1,
            _ => {}
        }
    }
    depth <= 0
}

fn split_top(text: &str, separator: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i64;
    let mut start = 0;
    for (index, c) in text.char_indices() {
        match c {
            '<' | '(' | '[' => depth += 1,
            '>' | ')' | ']' => depth -= 1,
            _ if c == separator && depth == 0 => {
                out.push(&text[start..index]);
                start = index + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&text[start..]);
    out
}

fn outer_args(ty: &str, outer: &str) -> Option<Vec<String>> {
    let head = ty.strip_prefix(outer)?;
    let head = head.trim_start();
    let rest = head.strip_prefix('<')?;
    let mut depth = 1i64;
    let mut end = None;
    for (index, c) in rest.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(index);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    if !rest[end + 1..].trim().is_empty() {
        return None;
    }
    Some(
        split_top(&rest[..end], ',')
            .into_iter()
            .map(|arg| arg.trim().to_string())
            .filter(|arg| !arg.is_empty())
            .collect(),
    )
}

fn outer_arg(ty: &str, outer: &str) -> Option<String> {
    let args = outer_args(ty, outer)?;
    args.first().cloned()
}

fn array_element(ty: &str) -> Option<String> {
    let rest = ty.strip_prefix('[')?;
    // `[T; N]` 与切片 `[T]` 的元素类型都是 `T`。
    if let Some((element, _)) = rest.split_once(';') {
        return Some(element.trim().to_string());
    }
    Some(rest.strip_suffix(']')?.trim().to_string())
}

fn strip_reference(ty: &str) -> &str {
    let mut rest = ty.trim();
    while let Some(after) = rest.strip_prefix('&') {
        rest = after.trim_start();
        if let Some(after) = rest.strip_prefix("'") {
            if let Some((_, after)) = after.split_once(char::is_whitespace) {
                rest = after.trim_start();
            }
        }
        if let Some(after) = rest.strip_prefix("mut ") {
            rest = after.trim_start();
        }
    }
    rest
}

fn last_segment(path: &str) -> String {
    path.rsplit("::").next().unwrap_or(path).to_string()
}

fn is_dynamic_name(ty: &str) -> bool {
    matches!(ty.trim(), "Value" | "Map" | "serde_json::Value")
        || ty.trim().ends_with("::Value")
        || ty.trim().starts_with("serde_json::Map")
}

fn is_leaf(name: &str) -> bool {
    matches!(
        name,
        "String"
            | "str"
            | "bool"
            | "char"
            | "usize"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "f32"
            | "f64"
            | "Path"
            | "PathBuf"
            | "Duration"
            | "SystemTime"
            | "Instant"
            | "DateTime"
            | "Utc"
            | "NaiveDateTime"
            | "IpAddr"
            | "SocketAddr"
            | "Url"
            | "Regex"
            | "Value"
            | "Map"
            | "Uuid"
            | "Status"
            | "Error"
            | "Range"
            | "Self"
    )
}

fn rename_case(name: &str, rename_all: Option<&str>) -> String {
    let Some(rename_all) = rename_all else {
        return name.to_string();
    };
    match rename_all {
        "camelCase" => {
            let mut parts = name.split('_');
            let head = parts.next().unwrap_or_default();
            let rest: String = parts
                .map(|part| {
                    let mut chars = part.chars();
                    match chars.next() {
                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                        None => String::new(),
                    }
                })
                .collect();
            format!("{head}{rest}")
        }
        "PascalCase" => name
            .split('_')
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    None => String::new(),
                }
            })
            .collect(),
        "kebab-case" => name.replace('_', "-"),
        "SCREAMING_SNAKE_CASE" | "UPPERCASE" => name.to_uppercase(),
        "lowercase" => name.to_lowercase(),
        _ => name.to_string(),
    }
}

/// The config where it is shipped: the chart's file, the static manifest the
/// cluster pulls, the rendered profiles, and the example template.
pub fn shipped_configs(root: &Path) -> Vec<(String, Value)> {
    let mut out = vec![(
        "cogneva.example.json".to_string(),
        read_config(&root.join("cogneva.example.json")),
    )];
    for relative in [
        "deploy/helm/cogneva/files/cogneva.json",
        "deploy/k3s/cogneva-json-configmap.yaml",
    ] {
        out.push((relative.to_string(), read_config(&root.join(relative))));
    }
    let rendered = root.join("deploy/rendered");
    let mut profiles: Vec<_> = std::fs::read_dir(&rendered)
        .unwrap_or_else(|e| panic!("read {}: {e}", rendered.display()))
        .map(|e| e.expect("readable dir entry").path())
        .filter(|p| p.is_dir())
        .collect();
    profiles.sort();
    for dir in profiles {
        let file = dir.join("10-configmap-cogneva-json.yaml");
        out.push((file.display().to_string(), read_config(&file)));
    }
    out
}

/// Read the config wherever it is shipped: the chart and the example keep it as
/// a file of its own, a ConfigMap manifest keeps it as a block scalar.
pub fn read_config(path: &Path) -> Value {
    match path.extension().and_then(|e| e.to_str()) {
        Some("json") => read_json(path),
        Some("yaml") => read_config_manifest(path),
        other => panic!("{} has no known config format ({other:?})", path.display()),
    }
}

fn read_json(path: &Path) -> Value {
    let raw = read(path);
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()))
}

/// Pull the config back out of a ConfigMap manifest.
///
/// The payload is a YAML block scalar indented under its key, so it is
/// de-indented and parsed as the JSON it is. A manifest that stops carrying the
/// key, or carries it empty, fails here rather than passing quietly.
fn read_config_manifest(path: &Path) -> Value {
    let raw = read(path);
    let lines: Vec<&str> = raw.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.starts_with("  ") && l.trim_start().starts_with("cogneva.json: |"))
        .unwrap_or_else(|| panic!("{} embeds no cogneva.json block", path.display()));
    let mut payload = String::new();
    for line in &lines[start + 1..] {
        match line.strip_prefix("    ") {
            Some(rest) => {
                payload.push_str(rest);
                payload.push('\n');
            }
            None if line.trim().is_empty() => payload.push('\n'),
            None => break,
        }
    }
    serde_json::from_str(&payload)
        .unwrap_or_else(|e| panic!("{} embeds an unparseable config: {e}", path.display()))
}

#[cfg(test)]
mod self_proofs {
    //! The walk's own red proofs: a key with no field inside a nested section
    //! and inside an array element is a finding, and `produced` really is read
    //! from the declaration rather than from a default's serialization.

    use super::*;
    use crate::common::routes::repo_root;

    /// A field the default's serialization omits (`skip_serializing_if`) is
    /// still a knob the type reads, so it is still produced here. This is the
    /// half a read-back-only judgement cannot see.
    #[test]
    fn a_field_serialization_omits_is_still_produced() {
        let graph = TypeGraph::new(&repo_root());
        let value = serde_json::json!({ "nats": {} });
        let shapes = graph.shapes_of::<cog_core::config::DagExecutorConfig>(
            "dag_executor",
            "dag_executor",
            &value,
        );
        let top = shapes
            .iter()
            .find(|shape| shape.path == "dag_executor")
            .expect("the section itself is a shape");
        assert!(
            top.produced.contains("nats_url"),
            "`dag_executor.nats_url` is an `Option` serialized with \
             `skip_serializing_if`, so the read-back of a default never shows it; the \
             declaration walk must"
        );
        let nats = shapes
            .iter()
            .find(|shape| shape.path == "dag_executor.nats")
            .expect("the nested NATS section is walked");
        assert!(nats.produced.contains("stream_retention"));
        assert!(nats.produced.contains("consumer_ack_wait_secs"));

        let typed: cog_core::config::DagExecutorConfig =
            serde_json::from_value(value).expect("the section fits its type");
        let read_back = serde_json::to_value(typed).expect("a section serializes");
        assert!(
            read_back.get("nats_url").is_none(),
            "the premise of the proof: the default's serialization omits the key"
        );
    }

    /// A key inside a nested object that no field stands behind is a finding —
    /// the shape the pre-graph walk could not see below the top level.
    #[test]
    #[should_panic(expected = "has no field behind it")]
    fn a_key_inside_a_nested_section_with_no_field_is_a_finding() {
        let graph = TypeGraph::new(&repo_root());
        graph.shapes_of::<cog_observability::config::ObservabilityExportersConfig>(
            "observability",
            "observability",
            &serde_json::json!({ "loki": { "not_a_field": 1 } }),
        );
    }

    /// The same, one level down again: an array element.
    #[test]
    #[should_panic(expected = "has no field behind it")]
    fn a_key_inside_an_array_element_with_no_field_is_a_finding() {
        let graph = TypeGraph::new(&repo_root());
        graph.shapes_of::<cog_observability::config::ObservabilityExportersConfig>(
            "observability",
            "observability",
            &serde_json::json!({ "infra_watch": { "rules": [{ "not_a_field": 1 }] } }),
        );
    }

    /// `produced` follows the declaration: removing a field from the source
    /// removes the key on the next read. A graph that had cached a default, or
    /// had judged only the read-back, would keep it.
    #[test]
    fn produced_is_read_from_the_declaration_not_from_a_default() {
        let dir = tempfile::tempdir().expect("a temporary tree");
        let src = dir.path().join("crates/fake-config/src");
        std::fs::create_dir_all(&src).expect("create the fake crate");
        let file = src.join("config.rs");
        let write = |extra: &str| {
            std::fs::write(
                &file,
                format!(
                    "pub struct Inner {{\n    pub a: u64,\n{extra}}}\n\n\
                     pub struct Outer {{\n    pub inner: Inner,\n    pub items: Vec<Inner>,\n}}\n"
                ),
            )
            .expect("write the fake source");
        };
        let value = serde_json::json!({ "inner": {}, "items": [{}] });

        write("    pub b: Option<String>,\n");
        let before = TypeGraph::new(dir.path());
        let shapes = before.shapes_named("fake", "fake_config::Outer", "outer", &value);
        let inner = shapes
            .iter()
            .find(|shape| shape.path == "outer.inner")
            .expect("the nested struct is walked");
        assert!(inner.produced.contains("b"));
        let element = shapes
            .iter()
            .find(|shape| shape.path == "outer.items[]")
            .expect("the array element is walked");
        assert!(element.produced.contains("b"));

        write("");
        let after = TypeGraph::new(dir.path());
        let shapes = after.shapes_named("fake", "fake_config::Outer", "outer", &value);
        let inner = shapes
            .iter()
            .find(|shape| shape.path == "outer.inner")
            .expect("the nested struct is still walked");
        assert!(
            !inner.produced.contains("b"),
            "the field was removed from the declaration, so the key must be gone"
        );
    }
}
