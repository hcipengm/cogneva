//! What each shipped skill declares about its role's tools, checked against
//! the registry the server actually builds.
//!
//! A boundary is worth what the two ends agree on. The skill files name tools,
//! the registry holds tools, and when the two sets drift the narrowing drops
//! the mismatch without a word: a name that matches nothing reads as a stale
//! entry and is passed over. So a typo in a skill file quietly takes a
//! capability away from a role, and nothing at runtime can tell that apart from
//! the role never having had it. The check therefore runs here, over the files
//! as shipped, where a missing name is a red test rather than a log line.
//!
//! The second thing checked is that the split is the one the roles mean: the
//! planner plans, so it holds nothing that changes the workspace. That is a
//! claim about the role, not about the list, so it is written as one — a
//! property over the tools that mutate the checkout rather than a copy of the
//! list, which would go stale the first time a role gains a tool.

use cog_agent::tools::builtins;
use cog_agent::ToolRegistry;
use cog_core::SkillRegistry;
use std::sync::Arc;

/// Tools that change the workspace. A role that only plans must hold none of
/// them; which other tools it holds is its own business.
const WORKSPACE_CHANGING: [&str; 3] = ["write_file", "run_command", "http_request"];

#[derive(Debug)]
struct UnreachableClient;

#[async_trait::async_trait]
impl cog_core::HttpClient for UnreachableClient {
    async fn execute(
        &self,
        req: cog_core::HttpRequest,
    ) -> cog_core::SFResult<cog_core::HttpResponse> {
        Ok(cog_core::HttpResponse {
            status: 200,
            headers: Default::default(),
            body: format!("{} {}", req.method, req.url).into_bytes(),
        })
    }
}

/// Answers a read out of the local filesystem. The point is not fidelity to the
/// executor but that a narrowed registry still reaches *an* executor: what is
/// being tested is that narrowing kept the machinery, not that the machinery
/// works — that is the sandbox's own business.
#[derive(Debug)]
struct ReadingSandbox;

#[async_trait::async_trait]
impl cog_core::SandboxBackend for ReadingSandbox {
    async fn execute(
        &self,
        req: &cog_core::SandboxRequest,
    ) -> cog_core::SFResult<cog_core::SandboxResult> {
        match &req.payload {
            cog_core::SandboxPayload::ReadFile { path } => match std::fs::read_to_string(path) {
                Ok(content) => Ok(cog_core::SandboxResult {
                    stdout: content,
                    exit_code: 0,
                    ..Default::default()
                }),
                Err(e) => Ok(cog_core::SandboxResult {
                    stderr: e.to_string(),
                    exit_code: 1,
                    ..Default::default()
                }),
            },
            _ => Ok(cog_core::SandboxResult {
                exit_code: 0,
                ..Default::default()
            }),
        }
    }

    async fn precompile(&self, _bytes: &[u8]) -> cog_core::SFResult<String> {
        Err(cog_core::SFError::Agent("unsupported".into()))
    }
}

/// The registry as the agent plugin builds it: the built-in execution tools,
/// plus `http_request` when an HTTP client is available. Kept in step with the
/// plugin by the vocabulary assertion below — a tool added there and forgotten
/// here surfaces as a skill naming something this registry cannot answer.
fn production_registry() -> ToolRegistry {
    let registry = ToolRegistry::new();
    cog_core::ToolRegistry::register(&registry, builtins::read_file());
    cog_core::ToolRegistry::register(&registry, builtins::write_file());
    cog_core::ToolRegistry::register(&registry, builtins::run_command());
    cog_core::ToolRegistry::register(
        &registry,
        builtins::http_request(Arc::new(UnreachableClient)),
    );
    registry
}

/// The shipped skills, loaded the way the running server loads them.
fn shipped_skills() -> SkillRegistry {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills");
    let mut skills = SkillRegistry::new();
    skills
        .load_skills_from_dir(&dir)
        .unwrap_or_else(|e| panic!("shipped skills failed to load from {}: {e}", dir.display()));
    assert!(
        !skills.get_all().is_empty(),
        "no skills loaded from {}; the assertions below would pass vacuously",
        dir.display()
    );
    skills
}

#[test]
fn every_tool_a_skill_names_is_one_the_registry_holds() {
    let registry = production_registry();
    let mut known: Vec<String> = registry.names();
    known.sort();

    for skill in shipped_skills().get_all() {
        for name in &skill.tools {
            assert!(
                known.contains(name),
                "skill '{}' declares tool '{name}', which no registered tool answers; \
                 the narrowing would drop it and take the capability away silently. \
                 Registered: {known:?}",
                skill.id
            );
        }
    }
}

#[test]
fn narrowing_keeps_exactly_the_declared_tools() {
    let registry = production_registry();

    for skill in shipped_skills().get_all() {
        let narrowed = registry.restricted_to(&skill.tools);
        let mut got: Vec<String> = narrowed.names();
        got.sort();
        let mut want: Vec<String> = skill.tools.clone();
        want.sort();
        want.dedup();

        assert_eq!(
            got, want,
            "skill '{}' declares {:?} but the narrowed registry holds {:?}",
            skill.id, skill.tools, got
        );
        assert!(
            !narrowed.is_empty() || skill.tools.is_empty(),
            "skill '{}' declares tools yet the narrowed registry came out empty",
            skill.id
        );
    }
}

#[test]
fn narrowing_one_role_does_not_shrink_the_shared_registry() {
    let registry = production_registry();
    let before = registry.names().len();

    let planner = shipped_skills();
    let planner = planner
        .get_all()
        .into_iter()
        .find(|s| s.id == "planner")
        .expect("the planner skill ships with the repository");
    let _ = registry.restricted_to(&planner.tools);

    assert_eq!(
        registry.names().len(),
        before,
        "a role's boundary must be a view of the shared registry, not an edit to it"
    );
}

#[test]
fn a_planning_role_holds_nothing_that_changes_the_workspace() {
    let registry = production_registry();

    for skill in shipped_skills().get_all() {
        if skill.id != "planner" {
            continue;
        }
        let narrowed = registry.restricted_to(&skill.tools);
        let held = narrowed.names();

        assert!(
            held.contains(&"read_file".to_string()),
            "a planner that cannot read cannot plan; it holds {held:?}"
        );
        for changing in WORKSPACE_CHANGING {
            assert!(
                !held.contains(&changing.to_string()),
                "the planning role holds '{changing}'; its boundary is to plan, \
                 not to change the checkout. It holds {held:?}"
            );
        }
    }
}

#[test]
fn a_writing_role_holds_the_tool_that_writes_and_the_reader_does_not() {
    let registry = production_registry();
    let skills = shipped_skills();

    let generator = skills
        .get_all()
        .into_iter()
        .find(|s| s.id == "generator")
        .expect("the generator skill ships with the repository");
    let held = registry.restricted_to(&generator.tools).names();
    assert!(
        held.contains(&"write_file".to_string()),
        "the generator produces the change and must be able to write; it holds {held:?}"
    );

    let evaluator = skills
        .get_all()
        .into_iter()
        .find(|s| s.id == "evaluator")
        .expect("the evaluator skill ships with the repository");
    let held = registry.restricted_to(&evaluator.tools).names();
    assert!(
        held.contains(&"run_command".to_string()),
        "the evaluator has to run what it judges; it holds {held:?}"
    );
    assert!(
        !held.contains(&"write_file".to_string()),
        "an evaluator that can write can author the thing it is judging; it holds {held:?}"
    );
}

#[tokio::test]
async fn a_tool_outside_the_boundary_has_nothing_behind_it() {
    let registry = production_registry();
    let declared = vec!["read_file".to_string()];
    let narrowed = registry.restricted_to(&declared);

    assert_eq!(narrowed.names(), vec!["read_file".to_string()]);

    // Offered definitions are only half a boundary; the other half is that a
    // call the model invents for something undeclared finds nothing to run.
    for name in ["write_file", "run_command", "http_request", "not_a_tool"] {
        let err = narrowed
            .execute(name, serde_json::json!({}))
            .await
            .expect_err("a call outside the declared set must not execute");
        let msg = err.to_string();
        assert!(
            msg.contains(name),
            "the refusal should name the tool that was reached for, got: {msg}"
        );
    }

    // The same call the role *is* allowed goes through the same machinery.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read.txt");
    std::fs::write(&path, "hello").unwrap();
    let narrowed = narrowed.with_sandbox_backend(Arc::new(ReadingSandbox));
    let out = narrowed
        .execute(
            "read_file",
            serde_json::json!({"path": path.to_str().unwrap()}),
        )
        .await
        .expect("a declared tool must still run on the narrowed registry");
    assert!(
        out.to_string().contains("hello"),
        "the narrowed registry lost the execution machinery, not just the tool list: {out}"
    );
}

/// Narrowing drops what it cannot resolve, so the drop has to be reportable:
/// this is the runtime half of the check above. The gate here reads the lists
/// as shipped in the repository, which says nothing about the lists a running
/// deployment loaded — a deployment carrying older skill files narrows every
/// role down and nothing in the repository can see it.
#[test]
fn the_names_narrowing_dropped_are_named_back() {
    let registry = production_registry();

    assert!(
        registry
            .missing_tools(&["read_file".to_string(), "run_command".to_string()])
            .is_empty(),
        "every name here resolves, so nothing was dropped"
    );

    assert_eq!(
        registry.missing_tools(&[
            "read_file".to_string(),
            "code".to_string(),
            "test".to_string(),
        ]),
        vec!["code".to_string(), "test".to_string()],
        "the report names the drops, in the order declared"
    );

    // The shape a deployment with stale skill files actually produces: a list
    // where every entry is unknown. The role ends up with no tools, and the
    // report has to say so rather than come back empty.
    let stale = vec!["code".to_string(), "test".to_string()];
    assert_eq!(registry.missing_tools(&stale).len(), stale.len());
    assert_eq!(
        registry.restricted_to(&stale).names(),
        Vec::<String>::new(),
        "an entirely unknown list leaves the role with nothing to call"
    );

    // A name repeated in the declaration is one drop, not two.
    assert_eq!(
        registry.missing_tools(&["code".to_string(), "code".to_string()]),
        vec!["code".to_string()]
    );
}
