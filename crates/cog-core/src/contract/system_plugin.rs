//!Internal system-plugin contracts — for first-party crate pluginisation.
//!Distinct from `plugin.rs` (external WASM plugins), this module defines
//!the contract for **first-party** components such as `cog-gateway`,
//!`cog-supervisor`, etc.  Each component implements [`SystemPlugin`] and
//!self-registers its capabilities into a [`PluginContext`].
//!`cogneva` (or any other binary root) is reduced to:
//!1. Load configuration
//!2. Instantiate plugins
//!3. Call `init()` → `start()` → wait for shutdown
//!
//!No single composition root knows the wiring details of every component.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Service<T> — universal trait-object wrapper for PluginContext
// ---------------------------------------------------------------------------

/// Wraps `Arc<T>` so that trait objects (`dyn Trait`) can be stored in
/// [`PluginContext`] without per-crate Holder structs.
/// `Service<T>` is always `Sized`, therefore it implements `Any` even when
/// `T` is `?Sized` (e.g. `dyn MessageBackend`).  This allows a single,
/// universal wrapper in `cog-core` to replace every ad-hoc Holder type.
pub struct Service<T: ?Sized + Send + Sync + 'static>(pub Arc<T>);

impl<T: ?Sized + Send + Sync + 'static> Service<T> {
    pub fn new(inner: Arc<T>) -> Self {
        Self(inner)
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for Service<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: ?Sized + Send + Sync + 'static> std::ops::Deref for Service<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized + Send + Sync + 'static> From<Arc<T>> for Service<T> {
    fn from(arc: Arc<T>) -> Self {
        Self(arc)
    }
}

impl<T: ?Sized + Send + Sync + 'static> std::fmt::Debug for Service<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// PluginContext — shared dependency lookup
// ---------------------------------------------------------------------------

/// Mutable context passed to every plugin during `init`.
/// Plugins **publish** capabilities (e.g. `dyn LlmClient`, `dyn SessionManager`)
/// and **consume** capabilities published by other plugins.
/// This eliminates direct crate-to-crate dependencies; plugins only depend on
/// `cog-core` traits.
pub struct PluginContext {
    services: std::sync::RwLock<HashMap<std::any::TypeId, Vec<Arc<dyn Any + Send + Sync>>>>,
    config: crate::Config,
}

impl PluginContext {
    pub fn new(config: crate::Config) -> Self {
        Self {
            services: std::sync::RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Publish a shared service so other plugins can look it up.
    /// Multiple plugins may publish the same type; all instances are retained.
    pub fn publish<T: Any + Send + Sync>(&self, service: Arc<T>) {
        let mut guard = self.services.write().unwrap();
        guard
            .entry(std::any::TypeId::of::<T>())
            .or_default()
            .push(service);
    }

    /// Consume a shared service published by another plugin.
    /// Returns the *first* published instance (backward-compatible).
    pub fn consume<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let guard = self.services.read().unwrap();
        guard
            .get(&std::any::TypeId::of::<T>())
            .and_then(|vec| vec.first())
            .and_then(|arc| arc.clone().downcast::<T>().ok())
    }

    /// Consume **all** shared services of a given type published by other plugins.
    pub fn consume_all<T: Any + Send + Sync>(&self) -> Vec<Arc<T>> {
        let guard = self.services.read().unwrap();
        guard
            .get(&std::any::TypeId::of::<T>())
            .map(|vec| {
                vec.iter()
                    .filter_map(|arc| arc.clone().downcast::<T>().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    // ── Service<T> helpers (trait objects without per-crate Holders) ────────

    /// Publish a trait object (or any `Arc<T>`) via the universal [`Service`]
    /// wrapper.  Prefer this over `publish` when `T` is a trait object.
    pub fn publish_service<T: ?Sized + Send + Sync + 'static>(&self, service: Arc<T>) {
        self.publish(Arc::new(Service(service)));
    }

    /// Consume a trait object published via [`publish_service`].
    /// Returns `Arc<T>` directly for ergonomic use.
    pub fn consume_service<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.consume::<Service<T>>().map(|s| s.0.clone())
    }

    /// Consume **all** trait objects of a given type published via [`publish_service`].
    pub fn consume_all_services<T: ?Sized + Send + Sync + 'static>(&self) -> Vec<Arc<T>> {
        self.consume_all::<Service<T>>()
            .into_iter()
            .map(|s| s.0.clone())
            .collect()
    }

    /// Access the global configuration.
    pub fn config(&self) -> &crate::Config {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// SystemPlugin trait
// ---------------------------------------------------------------------------

/// Contract for a first-party system component.
#[async_trait::async_trait]
pub trait SystemPlugin: Send + Sync {
    /// Human-readable plugin name (used for logging / diagnostics).
    fn name(&self) -> &'static str;

    /// Initialise the plugin.
    /// The plugin may **publish** services it provides and **consume**
    /// services it depends on.  Initialisation order is determined by the
    /// caller (usually `cogneva`).
    async fn init(&mut self, ctx: &PluginContext) -> crate::SFResult<()>;

    /// Start background work (e.g. HTTP server, supervisor loop).
    /// Called after *all* plugins have finished `init`.
    /// `ctx` is immutable; the plugin may **consume** services published by
    /// other plugins during their `init` phase.
    async fn start(&self, ctx: &PluginContext) -> crate::SFResult<()>;

    /// Graceful shutdown.
    /// Called when the process receives a shutdown signal.
    async fn shutdown(&self) -> crate::SFResult<()>;
}

// ---------------------------------------------------------------------------
// PluginDescriptor — static metadata for auto-discovery
// ---------------------------------------------------------------------------

/// Static descriptor for a system plugin.
/// Used by auto-discovery mechanisms (inventory push or build.rs pull)
/// so that the binary root never hard-codes plugin names or init order.
/// Specification of a consumed service type for static validation.
#[derive(Clone, Copy, Debug)]
pub struct ConsumeSpec {
    pub type_name: &'static str,
    pub required: bool,
}

#[derive(Clone, Copy)]
pub struct PluginDescriptor {
    pub name: &'static str,
    /// Core dependencies — if missing, startup fails with a clear diagnostic.
    pub requires: &'static [&'static str],
    /// Optional dependencies — if missing, the plugin degrades gracefully.
    pub optional_requires: &'static [&'static str],
    /// Service types this plugin publishes (output pins).
    pub provides: &'static [&'static str],
    /// Service types this plugin consumes (input pins).
    pub consumes: &'static [ConsumeSpec],
    pub factory: fn() -> Box<dyn SystemPlugin>,
}

// ---------------------------------------------------------------------------
// AssemblyReport — post-init topology summary
// ---------------------------------------------------------------------------

/// Human-readable summary of the plugin assembly topology.
#[derive(Debug)]
pub struct AssemblyReport {
    pub plugins_loaded: usize,
    pub init_layers: Vec<Vec<&'static str>>,
    pub strong_edges: usize,
    pub optional_edges: usize,
    pub missing_optional_deps: Vec<(&'static str, &'static str)>,
}

impl std::fmt::Display for AssemblyReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "[ASSEMBLY REPORT]")?;
        writeln!(f, "Plugins loaded: {}", self.plugins_loaded)?;
        writeln!(f, "Init order (topological layers):")?;
        for (i, layer) in self.init_layers.iter().enumerate() {
            writeln!(f, "  Layer {}: {}", i, layer.join(", "))?;
        }
        writeln!(
            f,
            "Dependency graph: {} strong edges, {} optional edges",
            self.strong_edges, self.optional_edges
        )?;
        if !self.missing_optional_deps.is_empty() {
            writeln!(f, "Missing optional deps (functional degradation):")?;
            for (plugin, dep) in &self.missing_optional_deps {
                writeln!(f, "  - {}: {} (disabled or not registered)", plugin, dep)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PluginRunner — thin orchestration layer
// ---------------------------------------------------------------------------

/// Owns a collection of [`SystemPlugin`]s and drives their lifecycle.
pub struct PluginRunner {
    plugins: Vec<Option<Box<dyn SystemPlugin>>>,
    descriptors: Vec<PluginDescriptor>,
    report: Option<AssemblyReport>,
}

impl Default for PluginRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRunner {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            descriptors: Vec::new(),
            report: None,
        }
    }

    /// Build a runner from descriptors, topologically sorted by dependencies.
    /// Also validates the dependency graph and generates an [`AssemblyReport`].
    pub fn from_descriptors(descriptors: &[PluginDescriptor]) -> crate::SFResult<Self> {
        Self::validate_dependency_graph(descriptors)?;
        Self::validate_pin_connectivity(descriptors)?;
        let sorted = Self::topological_sort(descriptors)?;
        let report = Self::build_report(&sorted);
        let sorted_descriptors: Vec<_> = sorted.iter().map(|&d| *d).collect();
        let plugins: Vec<_> = sorted.into_iter().map(|d| Some((d.factory)())).collect();
        Ok(Self {
            plugins,
            descriptors: sorted_descriptors,
            report: Some(report),
        })
    }

    pub fn register(&mut self, plugin: Box<dyn SystemPlugin>) {
        self.plugins.push(Some(plugin));
    }

    /// Retain only plugins whose names pass the predicate.
    /// Used for config-driven enable/disable filtering.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&str) -> bool,
    {
        self.plugins
            .retain(|p| p.as_ref().map(|plugin| f(plugin.name())).unwrap_or(false));
        self.descriptors.retain(|d| f(d.name));
        self.report = None; // invalidated by filtering
    }

    /// Validate that every remaining plugin's `requires` are still present
    /// after filtering (e.g. `enabled_plugins` / `disabled_plugins`).
    pub fn validate_after_filter(&self) -> crate::SFResult<()> {
        let names: std::collections::HashSet<&str> =
            self.descriptors.iter().map(|d| d.name).collect();
        let mut errors = Vec::new();
        for desc in &self.descriptors {
            for &req in desc.requires {
                if !names.contains(req) {
                    errors.push(format!(
                        "{} requires '{}' but '{}' is not registered (disabled or missing)",
                        desc.name, req, req
                    ));
                }
            }
        }
        if !errors.is_empty() {
            return Err(crate::SFError::Config(format!(
                "Plugin dependency validation failed:\n  - {}",
                errors.join("\n  - ")
            )));
        }
        Ok(())
    }

    /// Initialise every plugin in topological layers.
    /// Plugins within the same layer have no dependencies on each other and
    /// are initialised in parallel via [`futures::future::join_all`].
    pub async fn init_all(&mut self, ctx: &PluginContext) -> crate::SFResult<()> {
        let nodes: Vec<_> = self
            .descriptors
            .iter()
            .map(|d| dag_scheduler::Node {
                id: d.name,
                deps: d.requires.to_vec(),
            })
            .collect();
        let dag = dag_scheduler::Dag::new(nodes);
        let plan = dag.compute_plan().map_err(crate::SFError::Config)?;

        let name_to_idx: std::collections::HashMap<&str, usize> = self
            .descriptors
            .iter()
            .enumerate()
            .map(|(i, d)| (d.name, i))
            .collect();

        for (layer_idx, layer) in plan.layers.iter().enumerate() {
            if layer.len() == 1 {
                let idx = name_to_idx[layer[0]];
                if let Some(ref mut plugin) = self.plugins[idx] {
                    tracing::info!("init plugin: {} (layer {})", plugin.name(), layer_idx);
                    plugin.init(ctx).await?;
                }
            } else {
                let mut taken = Vec::new();
                for &name in layer {
                    let idx = name_to_idx[name];
                    if let Some(plugin) = self.plugins[idx].take() {
                        taken.push((idx, plugin));
                    }
                }

                let results: Vec<_> = taken
                    .into_iter()
                    .map(|(idx, mut plugin)| {
                        tracing::info!(
                            "init plugin: {} (layer {}, parallel)",
                            plugin.name(),
                            layer_idx
                        );
                        async move {
                            let result = plugin.init(ctx).await;
                            (idx, plugin, result)
                        }
                    })
                    .collect();

                for (idx, plugin, result) in futures::future::join_all(results).await {
                    self.plugins[idx] = Some(plugin);
                    result?;
                }
            }
        }

        if let Some(ref report) = self.report {
            tracing::info!("\n{}", report);
        }
        Ok(())
    }

    /// Start every plugin in parallel.
    pub async fn start_all(&self, ctx: &PluginContext) -> crate::SFResult<()> {
        let futures = self.plugins.iter().filter_map(|opt| {
            let plugin = opt.as_ref()?;
            Some(async move {
                tracing::info!("start plugin: {}", plugin.name());
                plugin.start(ctx).await
            })
        });
        futures::future::try_join_all(futures).await.map(|_| ())
    }

    /// Shut down every plugin in *reverse* order.
    pub async fn shutdown_all(&self) -> crate::SFResult<()> {
        for plugin in self.plugins.iter().rev().flatten() {
            tracing::info!("shutdown plugin: {}", plugin.name());
            if let Err(e) = plugin.shutdown().await {
                tracing::warn!("plugin {} shutdown error: {}", plugin.name(), e);
            }
        }
        Ok(())
    }

    /// Validate that every `requires` and `optional_requires` target exists
    /// in the descriptor set.  Missing `requires` are treated as errors;
    /// missing `optional_requires` are recorded as warnings.
    fn validate_dependency_graph(descriptors: &[PluginDescriptor]) -> crate::SFResult<()> {
        let names: std::collections::HashSet<&str> = descriptors.iter().map(|d| d.name).collect();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        for desc in descriptors {
            for &req in desc.requires {
                if !names.contains(req) {
                    errors.push(format!(
                        "{} requires '{}' but '{}' is not in the descriptor set",
                        desc.name, req, req
                    ));
                }
            }
            for &opt in desc.optional_requires {
                if !names.contains(opt) {
                    warnings.push(format!(
                        "{} optionally requires '{}' but '{}' is not in the descriptor set",
                        desc.name, opt, opt
                    ));
                }
            }
        }
        for w in &warnings {
            tracing::warn!("{}", w);
        }
        if !errors.is_empty() {
            return Err(crate::SFError::Config(format!(
                "Plugin dependency graph validation failed:\n  - {}",
                errors.join("\n  - ")
            )));
        }
        Ok(())
    }

    /// Validate that every consumed type (pin) has at least one publisher.
    fn validate_pin_connectivity(descriptors: &[PluginDescriptor]) -> crate::SFResult<()> {
        let mut published_by: std::collections::HashMap<&str, Vec<&str>> =
            std::collections::HashMap::new();
        for desc in descriptors {
            for &ty in desc.provides {
                published_by.entry(ty).or_default().push(desc.name);
            }
        }

        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        for desc in descriptors {
            for consume in desc.consumes {
                match published_by.get(consume.type_name) {
                    None if consume.required => errors.push(format!(
                        "{} consumes '{}' but no plugin publishes it",
                        desc.name, consume.type_name
                    )),
                    None => warnings.push(format!(
                        "{} optionally consumes '{}' but no plugin publishes it",
                        desc.name, consume.type_name
                    )),
                    Some(provs) if provs.len() > 1 => warnings.push(format!(
                        "{} consumes '{}' published by multiple plugins: {:?}",
                        desc.name, consume.type_name, provs
                    )),
                    _ => {}
                }
            }
        }

        for w in &warnings {
            tracing::warn!("{}", w);
        }
        if !errors.is_empty() {
            return Err(crate::SFError::Config(format!(
                "Pin connectivity validation failed:\n  - {}",
                errors.join("\n  - ")
            )));
        }
        Ok(())
    }

    /// Topological sort of descriptors by their `requires`.
    /// Dependencies that are **not** present in the descriptor set are treated
    /// as already satisfied (e.g. core infrastructure provided by the binary
    /// root before plugin init).
    fn topological_sort(
        descriptors: &[PluginDescriptor],
    ) -> crate::SFResult<Vec<&PluginDescriptor>> {
        let name_to_idx: HashMap<&str, usize> = descriptors
            .iter()
            .enumerate()
            .map(|(i, d)| (d.name, i))
            .collect();

        let mut in_degree = vec![0usize; descriptors.len()];
        let mut adj: Vec<Vec<usize>> = vec![vec![]; descriptors.len()];

        for (idx, desc) in descriptors.iter().enumerate() {
            for &dep in desc.requires {
                if let Some(&dep_idx) = name_to_idx.get(dep) {
                    // dep -> desc edge
                    adj[dep_idx].push(idx);
                    in_degree[idx] += 1;
                }
                // else: dependency not in this set, treated as satisfied
            }
        }

        let mut queue: VecDeque<usize> = in_degree
            .iter()
            .enumerate()
            .filter_map(|(i, &deg)| if deg == 0 { Some(i) } else { None })
            .collect();

        let mut sorted = Vec::with_capacity(descriptors.len());
        while let Some(idx) = queue.pop_front() {
            sorted.push(&descriptors[idx]);
            for &next in &adj[idx] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push_back(next);
                }
            }
        }

        if sorted.len() != descriptors.len() {
            let remaining: Vec<&str> = descriptors
                .iter()
                .enumerate()
                .filter_map(|(i, d)| if in_degree[i] > 0 { Some(d.name) } else { None })
                .collect();
            return Err(crate::SFError::Config(format!(
                "Plugin dependency cycle detected among: {:?}",
                remaining
            )));
        }

        Ok(sorted)
    }

    /// Build an [`AssemblyReport`] from topologically sorted descriptors.
    fn build_report(sorted: &[&PluginDescriptor]) -> AssemblyReport {
        let name_to_idx: HashMap<&str, usize> = sorted
            .iter()
            .enumerate()
            .map(|(i, d)| (d.name, i))
            .collect();

        let mut strong_edges = 0usize;
        let mut optional_edges = 0usize;
        let mut missing_optional_deps = Vec::new();
        let names_in_set: std::collections::HashSet<&str> = sorted.iter().map(|d| d.name).collect();

        for desc in sorted {
            strong_edges += desc.requires.len();
            optional_edges += desc.optional_requires.len();
            for &opt in desc.optional_requires {
                if !names_in_set.contains(opt) {
                    missing_optional_deps.push((desc.name, opt));
                }
            }
        }

        // Compute layers by BFS distance from source nodes
        let mut distance: HashMap<&str, usize> = HashMap::new();
        for desc in sorted {
            let parents_in_set: Vec<usize> = desc
                .requires
                .iter()
                .filter_map(|&req| name_to_idx.get(req).copied())
                .collect();
            let dist = if parents_in_set.is_empty() {
                0
            } else {
                parents_in_set
                    .iter()
                    .map(|&idx| *distance.get(sorted[idx].name).unwrap_or(&0))
                    .max()
                    .unwrap_or(0)
                    + 1
            };
            distance.insert(desc.name, dist);
        }

        let max_layer = distance.values().copied().max().unwrap_or(0);
        let mut layers: Vec<Vec<&'static str>> = vec![Vec::new(); max_layer + 1];
        for desc in sorted {
            let layer = *distance.get(desc.name).unwrap_or(&0);
            layers[layer].push(desc.name);
        }

        AssemblyReport {
            plugins_loaded: sorted.len(),
            init_layers: layers,
            strong_edges,
            optional_edges,
            missing_optional_deps,
        }
    }
}

mod dag_scheduler {
    //! Generic DAG scheduler — topological sort + layer grouping.
    //! Used by [`PluginRunner`] to derive parallelisable init layers.
    //! Zero business logic; pure graph algorithm.

    use std::collections::{HashMap, VecDeque};

    /// A node in the DAG.
    #[derive(Debug, Clone)]
    pub struct Node<T: Clone + Eq + std::hash::Hash> {
        pub id: T,
        /// Dependencies that must complete **before** this node.
        pub deps: Vec<T>,
    }

    /// Execution plan produced by [`Dag::compute_plan`].
    #[derive(Debug, Clone, PartialEq)]
    pub struct ExecutionPlan<T: Clone + Eq + std::hash::Hash> {
        /// Layers of node IDs.  Nodes inside a layer have no dependencies on each
        /// other and may be executed in parallel.
        pub layers: Vec<Vec<T>>,
        /// Total topological order (flattened layers).  Backward-compatible with
        /// serial execution.
        pub linear: Vec<T>,
    }

    /// Generic directed-acyclic-graph scheduler.
    #[derive(Debug, Clone)]
    pub struct Dag<T: Clone + Eq + std::hash::Hash> {
        nodes: Vec<Node<T>>,
    }

    impl<T: Clone + Eq + std::hash::Hash + std::fmt::Debug> Dag<T> {
        /// Build a DAG from a list of nodes.
        pub fn new(nodes: Vec<Node<T>>) -> Self {
            Self { nodes }
        }

        /// Compute both layered and linear execution plans.
        /// # Errors
        /// Returns `Err` when a cycle is detected.
        pub fn compute_plan(&self) -> Result<ExecutionPlan<T>, String> {
            let id_to_idx: HashMap<&T, usize> = self
                .nodes
                .iter()
                .enumerate()
                .map(|(i, n)| (&n.id, i))
                .collect();

            let mut in_degree = vec![0usize; self.nodes.len()];
            let mut adj: Vec<Vec<usize>> = vec![vec![]; self.nodes.len()];

            for (idx, node) in self.nodes.iter().enumerate() {
                for dep in &node.deps {
                    if let Some(&dep_idx) = id_to_idx.get(dep) {
                        adj[dep_idx].push(idx);
                        in_degree[idx] += 1;
                    }
                    // Dependencies not in the node set are treated as already
                    // satisfied (e.g. pre-inserted services).
                }
            }

            let mut queue: VecDeque<usize> = in_degree
                .iter()
                .enumerate()
                .filter_map(|(i, &deg)| if deg == 0 { Some(i) } else { None })
                .collect();

            let mut layers: Vec<Vec<T>> = Vec::new();
            let mut linear: Vec<T> = Vec::with_capacity(self.nodes.len());

            while !queue.is_empty() {
                // Every node currently in the queue has in-degree 0, therefore
                // they form an independent execution layer.
                let layer_size = queue.len();
                let mut layer: Vec<T> = Vec::with_capacity(layer_size);

                for _ in 0..layer_size {
                    let idx = queue.pop_front().unwrap();
                    let node = &self.nodes[idx];
                    layer.push(node.id.clone());
                    linear.push(node.id.clone());

                    for &next in &adj[idx] {
                        in_degree[next] -= 1;
                        if in_degree[next] == 0 {
                            queue.push_back(next);
                        }
                    }
                }
                layers.push(layer);
            }

            if linear.len() != self.nodes.len() {
                let remaining: Vec<String> = self
                    .nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(i, n)| {
                        if in_degree[i] > 0 {
                            Some(format!("{:?}", n.id))
                        } else {
                            None
                        }
                    })
                    .collect();
                return Err(format!(
                    "DAG cycle detected among: {}",
                    remaining.join(", ")
                ));
            }

            Ok(ExecutionPlan { layers, linear })
        }

        /// Convenience: return only the linear topological order.
        #[allow(dead_code)]
        pub fn topological_order(&self) -> Result<Vec<T>, String> {
            self.compute_plan().map(|p| p.linear)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn node(id: &str, deps: &[&str]) -> Node<String> {
            Node {
                id: id.to_string(),
                deps: deps.iter().map(|s| s.to_string()).collect(),
            }
        }

        #[test]
        fn test_empty() {
            let dag = Dag::<String>::new(vec![]);
            let plan = dag.compute_plan().unwrap();
            assert!(plan.layers.is_empty());
            assert!(plan.linear.is_empty());
        }

        #[test]
        fn test_single_node() {
            let dag = Dag::new(vec![node("a", &[])]);
            let plan = dag.compute_plan().unwrap();
            assert_eq!(plan.layers, vec![vec!["a"]]);
            assert_eq!(plan.linear, vec!["a"]);
        }

        #[test]
        fn test_chain() {
            // a -> b -> c
            let dag = Dag::new(vec![node("a", &[]), node("b", &["a"]), node("c", &["b"])]);
            let plan = dag.compute_plan().unwrap();
            assert_eq!(plan.layers, vec![vec!["a"], vec!["b"], vec!["c"]]);
            assert_eq!(plan.linear, vec!["a", "b", "c"]);
        }

        #[test]
        fn test_diamond() {
            //   a
            //  / \
            // b   c
            //  \ /
            //   d
            let dag = Dag::new(vec![
                node("a", &[]),
                node("b", &["a"]),
                node("c", &["a"]),
                node("d", &["b", "c"]),
            ]);
            let plan = dag.compute_plan().unwrap();
            assert_eq!(plan.layers, vec![vec!["a"], vec!["b", "c"], vec!["d"]]);
            assert_eq!(plan.linear, vec!["a", "b", "c", "d"]);
        }

        #[test]
        fn test_parallel_sources() {
            let dag = Dag::new(vec![
                node("x", &[]),
                node("y", &[]),
                node("z", &[]),
                node("w", &["x", "y", "z"]),
            ]);
            let plan = dag.compute_plan().unwrap();
            assert_eq!(plan.layers, vec![vec!["x", "y", "z"], vec!["w"]]);
        }

        #[test]
        fn test_cycle_detected() {
            // a -> b -> c -> a
            let dag = Dag::new(vec![
                node("a", &["c"]),
                node("b", &["a"]),
                node("c", &["b"]),
            ]);
            assert!(dag.compute_plan().is_err());
        }

        #[test]
        fn test_partial_deps_outside_set() {
            // "b" depends on "ext" which is not in the node set
            let dag = Dag::new(vec![node("a", &[]), node("b", &["ext"])]);
            let plan = dag.compute_plan().unwrap();
            // Both become layer 0 because "ext" is treated as satisfied
            assert_eq!(plan.layers, vec![vec!["a", "b"]]);
        }
    }
}
