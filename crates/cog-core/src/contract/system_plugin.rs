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

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

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
// Pin — the one identifier shared by the publish and the read side
// ---------------------------------------------------------------------------

/// Identity of a service pin: a thing one plugin publishes and another reads.
///
/// The identity is derived from the Rust type itself — `TypeId` for lookup, the
/// compiler's `type_name` for display.  Nothing is spelled by hand, so the two
/// sides cannot drift: whoever publishes `dyn LlmClient` and whoever reads it
/// necessarily name the same pin, because both derive it from that one type.
///
/// A pair of hand-maintained lists (`provides` next to a plugin's code,
/// `consumes` next to another's) cannot make that guarantee.  They agree with
/// each other and disagree with the runtime, and the disagreement is silent:
/// either a read is reported as unpublished forever while the publisher exists,
/// or a read is reported as satisfied and then panics.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Pin {
    id: TypeId,
    name: &'static str,
}

impl Pin {
    /// Pin of a value published through [`PluginContext::publish`].
    pub fn of<T: Any + Send + Sync + 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// Pin of a trait object published through [`PluginContext::publish_service`].
    ///
    /// Keyed on the [`Service`] wrapper that actually stores it, but named after
    /// the inner type, so a report reads `dyn LlmClient` rather than the storage
    /// wrapper around it.
    pub fn of_service<T: ?Sized + Send + Sync + 'static>() -> Self {
        Self {
            id: TypeId::of::<Service<T>>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// The compiler's name for the pinned type.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl std::fmt::Debug for Pin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

/// One party's read of a pin, as observed at runtime.
#[derive(Clone, Copy, Debug)]
pub struct PinConsumer {
    /// The plugin that read it; `None` when the binary root did.
    pub owner: Option<&'static str>,
    /// The read could not proceed without the pin (it went through
    /// [`PluginContext::require`]).  A missing publisher is then a wiring error
    /// rather than a designed degradation.
    pub required: bool,
    /// The read happened while `init` was running.  Init runs a topological
    /// layer at a time and its plugins concurrently, so an init-time read of a
    /// pin published by a plugin the reader does not depend on can race the
    /// publish; a read during `start` cannot.
    pub during_init: bool,
}

/// Everything the runtime observed about one pin.
#[derive(Clone, Debug)]
pub struct PinWiring {
    pub pin: Pin,
    /// Parties that published it, in publish order; `None` = the binary root.
    pub publishers: Vec<Option<&'static str>>,
    /// Parties that read it, in read order.
    pub consumers: Vec<PinConsumer>,
}

#[derive(Default)]
struct WiringLedger {
    published: HashMap<Pin, Vec<Option<&'static str>>>,
    demanded: HashMap<Pin, Vec<PinConsumer>>,
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
    inner: Arc<PluginContextInner>,
    /// The plugin whose `init`/`start` is running against this view, or `None`
    /// for the binary root.  Carried on the view rather than in the shared inner
    /// state because a topological layer initialises its plugins concurrently:
    /// a single shared "current plugin" slot would attribute every publish in
    /// the layer to whichever plugin happened to be polled last.
    owner: Option<&'static str>,
}

struct PluginContextInner {
    services: RwLock<HashMap<TypeId, Vec<Arc<dyn Any + Send + Sync>>>>,
    config: crate::Config,
    /// What was published and read, by whom.  Recorded on every access so the
    /// wiring graph is an observation rather than a claim.
    wiring: RwLock<WiringLedger>,
    /// Set by the runner for the duration of the init phase.
    during_init: AtomicBool,
}

impl Clone for PluginContext {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            owner: self.owner,
        }
    }
}

impl PluginContext {
    pub fn new(config: crate::Config) -> Self {
        Self {
            inner: Arc::new(PluginContextInner {
                services: RwLock::new(HashMap::new()),
                config,
                wiring: RwLock::new(WiringLedger::default()),
                during_init: AtomicBool::new(false),
            }),
            owner: None,
        }
    }

    /// A view of this context that attributes everything recorded through it to
    /// `owner`.  The runner hands one to each plugin's `init`/`start`, which is
    /// what lets the wiring graph name the publisher and the reader instead of
    /// reporting an anonymous pin.
    pub fn as_owner(&self, owner: &'static str) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            owner: Some(owner),
        }
    }

    /// Mark the point where the runner leaves `init` and enters `start`.  Reads
    /// recorded while it is set are the ones a missing dependency edge can make
    /// race their publisher.
    pub(crate) fn set_during_init(&self, during_init: bool) {
        self.inner.during_init.store(during_init, Ordering::Relaxed);
    }

    /// Publish a shared service so other plugins can look it up.
    /// Multiple plugins may publish the same type; all instances are retained.
    pub fn publish<T: Any + Send + Sync>(&self, service: Arc<T>) {
        let pin = Pin::of::<T>();
        self.inner
            .services
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(pin.id)
            .or_default()
            .push(service);
        self.record_publish(pin);
    }

    /// Consume a shared service published by another plugin.
    /// Returns the *first* published instance (backward-compatible).
    pub fn consume<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let pin = Pin::of::<T>();
        self.record_demand(pin, false);
        self.lookup::<T>(pin)
    }

    /// Consume a shared service the caller cannot run without.
    ///
    /// The read is recorded as required, so a missing publisher is answered here
    /// with a diagnostic that names the pin and the reader.  The alternative —
    /// `consume(..).expect("...")` — leaves the same failure as an opaque panic
    /// whose message is a local variable name.
    pub fn require<T: Any + Send + Sync>(&self) -> crate::SFResult<Arc<T>> {
        let pin = Pin::of::<T>();
        self.record_demand(pin, true);
        self.lookup::<T>(pin).ok_or_else(|| self.unpublished(pin))
    }

    /// Consume **all** shared services of a given type published by other plugins.
    pub fn consume_all<T: Any + Send + Sync>(&self) -> Vec<Arc<T>> {
        let pin = Pin::of::<T>();
        self.record_demand(pin, false);
        self.lookup_all::<T>(pin)
    }

    // ── Service<T> helpers (trait objects without per-crate Holders) ────────

    /// Publish a trait object (or any `Arc<T>`) via the universal [`Service`]
    /// wrapper.  Prefer this over `publish` when `T` is a trait object.
    pub fn publish_service<T: ?Sized + Send + Sync + 'static>(&self, service: Arc<T>) {
        let pin = Pin::of_service::<T>();
        self.inner
            .services
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(pin.id)
            .or_default()
            .push(Arc::new(Service(service)));
        self.record_publish(pin);
    }

    /// Publish an [`crate::Observable`] for metrics collection.
    ///
    /// The registry is keyed by static `TypeId`, so publishing a concrete
    /// `Arc<MyObservable>` via [`Self::publish_service`] is invisible to
    /// `consume_all_services::<dyn Observable>()`; always publish observables
    /// through this method so the coercion happens at the call site.
    pub fn publish_observable(&self, observable: Arc<dyn crate::Observable>) {
        self.publish_service::<dyn crate::Observable>(observable);
    }

    /// Consume a trait object published via [`Self::publish_service`].
    /// Returns `Arc<T>` directly for ergonomic use.
    pub fn consume_service<T: ?Sized + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let pin = Pin::of_service::<T>();
        self.record_demand(pin, false);
        self.lookup::<Service<T>>(pin).map(|s| s.0.clone())
    }

    /// Consume a trait object the caller cannot run without.  See [`Self::require`].
    pub fn require_service<T: ?Sized + Send + Sync + 'static>(&self) -> crate::SFResult<Arc<T>> {
        let pin = Pin::of_service::<T>();
        self.record_demand(pin, true);
        self.lookup::<Service<T>>(pin)
            .map(|s| s.0.clone())
            .ok_or_else(|| self.unpublished(pin))
    }

    /// Consume **all** trait objects of a given type published via
    /// [`Self::publish_service`].
    pub fn consume_all_services<T: ?Sized + Send + Sync + 'static>(&self) -> Vec<Arc<T>> {
        let pin = Pin::of_service::<T>();
        self.record_demand(pin, false);
        self.lookup_all::<Service<T>>(pin)
            .into_iter()
            .map(|s| s.0.clone())
            .collect()
    }

    /// Access the global configuration.
    pub fn config(&self) -> &crate::Config {
        &self.inner.config
    }

    // ── wiring ledger ───────────────────────────────────────────────────────

    fn lookup<T: Any + Send + Sync>(&self, pin: Pin) -> Option<Arc<T>> {
        let guard = self
            .inner
            .services
            .read()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .get(&pin.id)
            .and_then(|vec| vec.first())
            .and_then(|arc| arc.clone().downcast::<T>().ok())
    }

    fn lookup_all<T: Any + Send + Sync>(&self, pin: Pin) -> Vec<Arc<T>> {
        let guard = self
            .inner
            .services
            .read()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .get(&pin.id)
            .map(|vec| {
                vec.iter()
                    .filter_map(|arc| arc.clone().downcast::<T>().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn record_publish(&self, pin: Pin) {
        self.inner
            .wiring
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .published
            .entry(pin)
            .or_default()
            .push(self.owner);
    }

    fn record_demand(&self, pin: Pin, required: bool) {
        self.inner
            .wiring
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .demanded
            .entry(pin)
            .or_default()
            .push(PinConsumer {
                owner: self.owner,
                required,
                during_init: self.inner.during_init.load(Ordering::Relaxed),
            });
    }

    fn unpublished(&self, pin: Pin) -> crate::SFError {
        crate::SFError::Config(format!(
            "{} requires pin '{}' but nothing published it",
            self.owner.unwrap_or("<binary root>"),
            pin.name()
        ))
    }

    /// Snapshot of the wiring observed so far, one entry per pin, ordered by pin
    /// name so two runs of the same binary produce the same report.
    pub fn pin_wiring(&self) -> Vec<PinWiring> {
        let guard = self.inner.wiring.read().unwrap_or_else(|e| e.into_inner());
        let mut pins: Vec<Pin> = guard
            .published
            .keys()
            .chain(guard.demanded.keys())
            .copied()
            .collect();
        pins.sort_by_key(|p| p.name());
        pins.dedup();
        pins.into_iter()
            .map(|pin| PinWiring {
                pin,
                publishers: guard.published.get(&pin).cloned().unwrap_or_default(),
                consumers: guard.demanded.get(&pin).cloned().unwrap_or_default(),
            })
            .collect()
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
///
/// Which pins a plugin publishes and reads is deliberately **not** declared
/// here.  Such a list would be a second, hand-maintained copy of what the
/// plugin's code already says, and it can only ever be right by being kept
/// right.  The runtime records the real graph instead — see [`PinWiring`] and
/// [`PluginRunner::audit_pins`].
#[derive(Clone, Copy)]
pub struct PluginDescriptor {
    pub name: &'static str,
    /// Core dependencies — if missing, startup fails with a clear diagnostic.
    pub requires: &'static [&'static str],
    /// Optional dependencies — if missing, the plugin degrades gracefully.
    pub optional_requires: &'static [&'static str],
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
// PinAudit — mechanical verdict on the observed wiring
// ---------------------------------------------------------------------------

/// A read during `init` of a pin whose publisher the reader does not depend on.
#[derive(Clone, Debug)]
pub struct UnorderedInitRead {
    pub pin: Pin,
    pub reader: &'static str,
    pub publisher: &'static str,
}

/// Verdict on the pin wiring the runtime observed.
#[derive(Debug, Default)]
pub struct PinAudit {
    /// Pins that were read and never published.
    pub unsatisfied: Vec<(Pin, Vec<PinConsumer>)>,
    /// Pins that were published and never read.
    pub unconsumed: Vec<(Pin, Vec<Option<&'static str>>)>,
    /// Pins with more than one publisher.  `consume` returns the first, so
    /// which instance a reader gets depends on init order.
    pub multi_published: Vec<(Pin, Vec<Option<&'static str>>)>,
    /// Init-time reads that a missing `requires` edge left unordered.
    pub unordered_init_reads: Vec<UnorderedInitRead>,
    /// Distinct pins observed, and the totals behind them.
    pub pins: usize,
    pub publishes: usize,
    pub reads: usize,
}

impl PinAudit {
    /// Derive the verdict from an observed wiring snapshot and the dependency
    /// graph.  Pure: no logging, so it can be asserted on directly.
    pub fn evaluate(wiring: &[PinWiring], descriptors: &[PluginDescriptor]) -> Self {
        let mut audit = PinAudit {
            pins: wiring.len(),
            publishes: wiring.iter().map(|w| w.publishers.len()).sum(),
            reads: wiring.iter().map(|w| w.consumers.len()).sum(),
            ..Default::default()
        };

        for w in wiring {
            if w.publishers.is_empty() {
                if !w.consumers.is_empty() {
                    audit.unsatisfied.push((w.pin, w.consumers.clone()));
                }
                continue;
            }
            if w.publishers.len() > 1 {
                audit.multi_published.push((w.pin, w.publishers.clone()));
            }
            if w.consumers.is_empty() {
                audit.unconsumed.push((w.pin, w.publishers.clone()));
            }
            // Only a single, plugin-owned publisher can be ordered against: with
            // several, the pin does not identify one producer, and the binary
            // root publishes before any plugin runs.
            if w.publishers.len() != 1 {
                continue;
            }
            let Some(publisher) = w.publishers[0] else {
                continue;
            };
            for consumer in &w.consumers {
                let Some(reader) = consumer.owner else {
                    continue;
                };
                if !consumer.during_init || reader == publisher {
                    continue;
                }
                if !requires_closure(descriptors, reader).contains(publisher) {
                    audit.unordered_init_reads.push(UnorderedInitRead {
                        pin: w.pin,
                        reader,
                        publisher,
                    });
                }
            }
        }
        audit
    }

    /// Log the verdict.  Anomalies go where an operator sees them at default
    /// verbosity; the healthy part of the graph is only dumped at `debug`,
    /// because a fact is not a finding.
    pub fn report(&self) {
        for (pin, publishers) in &self.multi_published {
            tracing::warn!(
                "pin '{}' has {} publishers {:?}; readers get the first one, which depends on init order",
                pin.name(),
                publishers.len(),
                publishers
            );
        }
        for read in &self.unordered_init_reads {
            tracing::warn!(
                "plugin '{}' reads pin '{}' during init but does not require '{}'; same-layer plugins init in parallel, so the read can race the publish",
                read.reader,
                read.pin.name(),
                read.publisher
            );
        }
        for (pin, consumers) in &self.unsatisfied {
            let owners: Vec<&str> = consumers
                .iter()
                .map(|c| c.owner.unwrap_or("<binary root>"))
                .collect();
            tracing::info!(
                "pin '{}' was read by {:?} but never published; optional reads degrade, required ones fail startup",
                pin.name(),
                owners
            );
        }
        for (pin, publishers) in &self.unconsumed {
            tracing::info!(
                "pin '{}' was published by {:?} but never read in this process",
                pin.name(),
                publishers
            );
        }
        tracing::debug!(
            "pin wiring: {} pins, {} publishes, {} reads, {} anomalies",
            self.pins,
            self.publishes,
            self.reads,
            self.multi_published.len() + self.unordered_init_reads.len() + self.unsatisfied.len()
        );
    }

    /// Fail startup when a read that said it could not proceed without the pin
    /// found nothing to read.
    pub fn enforce(&self) -> crate::SFResult<()> {
        let mut errors = Vec::new();
        for (pin, consumers) in &self.unsatisfied {
            for consumer in consumers.iter().filter(|c| c.required) {
                errors.push(format!(
                    "{} requires pin '{}' but nothing published it",
                    consumer.owner.unwrap_or("<binary root>"),
                    pin.name()
                ));
            }
        }
        if errors.is_empty() {
            return Ok(());
        }
        Err(crate::SFError::Config(format!(
            "Pin wiring validation failed:\n  - {}",
            errors.join("\n  - ")
        )))
    }
}

/// Every plugin `reader` transitively depends on, by descriptor name.
fn requires_closure(descriptors: &[PluginDescriptor], reader: &str) -> HashSet<&'static str> {
    let edges: HashMap<&str, &[&'static str]> =
        descriptors.iter().map(|d| (d.name, d.requires)).collect();
    let mut seen: HashSet<&'static str> = HashSet::new();
    let mut queue: Vec<&str> = vec![reader];
    while let Some(name) = queue.pop() {
        for &dep in edges.get(name).copied().unwrap_or(&[]) {
            if seen.insert(dep) {
                queue.push(dep);
            }
        }
    }
    seen
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

        ctx.set_during_init(true);
        for (layer_idx, layer) in plan.layers.iter().enumerate() {
            if layer.len() == 1 {
                let idx = name_to_idx[layer[0]];
                if let Some(ref mut plugin) = self.plugins[idx] {
                    tracing::info!("init plugin: {} (layer {})", plugin.name(), layer_idx);
                    plugin.init(&ctx.as_owner(plugin.name())).await?;
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
                        let view = ctx.as_owner(plugin.name());
                        async move {
                            let result = plugin.init(&view).await;
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
        ctx.set_during_init(false);

        if let Some(ref report) = self.report {
            tracing::info!("\n{}", report);
        }
        Ok(())
    }

    /// Start every plugin in parallel, then run the pin-wiring audit.
    ///
    /// The audit belongs here rather than in the caller: it is the last point at
    /// which the whole graph has been observed, and a composition root that
    /// forgot to call it would silently lose the only mechanical check on the
    /// wiring.
    pub async fn start_all(&self, ctx: &PluginContext) -> crate::SFResult<()> {
        let futures = self.plugins.iter().filter_map(|opt| {
            let plugin = opt.as_ref()?;
            Some(async move {
                tracing::info!("start plugin: {}", plugin.name());
                plugin.start(&ctx.as_owner(plugin.name())).await
            })
        });
        futures::future::try_join_all(futures).await.map(|_| ())?;
        self.audit_pins(ctx)?;
        Ok(())
    }

    /// Compare the pin wiring the runtime observed against itself and report
    /// every asymmetry: a read nothing published, a pin nobody read, one pin
    /// with several publishers, an init-time read whose publisher the reader
    /// does not depend on.
    ///
    /// Only the first of those can fail startup, and only when the reader said
    /// it could not proceed without the pin.  A read that degrades, or a pin
    /// published for a consumer outside this process, is a fact worth logging
    /// and not a reason to refuse to run.
    pub fn audit_pins(&self, ctx: &PluginContext) -> crate::SFResult<PinAudit> {
        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &self.descriptors);
        audit.report();
        audit.enforce()?;
        Ok(audit)
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

#[cfg(test)]
mod pin_tests {
    use super::*;

    trait Alpha: Send + Sync {}
    struct AlphaImpl;
    impl Alpha for AlphaImpl {}

    /// What a fake plugin does during `init`.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Step {
        PublishAlpha,
        ReadAlpha,
    }

    struct FakePlugin {
        name: &'static str,
        steps: &'static [Step],
    }

    #[async_trait::async_trait]
    impl SystemPlugin for FakePlugin {
        fn name(&self) -> &'static str {
            self.name
        }

        async fn init(&mut self, ctx: &PluginContext) -> crate::SFResult<()> {
            for step in self.steps {
                match step {
                    Step::PublishAlpha => ctx.publish_service::<dyn Alpha>(Arc::new(AlphaImpl)),
                    Step::ReadAlpha => {
                        let _ = ctx.consume_service::<dyn Alpha>();
                    }
                }
            }
            Ok(())
        }

        async fn start(&self, _ctx: &PluginContext) -> crate::SFResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> crate::SFResult<()> {
            Ok(())
        }
    }

    fn publisher_factory() -> Box<dyn SystemPlugin> {
        Box::new(FakePlugin {
            name: "publisher",
            steps: &[Step::PublishAlpha],
        })
    }

    fn reader_factory() -> Box<dyn SystemPlugin> {
        Box::new(FakePlugin {
            name: "reader",
            steps: &[Step::ReadAlpha],
        })
    }

    fn ctx() -> PluginContext {
        PluginContext::new(crate::Config::default())
    }

    fn descriptor(name: &'static str, requires: &'static [&'static str]) -> PluginDescriptor {
        PluginDescriptor {
            name,
            requires,
            optional_requires: &[],
            factory: if name == "publisher" {
                publisher_factory
            } else {
                reader_factory
            },
        }
    }

    #[test]
    fn a_pin_is_derived_from_the_type_so_both_sides_name_the_same_one() {
        assert_eq!(
            Pin::of_service::<dyn Alpha>(),
            Pin::of_service::<dyn Alpha>()
        );
        assert_ne!(Pin::of_service::<dyn Alpha>(), Pin::of::<AlphaImpl>());
        assert!(Pin::of_service::<dyn Alpha>().name().ends_with("Alpha"));
        assert!(Pin::of_service::<AlphaImpl>().name().ends_with("AlphaImpl"));
    }

    #[test]
    fn the_read_side_finds_what_the_write_side_published() {
        let ctx = ctx();
        assert!(ctx.consume_service::<dyn Alpha>().is_none());
        ctx.publish_service::<dyn Alpha>(Arc::new(AlphaImpl));
        assert!(ctx.consume_service::<dyn Alpha>().is_some());
        assert!(ctx.require_service::<dyn Alpha>().is_ok());
    }

    #[test]
    fn a_required_read_that_finds_nothing_names_the_pin_and_the_reader() {
        let ctx = ctx();
        let err = ctx
            .as_owner("requirer")
            .require_service::<dyn Alpha>()
            .err()
            .expect("an unpublished pin cannot be required");
        let text = err.to_string();
        assert!(text.contains("requirer"), "{text}");
        assert!(text.contains("Alpha"), "{text}");
    }

    #[test]
    fn an_unsatisfied_optional_read_is_reported_but_does_not_fail_startup() {
        let ctx = ctx();
        let _ = ctx.as_owner("reader").consume_service::<dyn Alpha>();
        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &[]);
        assert_eq!(audit.unsatisfied.len(), 1);
        assert!(audit.enforce().is_ok());
    }

    #[test]
    fn an_unsatisfied_required_read_fails_startup() {
        let ctx = ctx();
        let _ = ctx.as_owner("requirer").require_service::<dyn Alpha>();
        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &[]);
        let text = audit.enforce().unwrap_err().to_string();
        assert!(text.contains("requirer"), "{text}");
        assert!(text.contains("Alpha"), "{text}");
    }

    #[test]
    fn a_pin_published_twice_and_read_nowhere_is_reported() {
        let ctx = ctx();
        ctx.as_owner("a")
            .publish_service::<dyn Alpha>(Arc::new(AlphaImpl));
        ctx.as_owner("b")
            .publish_service::<dyn Alpha>(Arc::new(AlphaImpl));
        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &[]);
        assert_eq!(audit.unconsumed.len(), 1);
        assert_eq!(audit.multi_published.len(), 1);
        assert!(audit.enforce().is_ok());
    }

    #[test]
    fn an_init_read_without_a_dependency_edge_is_reported() {
        let descriptors = [descriptor("publisher", &[]), descriptor("reader", &[])];
        let mut runner = PluginRunner::from_descriptors(&descriptors).expect("descriptors");
        let ctx = ctx();
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(runner.init_all(&ctx))
            .expect("init");

        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &descriptors);
        assert_eq!(audit.unordered_init_reads.len(), 1, "{audit:?}");
        assert_eq!(audit.unordered_init_reads[0].reader, "reader");
        assert_eq!(audit.unordered_init_reads[0].publisher, "publisher");
        assert!(audit.enforce().is_ok());
    }

    #[test]
    fn a_dependency_edge_on_the_publisher_settles_the_init_read() {
        let descriptors = [
            descriptor("publisher", &[]),
            descriptor("reader", &["publisher"]),
        ];
        let mut runner = PluginRunner::from_descriptors(&descriptors).expect("descriptors");
        let ctx = ctx();
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(runner.init_all(&ctx))
            .expect("init");

        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &descriptors);
        assert!(audit.unordered_init_reads.is_empty(), "{audit:?}");
        assert!(audit.enforce().is_ok());
    }

    #[test]
    fn an_indirect_dependency_settles_the_init_read() {
        let descriptors = [
            descriptor("publisher", &[]),
            descriptor("middle", &["publisher"]),
            descriptor("reader", &["middle"]),
        ];
        let mut runner = PluginRunner::from_descriptors(&descriptors).expect("descriptors");
        let ctx = ctx();
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(runner.init_all(&ctx))
            .expect("init");

        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &descriptors);
        assert!(audit.unordered_init_reads.is_empty(), "{audit:?}");
    }

    #[test]
    fn a_read_during_start_is_not_ordering_checked() {
        let descriptors = [descriptor("publisher", &[]), descriptor("reader", &[])];
        let mut runner = PluginRunner::from_descriptors(&descriptors).expect("descriptors");
        let ctx = ctx();
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(runner.init_all(&ctx))
            .expect("init");
        // Same pin, same pair of plugins, but the read happens once init is over,
        // so no ordering is required of it.
        ctx.set_during_init(false);
        let _ = ctx.as_owner("reader").consume_service::<dyn Alpha>();

        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &descriptors);
        assert_eq!(audit.unordered_init_reads.len(), 1, "{audit:?}");
        assert_eq!(
            audit.unordered_init_reads[0].reader, "reader",
            "only the init-time read may be flagged"
        );
    }

    #[test]
    fn the_root_reading_during_init_is_not_ordering_checked() {
        let ctx = ctx();
        ctx.set_during_init(true);
        ctx.as_owner("publisher")
            .publish_service::<dyn Alpha>(Arc::new(AlphaImpl));
        let _ = ctx.consume_service::<dyn Alpha>();

        let audit = PinAudit::evaluate(&ctx.pin_wiring(), &[]);
        assert!(audit.unordered_init_reads.is_empty(), "{audit:?}");
    }

    #[test]
    fn the_runner_audits_the_wiring_the_plugins_actually_produced() {
        let descriptors = [
            descriptor("publisher", &[]),
            descriptor("reader", &["publisher"]),
        ];
        let mut runner = PluginRunner::from_descriptors(&descriptors).expect("descriptors");
        let ctx = ctx();
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime
            .block_on(runner.init_all(&ctx))
            .expect("init of a well-wired set");
        runtime
            .block_on(runner.start_all(&ctx))
            .expect("start audits clean");

        let wiring = ctx.pin_wiring();
        assert_eq!(wiring.len(), 1, "{wiring:?}");
        assert_eq!(wiring[0].publishers, vec![Some("publisher")]);
        assert_eq!(wiring[0].consumers.len(), 1);
        assert_eq!(wiring[0].consumers[0].owner, Some("reader"));
    }
}
