//! The host's memory, decomposed by the slices the kernel already keeps it in.
//!
//! This platform shares a machine with whatever else runs on it, and the
//! question an admission decision needs answered is not "how much is free" but
//! "how much of it is ours". A bound derived from host-global readings cannot
//! tell a host that is busy for reasons of its own from one this platform has
//! filled, so the readings that feed it have to be attributed to a part of the
//! host rather than to the whole of it.
//!
//! Nothing already deployed answers it. The node exporter's cgroups collector is
//! written against cgroup v1: run on a v2 host it publishes a `cgroups` and an
//! `enabled` cell per subsystem and no per-slice bytes at all. The container
//! advisor walks the subtree it is rooted at, which is not the host's own
//! slices. So the decomposition is produced here, by a process that is handed
//! exactly one thing: a read-only bind of the host's cgroup v2 root.
//!
//! Read-only is the whole privilege. `memory.stat` is world-readable, so the
//! process needs no capability, no host PID namespace and no device -- it opens
//! `<slice>/memory.stat` and reports numbers the kernel had already accounted.
//! `anon` is memory a process is holding and `file` is page cache; a slice's
//! `memory.current` is those two plus a kernel remainder, which is why the two
//! are published separately instead of one total that hides which of them moved.
//!
//! Two things make the reading usable on a cluster rather than on one machine.
//!
//! Every sample carries the node it was taken on, as a label this process
//! stamps rather than one the scraper infers. The scraper's own label for a
//! DaemonSet target is the pod's address, which changes when the pod is
//! recreated and does not match the node name the node exporter is labelled
//! with -- so without this label the same series from N nodes is N samples with
//! identical labels, and nothing can divide one node's host-side memory by that
//! node's total.
//!
//! And a node reporting *nothing* is a different fact from a node reporting
//! zero, which is why the presence series exists: a DaemonSet that fails to
//! start on one node does not produce a zero, it produces no sample, and those
//! two look the same on a graph. `node_info` is the series whose presence says
//! "this node's ledger ran", and `slices_readable` says how much of the host
//! tree it could actually see -- a cgroup v1 host has the mount point and none
//! of the slices, and reporting that as three zeroes would state a reading that
//! no one took.
//!
//! A slice whose `memory.stat` cannot be read contributes no bytes sample rather
//! than a zero: "the slice holds nothing" and "the mount does not show that
//! slice" are different facts. A slice that reads fine contributes both cells
//! even when both are zero -- a zero is a measurement, and a producer that
//! skipped it would be indistinguishable from one that never ran.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Anonymous memory held under one host slice.
pub const HOST_SLICE_ANON_BYTES_METRIC: &str = "cogneva_host_slice_anon_bytes";

/// Page cache and other file-backed memory held under one host slice.
pub const HOST_SLICE_FILE_BYTES_METRIC: &str = "cogneva_host_slice_file_bytes";

/// One series per node whose ledger ran, always 1.
pub const HOST_LEDGER_NODE_INFO_METRIC: &str = "cogneva_host_ledger_node_info";

/// How many of [`SLICES`] this node's ledger could actually read.
pub const HOST_LEDGER_SLICES_READABLE_METRIC: &str = "cogneva_host_ledger_slices_readable";

/// Name of the rule that reads the anon gauge, so the pairing can be asserted.
pub const HOST_SLICE_ANON_FLOOR_RULE: &str = "host_slice_anon_floor_unmeasured";

/// Name of the rule that states a node whose ledger is missing.
pub const HOST_LEDGER_NODE_UNREPORTED_RULE: &str = "host_ledger_node_unreported";

/// Name of the rule that states a node whose cgroup tree has no slices.
pub const HOST_LEDGER_SLICES_INVISIBLE_RULE: &str = "host_ledger_slices_invisible";

/// The floor the anon rule compares `user.slice` against, in bytes.
///
/// Re-derived from the series now that it exists, against the rule's own
/// reading -- `max by (node) (min_over_time(...[15m]))`, so the number this
/// floor is compared with is the fifteen-minute *minimum*, not the instant.
/// Over the first week of it running, that minimum peaked at 4.61 GB on this
/// host, putting this floor just under twice (1.87x) above the worst the
/// window has produced, and the rule did not fire once.
///
/// The raw series is a different number and the window is the whole reason:
/// it peaked at 12.84 GB in the same week, with 31 of 10080 scrape-minutes
/// over this floor. Those minutes are ordinary build work rather than a host
/// in trouble, so a floor on the instant would have to sit above 12.84 GB to
/// stay quiet -- and a bound nothing reaches says nothing about anything.
/// What this floor is set to catch is the host being *held* under pressure,
/// which is why the derived number comes from the minimum and not the peak.
pub const HOST_SLICE_ANON_FLOOR_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Environment variable naming the node this process is running on.
///
/// Declared in `cog_core::env`: the process census on the node reads the same
/// name, and a row whose node is empty reads as a reading from nowhere rather
/// than as a renamed variable.
pub const NODE_ENV: &str = cog_core::env::NODE_NAME;

/// Environment variable naming the root of the host's cgroup v2 tree, as this
/// process sees it.
pub const ROOT_ENV: &str = "COGNEVA_HOST_CGROUP_ROOT";

/// The mount point the deployment uses when it names none.
pub const DEFAULT_ROOT: &str = "/host/cgroup";

/// The slices this reading decomposes a host by.
///
/// These three partition a cgroup v2 host: what the kernel charges to a user
/// session, to system services (the container runtime among them), and to pods.
/// A host missing one of them is reported as missing rather than as empty.
pub const SLICES: [&str; 3] = ["user.slice", "system.slice", "kubepods.slice"];

/// The directories a logical slice's bytes may live under, in the order tried.
///
/// A slice's logical name is the fact this reading states; the path is a
/// property of how the host's runtime spells its cgroup tree. `kubepods.slice`
/// is the systemd spelling, and a node whose kubelet runs the cgroupfs driver
/// puts pods under `kubepods` -- no `.slice` -- while systemd is still init, so
/// the other two names are the same either way. Following the alternative keeps
/// the coverage reading about "could this process see the host's tree" rather
/// than about which driver a node happens to configure: a cgroupfs node is not
/// a fault, and a reading that called it one would fire forever on a healthy
/// host, which is how a bound stops being read.
pub fn slice_dirs(slice: &str) -> &'static [&'static str] {
    match slice {
        "user.slice" => &["user.slice"],
        "system.slice" => &["system.slice"],
        "kubepods.slice" => &["kubepods.slice", "kubepods"],
        // An unknown slice has no known path: reporting it as absent is the
        // only honest answer, since guessing one would publish a directory
        // nobody named.
        _ => &[],
    }
}

/// Reads one host's cgroup v2 tree.
pub struct HostLedgerObservable {
    root: PathBuf,
    node: String,
}

impl HostLedgerObservable {
    /// `node` is the name every sample is stamped with. An empty name is
    /// published as an empty label rather than dropped: the label's absence and
    /// an empty value group the same way, and a series that loses its label
    /// silently is harder to see than one whose label is blank.
    pub fn new(root: impl Into<PathBuf>, node: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            node: node.into(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// `anon` and `file`, in bytes, from one slice's `memory.stat`.
    ///
    /// A slice's logical name is what the reading is about; which directory
    /// holds it is the host's business, so every spelling [`slice_dirs`] allows
    /// is tried in turn.
    ///
    /// `None` when none of them has a readable `memory.stat`, or when the file
    /// exists without both keys -- a partial reading is not published as a whole
    /// one, because a consumer cannot tell which half is real.
    pub fn slice_bytes(&self, slice: &str) -> Option<(f64, f64)> {
        slice_dirs(slice).iter().find_map(|dir| self.read_stat(dir))
    }

    /// The two cells out of one directory's `memory.stat`.
    fn read_stat(&self, dir: &str) -> Option<(f64, f64)> {
        let text = std::fs::read_to_string(self.root.join(dir).join("memory.stat")).ok()?;
        let (mut anon, mut file) = (None, None);
        for line in text.lines() {
            // `anon_thp` and `file_dirty` are different keys, not different
            // spellings of these two, so the split on whitespace is exact.
            let mut fields = line.split_whitespace();
            let Some(key) = fields.next() else { continue };
            let value = match key {
                "anon" => &mut anon,
                "file" => &mut file,
                _ => continue,
            };
            *value = fields.next().and_then(|raw| raw.parse::<u64>().ok());
        }
        Some((anon? as f64, file? as f64))
    }

    /// How many of [`SLICES`] this process can read right now.
    pub fn readable_slices(&self) -> usize {
        SLICES
            .into_iter()
            .filter(|slice| self.slice_bytes(slice).is_some())
            .count()
    }
}

#[async_trait]
impl Observable for HostLedgerObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let node = self.node.as_str();
        let mut out = Vec::new();
        let mut readable = 0usize;
        for slice in SLICES {
            let Some((anon, file)) = self.slice_bytes(slice) else {
                tracing::warn!(
                    slice,
                    node,
                    root = %self.root.display(),
                    "host slice has no readable memory.stat; reporting it as absent, not as empty"
                );
                continue;
            };
            readable += 1;
            out.push(
                RawMetric::new(HOST_SLICE_ANON_BYTES_METRIC, anon)
                    .with_label(crate::LABEL_SLICE, slice)
                    .with_label(crate::LABEL_NODE, node),
            );
            out.push(
                RawMetric::new(HOST_SLICE_FILE_BYTES_METRIC, file)
                    .with_label(crate::LABEL_SLICE, slice)
                    .with_label(crate::LABEL_NODE, node),
            );
        }
        // Published whether or not anything was readable: this is the series
        // whose presence says the node's ledger is running, and a node that is
        // silent has to be distinguishable from a node reporting zero.
        out.push(
            RawMetric::new(HOST_LEDGER_NODE_INFO_METRIC, 1.0).with_label(crate::LABEL_NODE, node),
        );
        out.push(
            RawMetric::new(HOST_LEDGER_SLICES_READABLE_METRIC, readable as f64)
                .with_label(crate::LABEL_NODE, node),
        );
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The reading is the same for every dimension: it describes the host the
    /// process is on, and which task asked does not change the host.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::chart_rule;
    use std::collections::BTreeMap;

    const NODE: &str = "node-a";

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-host-ledger-{}-{}", name, std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_slice(root: &Path, slice: &str, stat: &str) {
        let dir = root.join(slice);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.stat"), stat).unwrap();
    }

    fn observable(root: &Path) -> HostLedgerObservable {
        HostLedgerObservable::new(root, NODE)
    }

    /// Readings keyed by (name, slice); the host-wide series have no slice.
    async fn collect(root: &Path) -> BTreeMap<(String, String), f64> {
        let readings = observable(root).collect_metrics("").await.unwrap();
        readings
            .into_iter()
            .map(|m| {
                (
                    (
                        m.name,
                        m.labels
                            .get(crate::LABEL_SLICE)
                            .cloned()
                            .unwrap_or_default(),
                    ),
                    m.value,
                )
            })
            .collect()
    }

    fn cell(name: &str, slice: &str) -> (String, String) {
        (name.to_string(), slice.to_string())
    }

    /// A zero is a reading. An idle slice is the ordinary state of `user.slice`
    /// on a fresh node, and a producer that dropped it would leave the same
    /// empty scrape behind as a producer that never ran -- which is the one
    /// distinction the series exists to make.
    #[tokio::test]
    async fn an_idle_slice_still_reports_zero_rather_than_nothing() {
        let root = scratch("idle");
        write_slice(
            &root,
            "user.slice",
            "anon 0\nfile 0\nanon_thp 0\nfile_dirty 0\n",
        );

        let readings = collect(&root).await;
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "user.slice")),
            Some(&0.0),
            "an idle slice's anon was not published: {readings:?}"
        );
        assert_eq!(
            readings.get(&cell(HOST_SLICE_FILE_BYTES_METRIC, "user.slice")),
            Some(&0.0),
            "an idle slice's file was not published: {readings:?}"
        );
    }

    /// The two cells are different quantities, and `anon_thp` / `file_dirty`
    /// are different keys: a reader that answered all of them with one number
    /// would make the decomposition useless.
    #[tokio::test]
    async fn the_two_cells_are_read_from_their_own_keys() {
        let root = scratch("keys");
        write_slice(
            &root,
            "kubepods.slice",
            "anon 111\nfile 222\nanon_thp 333\nfile_dirty 444\nfile_mapped 555\n",
        );

        let readings = collect(&root).await;
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "kubepods.slice")),
            Some(&111.0)
        );
        assert_eq!(
            readings.get(&cell(HOST_SLICE_FILE_BYTES_METRIC, "kubepods.slice")),
            Some(&222.0)
        );
        // Two cells for the slice plus the two host-wide series.
        assert_eq!(
            readings.len(),
            4,
            "extra cells came from a neighbouring key"
        );
    }

    /// Every slice is labelled with its own name, and a slice with no readable
    /// `memory.stat` is absent from the scrape rather than reported as holding
    /// nothing.
    #[tokio::test]
    async fn an_unreadable_slice_is_absent_and_the_others_still_report() {
        let root = scratch("partial");
        write_slice(&root, "user.slice", "anon 10\nfile 20\n");
        write_slice(&root, "system.slice", "anon 30\nfile 40\n");
        // kubepods.slice is missing entirely.

        let readings = collect(&root).await;
        assert_eq!(readings.len(), 6);
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "user.slice")),
            Some(&10.0)
        );
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "system.slice")),
            Some(&30.0)
        );
        assert!(
            !readings.keys().any(|(_, slice)| slice == "kubepods.slice"),
            "a slice with no memory.stat was reported as a reading: {readings:?}"
        );
    }

    /// Half a `memory.stat` is not half a reading: a consumer cannot tell which
    /// cell is real, so neither is published, and the slice does not count as
    /// readable either.
    #[tokio::test]
    async fn a_slice_missing_one_key_publishes_neither_cell() {
        let root = scratch("half");
        write_slice(&root, "user.slice", "file 4096\n");

        let readings = collect(&root).await;
        assert_eq!(
            readings.get(&cell(HOST_LEDGER_SLICES_READABLE_METRIC, "")),
            Some(&0.0),
            "a slice that published nothing was counted as readable: {readings:?}"
        );
        assert!(
            !readings
                .keys()
                .any(|(name, _)| name == HOST_SLICE_FILE_BYTES_METRIC
                    || name == HOST_SLICE_ANON_BYTES_METRIC),
            "{readings:?}"
        );
    }

    /// The presence series and the node label are what make the reading work on
    /// more than one machine: a node whose ledger did not run has to be a
    /// missing series (against a node inventory that still lists it), and every
    /// sample has to say which node it came from without the scraper's help.
    #[tokio::test]
    async fn every_sample_carries_the_node_and_presence_is_published_without_slices() {
        let root = scratch("empty");
        // No slice directories at all: a cgroup v1 host, or a mount that shows
        // the root and nothing under it.
        let readings = observable(&root).collect_metrics("").await.unwrap();

        let names: Vec<&str> = readings.iter().map(|m| m.name.as_str()).collect();
        assert!(
            names.contains(&HOST_LEDGER_NODE_INFO_METRIC),
            "a node that could read nothing is invisible: {names:?}"
        );
        let info = readings
            .iter()
            .find(|m| m.name == HOST_LEDGER_NODE_INFO_METRIC)
            .unwrap();
        assert_eq!(info.value, 1.0);
        let readable = readings
            .iter()
            .find(|m| m.name == HOST_LEDGER_SLICES_READABLE_METRIC)
            .unwrap();
        assert_eq!(readable.value, 0.0);
        for reading in &readings {
            assert_eq!(
                reading.labels.get(crate::LABEL_NODE).map(String::as_str),
                Some(NODE),
                "{} lost its node label",
                reading.name
            );
        }
    }

    /// The node name is a required input from the deployment, and an unset one
    /// must not silently produce a series that is indistinguishable across
    /// nodes: the label is present and empty, which is the shape a rule groups
    /// under the empty value instead of under some real node's name.
    #[tokio::test]
    async fn an_unnamed_node_publishes_an_empty_label_rather_than_dropping_it() {
        let root = scratch("nonode");
        write_slice(&root, "user.slice", "anon 1\nfile 2\n");
        let readings = HostLedgerObservable::new(&root, "")
            .collect_metrics("")
            .await
            .unwrap();
        for reading in &readings {
            assert_eq!(
                reading.labels.get(crate::LABEL_NODE).map(String::as_str),
                Some(""),
                "{} lost the node label when the name was unknown",
                reading.name
            );
        }
    }

    /// Which directory a slice's bytes live in is a property of the host's
    /// runtime, not of the reading. A node whose kubelet runs the cgroupfs
    /// driver keeps pods under `kubepods`, with no `.slice`, and calling that
    /// node unable to see the slices would keep a bound red on a healthy host
    /// until nobody reads it.
    #[tokio::test]
    async fn the_pod_slice_is_found_under_either_driver_spelling() {
        let root = scratch("cgroupfs");
        write_slice(&root, "user.slice", "anon 1\nfile 2\n");
        write_slice(&root, "system.slice", "anon 3\nfile 4\n");
        write_slice(&root, "kubepods", "anon 5\nfile 6\n");

        let readings = collect(&root).await;
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "kubepods.slice")),
            Some(&5.0),
            "the pod slice was not found under the cgroupfs spelling: {readings:?}"
        );
        assert_eq!(
            readings.get(&cell(HOST_LEDGER_SLICES_READABLE_METRIC, "")),
            Some(&3.0),
            "a host spelling the pod slice the other way was counted as incomplete: {readings:?}"
        );
        assert_eq!(
            readings.get(&cell(HOST_SLICE_ANON_BYTES_METRIC, "kubepods")),
            None,
            "the cgroupfs path leaked out as a slice of its own: {readings:?}"
        );
    }

    /// A name nobody declared has no path to read, and guessing one would
    /// publish bytes from some directory that was never named.
    #[test]
    fn an_unknown_slice_has_no_path() {
        assert!(slice_dirs("mystery.slice").is_empty());
        assert_eq!(slice_dirs("user.slice"), &["user.slice"]);
        assert_eq!(
            slice_dirs("kubepods.slice"),
            &["kubepods.slice", "kubepods"]
        );
    }

    /// The default root is only used when the deployment names no path, so the
    /// mount point has to be reachable without a name.
    #[test]
    fn the_root_falls_back_to_the_deployment_mount_point() {
        assert_eq!(DEFAULT_ROOT, "/host/cgroup");
        assert_eq!(ROOT_ENV, "COGNEVA_HOST_CGROUP_ROOT");
        assert_eq!(
            observable(Path::new(DEFAULT_ROOT)).root(),
            Path::new(DEFAULT_ROOT)
        );
    }

    /// The gauge only becomes an alert if a rule queries its exact name. A rename
    /// on either side leaves both halves internally consistent and the signal
    /// silently absent, so the two are pinned against each other here.
    #[test]
    fn deployed_rule_queries_the_metric_this_module_publishes() {
        let rule = chart_rule(HOST_SLICE_ANON_FLOOR_RULE);
        let promql = rule["promql"].as_str().expect("promql is a string");
        assert!(
            promql.contains(HOST_SLICE_ANON_BYTES_METRIC),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must query {HOST_SLICE_ANON_BYTES_METRIC}, got: {promql}"
        );
        // The slices are the same series with one label between them, and only
        // one of them states "something outside this cluster is holding memory".
        // A rule reading the family without pinning the label would compare
        // kubepods.slice -- this platform's own memory -- against the floor.
        assert!(
            promql.contains("slice=\"user.slice\""),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must pin the slice it is about, got: {promql}"
        );
        // The alert has to be per node. Aggregating the family without a
        // grouping compares the whole fleet's sum against a floor derived from
        // one machine, which is the reading that is wrong exactly when there is
        // more than one node.
        assert!(
            promql.contains("by (node)"),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must keep the result per node, got: {promql}"
        );
        // The floor is one number in two carriers: the query and the sentence
        // that tells a reader what it means. Changing one without the other
        // leaves the alert describing a bound it does not use.
        assert!(
            promql.contains(&HOST_SLICE_ANON_FLOOR_BYTES.to_string()),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} does not compare against the declared floor \
             {HOST_SLICE_ANON_FLOOR_BYTES}, got: {promql}"
        );
        let summary = rule["summary"].as_str().expect("summary is a string");
        assert!(
            summary.contains("8 GiB"),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} compares against 8 GiB but its summary does not \
             say so: {summary}"
        );
        // Where the number comes from is the sentence a reader sees, and it has
        // to name both halves of the derivation: that the floor was set from the
        // measured distribution, and that what it is compared against is the
        // window minimum rather than the instant. A summary that stayed silent
        // on the second half describes a floor a peak crosses, which is a
        // different rule from the one deployed -- and the number was declared
        // from a guess until the series existed, so the sentence is also the
        // only place saying that is no longer true.
        assert!(
            summary.contains("实测") && summary.contains("最小值"),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must say the floor came from the measured \
             distribution and is compared against the window minimum: {summary}"
        );
        // Persistence is a property of a whole window, which only a range
        // aggregate reads. The coverage term is required by the shared gate; it
        // is asserted here as well because this rule is the one that would
        // otherwise fire once per ledger pod start and claim fifteen minutes.
        //
        // The window can be spelled two ways and both ask the same question: a
        // plain range selector (`[15m]`) or a subquery (`[15m:1m]`, needed once
        // the inner selector itself is an over-time aggregate). Pinning the
        // literal `[15m]` made this test describe a spelling rather than the
        // window, so it reddened when the rule moved to the subquery form
        // without the rule reading any less of the window.
        let covers = |window: &str| {
            promql.contains(&format!("[{window}]")) || promql.contains(&format!("[{window}:"))
        };
        assert!(
            promql.contains("min_over_time(") && covers("15m"),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must read the whole window rather than a \
             single sample, got: {promql}"
        );
        assert!(
            promql.contains("count_over_time(") && covers("30m"),
            "rule {HOST_SLICE_ANON_FLOOR_RULE} must require the 15m window to be covered \
             rather than read whatever samples it happens to hold, got: {promql}"
        );
    }

    /// A node whose ledger is not running produces no sample at all, so the rule
    /// that states it has to be written against the absence -- and against the
    /// cluster's own list of nodes, so that a node which was removed stops being
    /// reported while a node that is still there and silent keeps being one.
    #[test]
    fn the_missing_node_rule_reads_the_presence_series_against_the_node_inventory() {
        let rule = chart_rule(HOST_LEDGER_NODE_UNREPORTED_RULE);
        let promql = rule["promql"].as_str().expect("promql is a string");
        assert!(
            promql.contains(HOST_LEDGER_NODE_INFO_METRIC),
            "rule {HOST_LEDGER_NODE_UNREPORTED_RULE} must read \
             {HOST_LEDGER_NODE_INFO_METRIC}, got: {promql}"
        );
        // `kube_node_info` is the reference set: judging by the nodes that are
        // there is what separates "removed" from "silent".
        assert!(
            promql.contains("kube_node_info"),
            "rule {HOST_LEDGER_NODE_UNREPORTED_RULE} must compare against the cluster's node \
             list, got: {promql}"
        );
        // The direction matters: the subject is the node list with no ledger to
        // match it. A rule that reads the ledger and judges what it finds can
        // only report what arrived, which is the shape that goes green when the
        // carrier disappears.
        assert!(
            promql.contains("unless"),
            "rule {HOST_LEDGER_NODE_UNREPORTED_RULE} must be written as an absence against the \
             node list, got: {promql}"
        );
    }

    /// A cgroup v1 host has the mount point and none of the slices. That is
    /// reported as its own fact rather than as three zeroes, so it needs a rule
    /// that reads the coverage series.
    #[test]
    fn the_invisible_slices_rule_reads_the_coverage_series() {
        let rule = chart_rule(HOST_LEDGER_SLICES_INVISIBLE_RULE);
        let promql = rule["promql"].as_str().expect("promql is a string");
        assert!(
            promql.contains(HOST_LEDGER_SLICES_READABLE_METRIC),
            "rule {HOST_LEDGER_SLICES_INVISIBLE_RULE} must read \
             {HOST_LEDGER_SLICES_READABLE_METRIC}, got: {promql}"
        );
        assert_eq!(
            rule["condition"]["less_than"],
            SLICES.len() as f64,
            "rule {HOST_LEDGER_SLICES_INVISIBLE_RULE} must fire below the full slice count"
        );
    }
}
