//! What a claim-backed directory holds, and the series that reading is published under.
//!
//! A persistent volume's declared size is a claim, not a measurement: nothing
//! enforces it, and for a directory-backed volume the per-volume capacity the
//! kubelet reports is the node's filesystem, so the only party that can measure
//! what a volume holds is the process writing to it. More than one process does
//! — each workload writes its own volume and publishes this one series — and
//! they have to agree on three things that a second implementation would drift
//! on: the series name, the label the declaration is joined on, and the meaning
//! of "not measured yet". Hence this module, beside the walker both of them read.
//!
//! A volume's *contents* are not what this measures. A zero would be a claim
//! that the directory is empty, which is the confusion the reading exists to
//! end, so a footprint publishes nothing at all until a walk has succeeded, and
//! a failed walk keeps the last measurement rather than replacing it with a
//! smaller number that can only silence the comparison it is used for.
//!
//! Directories below a volume that are separate mount points belong to another
//! claim, and their bytes would otherwise be reported against both. Which
//! directories those are is read from this process's own mount table rather than
//! restated in configuration: a hand-kept list cannot tell when a mount has
//! moved, and a stale entry fails silently in both directions — one naming a
//! mount that no longer exists inflates the parent, one omitting a mount that
//! now exists hands the child's bytes to the parent.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Series carrying the measured footprint of a claim-backed directory.
pub const USED_METRIC: &str = "cogneva_data_volume_used_bytes";

/// Label naming the claim the reading belongs to. The deployed rule divides the
/// series by the request `kube-state-metrics` reports, joined on this label and
/// the namespace — so a reading attributed to the wrong claim invents an overrun
/// on one volume and hides the one on another.
pub const CLAIM_LABEL: &str = "persistentvolumeclaim";

/// Fastest cadence a claim-backed directory may be re-walked at.
///
/// The walk is metadata-only and cheap, but it holds no value being fresher than
/// the scrape interval, and a deployment that asks for more gets this instead of
/// a knob that reads as "how wrong the setting was".
pub const MIN_SCAN_INTERVAL_SECS: u64 = 30;

/// Cadence a process re-walks its volumes at when nothing states one.
///
/// Every process that measures a volume has to pick this, and a reading is only
/// comparable across volumes when they are all as fresh as each other: a walker
/// on a one-minute cadence would report a volume as holding less than a walker
/// on a ten-minute one for the same workload. It sits beside the floor because
/// the two are read together — a deployment's own cadence is clamped by the
/// floor and falls back to this.
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Deployment variable stating the `claim=path` mounts a pod measures.
///
/// The name is part of the contract for the same reason the series name is: the
/// deployment states the pairing with it and the process reads the pairing from
/// it, and the two are written in different files. A producer that spells the
/// variable its own way publishes nothing while the manifest that set it reads
/// as a pod whose volumes are measured.
pub const MOUNTS_ENV: &str = "COGNEVA_DATA_VOLUME_MOUNTS";

/// Deployment variable naming the claim behind the application data directory.
///
/// Only a process that has that directory can act on it; the standalone entries
/// have none, and a declaration there is a misdirected one rather than a
/// setting.
pub const CLAIM_ENV: &str = "COGNEVA_DATA_VOLUME_CLAIM";

/// Deployment variable overriding [`DEFAULT_SCAN_INTERVAL_SECS`].
pub const INTERVAL_ENV: &str = "COGNEVA_DATA_VOLUME_INTERVAL_SECS";

/// One declaration from a deployment's `claim=path` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimMount {
    /// Claim backing `path`; the label value the reading carries and the key the
    /// declared size is joined on.
    pub claim: String,
    /// Directory to walk. Text, because that is the form the deployment states
    /// it in; it becomes a path where it is walked.
    pub path: String,
}

/// Parse a deployment's `claim=path` list, one entry per line.
///
/// A malformed entry is an error, never a skip: a mount the operator declared
/// and the process silently dropped looks exactly like a volume that is small,
/// which is the blindness this reading exists to end.
pub fn parse_claim_paths(raw: &str) -> Result<Vec<ClaimMount>, String> {
    let mut out = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((claim, path)) = line.split_once('=') else {
            return Err(format!("entry `{line}` is not `claim=path`"));
        };
        let claim = claim.trim();
        // One spelling per mount point: a trailing slash survives a `starts_with`
        // check but not a comparison against the same path written without one,
        // and the two spellings of one directory would then differ in whatever a
        // consumer does with the text.
        let path = path.trim().trim_end_matches('/');
        if claim.is_empty() || path.is_empty() {
            return Err(format!("entry `{line}` leaves the claim or the path empty"));
        }
        out.push(ClaimMount {
            claim: claim.to_string(),
            path: path.to_string(),
        });
    }
    Ok(out)
}

/// Bytes a Kubernetes quantity string denotes, `None` if it is not one.
///
/// A declaration and a measurement are compared in one unit, and both sides are
/// written as quantity strings: the claim declares `10Gi`, the API speaks either
/// that spelling or the byte count it normalizes to, and the walker counts
/// bytes. Parsing them in one place keeps the sides from disagreeing about a
/// suffix — a parser reading `Gi` as `10^9` makes every volume look larger than
/// its declaration, and one reading a bare number as zero makes them all look
/// empty. Decimal suffixes are powers of a thousand, binary ones powers of 1024,
/// and `m` is a thousandth. Text that is not a quantity is `None`, never zero: an
/// unreadable declaration is not a declaration of nothing.
pub fn quantity_bytes(text: &str) -> Option<f64> {
    let text = text.trim();
    let digits_end = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    let (num, suffix) = text.split_at(digits_end);
    let value: f64 = num.parse().ok()?;
    let factor = match suffix {
        "" => 1.0,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024.0,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        _ => return None,
    };
    Some(value * factor)
}

/// The footprint of one claim, read back out of a `/metrics` page.
///
/// The consumer side of the same contract as [`ClaimFootprint`]: the series
/// names the quantity and the label says which volume it belongs to, so both
/// have to match before a number is used. Matching the name alone would hand one
/// volume's reading to another, and returning zero when nothing matches would
/// read as an empty volume — the reading's whole purpose is to tell "nothing
/// measured yet" apart from "nothing there".
pub fn used_bytes_from_exposition(text: &str, claim: &str) -> Option<u64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|line| {
            let (head, value) = line.rsplit_once(' ')?;
            let rest = head.strip_prefix(USED_METRIC)?;
            let labels = rest.strip_prefix('{')?.strip_suffix('}')?;
            let matched = labels.split(',').any(|pair| {
                let Some((k, v)) = pair.split_once('=') else {
                    return false;
                };
                k.trim() == CLAIM_LABEL && v.trim().trim_matches('"') == claim
            });
            if !matched {
                return None;
            }
            value.trim().parse::<u64>().ok()
        })
}

/// Mount points of this process's own mount namespace, unreadable ones omitted.
///
/// Empty is a real answer — a container with no submounts under its volumes, or
/// a platform where the table cannot be read. It degrades towards over-counting
/// rather than blindness: a parent that absorbs a nested volume reports a number
/// that is too large, which is visible, whereas leaving out a mount that should
/// have been counted would report a volume as smaller than it is and silence its
/// alert.
pub fn current_mounts() -> Vec<PathBuf> {
    let Ok(text) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            // `<id> <parent> <maj:min> <root> <mount point> <opts> [more] - <fstype> <src> <super opts>`
            let head = line.split(" - ").next()?;
            let field = head.split_whitespace().nth(4)?;
            Some(PathBuf::from(unescape_mount_field(field)))
        })
        .collect()
}

/// Undo the octal escaping the kernel applies to spaces, tabs, newlines and
/// backslashes in a mount point.
fn unescape_mount_field(field: &str) -> String {
    let raw = field.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' && i + 3 < raw.len() {
            let digits = std::str::from_utf8(&raw[i + 1..i + 4]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(digits, 8) {
                out.push(byte);
                i += 4;
                continue;
            }
        }
        out.push(raw[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The mounts below `root` that its own walk has to leave out.
///
/// `root` itself is not among them: excluding the measured directory would make
/// it report nothing forever. Comparison is by path component, so a sibling
/// whose name merely starts the same way is not a child of it.
pub fn nested_mounts_under(root: &Path, mounts: &[PathBuf]) -> Vec<PathBuf> {
    mounts
        .iter()
        .filter(|mount| mount.as_path() != root && mount.starts_with(root))
        .cloned()
        .collect()
}

/// The measured footprint of one claim-backed directory.
///
/// The measurement is written by the scanning task and read by the metrics pull,
/// which are different tasks, hence the atomics rather than a lock. Cloning the
/// handle shares the measurement — the two sides have to see one number, not a
/// copy each.
#[derive(Debug)]
pub struct ClaimFootprint {
    claim: String,
    dir: PathBuf,
    exclude: Vec<PathBuf>,
    used_bytes: AtomicU64,
    measured: AtomicBool,
}

impl ClaimFootprint {
    /// A footprint of `dir`, leaving out the directories named in `exclude`.
    pub fn new(claim: impl Into<String>, dir: PathBuf, exclude: Vec<PathBuf>) -> Self {
        Self {
            claim: claim.into(),
            dir,
            exclude,
            used_bytes: AtomicU64::new(0),
            measured: AtomicBool::new(false),
        }
    }

    /// A footprint of a mounted volume, with the nested mounts this process is
    /// running under left out of its own walk.
    pub fn for_mounted_volume(claim: impl Into<String>, dir: PathBuf) -> Self {
        let exclude = nested_mounts_under(&dir, &current_mounts());
        Self::new(claim, dir, exclude)
    }

    pub fn claim(&self) -> &str {
        &self.claim
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn exclude(&self) -> &[PathBuf] {
        &self.exclude
    }

    pub fn set_used_bytes(&self, bytes: u64) {
        self.used_bytes.store(bytes, Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
    }

    /// The last successful measurement, or `None` before one has happened.
    ///
    /// Not zero: a reading that exists before anything was walked says the
    /// volume is empty, which is a claim about the volume rather than an absence
    /// of evidence.
    pub fn value(&self) -> Option<u64> {
        self.measured
            .load(Ordering::Acquire)
            .then(|| self.used_bytes.load(Ordering::Relaxed))
    }

    /// Walk the directory and record what it holds.
    ///
    /// A failure leaves the last measurement in place and reports why: a number
    /// that is too small can only silence the comparison this feeds, so there is
    /// nothing a caller can do with it that is better than standing still.
    pub fn measure(&self) -> std::io::Result<()> {
        let bytes = crate::fs_size::dir_size_bytes(&self.dir, &self.exclude)?;
        self.set_used_bytes(bytes);
        Ok(())
    }

    /// Walk on the blocking pool, so a large tree does not hold a runtime thread.
    pub async fn measure_blocking(self: &Arc<Self>) -> std::io::Result<()> {
        let this = Arc::clone(self);
        tokio::task::spawn_blocking(move || this.measure())
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("cog-core-claim-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(path: &Path, bytes: usize) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    #[test]
    fn claim_list_parses_lines_and_refuses_the_rest() {
        let parsed = parse_claim_paths(
            "cogneva-evolution-pvc = /opt/cogneva/sandbox\n\
             \n\
             cogneva-sandbox-pvc=/opt/cogneva/sandbox/\n",
        )
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].claim, "cogneva-evolution-pvc");
        assert_eq!(parsed[0].path, "/opt/cogneva/sandbox");
        assert_eq!(parsed[1].claim, "cogneva-sandbox-pvc");
        // A trailing slash must not reach a consumer: two spellings of one mount
        // point differ wherever the text is compared by path.
        assert_eq!(parsed[1].path, "/opt/cogneva/sandbox");

        // A half-written entry is an error, not a skip: the volume that was
        // silently dropped looks exactly like a volume that is small.
        assert!(parse_claim_paths("/opt/cogneva/sandbox").is_err());
        assert!(parse_claim_paths("= /opt/cogneva/sandbox").is_err());
        assert!(parse_claim_paths("cogneva-pvc=").is_err());
        assert!(parse_claim_paths("").unwrap().is_empty());
    }

    #[test]
    fn quantities_parse_in_one_unit_and_the_rest_is_not_a_size() {
        assert_eq!(quantity_bytes("1"), Some(1.0));
        assert_eq!(quantity_bytes("10Gi"), Some(10.0 * 1024f64.powi(3)));
        assert_eq!(quantity_bytes(" 512Mi "), Some(512.0 * 1024f64.powi(2)));
        // Decimal suffixes mean powers of a thousand; reading `G` as `Gi` makes a
        // 300G request look like 322 GB and hides a real overrun.
        assert_eq!(quantity_bytes("300G"), Some(300e9));
        assert_eq!(quantity_bytes("500m"), Some(0.5));
        assert_eq!(quantity_bytes("1Ti"), Some(1024f64.powi(4)));
        // An unreadable declaration is not a declaration of zero: the caller has
        // to see "no number" rather than compare everything against 0.
        assert_eq!(quantity_bytes("abc"), None);
        assert_eq!(quantity_bytes(""), None);
        assert_eq!(quantity_bytes("5Zi"), None);
    }

    #[test]
    fn the_reading_is_matched_by_claim_and_absence_is_not_zero() {
        let page = "# HELP cogneva_data_volume_used_bytes ...\n\
                    cogneva_data_volume_used_bytes{persistentvolumeclaim=\"other\",namespace=\"cogneva\"} 7\n\
                    cogneva_data_volume_used_bytes{persistentvolumeclaim=\"registry-pvc\",namespace=\"cogneva\"} 4096\n";
        assert_eq!(used_bytes_from_exposition(page, "registry-pvc"), Some(4096));
        // Another volume's number is not this volume's: the label is the whole
        // reason the reading is attributed at all.
        assert_eq!(used_bytes_from_exposition(page, "absent-pvc"), None);
        assert_eq!(used_bytes_from_exposition("", "registry-pvc"), None);
        // A page whose value cannot be read is not a small volume.
        let bad = "cogneva_data_volume_used_bytes{persistentvolumeclaim=\"p\"} NaN\n";
        assert_eq!(used_bytes_from_exposition(bad, "p"), None);
    }

    #[test]
    fn mount_field_escaping_is_undone() {
        assert_eq!(unescape_mount_field("/a\\040b"), "/a b");
        assert_eq!(unescape_mount_field("/a\\134b"), "/a\\b");
        assert_eq!(unescape_mount_field("/plain"), "/plain");
        // A lone backslash that escapes nothing stays as it is: reading it as a
        // prefix would eat the character after it.
        assert_eq!(unescape_mount_field("/a\\b"), "/a\\b");
    }

    #[test]
    fn nested_mounts_leave_out_the_root_and_the_neighbours() {
        let root = PathBuf::from("/opt/cogneva/sandbox");
        let mounts = vec![
            PathBuf::from("/"),
            PathBuf::from("/etc/hosts"),
            PathBuf::from("/opt/cogneva/sandbox"),
            PathBuf::from("/opt/cogneva/sandbox/src"),
            // Same prefix, different directory: a component-wise comparison must
            // not call this a child.
            PathBuf::from("/opt/cogneva/sandbox2"),
        ];
        assert_eq!(
            nested_mounts_under(&root, &mounts),
            vec![PathBuf::from("/opt/cogneva/sandbox/src")]
        );
        // The root is not its own exclusion: excluding it would make the volume
        // report zero forever.
        assert!(nested_mounts_under(&root, std::slice::from_ref(&root)).is_empty());
    }

    /// The two facts the exclusion needs — that a mount exists and where — come
    /// from the kernel, and the parser has to read the real table rather than a
    /// shape invented for the test.
    #[test]
    fn current_mounts_reads_this_processes_own_table() {
        let mounts = current_mounts();
        assert!(
            mounts.iter().any(|m| m == Path::new("/")),
            "the root mount is always present, got {mounts:?}"
        );
        assert!(
            !mounts.iter().any(|m| m.as_os_str().is_empty()),
            "an unparsed line yields an empty path that would match nothing"
        );
    }

    #[test]
    fn a_footprint_publishes_nothing_until_a_walk_succeeds() {
        let root = scratch("nothing-yet");
        write(&root.join("a.bin"), 1000);
        let footprint = ClaimFootprint::new("claim", root.clone(), Vec::new());
        assert_eq!(footprint.value(), None);

        footprint.measure().unwrap();
        assert_eq!(footprint.value(), Some(1000));

        // A walk that fails keeps the last measurement: a smaller number can only
        // silence the comparison, and a missing directory is exactly when a
        // reader would otherwise see "empty".
        std::fs::remove_dir_all(&root).unwrap();
        assert!(footprint.measure().is_err());
        assert_eq!(footprint.value(), Some(1000));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn an_excluded_mount_is_not_counted_into_the_parent() {
        let root = scratch("excluded");
        write(&root.join("own.bin"), 1000);
        let child = root.join("src");
        write(&child.join("child.bin"), 4000);

        let without = ClaimFootprint::new("parent", root.clone(), Vec::new());
        without.measure().unwrap();
        assert_eq!(without.value(), Some(5000));

        let with = ClaimFootprint::new("parent", root.clone(), vec![child]);
        with.measure().unwrap();
        assert_eq!(with.value(), Some(1000));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn two_handles_see_one_measurement() {
        let handle = Arc::new(ClaimFootprint::new(
            "claim",
            PathBuf::from("/nowhere"),
            Vec::new(),
        ));
        let reader = Arc::clone(&handle);
        handle.set_used_bytes(4096);
        assert_eq!(reader.value(), Some(4096));
        assert_eq!(reader.claim(), "claim");
    }
}
