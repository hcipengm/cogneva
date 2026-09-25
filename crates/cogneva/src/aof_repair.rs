//! Startup repair for a torn tail in Redis' append-only file.
//!
//! Cause, reproduced on 2026-09-23: when the host goes down uncleanly, the file
//! length reaches disk but the bytes that length covers do not, so the region
//! reads back as 0x00. Redis classifies such a region as a malformed record
//! rather than as end-of-file, which makes `aof-load-truncated` (yes by default,
//! and useless to set explicitly) inapplicable: the instance refuses to start and
//! the whole platform stays down until a person truncates the file by hand. This
//! command is that truncation, moved onto the pod's startup path so no person is
//! needed.
//!
//! The criterion is deliberately one-sided: **only a trailing run of 0x00 is
//! unwritten data**. A RESP record always ends in '\n', so a complete record can
//! never end in 0x00, and any 0x00 at the tail is either unwritten or belongs to
//! a record that never finished. Dropping it loses no complete record and lets
//! Redis load. A long zero run *inside* the file is reported but never modified —
//! it may be real payload bytes, and deleting those is a human decision. The same
//! holds for files Redis owns in full (a wrong repair there is unrecoverable),
//! so only the append-only increment is ever written to.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Suffix of the append-only increment in Redis 7's multi-part layout. This is
/// the only file Redis appends to *and* the only one where dropping a torn tail
/// is a valid repair.
const INCR_SUFFIX: &str = ".incr.aof";

/// Operand naming the file this pass publishes its verdict to.
///
/// The verdict used to be a set of `level=warn` lines on the init container's
/// stdout, and container logs are shipped nowhere: the reading existed and had
/// no reader. The same findings written in the Prometheus text format are
/// readable by a rule, so "the host stopped uncleanly and redis had to give up
/// the tail of its AOF at startup" stops depending on someone thinking to look.
pub const METRICS_FILE_OPERAND: &str = "--metrics-file";

/// One gauge per finding class, never one per verdict: bytes given up, a torn
/// tail left in a file redis owns whole, a suspected hole inside an increment,
/// and a directory whose layout this repair does not know are four different
/// things to know, and a reader who is handed their sum cannot act on it.
pub const AOF_REPAIR_DROPPED_BYTES_METRIC: &str = "cogneva_aof_repair_dropped_bytes";
pub const AOF_REPAIR_UNTOUCHED_TORN_TAIL_BYTES_METRIC: &str =
    "cogneva_aof_repair_untouched_torn_tail_bytes";
pub const AOF_REPAIR_SUSPECTED_INTERIOR_HOLES_METRIC: &str =
    "cogneva_aof_repair_suspected_interior_holes";
pub const AOF_REPAIR_UNHANDLED_LAYOUT_METRIC: &str = "cogneva_aof_repair_unhandled_layout";
pub const AOF_REPAIR_EXAMINED_INCREMENTS_METRIC: &str = "cogneva_aof_repair_examined_increments";
pub const AOF_REPAIR_PASS_TIMESTAMP_METRIC: &str = "cogneva_aof_repair_pass_timestamp_seconds";

/// Whether a verdict is being published at all.
///
/// The repair runs from the image the deployment floats, so a pod can start an
/// image that predates the operand that publishes this — and then every series
/// above is simply absent, which a rule keyed on `> 0` reads as "clean". This
/// gauge is the positive statement that the reading exists; the wrapper in the
/// deployment writes 0 when the image it got cannot produce one.
pub const AOF_REPAIR_VERDICT_PUBLISHED_METRIC: &str = "cogneva_aof_repair_verdict_published";

/// Zero run length that makes an interior hole worth reporting. A hole punched
/// by the page cache is at least a partial write, but a legitimate payload can
/// hold a long run of real 0x00 bytes; 4096 keeps the report from crying wolf on
/// ordinary binary payloads while still catching anything page-shaped. Nothing
/// is modified on this verdict, so a false positive costs one log line.
const SUSPECTED_INTERIOR_RUN: usize = 4096;

/// Read window for the tail scan and the interior scan.
const SCAN_CHUNK: usize = 64 * 1024;

/// What happened to one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReport {
    pub name: String,
    pub size_before: u64,
    /// Bytes dropped from the tail; 0 means the file was left alone.
    pub dropped: u64,
    /// First interior zero run at or above `SUSPECTED_INTERIOR_RUN` that is
    /// followed by non-zero data, as (offset, run length). Reported only.
    pub suspected_interior: Option<(u64, usize)>,
}

impl FileReport {
    fn was_repaired(&self) -> bool {
        self.dropped > 0
    }
}

/// Whether the pass had anything to look at. Kept apart from the file results
/// because "the directory is not there yet" and "the directory holds a layout
/// this repair does not know" are both empty results, and only one of them is
/// good news.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutcome {
    /// The directory exists and every file in it was examined.
    Examined,
    /// The directory does not exist: a first start, nothing can be torn yet.
    FirstStart,
    /// Nothing was examined, and a file of a foreign layout sits where the AOF
    /// directory should be. Never silently reported as "nothing to repair":
    /// that is how a moved layout would look like a clean pass.
    ForeignLayout(PathBuf),
}

/// Outcome of one pass over an AOF directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    pub dir: PathBuf,
    pub outcome: PassOutcome,
    /// Append-only increments that were examined.
    pub increments: Vec<FileReport>,
    /// Non-increment files whose text tail reads as unwritten data, as
    /// (name, trailing zero run). Reported only: Redis rewrites these files
    /// whole, so truncating one is as likely to destroy the dataset as to
    /// recover it.
    pub untouched_tails: Vec<(String, u64)>,
}

impl RepairReport {
    /// True when this pass found the dataset intact as far as it could tell.
    /// A pass that examined nothing is not clean, it is unknown.
    pub fn is_clean(&self) -> bool {
        !matches!(self.outcome, PassOutcome::ForeignLayout(_))
            && self.untouched_tails.is_empty()
            && self
                .increments
                .iter()
                .all(|f| !f.was_repaired() && f.suspected_interior.is_none())
    }

    /// True when bytes were actually dropped, i.e. the dataset lost its tail.
    pub fn dropped_bytes(&self) -> u64 {
        self.increments.iter().map(|f| f.dropped).sum()
    }

    /// One greppable line per event. `level=warn` marks a repair that gave up
    /// data, so a log query can separate "checked, was clean" from "checked,
    /// dropped N bytes".
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        match &self.outcome {
            PassOutcome::Examined => {}
            PassOutcome::FirstStart => out.push(format!(
                "aof-repair: dir={} no-such-dir (first start, nothing to repair)",
                self.dir.display()
            )),
            PassOutcome::ForeignLayout(path) => out.push(format!(
                "aof-repair: level=warn dir={} no-such-dir; found {} instead, which is a layout \
                 this repair does not handle — nothing was inspected",
                self.dir.display(),
                path.display()
            )),
        }
        for (name, run) in &self.untouched_tails {
            out.push(format!(
                "aof-repair: level=warn file={name} torn_tail_bytes={run} action=reported-only \
                 (redis owns this file whole; truncating it is not a repair)"
            ));
        }
        for f in &self.increments {
            if let Some((offset, run)) = f.suspected_interior {
                out.push(format!(
                    "aof-repair: level=warn file={} suspected_interior_hole offset={offset} \
                     run={run} action=reported-only (may be real payload bytes)",
                    f.name
                ));
            }
            if f.was_repaired() {
                out.push(format!(
                    "aof-repair: level=warn file={} dropped_bytes={} size_before={} size_after={} \
                     action=truncated-torn-tail",
                    f.name,
                    f.dropped,
                    f.size_before,
                    f.size_before - f.dropped
                ));
            } else {
                out.push(format!(
                    "aof-repair: file={} size={} action=clean",
                    f.name, f.size_before
                ));
            }
        }
        out.push(format!(
            "aof-repair: dir={} files={} dropped_bytes={}",
            self.dir.display(),
            self.increments.len(),
            self.dropped_bytes()
        ));
        out
    }

    /// This pass as Prometheus text, with `pass_unix_seconds` as its date.
    ///
    /// Every series is published on every pass, zeros included. A rule keyed on
    /// `> 0` cannot tell an absent reading from a clean one, so the clean pass is
    /// the thing that has to be sayable — otherwise the observation face is
    /// silent in exactly the two situations it exists to separate.
    pub fn metrics(&self, pass_unix_seconds: u64) -> String {
        let untouched_bytes: u64 = self.untouched_tails.iter().map(|(_, run)| run).sum();
        let holes = self
            .increments
            .iter()
            .filter(|f| f.suspected_interior.is_some())
            .count() as u64;

        let mut out = String::new();
        push_gauge(
            &mut out,
            AOF_REPAIR_DROPPED_BYTES_METRIC,
            "Bytes dropped from a torn AOF tail at this startup; 0 means nothing was given up",
            self.dropped_bytes(),
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_UNTOUCHED_TORN_TAIL_BYTES_METRIC,
            "Zero bytes at the tail of a file redis owns whole: reported, never modified, \
             because truncating a file redis rewrites is not a repair",
            untouched_bytes,
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_SUSPECTED_INTERIOR_HOLES_METRIC,
            "Zero runs inside an increment that are followed by data: reported, never \
             modified, because they may be real payload bytes",
            holes,
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_UNHANDLED_LAYOUT_METRIC,
            "1 when the AOF directory holds a layout this repair does not handle: nothing \
             was inspected, and a pass that inspected nothing is not a clean pass",
            u64::from(matches!(self.outcome, PassOutcome::ForeignLayout(_))),
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_EXAMINED_INCREMENTS_METRIC,
            "Append-only increments examined at this startup; 0 means there was nothing to \
             look at, which is a first start rather than a clean one",
            self.increments.len() as u64,
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_PASS_TIMESTAMP_METRIC,
            "When this pass ran. The reading stands for as long as the pod does, so it \
             carries its own date instead of borrowing the scrape's",
            pass_unix_seconds,
        );
        push_gauge(
            &mut out,
            AOF_REPAIR_VERDICT_PUBLISHED_METRIC,
            "1 when the image that ran this repair could publish its verdict; 0 means the \
             findings above are absent rather than empty",
            1,
        );
        out
    }
}

/// One gauge, in the text format a textfile collector reads.
fn push_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
    ));
}

/// Wall-clock seconds of this pass.
fn pass_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// The file named by [`METRICS_FILE_OPERAND`], or `None` when the operand is
/// absent.
///
/// An operand with no path is an error rather than "no file": a manifest writes
/// this, and a flag without its value is a wiring mistake that would otherwise
/// look exactly like a clean pass.
pub fn metrics_file_operand(args: &[String]) -> Result<Option<PathBuf>, String> {
    match args.iter().position(|a| a == METRICS_FILE_OPERAND) {
        None => Ok(None),
        Some(i) => match args.get(i + 1) {
            Some(path) => Ok(Some(PathBuf::from(path))),
            None => Err(format!(
                "usage: cogneva repair-aof <aof-dir> {METRICS_FILE_OPERAND} <path>"
            )),
        },
    }
}

/// Publish a pass where a scraper can read it.
///
/// Written beside the destination and renamed into place. The rename is not for
/// this process's readers — an init container has exited before anything serves
/// the directory — but for the next writer: an exporter that collects a
/// half-written file reports a parse error and then nothing at all for that
/// target, which is a wider silence than the one being fixed.
pub fn publish_metrics(
    path: &Path,
    report: &RepairReport,
    pass_unix_seconds: u64,
) -> std::io::Result<()> {
    let staging = path.with_extension("prom.tmp");
    fs::write(&staging, report.metrics(pass_unix_seconds))?;
    fs::rename(&staging, path)
}

/// Number of 0x00 bytes at the end of `path`, counting back in [`SCAN_CHUNK`]
/// windows so a torn tail larger than one window is measured exactly.
fn trailing_zero_run(path: &Path) -> std::io::Result<u64> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut run = 0u64;
    let mut end = len;
    while end > 0 {
        let start = end.saturating_sub(SCAN_CHUNK as u64);
        let span = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf[..span])?;
        let mut i = span;
        while i > 0 && buf[i - 1] == 0 {
            i -= 1;
        }
        run += (span - i) as u64;
        if i > 0 {
            break;
        }
        end = start;
    }
    Ok(run)
}

/// First zero run of at least [`SUSPECTED_INTERIOR_RUN`] bytes that is followed
/// by non-zero data. A run that reaches end-of-file is the torn-tail case, not
/// an interior hole, so it is not reported here.
fn suspected_interior_hole(path: &Path) -> std::io::Result<Option<(u64, usize)>> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut offset = 0u64;
    let mut run_start: Option<u64> = None;
    let mut run_len = 0usize;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        for (i, byte) in buf[..n].iter().enumerate() {
            if *byte == 0 {
                if run_start.is_none() {
                    run_start = Some(offset + i as u64);
                }
                run_len += 1;
            } else {
                if run_len >= SUSPECTED_INTERIOR_RUN {
                    return Ok(Some((run_start.unwrap_or(0), run_len)));
                }
                run_start = None;
                run_len = 0;
            }
        }
        offset += n as u64;
    }
}

/// Drop the last `dropped` bytes and make the new length durable.
///
/// `set_len` on a write handle, not a truncating open: the file must keep every
/// byte before the tear, and opening with truncation would drop the dataset.
fn truncate_tail(path: &Path, new_len: u64) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(new_len)?;
    file.sync_all()
}

/// A single-file `appendonly.aof` next to `dir` means this deployment is not on
/// the multi-part layout this repair understands.
fn legacy_aof_beside(dir: &Path) -> Option<PathBuf> {
    let parent = dir.parent()?;
    let entries = fs::read_dir(parent).ok()?;
    entries
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|e| e == "aof"))
}

/// Examine and repair every append-only increment in `dir`.
///
/// Errors are returned rather than swallowed so a caller can fail closed: a file
/// this repair could not even read is one Redis is unlikely to load either.
pub fn repair_dir(dir: &Path) -> std::io::Result<RepairReport> {
    let mut report = RepairReport {
        dir: dir.to_path_buf(),
        outcome: PassOutcome::Examined,
        increments: Vec::new(),
        untouched_tails: Vec::new(),
    };

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            // Nothing is on disk yet, so nothing can be torn — unless something
            // of a layout we do not handle is sitting beside where the AOF
            // directory belongs, which is a difference the caller must see.
            report.outcome = match legacy_aof_beside(dir) {
                Some(path) => PassOutcome::ForeignLayout(path),
                None => PassOutcome::FirstStart,
            };
            return Ok(report);
        }
        Err(err) => return Err(err),
    };

    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();

    for path in paths {
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };
        if !path.is_file() {
            continue;
        }
        let is_increment = name.ends_with(INCR_SUFFIX);
        // An RDB file ends in an 8-byte checksum whose last byte may legitimately
        // be 0x00, so a trailing zero there proves nothing; the text-shaped files
        // end in '\n' and a zero byte does mean unwritten data.
        let is_text_shaped = name.ends_with(".aof") || name.ends_with(".manifest");
        if !is_increment && !is_text_shaped {
            continue;
        }

        let size_before = path.metadata()?.len();
        let torn = trailing_zero_run(&path)?;
        if !is_increment {
            if torn > 0 {
                report.untouched_tails.push((name, torn));
            }
            continue;
        }

        let mut entry = FileReport {
            name,
            size_before,
            dropped: 0,
            suspected_interior: None,
        };
        if torn > 0 {
            truncate_tail(&path, size_before - torn)?;
            entry.dropped = torn;
        }
        // Only worth scanning when the file survived with content: an increment
        // that was one solid hole has nothing left to inspect.
        if size_before - torn > 0 {
            entry.suspected_interior = suspected_interior_hole(&path)?;
        }
        report.increments.push(entry);
    }

    Ok(report)
}

/// `cogneva repair-aof <aof-dir> [--metrics-file <path>]`: repair, report, exit.
///
/// A repair that dropped bytes still exits 0 — the point of running on the
/// startup path is that Redis comes up afterwards. What was dropped leaves as a
/// `level=warn` line and, when the operand names a place, as a published
/// reading; the exit code is reserved for "this could not run".
pub fn run_from_args() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let dir = args.first().ok_or("usage: cogneva repair-aof <aof-dir>")?;
    let metrics_file = metrics_file_operand(&args)?;
    let report = repair_dir(Path::new(dir))?;
    let mut out = std::io::stdout();
    for line in report.lines() {
        writeln!(out, "{line}")?;
    }
    if let Some(path) = metrics_file {
        publish_metrics(&path, &report, pass_unix_seconds())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_file(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        let mut f = File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
        path
    }

    /// A tiny but well-formed AOF: three RESP commands.
    const HEALTHY: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n\
*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\nd\r\n\
*1\r\n$4\r\nPING\r\n";

    #[test]
    fn trailing_zero_run_measures_the_tear_not_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let clean = write_file(dir.path(), "clean.aof", HEALTHY);
        assert_eq!(trailing_zero_run(&clean).unwrap(), 0);

        let mut torn = HEALTHY.to_vec();
        torn.extend_from_slice(&[0u8; 208]);
        let torn = write_file(dir.path(), "torn.aof", &torn);
        assert_eq!(trailing_zero_run(&torn).unwrap(), 208);

        let solid = write_file(dir.path(), "solid.aof", &[0u8; 1000]);
        assert_eq!(trailing_zero_run(&solid).unwrap(), 1000);

        // A tear larger than one scan window must be measured exactly.
        let mut big = HEALTHY.to_vec();
        big.extend_from_slice(&vec![0u8; SCAN_CHUNK + 7]);
        let big = write_file(dir.path(), "big.aof", &big);
        assert_eq!(trailing_zero_run(&big).unwrap(), (SCAN_CHUNK + 7) as u64);
    }

    #[test]
    fn a_healthy_increment_is_left_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "appendonly.aof.1.incr.aof", HEALTHY);
        let report = repair_dir(dir.path()).unwrap();

        assert!(report.is_clean());
        assert_eq!(report.dropped_bytes(), 0);
        assert_eq!(fs::read(&path).unwrap(), HEALTHY);
        assert!(report.lines().iter().any(|l| l.contains("action=clean")));
    }

    #[test]
    fn a_torn_tail_is_cut_back_to_the_last_complete_byte() {
        let dir = tempfile::tempdir().unwrap();
        let mut torn = HEALTHY.to_vec();
        torn.extend_from_slice(&[0u8; 208]);
        let path = write_file(dir.path(), "appendonly.aof.1.incr.aof", &torn);

        let report = repair_dir(dir.path()).unwrap();

        assert_eq!(report.dropped_bytes(), 208);
        assert!(!report.is_clean());
        // The surviving bytes are the healthy prefix, byte for byte.
        assert_eq!(fs::read(&path).unwrap(), HEALTHY);
        assert!(report
            .lines()
            .iter()
            .any(|l| l.contains("dropped_bytes=208") && l.contains("action=truncated-torn-tail")));
    }

    #[test]
    fn a_repair_that_gives_up_data_says_so_and_still_succeeds() {
        // The startup path must reach Redis even when the tear swallowed the
        // whole increment; an exit code of 0 plus a warn line is the contract.
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "appendonly.aof.1.incr.aof", &[0u8; 4096]);
        let report = repair_dir(dir.path()).unwrap();

        assert_eq!(report.dropped_bytes(), 4096);
        assert_eq!(fs::read(&path).unwrap().len(), 0);
        assert!(report
            .lines()
            .iter()
            .any(|l| l.starts_with("aof-repair: level=warn")));
    }

    #[test]
    fn an_interior_hole_is_reported_but_never_modified() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = HEALTHY.to_vec();
        bytes.extend_from_slice(&vec![0u8; SUSPECTED_INTERIOR_RUN]);
        bytes.extend_from_slice(HEALTHY);
        let path = write_file(dir.path(), "appendonly.aof.1.incr.aof", &bytes);

        let report = repair_dir(dir.path()).unwrap();

        assert_eq!(
            fs::read(&path).unwrap(),
            bytes,
            "interior damage must not be rewritten"
        );
        assert_eq!(report.dropped_bytes(), 0);
        assert!(report.increments[0].suspected_interior.is_some());
        assert!(!report.is_clean());
        assert!(report
            .lines()
            .iter()
            .any(|l| l.contains("suspected_interior_hole") && l.contains("action=reported-only")));
    }

    #[test]
    fn a_real_zero_run_inside_a_payload_is_not_called_a_hole() {
        // Below the threshold a zero run is ordinary payload; reporting it would
        // train the reader to ignore the report.
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = HEALTHY.to_vec();
        bytes.extend_from_slice(&vec![0u8; SUSPECTED_INTERIOR_RUN - 1]);
        bytes.extend_from_slice(HEALTHY);
        write_file(dir.path(), "appendonly.aof.1.incr.aof", &bytes);

        let report = repair_dir(dir.path()).unwrap();
        assert!(report.increments[0].suspected_interior.is_none());
    }

    #[test]
    fn files_redis_owns_whole_are_reported_not_repaired() {
        let dir = tempfile::tempdir().unwrap();
        // A base file ends in a checksum whose last byte can be 0x00 by chance,
        // so it must not even be reported.
        let mut base_bytes = vec![0x52u8; 512];
        base_bytes[511] = 0x00;
        let base = write_file(dir.path(), "appendonly.aof.1.base.rdb", &base_bytes);
        // The manifest is text: a zero tail there is real unwritten data.
        let manifest = write_file(dir.path(), "appendonly.aof.manifest", &[0u8; 64]);

        let report = repair_dir(dir.path()).unwrap();

        assert_eq!(fs::read(&base).unwrap(), base_bytes);
        assert!(report.increments.is_empty());
        assert_eq!(
            report.untouched_tails,
            vec![("appendonly.aof.manifest".to_string(), 64)]
        );
        assert!(
            fs::read(&manifest).unwrap().iter().all(|b| *b == 0),
            "must not be truncated"
        );
        assert!(!report.is_clean());
    }

    #[test]
    fn every_increment_in_the_directory_is_examined() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path(), "a.incr.aof", HEALTHY);
        let mut torn = HEALTHY.to_vec();
        torn.extend_from_slice(&[0u8; 32]);
        write_file(dir.path(), "b.incr.aof", &torn);

        let report = repair_dir(dir.path()).unwrap();
        assert_eq!(report.increments.len(), 2);
        assert_eq!(report.dropped_bytes(), 32);
    }

    #[test]
    fn a_missing_directory_is_clean_but_not_silent() {
        let dir = tempfile::tempdir().unwrap();
        let report = repair_dir(&dir.path().join("appendonlydir")).unwrap();
        assert!(report.is_clean());
        assert_eq!(report.outcome, PassOutcome::FirstStart);
        assert!(report
            .lines()
            .iter()
            .any(|l| l.contains("no-such-dir") && l.contains("first start")));
    }

    #[test]
    fn a_foreign_layout_is_called_out_instead_of_looking_clean() {
        // If the deployment ever moves to the single-file layout, "no directory"
        // must not read as "nothing was wrong".
        let dir = tempfile::tempdir().unwrap();
        let legacy = write_file(dir.path(), "appendonly.aof", HEALTHY);
        let report = repair_dir(&dir.path().join("appendonlydir")).unwrap();

        assert_eq!(report.outcome, PassOutcome::ForeignLayout(legacy));
        assert!(!report.is_clean());
        assert!(report
            .lines()
            .iter()
            .any(|l| l.contains("level=warn") && l.contains("nothing was inspected")));
    }

    /// Value of one published series, or `None` when the series is absent.
    ///
    /// Deliberately a real parse of the text rather than a substring search: the
    /// point of these tests is that the collector will find the reading, and
    /// `# HELP` lines mention the same names.
    fn published(text: &str, name: &str) -> Option<u64> {
        text.lines()
            .find(|l| l.starts_with(name) && l.as_bytes().get(name.len()) == Some(&b' '))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
    }

    /// A clean pass has to be sayable: an absent series and a zero read the same
    /// way to a rule that only asks `> 0`, and "nothing was wrong" is precisely
    /// what this face exists to be able to report.
    #[test]
    fn a_clean_pass_publishes_its_zeros_and_its_date() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path(), "appendonly.aof.1.incr.aof", HEALTHY);
        let report = repair_dir(dir.path()).unwrap();

        let text = report.metrics(1_700_000_000);
        assert_eq!(published(&text, AOF_REPAIR_DROPPED_BYTES_METRIC), Some(0));
        assert_eq!(
            published(&text, AOF_REPAIR_UNTOUCHED_TORN_TAIL_BYTES_METRIC),
            Some(0)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_SUSPECTED_INTERIOR_HOLES_METRIC),
            Some(0)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_UNHANDLED_LAYOUT_METRIC),
            Some(0)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_EXAMINED_INCREMENTS_METRIC),
            Some(1)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_VERDICT_PUBLISHED_METRIC),
            Some(1)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_PASS_TIMESTAMP_METRIC),
            Some(1_700_000_000)
        );
    }

    /// The reading the row exists for: this startup gave up bytes, which is the
    /// record of a host that did not stop cleanly.
    #[test]
    fn a_given_up_tail_is_published_as_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut torn = HEALTHY.to_vec();
        torn.extend_from_slice(&[0u8; 208]);
        write_file(dir.path(), "appendonly.aof.1.incr.aof", &torn);

        let text = repair_dir(dir.path()).unwrap().metrics(0);
        assert_eq!(published(&text, AOF_REPAIR_DROPPED_BYTES_METRIC), Some(208));
        assert_eq!(
            published(&text, AOF_REPAIR_EXAMINED_INCREMENTS_METRIC),
            Some(1)
        );
    }

    /// A pass that inspected nothing must not be publishable as a clean one, and
    /// the two ways of inspecting nothing are distinguished by the examined
    /// count: a first start has no files, a foreign layout has them under a name
    /// this repair does not read.
    #[test]
    fn a_blind_pass_publishes_that_it_inspected_nothing() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path(), "appendonly.aof", HEALTHY);

        let text = repair_dir(&dir.path().join("appendonlydir"))
            .unwrap()
            .metrics(0);
        assert_eq!(
            published(&text, AOF_REPAIR_UNHANDLED_LAYOUT_METRIC),
            Some(1)
        );
        assert_eq!(
            published(&text, AOF_REPAIR_EXAMINED_INCREMENTS_METRIC),
            Some(0)
        );
        assert_eq!(published(&text, AOF_REPAIR_DROPPED_BYTES_METRIC), Some(0));
    }

    /// Findings this repair declines to act on are still findings: they are the
    /// ones a person has to look at, and a log line nobody reads is not a
    /// reading.
    #[test]
    fn findings_that_were_left_alone_are_published_separately() {
        let dir = tempfile::tempdir().unwrap();
        let mut owned_whole = HEALTHY.to_vec();
        owned_whole.extend_from_slice(&[0u8; 64]);
        write_file(dir.path(), "appendonly.aof.1.base.aof", &owned_whole);

        let text = repair_dir(dir.path()).unwrap().metrics(0);
        assert_eq!(
            published(&text, AOF_REPAIR_UNTOUCHED_TORN_TAIL_BYTES_METRIC),
            Some(64)
        );
        assert_eq!(published(&text, AOF_REPAIR_DROPPED_BYTES_METRIC), Some(0));
    }

    #[test]
    fn the_operand_names_the_file_and_needs_a_path() {
        let none = metrics_file_operand(&["/data/appendonlydir".to_string()]).unwrap();
        assert_eq!(none, None);

        let named = metrics_file_operand(&[
            "/data/appendonlydir".to_string(),
            METRICS_FILE_OPERAND.to_string(),
            "/report/aof-repair.prom".to_string(),
        ])
        .unwrap();
        assert_eq!(named, Some(PathBuf::from("/report/aof-repair.prom")));

        // A flag with nothing after it is a wiring mistake, not "no file": read
        // as "no file" it would look exactly like a clean pass.
        let dangling = metrics_file_operand(&[
            "/data/appendonlydir".to_string(),
            METRICS_FILE_OPERAND.to_string(),
        ]);
        assert!(dangling.is_err());
    }

    #[test]
    fn the_published_file_holds_the_pass_and_leaves_no_staging_behind() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path(), "appendonly.aof.1.incr.aof", HEALTHY);
        let report = repair_dir(dir.path()).unwrap();

        let report_dir = tempfile::tempdir().unwrap();
        let target = report_dir.path().join("aof-repair.prom");
        publish_metrics(&target, &report, 42).unwrap();

        assert_eq!(fs::read_to_string(&target).unwrap(), report.metrics(42));
        assert_eq!(
            published(
                &fs::read_to_string(&target).unwrap(),
                AOF_REPAIR_PASS_TIMESTAMP_METRIC
            ),
            Some(42)
        );
        let left: Vec<String> = fs::read_dir(report_dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(left, vec!["aof-repair.prom".to_string()]);
    }

    /// The rule that reads the published gauge has to treat it as a presence
    /// statement. A rename on either side of the contract is caught by the
    /// alert-rule table; what that table cannot see is the comparison, and
    /// `> 0` on a gauge that is 1 whenever the reading exists alerts on every
    /// healthy redis instead of on the one that cannot publish.
    #[test]
    fn the_deployed_rule_reads_the_published_gauge_as_a_presence_statement() {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = fs::read_to_string(&chart)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", chart.display()));
        let root: serde_json::Value = serde_json::from_str(&text).expect("chart config is JSON");
        let rules = root
            .pointer("/observability/infra_watch/rules")
            .and_then(|v| v.as_array())
            .expect("infra_watch.rules present");

        let rule = rules
            .iter()
            .find(|r| {
                r["promql"]
                    .as_str()
                    .is_some_and(|p| p.contains(AOF_REPAIR_VERDICT_PUBLISHED_METRIC))
            })
            .unwrap_or_else(|| panic!("no rule queries {AOF_REPAIR_VERDICT_PUBLISHED_METRIC}"));
        let promql = rule["promql"].as_str().unwrap();
        assert!(
            promql.contains("== 0"),
            "the absence of a verdict is what the rule is for, got: {promql}"
        );
    }
}
