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

/// `cogneva repair-aof <aof-dir>`: repair, report, exit.
///
/// A repair that dropped bytes still exits 0 — the point of running on the
/// startup path is that Redis comes up afterwards. The record of what was
/// dropped is the `level=warn` line, not the exit code.
pub fn run_from_args() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(2)
        .ok_or("usage: cogneva repair-aof <aof-dir>")?;
    let report = repair_dir(Path::new(&dir))?;
    let mut out = std::io::stdout();
    for line in report.lines() {
        writeln!(out, "{line}")?;
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
}
