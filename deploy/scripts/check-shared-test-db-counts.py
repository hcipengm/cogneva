#!/usr/bin/env python3
"""A test on the shared test database must not count the whole table.

Why this is a gate and not a review item: `COGNEVA_TEST_DATABASE_URL` points
every test in the suite at *one* database. A test that walks a table and
asserts the row count is therefore asserting something about the rest of the
suite, and about every earlier run that panicked before its own cleanup and
left its rows behind. The failure is not merely someone else's data showing up:
the run that trips it never reaches its own `cleanup`, so it leaves the rows
that made it fail, and every later run fails on the same number. Measured
2026-10-26 on CI: `every_dataset_walks_out_newest_first` read `left: 4, right:
2` and kept reading it.

The shape being judged, from that case as it stood before the repair:

    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);

What is judged, by shape rather than by method or table name:

  * Only files that run against the shared database -- the ones whose text
    mentions `COGNEVA_TEST_DATABASE_URL`. A test on an in-memory fake can walk
    everything it likes: its "whole table" is the batch it just made. Measured
    2026-10-26: 34 of the 202 `crates/*/tests/*.rs` files are shared-database;
    the 81 otherwise identical `.len()` assertions are all outside them
    (`read_since("sess-1", 0)` takes this run's own key as an argument).

  * The population is not `crates/*/tests/*.rs`, which is a directory and not a
    definition of a test. A `#[cfg(test)]` module under `src/` runs on the same
    database, and one of them does: it was the 35th of 35 shared-database files
    and the check printed nothing about it -- not even a denominator it was
    absent from. Those files are read only when they name the database, so the
    wider glob costs a filename scan and nothing else; `src_test_modules` in the
    summary line keeps the class in the count.

  * Files that build and drop their own schema are exempt as a whole: `CREATE
    SCHEMA` together with `DROP SCHEMA` (or a `search_path` pointed into it)
    means every table in the read is this run's own. Measured 2026-10-26: 6 of
    the 35 do this. The exemption was measured, not assumed -- without it the
    first version of this check went red on 8 assertions across 2 of those 5
    files, every one of them a false red. The exempted files are printed on
    every run: a population that widens in silence is read next time as "the
    check is still looking".

  * The assertion compares a row count against a literal:
    `assert_eq!(<root>.len(), N)` in either argument order.

  * `<root>` is bound by a `let` in the same function, and that binding's chain
    carries `.await` -- this is a read, not a vector the test built itself.

  * The read names no row: the call before `.await` (`.all(10)`,
    `.pending_intents(10)`) takes nothing but numeric literals. One argument
    that is a name, a key or `&probe.name` scopes the read already.

  * Nothing narrows the count: no `.filter(` / `.retain(` between the read and
    the `.len()`.

  * The table is not emptied first: no `cleanup(`, `DELETE FROM` or `TRUNCATE`
    anywhere before the assertion in the same function. Clear-then-count counts
    rows this run made.

All of those together make a red. Each violation is named with `file:line`, so
the repair is a jump, not a hunt.

Reported, not judged: a whole-table read narrowed in process before the count
(`load_all().await` then `.filter(|c| c.category == probe.category())`). It is
safe today only because the filter's key happens to be unique per run; whether
that stays true is a question about the key's value, which this cannot read. So
it is counted and printed on every run, with its `file:line`, and it does not
turn anything red. Measured 2026-10-26: two sites, `decision_stats_store.rs`
and the repaired `dataset_catalog.rs`, whose filter keys compare against a
per-run uuid.

What this deliberately does not catch:

  * A test that narrows inside the predicate (`assert!(all.iter().all(|r| r.name
    == own_name))`) is scoped correctly and is not seen as scoped here — there is
    no `.filter` to find, so it is not even counted.

    Widening the judged shape past `.len()` was measured, not waved off. Loose
    version -- zero-argument read, an assertion anywhere after it: 89 lines
    across the population. Sharpened to "the assertion itself names that
    variable and uses `.iter()` / `.is_empty()` / `vec![`": 9 lines, and two of
    those nine are not reads at all (`to_string()`, `read_latest_drill()`), because
    "takes no arguments" is not "walks a table". The ones that are reads are
    already scoped by construction -- `change_ledger_store.rs` narrows through a
    `marker` prefix inside an `order()` closure before asserting on the `vec!`.
    A census that prints nine lines of which two are noise is read once and
    skipped after that, so the shape stays prose.
  * `assert!(all(0).is_empty())` is not judged: `LIMIT 0` returning no rows is
    the interface's contract and has nothing to do with how full the table is.
    Only "row count equals a literal" is judged.
  * The schema exemption is per file, not per test. A file with one
    schema-building helper exempts all of its tests, including any that do read
    the shared tables. Per-test would mean following the call that builds the
    schema, which the text does not give.
  * `.filter(` is matched by position, not by data flow: a function that reads
    the whole table, then filters a different variable, then counts the first,
    is passed over. A false negative is cheaper here than a false red.
  * A count on a field chain (`self.rows.len()`) has no `let` to resolve and is
    not judged.
  * Sharing some other service (a Redis, a queue) without saying
    `COGNEVA_TEST_DATABASE_URL` is out of scope: the population is decided by
    what the file itself declares, not guessed.
  * The population condition is "these bytes appear in this file". A test that
    takes its pool from a shared helper -- `tests/common/`, a crate-local
    fixture -- without naming the variable drops out of the population, and
    nothing in the summary would show it: `test_files` and `src_test_modules`
    count what the globs found, not what they missed. Today every shared-database
    file names the variable itself, so the count is whole; the trigger for
    widening the rule is the second test that gets its pool by inheritance, not
    the first.

Exit codes: 0 nothing to report; 1 violations named one by one; 3 nothing was
judged (no test files, or no shared-database file among them).
"""

import re
import sys
from pathlib import Path

SHARED = "COGNEVA_TEST_DATABASE_URL"

# `assert_eq!(<root>.len(), N)` and `assert_eq!(N, <root>.len())`.
# Only "row count equals a literal" -- not `assert_ne!`, not `is_empty()`.
LEN_EQ = re.compile(
    r"assert_eq!\s*\(\s*(?:"
    r"(?P<root_a>[A-Za-z_]\w*)\.len\(\)\s*,\s*(?P<n_a>\d+)"
    r"|"
    r"(?P<n_b>\d+)\s*,\s*(?P<root_b>[A-Za-z_]\w*)\.len\(\)"
    r")\s*[,)]"
)
CLEAN = re.compile(r"\bcleanup\s*\(|\bDELETE\s+FROM\b|\bTRUNCATE\b")
# A file that makes its own schema and takes it away again owns every table its
# reads can see, so a whole-table count there is this run's own rows.
OWN_SCHEMA = re.compile(r"\bCREATE\s+SCHEMA\b")
OWN_SCHEMA_END = re.compile(r"\bDROP\s+SCHEMA\b|\bsearch_path\b")
NARROW = re.compile(r"\.(?:filter|retain)\s*\(")
CFG_TEST = re.compile(r"#!?\[cfg\(test\)\]")
FN = re.compile(r"\bfn\s+(\w+)")


def fn_bodies(text):
    """(start, end, name) for each `fn name { ... }`, by brace counting."""
    out = []
    for m in FN.finditer(text):
        b = text.find("{", m.end())
        if b < 0:
            continue
        depth = 0
        i = b
        while i < len(text):
            c = text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        out.append((m.start(), i, m.group(1)))
    return out


def binding(body, ident):
    """The last `let <ident> ... = <expr>;` in the body: (expr, end offset)."""
    ms = list(
        re.finditer(
            r"\blet\s+(?:mut\s+)?" + re.escape(ident) + r"\b[^=;]*=\s*(.+?);", body, re.S
        )
    )
    if not ms:
        return None, None
    return ms[-1].group(1), ms[-1].end()


def read_call(init):
    """The last `.method(args)` before `.await`: (name, args)."""
    head = init.split(".await")[0]
    calls = re.findall(r"\.(\w+)\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)", head)
    return calls[-1] if calls else (None, None)


def only_limits(args):
    """True when every argument is a numeric literal -- the read names no row.

    No arguments at all counts: `load_all()` is a whole-table read in the
    strongest form, and treating "takes nothing" as "not a limit" would let the
    shape through by the most direct route.
    """
    args = args.strip()
    if not args:
        return True
    for a in args.split(","):
        a = a.strip()
        if not a:
            continue
        if not re.fullmatch(r"\d+|0", a):
            return False
    return True


def awaited_read(body, root):
    """The awaited read behind `<root>`: (init, end offset) or None.

    One level of indirection is followed, because the shape worth counting is
    exactly that one: `let cells = store.load_all().await; let mine = cells
    .into_iter().filter(..).collect(); assert_eq!(mine.len(), 2)` reads the
    whole table and narrows it in process. Deeper chains are not followed --
    one hop is where the shape lives, and following more turns this into a data
    flow analysis that text cannot carry.
    """
    init, end = binding(body, root)
    if init is None:
        return None
    if ".await" in init:
        return init, end
    for ident in dict.fromkeys(re.findall(r"\b([a-z_]\w*)\b", init)):
        if ident == root:
            continue
        sub, sub_end = binding(body, ident)
        if sub is not None and ".await" in sub:
            return sub, sub_end
    return None


def judge(text, shown):
    """Yield (severity, message) per whole-table count in this file.

    `RED` is a violation. `CENSUS` is the same read narrowed in-process before
    the count: it is safe today only because the filter's key is unique per run,
    and nothing here can tell whether that is still true, so it is counted and
    printed rather than judged.
    """
    for start, end, _name in fn_bodies(text):
        body = text[start:end]
        for m in LEN_EQ.finditer(body):
            root = m.group("root_a") or m.group("root_b")
            found = awaited_read(body, root)
            if found is None:
                continue  # built here, not read
            init, read_end = found
            method, args = read_call(init)
            if method is None or not only_limits(args):
                continue  # the read already names what it wants
            line = text[: start + m.start()].count("\n") + 1
            where = f"`{root}` comes from `{method}({args.strip()})`"
            if NARROW.search(body[read_end : m.start()]):
                yield (
                    "CENSUS",
                    f"CENSUS narrowed {shown}:{line} whole-table read, filtered in "
                    f"process before the count: {where}",
                )
                continue
            if CLEAN.search(body[: m.start()]):
                continue  # emptied first, so the count is this run's own
            yield (
                "RED",
                f"RED  {shown}:{line} whole-table count with nothing narrowing it: {where}",
            )


def main(argv):
    root = Path(argv[1]) if len(argv) > 1 and not argv[1].startswith("-") else None
    if root is None:
        root = Path(__file__).resolve().parents[2]
    crates = root / "crates"
    if not crates.is_dir():
        print(
            f"check-shared-test-db-counts: cannot judge, no crates/ under {root} "
            "-- this run judged nothing.",
            file=sys.stderr,
        )
        return 3

    files = sorted(crates.glob("*/tests/*.rs"))
    texts = {p: p.read_text(encoding="utf-8", errors="replace") for p in files}
    # A `#[cfg(test)]` module under src/ runs on the same database and was not in
    # the population at all until this became visible: `crates/*/tests/*.rs` is a
    # directory, not a definition of "a test", and the shared-database file it
    # missed was the 35th of 35. Only src files that name the database are read,
    # so the glob can be wide without paying for the whole tree.
    src_modules = []
    for p in sorted(crates.glob("*/src/**/*.rs")):
        t = p.read_text(encoding="utf-8", errors="replace")
        if SHARED in t and CFG_TEST.search(t):
            src_modules.append(p)
            texts[p] = t
    files = files + src_modules
    shared = [p for p in files if SHARED in texts[p]]
    if not shared:
        print(
            f"check-shared-test-db-counts: cannot judge, none of the {len(files)} test "
            f"files mentions {SHARED} -- this run judged nothing (that is not zero "
            "defects).",
            file=sys.stderr,
        )
        return 3

    own = [p for p in shared if OWN_SCHEMA.search(texts[p]) and OWN_SCHEMA_END.search(texts[p])]
    reds = []
    narrowed = []
    for p in shared:
        if p in own:
            continue
        shown = p.relative_to(root)
        for severity, message in judge(texts[p], shown):
            (reds if severity == "RED" else narrowed).append(message)

    for v in reds:
        print(v)
        print(
            "     repair: narrow the read to the rows this run wrote (filter on its own "
            "name), or empty the table before counting -- the database is shared with "
            "every other test and with any earlier run that died before its cleanup"
        )
    for v in narrowed:
        print(v)
    for p in own:
        print(f"CENSUS exempt {p.relative_to(root)}: builds and drops its own schema")
    print(
        f"check-shared-test-db-counts: test_files={len(files) - len(src_modules)} "
        f"src_test_modules={len(src_modules)} shared={len(shared)} "
        f"own_schema={len(own)} judged={len(shared) - len(own)} "
        f"narrowed={len(narrowed)} violations={len(reds)}"
    )
    return 1 if reds else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
