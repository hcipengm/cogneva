#!/usr/bin/env bash
#
# `deploy/scripts/check-shared-test-db-counts.py` offline.
#
# Six shapes, one cell each:
#   1. a whole-table walk counted outright -- RED, named at file:line
#   2. the same shape with the table emptied first -- green
#   3. a read that names the probe -- green
#   4. counted after filtering down to this run's own rows -- green
#   5. the same shape in a file that never mentions COGNEVA_TEST_DATABASE_URL -- green
#   6. a collection the test built itself (no `.await`) -- green
# plus a seventh: a file that builds and drops its own schema -- green, and the
# exemption printed by name.
#
# The red case is the code that was actually red: `dataset_catalog.rs` before
# 7456c6bf, `let all = probe.catalog.all(10).await…; assert_eq!(all.len(), 2);`,
# which CI read as "left: 4, right: 2". Delete that shape from cell 1 and the
# check has nothing left to go red on.
#
# "Nothing judged" is not "nothing wrong": no shared-database file among the
# test files is exit 3, and so is a missing crates/ tree.
#
# Everything here is synthetic: the fake tree lives under mktemp -d, and the
# repository's own sources are not read.

set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="${here}/../check-shared-test-db-counts.py"

pass=0
fail=0
ok() { printf 'ok    - %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf 'FAIL- %s\n' "$1" >&2; fail=$((fail + 1)); }

[ -f "$check" ] || {
    bad "check-shared-test-db-counts.py is not at ${check}"
    exit 1
}

work="$(mktemp -d)" || exit 1
trap 'rm -rf "${work}"' EXIT
root="${work}/repo"

# One shared-database test file; only the body of the test changes per case.
shared_file() {
    cat <<EOF
//! against a live PostgreSQL
//!
//! COGNEVA_TEST_DATABASE_URL=postgres://t:t@127.0.0.1:5432/throwaway \\
//!   cargo test -p cog-storage --test demo -- --ignored

struct Probe {
    catalog: Catalog,
    name: String,
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_case() {
$1
}
EOF
}

tree() { # rebuild the fake tree, putting $1 into demo.rs (nothing when empty)
    rm -rf "${root}"
    mkdir -p "${root}/crates/cog-storage/tests"
    [ -n "${1}" ] && printf '%s\n' "$1" >"${root}/crates/cog-storage/tests/demo.rs"
    return 0
}

run() { python3 "$check" "${root}" 2>&1; }

# 1. whole-table count -- RED, named at file:line
tree "$(shared_file '    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 1 ] \
    && grep -q 'demo.rs:1[0-9] whole-table count with nothing narrowing it' < <(printf '%s' "$out") \
    && grep -q 'all(10)' < <(printf '%s' "$out"); then
    ok "1 whole-table count is red and names the read"
else
    bad "1 whole-table count should exit 1 and name file:line, got rc=${rc}: ${out}"
fi

# 2. emptied first -- green
tree "$(shared_file '    cleanup(&probe).await;
    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 1);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then
    ok "2 emptied before the count is green"
else
    bad "2 emptied first should be green, got rc=${rc}: ${out}"
fi

# 3. the read names the probe -- green
tree "$(shared_file '    let rows = probe.catalog.list(&probe.name, 10).await.expect("list");
    assert_eq!(rows.len(), 1);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then
    ok "3 a read that names the probe is green"
else
    bad "3 a scoped read should be green, got rc=${rc}: ${out}"
fi

# 4. counted after filtering to this run's rows -- green
tree "$(shared_file '    let all = probe.catalog.all(1000).await.expect("walk every dataset");
    let mine: Vec<_> = all.iter().filter(|e| e.name == probe.name).collect();
    assert_eq!(mine.len(), 2);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then
    ok "4 narrowed before counting is green"
else
    bad "4 a narrowed count should be green, got rc=${rc}: ${out}"
fi

# 5. a file that never mentions the shared database -- out of the population
tree "$(shared_file '    let rows = probe.catalog.list(&probe.name, 10).await.expect("list");
    assert_eq!(rows.len(), 1);')"
cat >"${root}/crates/cog-storage/tests/in_memory.rs" <<'EOF'
//! no database here
#[tokio::test]
async fn the_case() {
    let all = backend.all(10).await.unwrap();
    assert_eq!(all.len(), 2);
}
EOF
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'shared=1 ' < <(printf '%s' "$out"); then
    ok "5 only files that declare the shared database are in the population"
else
    bad "5 the in-memory file should stay out, got rc=${rc}: ${out}"
fi

# 6. a collection the test built itself -- green
tree "$(shared_file '    let xs = vec![1, 2];
    assert_eq!(xs.len(), 2);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ]; then
    ok "6 a locally built collection is green"
else
    bad "6 a local collection should be green, got rc=${rc}: ${out}"
fi

# 7. a file that builds and drops its own schema -- green, exemption printed
# This exemption was measured, not assumed: without it the check went red on 8
# assertions across 2 files whose stores are re-homed into a schema they create
# per run and drop on the way out.
tree "$(shared_file '    let schema = format!("probe_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}")).execute(&pool).await.unwrap();
    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE")).execute(&pool).await.unwrap();')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'CENSUS exempt .*demo.rs' < <(printf '%s' "$out"); then
    ok "7 an own-schema file is exempt and the exemption is printed by name"
else
    bad "7 own schema should be exempt and named, got rc=${rc}: ${out}"
fi

# 8. a whole-table read filtered in process before the count -- counted, not red
# Safe today only because the filter's key is unique per run, which the check
# cannot read; printing it keeps the shape visible without holding the tree to a
# semantic question.
tree "$(shared_file '    let all = probe.catalog.load_all().await.expect("load every row");
    let mine: Vec<_> = all.iter().filter(|c| c.category == probe.category()).collect();
    assert_eq!(mine.len(), 1);')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'CENSUS narrowed .*demo.rs' < <(printf '%s' "$out"); then
    ok "8 in-process narrowing is counted and green, not silent"
else
    bad "8 in-process narrowing should print CENSUS and stay green, got rc=${rc}: ${out}"
fi

# 9. no shared-database file among the test files -- nothing judged
tree '//! nothing shared here'
out="$(run)"; rc=$?
if [ "$rc" -eq 3 ] && grep -q 'judged nothing' < <(printf '%s' "$out"); then
    ok "9 no shared-database file is exit 3, not zero defects"
else
    bad "9 should exit 3 and say nothing was judged, got rc=${rc}: ${out}"
fi

# 10. no crate tree -- nothing judged
rm -rf "${root}"
out="$(run)"; rc=$?
if [ "$rc" -eq 3 ] && grep -q 'cannot judge' < <(printf '%s' "$out"); then
    ok "10 a missing crates/ tree is exit 3"
else
    bad "10 a missing tree should exit 3, got rc=${rc}: ${out}"
fi

# 11. a #[cfg(test)] module under src/ is in the population and judged
# `crates/*/tests/*.rs` is a directory, not a definition of "a test": the file
# this glob used to miss was the 35th of 35 shared-database files, and it
# printed nothing at all -- not even a denominator it was absent from.
tree "$(shared_file '    let rows = probe.catalog.list(&probe.name, 10).await.expect("list");
    assert_eq!(rows.len(), 1);')"
mkdir -p "${root}/crates/cog-observability/src"
cat >"${root}/crates/cog-observability/src/infra_watch.rs" <<'EOF'
//! no database here in the non-test build
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
    async fn the_case() {
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 2);
    }
}
EOF
out="$(run)"; rc=$?
if [ "$rc" -eq 1 ] \
    && grep -q 'RED  crates/cog-observability/src/infra_watch.rs:1[0-9]' < <(printf '%s' "$out") \
    && grep -q 'src_test_modules=1 ' < <(printf '%s' "$out"); then
    ok "11 a cfg(test) module under src/ is counted and judged"
else
    bad "11 a src test module should be judged and counted, got rc=${rc}: ${out}"
fi

# 12. a schema built through the support crate -- exempt
# The DDL moved into `cog-test-support`, so the file no longer spells
# `CREATE SCHEMA` anywhere and the exemption was earned by a string the file
# stopped containing: `own_schema=0` on a run where a dozen files do build their
# own schema, and eight whole-table counts judged as shared that are nobody
# else's rows.
tree "$(shared_file '    let schema = cog_test_support::own_schema(&url, "probe").await;
    let pool = cog_test_support::schema_pool(&url, &schema).await;
    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);
    cog_test_support::drop_schema(&url, &schema).await;')"
out="$(run)"; rc=$?
if [ "$rc" -eq 0 ] && grep -q 'CENSUS exempt .*demo.rs' < <(printf '%s' "$out"); then
    ok "12 a schema built through the support crate is exempt"
else
    bad "12 support-crate schema should be exempt, got rc=${rc}: ${out}"
fi

# 13. the same names written as *definitions* -- not an exemption
# A gate that lists the entry points it counts mentions `own_schema(` and
# `schema_pool(` without ever building a schema. Both halves have to be in the
# file for this cell to isolate the rule: an exemption needs a start and an end,
# and it is the pair that a pair of strings can forge.
tree "$(shared_file '    const COUNTED: &[&str] = &["pub async fn own_schema(", "pub async fn schema_pool("];
    let all = probe.catalog.all(10).await.expect("walk every dataset");
    assert_eq!(all.len(), 2);
    assert!(!COUNTED.is_empty());')"
out="$(run)"; rc=$?
if [ "$rc" -eq 1 ] && ! grep -q 'CENSUS exempt' < <(printf '%s' "$out"); then
    ok "13 naming a definition is not calling it"
else
    bad "13 a definition in a string is not an exemption, got rc=${rc}: ${out}"
fi

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[ "${fail}" -eq 0 ]
