#!/usr/bin/env bash
# 临时构建树回收器：判据在**两个读数的大小关系**上，不在水位线上。
#
# 这台机器 2026-11-23 被一棵临时工作树的 target 填到 0 MB：缓存回收器每半小时跑一次，
# 它跑的时候那棵构建还没开始。所以规则改成按「空闲空间 < 这些临时树自己占的空间」，
# 并在每次编译之前也问一遍。这一份判的就是那三条：
#   1. 空闲读数是注入的：小于临时树总和 ⇒ 无人持有的树被删；大于 ⇒ 一棵不动；
#   2. 有进程把 cwd 钉在树里 ⇒ 那棵树不删（其余照删）；
#   3. 没有 cargo 缓存标记的目录 ⇒ 从来不是候选，哪怕名字就叫 target；
#   4. dry-run 只报计划、不删；读数写 `planned` 而不是 `released`；
#   5. 空跑（没有候选）也必须留下一份读数，不能什么都不说；
#   6. 属主的进程读不出来 ⇒ 整跑拒绝释放，读数写 `unattributable` 并报出条数。
#
# 「读不出来的进程」这一读数是钉住的（见 run_gc）：哪些进程同一 uid 读不了是宿主
# 的属性，CI 的 runner 上就有，拿它判规则等于拿跑测试的机器判规则。
#
# 跑法：bash deploy/scripts/tests/host-scratch-gc.test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
gc="${repo}/deploy/scripts/host-scratch-gc.sh"

pass=0
fail=0
ok() { pass=$((pass + 1)); printf 'ok    - %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL- %s\n' "$1"; [ -n "${2:-}" ] && printf '        期望 %s\n' "$2"; [ -n "${3:-}" ] && printf '        实得 %s\n' "$3"; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 一棵看起来就是 cargo 产出的树。
make_cache() { # dir
    mkdir -p "$1"
    printf 'Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by cargo.\n' >"$1/CACHEDIR.TAG"
    printf '{"rustc_fingerprint":1}\n' >"$1/.rustc_info.json"
    head -c 200000 /dev/zero >"$1/blob"
}

run_gc() { # avail_kb [dry_run]
    COGNEVA_SCRATCH_GC_ROOTS="${work}/roots" \
        COGNEVA_SCRATCH_GC_STATE="${work}/state.json" \
        COGNEVA_SCRATCH_GC_DRY_RUN="${2:-0}" \
        COGNEVA_SCRATCH_GC_AVAIL_KB="$1" \
        COGNEVA_SCRATCH_GC_UNREADABLE_PIDS_FILE="${work}/unreadable" \
        bash "$gc" >"${work}/out" 2>&1
}

# 「属主的哪些进程读不出来」随宿主变：CI 的 runner 上就有这类进程，读到它会让
# 整跑拒绝释放，于是每一条关于大小关系的判据都读成 unattributable。这里把它也
# 钉住（默认为空＝这台机器没有读不出来的进程），第 6 条再单独喂一个给它。
: >"${work}/unreadable"

# 1) 空闲 < 临时树总和 ⇒ 删掉；空闲 ≥ 总和 ⇒ 留下。
roots="${work}/roots"
mkdir -p "$roots/one" "$roots/two"
make_cache "$roots/one/target"
make_cache "$roots/two/target"

run_gc 1
if [ -d "$roots/one/target" ] || [ -d "$roots/two/target" ]; then
    bad "空闲小于临时空间时两棵树都应被删" "两棵都不在" "$(ls "$roots")"
else
    ok "空闲小于临时空间：无人持有的树被删"
fi
grep -q '"outcome":"released"' "${work}/state.json" \
    && ok "读数写 released" || bad "读数应写 released" '"outcome":"released"' "$(cat "${work}/state.json")"

make_cache "$roots/one/target"
run_gc 999999999
[ -d "$roots/one/target" ] \
    && ok "空闲大于临时空间：一棵都不动" || bad "空闲充足时不该删" "树还在" "被删了"
grep -q '"outcome":"kept"' "${work}/state.json" \
    && ok "读数写 kept" || bad "读数应写 kept" '"outcome":"kept"' "$(cat "${work}/state.json")"

# 2) 有进程把 cwd 钉在树里 ⇒ 那一棵不删。
mkdir -p "$roots/held"
make_cache "$roots/held/target"
(
    cd "$roots/held/target" || exit 1
    sleep 30
) &
pinned=$!
# 等它真的进去。
for _ in $(seq 1 50); do
    [ "$(readlink "/proc/${pinned}/cwd" 2>/dev/null || true)" = "$(realpath "$roots/held/target")" ] && break
    sleep 0.1
done
run_gc 1
if [ -d "$roots/held/target" ]; then
    ok "被进程钉住的树不删"
else
    bad "被钉住的树不该删" "树还在" "被删了"
fi
kill "$pinned" 2>/dev/null || true
wait "$pinned" 2>/dev/null || true

# 3) 没有 cargo 标记的目录：名字叫 target 也不是候选。
rm -rf "$roots/held"
mkdir -p "$roots/plain/target"
head -c 1000 /dev/zero >"$roots/plain/target/blob"
run_gc 1
[ -d "$roots/plain/target" ] \
    && ok "没有缓存标记的目录从来不是候选" || bad "无标记目录不该被删" "目录还在" "被删了"

# 4) dry-run：只报计划，不删。
rm -rf "$roots/plain"
make_cache "$roots/dry/target"
run_gc 1 1
[ -d "$roots/dry/target" ] \
    && ok "dry-run 不删" || bad "dry-run 不该删" "树还在" "被删了"
grep -q '"outcome":"planned"' "${work}/state.json" \
    && ok "dry-run 读数写 planned" || bad "dry-run 读数应写 planned" '"outcome":"planned"' "$(cat "${work}/state.json")"

# 5) 空跑也留读数。
rm -rf "$roots"
mkdir -p "$roots"
run_gc 1
grep -q '"outcome":"nothing"' "${work}/state.json" \
    && ok "没有候选时也写一份读数" || bad "空跑应写 nothing" '"outcome":"nothing"' "$(cat "${work}/state.json")"

# 6) 属主的进程读不出来 ⇒ 整跑不释放，读数写 unattributable（归不了因不等于不在）。
rm -rf "$roots"
mkdir -p "$roots"
make_cache "$roots/blind/target"
printf '424242\n' >"${work}/unreadable"
run_gc 1
[ -d "$roots/blind/target" ] \
    && ok "进程读不出来时一棵都不删" || bad "归不了因时不该删" "树还在" "被删了"
grep -q '"outcome":"unattributable"' "${work}/state.json" \
    && ok "读数写 unattributable" || bad "读数应写 unattributable" '"outcome":"unattributable"' "$(cat "${work}/state.json")"
grep -q '"unreadable":1' "${work}/state.json" \
    && ok "读数报出读不出来的条数" || bad "读数应报 unreadable 的条数" '"unreadable":1' "$(cat "${work}/state.json")"
: >"${work}/unreadable"

printf '%s: %d passed, %d failed\n' "$(basename "${BASH_SOURCE[0]}")" "$pass" "$fail"
[ "$fail" -eq 0 ]
