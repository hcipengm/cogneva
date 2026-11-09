#!/usr/bin/env bash
# check-web-workbench-links.sh 的判据：工作台每一格、每一盏灯点进去都得真有一页。
#
# 这条门守的坏法全都不出声——数据照常读、格子照常画，只是每次点都落到别处或者落空。
# 所以每一段剪一处必须红，而且红的是那一处；最后在真树上跑一遍。真树那一步是这条
# 测试自己的腿：门里的正则按现在的文件形状取数，形状一变它就取不到，取不到必须红，
# 而不是静静地放行（见末尾第 7 段）。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-workbench-links.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web" "${work}/crates"
  mkdir -p "${work}/web/src/shell" "${work}/web/src/views" "${work}/crates/cog-gateway/src"
  cat >"${work}/web/src/shell/store.ts" <<'EOF'
export type ActivityId =
  | 'workbench'
  | 'progress'
  | 'output'
  | 'capabilities';
EOF
  cat >"${work}/web/src/shell/ActivityBar.tsx" <<'EOF'
const ITEMS: { id: ActivityId; label: string; icon: IconName }[] = [
  { id: 'workbench', label: '工作台', icon: 'platform' },
  { id: 'progress', label: '进度', icon: 'platform' },
  { id: 'output', label: '产出', icon: 'output' },
  { id: 'capabilities', label: '能力', icon: 'capabilities' },
];
export function ActivityBar() {
  return <nav>{ITEMS.map((item) => <span key={item.id}>{item.label}</span>)}</nav>;
}
EOF
  cat >"${work}/web/src/views/registry.tsx" <<'EOF'
export function ViewFor({ view }: { view: string }) {
  switch (view) {
    case 'workbench':
      return <WorkbenchView />;
    case 'progress':
      return <ProgressView />;
    case 'output':
      return <OutputView />;
    case 'capabilities':
      return <CapabilitiesView />;
    default:
      return null;
  }
}
EOF
  cat >"${work}/web/src/views/WorkbenchView.tsx" <<'EOF'
const CELL_ICONS: Record<WorkbenchCellId, IconName> = {
  invest: 'token',
  output: 'output',
  capability: 'capabilities',
  project: 'projects',
};
EOF
  cat >"${work}/crates/cog-gateway/src/workbench.rs" <<'EOF'
pub struct Cell {
    pub href: String,
}

fn output_cell() -> Cell {
    Cell {
        href: "output",
    }
}

fn capability_cell() -> Cell {
    Cell {
        href: "capabilities",
    }
}
EOF
}

expect_red() {
  local name="$1" substr="$2" got=0
  bash "${checker}" "${work}" >"${work}/out" 2>&1 || got=$?
  [ "${got}" -eq 1 ] || { cat "${work}/out" >&2; fail "${name}: exit ${got}, wanted 1"; }
  grep -qF "${substr}" "${work}/out" || {
    cat "${work}/out" >&2
    fail "${name}: 判词里没有「${substr}」"
  }
}

expect_green() {
  local name="$1" got=0
  bash "${checker}" "${work}" >"${work}/out" 2>&1 || got=$?
  [ "${got}" -eq 0 ] || { cat "${work}/out" >&2; fail "${name}: exit ${got}, wanted 0"; }
}

confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#"${work}"/}：找不到「$2」"
}

fixture
expect_green "完整夹具"

# 1. href 指着一个不存在的活动 id。属主撞上的正是这一种：那盏灯写成 `deploy`，
#    而左栏那一面叫 `delivery`，点下去是一句「没有叫这一页」。
fixture
sed -i 's/href: "capabilities",/href: "deploy",/' "${work}/crates/cog-gateway/src/workbench.rs"
confirm "${work}/crates/cog-gateway/src/workbench.rs" 'href: "deploy",'
expect_red "href 指向不存在的面" '联合类型'

# 2. id 在联合类型里，但活动栏没有这一项：这一面在界面上没有入口，只能靠命令
#    面板撞见。
fixture
sed -i "/id: 'output', label: '产出'/d" "${work}/web/src/shell/ActivityBar.tsx"
if grep -q "label: '产出'" "${work}/web/src/shell/ActivityBar.tsx"; then
  fail "活动栏那一项没被删掉"
fi
expect_red "活动栏没有这一面" '活动栏名单'

# 3. id 在联合类型与活动栏里，但对照表没有 case：活动面切过去了，没有组件画。
fixture
sed -i "/case 'capabilities':/d" "${work}/web/src/views/registry.tsx"
if grep -q "case 'capabilities'" "${work}/web/src/views/registry.tsx"; then
  fail "对照表那一支没被删掉"
fi
expect_red "对照表没有这一面" '对照表'

# 4. 两格共用一个图标：这两件事在屏幕上长得一样，读者靠图标分格就会分错。
fixture
sed -i "s/  capability: 'capabilities',/  capability: 'output',/" \
  "${work}/web/src/views/WorkbenchView.tsx"
confirm "${work}/web/src/views/WorkbenchView.tsx" "  capability: 'output',"
expect_red "两格共用一个图标" '共用图标'

# 5. href 不是字面量：判不了它指向哪一面，门就不会在它指错时红——判不了的 href
#    与错的 href 在读数上一样。
fixture
sed -i 's/href: "output",/href: output_href,/' "${work}/crates/cog-gateway/src/workbench.rs"
confirm "${work}/crates/cog-gateway/src/workbench.rs" 'href: output_href,'
expect_red "href 不是字面量" '不是字面量'

# 6. 活动 id 的联合类型换了形状、取不到成员：门必须红着说「数不出来」，不许把
#    空集当全集，静静地把每一格都放行。
fixture
cat >"${work}/web/src/shell/store.ts" <<'EOF'
export const ACTIVITY_IDS = ['workbench', 'progress', 'output', 'capabilities'];
EOF
expect_red "联合类型换了形状" '读不出'

# 真树必须绿：夹具只证明判据认得自己，这一句才是「今天这棵树过得了」。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-workbench-links.test: all checks passed"
