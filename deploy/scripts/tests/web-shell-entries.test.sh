#!/usr/bin/env bash
# check-web-shell-entries.sh 的判据：问号、模型、设置三条路都得真的通。
#
# 这条门守的坏法全都是静默的：图标在、位置对，点下去什么都不发生；问号打开的是
# 动作面板；「帮助」条目落不到任何去处；模型入口绕一圈先跳设置页；状态点不说
# 「这是哪一刻的读数」。每剪一段必须红，最后在真树上跑一遍。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-shell-entries.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web/src/shell" "${work}/web/src/views"
  cat >"${work}/web/src/shell/TitleBar.tsx" <<'EOF'
export function TitleBar() {
  const { llm, overlays } = useServices();
  const configured = llm.status?.configured;
  const readAt = llm.at;
  return (
    <header>
      <button onClick={overlays.openLlmSetup} title={`模型 · 读于 ${readAt}`}>
        {configured ? '已配置' : '未配置'}
      </button>
      <button onClick={() => setActivity('settings')} title="设置">设置</button>
      <button onClick={() => setPalette('help')} title="去哪找什么">?</button>
    </header>
  );
}
EOF
  cat >"${work}/web/src/shell/Palette.tsx" <<'EOF'
export function Palette({ mode }: { mode: string }) {
  const help = useMemo<Command[]>(() => {
    const go = (id: string, title: string): Command => ({
      id: `help:${id}`,
      group: '帮助',
      title,
      run: () => setActivity(id),
    });
    return [
      { id: 'help:llm', group: '帮助', title: '模型上游在哪配？', run: openLlmSetup },
      go('observability', '集群现在什么状态？'),
      go('tasks', '这件事排到哪一步了？'),
    ];
  }, []);
  const source = mode === 'help' ? help : commands;
  return <input value={source.filter(Boolean).length} />;
}
EOF
  cat >"${work}/web/src/views/SettingsView.tsx" <<'EOF'
export function SettingsView() {
  const { overlays } = useServices();
  return (
    <section>
      <Readings source="GET /api/v1/llm/status">
        <span>{overlays ? '模型上游' : ''}</span>
      </Readings>
      <Button onClick={overlays.openLlmSetup}>配置模型</Button>
    </section>
  );
}
EOF
  cat >"${work}/web/src/shell/ActivityBar.tsx" <<'EOF'
const items = [
  { id: 'workspace', title: '工作台' },
  { id: 'settings', title: '设置' },
];
export function ActivityBar() {
  return <nav>{items.map((item) => <span key={item.id}>{item.title}</span>)}</nav>;
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

# 1. 问号打开的是动作面板。
fixture
sed -i "s/setPalette('help')/setPalette('commands')/" "${work}/web/src/shell/TitleBar.tsx"
confirm "${work}/web/src/shell/TitleBar.tsx" "setPalette('commands')"
expect_red "问号不开帮助" "没有调 setPalette('help')"

# 2. 帮助面板没有自己的一份源。
fixture
sed -i "s/mode === 'help' ? help : commands/commands/" "${work}/web/src/shell/Palette.tsx"
confirm "${work}/web/src/shell/Palette.tsx" "const source = commands;"
expect_red "帮助沿用动作面板" "帮助没有自己的一份条目"

# 3. 有帮助条目没有去处。
fixture
python3 - "${work}/web/src/shell/Palette.tsx" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
src = src.replace("{ id: 'help:llm', group: '帮助', title: '模型上游在哪配？', run: openLlmSetup },",
                  "{ id: 'help:llm', group: '帮助', title: '模型上游在哪配？' },")
src = src.replace("      run: () => setActivity(id),\n", "")
src = src.replace("const go = (id: string, title: string): Command => ({\n      id: `help:${id}`,\n      group: '帮助',\n      title,\n    });",
                  "const go = (id: string, title: string): Command => ({\n      id: `help:${id}`,\n      group: '帮助',\n      title,\n    });")
open(path, "w", encoding="utf-8").write(src)
PY
if grep -q "run:" "${work}/web/src/shell/Palette.tsx"; then fail "run 没被剪干净"; fi
expect_red "条目没有去处" "没有去处"

# 4. 模型入口绕一圈先跳设置页。
fixture
sed -i "s/onClick={overlays.openLlmSetup}/onClick={() => setActivity('settings')}/" \
  "${work}/web/src/shell/TitleBar.tsx"
confirm "${work}/web/src/shell/TitleBar.tsx" "setActivity('settings')"
expect_red "模型入口不直达" "顶栏没有直达模型配置的入口"

# 5. 状态点不说读数时刻。
fixture
sed -i 's/ · 读于 ${readAt}//' "${work}/web/src/shell/TitleBar.tsx"
sed -i 's/const readAt = llm.at;//' "${work}/web/src/shell/TitleBar.tsx"
if grep -q "llm.at" "${work}/web/src/shell/TitleBar.tsx"; then fail "读数时刻没被剪掉"; fi
expect_red "不写读数时刻" "没有读数时刻"

# 6. 入口不读配置状态。
fixture
sed -i "s/const configured = llm.status?.configured;/const configured = true;/" \
  "${work}/web/src/shell/TitleBar.tsx"
confirm "${work}/web/src/shell/TitleBar.tsx" "const configured = true;"
expect_red "不读配置状态" "没有读 llm.status.configured"

# 7. 设置面没有模型入口。
fixture
sed -i 's#<Button onClick={overlays.openLlmSetup}>配置模型</Button>#<span>模型</span>#' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" "<span>模型</span>"
expect_red "设置面没有模型入口" "设置面没有模型配置入口"

# 8. 设置面的模型状态没有来源。
fixture
sed -i 's#source="GET /api/v1/llm/status"#source="模型状态"#' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" 'source="模型状态"'
expect_red "模型状态没有来源" "没有写出来源"

# 9. 活动栏里没有设置。
fixture
sed -i "/id: 'settings'/d" "${work}/web/src/shell/ActivityBar.tsx"
if grep -q "settings" "${work}/web/src/shell/ActivityBar.tsx"; then fail "设置项没被删掉"; fi
expect_red "活动栏没有设置" "活动栏里没有设置这一项"

# 真树必须绿。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-shell-entries.test: all checks passed"
