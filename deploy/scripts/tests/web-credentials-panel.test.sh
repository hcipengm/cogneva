#!/usr/bin/env bash
# check-web-credentials-panel.sh 的判据：代码平台那一格必须两家都报、且能在这里写。
#
# 这条门守的不是「有没有连上」，是**两家各自的连接状态有没有分开报出来**。它防的坏法
# 都是静默的：屏上有一格「已连接 github」，读起来像一切正常——而 Gitee 连没连根本没
# 报；粘贴令牌只写在浮层里，这一页读得到状态而写不进去；下拉里只写了 github，选 Gitee
# 也照写 github，两次都报成功。
#
# 夹具的形状是「按平台的两份读数 + 一行粘贴令牌」，然后逐段剪断，每剪一段必须红：
#   - 只读单一的 provider           → 另一家连没连在屏上不存在
#   - 粘贴令牌行没有下拉            → 一段令牌只能靠人猜着贴
#   - 下拉里只有一家                → 选另一家那天，令牌写进了错的那家
#   - 选中的平台没进请求            → 下拉形同虚设
#   - 写完了不说「写入 ≠ 生效」     → 人会把「写入被接受」当成「已连上」
#   - 三格里只有它没有右上入口      → 读者会以为这一格不用配
# 每段改动都先确认**落到了文件上**（否则「改动没生效」与「门没抓住」不可区分）。
#
# 最后一条断言是在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-credentials-panel.sh"
fail() { echo "FAIL: $*"; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fixture() {
  rm -rf "${work}/web"
  mkdir -p "${work}/web/src/api" "${work}/web/src/views"

  cat >"${work}/web/src/api/client.ts" <<'EOF'
export interface ContributionStatus {
  configured: boolean;
  provider: 'github' | 'gitee' | 'none';
  github?: { configured: boolean };
  gitee?: { configured: boolean };
}
EOF

  cat >"${work}/web/src/views/SettingsView.tsx" <<'EOF'
export function SettingsView() {
  const platforms = [
    { id: 'github', connected: contrib?.github?.configured ?? false },
    { id: 'gitee', connected: contrib?.gitee?.configured ?? false },
  ];
  return (
    <div>
      <header className="flex items-center gap-3 px-4 py-2">
        <h2 className="text-[12px]">模型上游</h2>
        <div className="ml-auto"><Button onClick={openLlmSetup}>配置模型</Button></div>
      </header>
      <header className="flex items-center gap-3 px-4 py-2">
        <h2 className="text-[12px]">上网检索与抓取</h2>
        <div className="ml-auto"><Button onClick={openWebSearch}>配置后备</Button></div>
      </header>
      <header className="flex items-center gap-3 px-4 py-2">
        <h2 className="text-[12px]">代码平台与凭据</h2>
        <div className="ml-auto"><Button onClick={openCredentials}>管理连接</Button></div>
      </header>
      <PatRow />
    </div>
  );
}

function PatRow() {
  return (
    <div>
      <span>粘贴令牌</span>
      <select value={platform} onChange={(e) => setPlatform(e.target.value)}>
        <option value="github">GitHub</option>
        <option value="gitee">Gitee</option>
      </select>
      <Button onClick={() => void connectContribution(platform, token)}>写入</Button>
      <span>写入 ≠ 生效：网关滚动换 Pod 之后，上面那张表读回来的才是新状态。</span>
    </div>
  );
}
EOF
}

confirm() {
  grep -qF "$2" "$1" || fail "改动没落到 ${1#"${work}"/}：找不到「$2」"
}

expect_green() {
  bash "${checker}" "${work}" >"${work}/green.out" 2>&1 \
    || { cat "${work}/green.out" >&2; fail "完整夹具：应当绿却红了"; }
}

expect_red() {
  local what="$1" needle="$2"
  local got=0
  bash "${checker}" "${work}" >"${work}/red.out" 2>&1 || got=$?
  [ "${got}" -ne 0 ] || { fail "${what}：应当红却绿了"; }
  grep -qF "${needle}" "${work}/red.out" \
    || { cat "${work}/red.out" >&2; fail "${what}：红了，但没点到「${needle}」"; }
}

fixture
expect_green

# 1. 只读单一的 provider：另一家连没连在屏上不存在。
fixture
sed -i 's/contrib?.gitee?.configured ?? false/contrib?.configured ?? false/' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" "contrib?.configured ?? false"
expect_red "只读 provider" "没有读 contrib?.gitee?.configured"

# 2. 类型里没有按平台的那一份。
fixture
sed -i '/gitee?: { configured: boolean };/d' "${work}/web/src/api/client.ts"
confirm "${work}/web/src/api/client.ts" "github?: { configured: boolean };"
expect_red "类型缺 gitee" "gitee?.configured"

# 3. 下拉里只有一家。
fixture
sed -i '/value="gitee"/d' "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" 'value="github"'
expect_red "下拉只有 github" "gitee 这个选项"

# 4. 选中的平台没进请求。
fixture
sed -i 's/connectContribution(platform, token)/connectContribution("github", token)/' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" 'connectContribution("github"'
expect_red "写死一家" "connectContribution(platform"

# 5. 写完了不说「写入 ≠ 生效」。
fixture
sed -i 's/写入 ≠ 生效：网关滚动换 Pod 之后，上面那张表读回来的才是新状态。/已写入。/' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" "已写入。"
expect_red "不写生效判据" "写入 ≠ 生效"

# 6. 凭据这一格没有右上入口。
fixture
sed -i 's/<Button onClick={openCredentials}>管理连接<\/Button>//' \
  "${work}/web/src/views/SettingsView.tsx"
confirm "${work}/web/src/views/SettingsView.tsx" "代码平台与凭据"
expect_red "凭据格没有入口" "openCredentials"

# 真树：今天必须绿，否则这条门守的是夹具、不是这棵树。
got=0
bash "${checker}" "${repo}" >"${work}/real.out" 2>&1 || got=$?
[ "${got}" -eq 0 ] || { cat "${work}/real.out" >&2; fail "真树上红了"; }

echo "web-credentials-panel.test: all checks passed"
