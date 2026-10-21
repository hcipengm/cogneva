#!/usr/bin/env bash
# check-web-alert-panel.sh 的判据：站内告警中心必须有一格**从入口可达、被渲染、
# 同时接上列表与确认回调**的界面，且确认这一半在服务端闭合（写进表、读得出、
# 重新 firing 会清）。
#
# 要防的坏法全是静默的：队列里有行、路由在、契约测试绿，而
#   面板消失、按钮接空回调、确认写了列表不读（刷新又变未确认）、
#   重新 firing 不清旧标记（上次发作的确认盖在新一次上）
# 都不会让别的检查失败。每一条都在夹具上**只剪一处**，然后断言它红的正是那一处。
#
# 每段改动都先确认**落到了文件上**，否则「改动没生效」与「门没抓住」不可区分。
# 最后一条在**真树**上跑：夹具全绿而真树红，说明这条门只是在测自己。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-web-alert-panel.sh"
fail() { echo "FAIL: $*"; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"
command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：一条从入口到告警面板的链 ──
fixture() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/web/src/api" "${work}/root/web/src/hooks" \
    "${work}/root/web/src/overlay" "${work}/root/crates/cog-gateway/src" \
    "${work}/root/crates/cog-observability/src" "${work}/root/deploy/scripts"
  cp "${checker}" "${work}/root/deploy/scripts/"

  cat >"${work}/root/web/src/main.tsx" <<'EOF'
import App from './App';
render(<App />);
EOF
  cat >"${work}/root/web/src/App.tsx" <<'EOF'
import { useAlerts } from './hooks/useAlerts';
import { Panel } from './overlay/Panel';

export default function App() {
  const alerts = useAlerts();
  return <Panel alerts={alerts.alerts} acknowledge={alerts.acknowledge} />;
}
EOF
  cat >"${work}/root/web/src/hooks/useAlerts.ts" <<'EOF'
import { acknowledgeAlert, getActiveAlerts } from '@/api/client';

export function useAlerts() {
  const ack = async (id: string) => {
    const recorded = await acknowledgeAlert(id);
    return recorded.acknowledged_at;
  };
  const load = () => getActiveAlerts();
  return { acknowledge: ack, alerts: load() };
}
EOF
  cat >"${work}/root/web/src/overlay/Panel.tsx" <<'EOF'
export function Panel({
  alerts,
  acknowledge,
}: {
  alerts: unknown;
  acknowledge: (id: string) => void;
}) {
  return <button onClick={() => acknowledge('k')}>{String(alerts)}</button>;
}
EOF
  cat >"${work}/root/web/src/api/client.ts" <<'EOF'
export interface ActiveAlert {
  id: string;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
}
export async function getActiveAlerts(): Promise<ActiveAlert[]> {
  return api.get('/api/v1/alerts/active');
}
export async function acknowledgeAlert(id: string): Promise<{ acknowledged_at: string }> {
  return api.post(`/api/v1/alerts/${encodeURIComponent(id)}/ack`, {});
}
EOF

  cat >"${work}/root/crates/cog-gateway/src/lib.rs" <<'EOF'
        .route("/api/v1/alerts/active", get(alerts_active_handler))
        .route("/api/v1/alerts/{id}/ack", post(alerts_ack_handler))
EOF
  cat >"${work}/root/crates/cog-gateway/src/alert_store.rs" <<'EOF'
pub struct AlertEntry {
    pub acknowledged_at: Option<String>,
    pub acknowledged_by: Option<String>,
}
EOF
  cat >"${work}/root/crates/cog-observability/src/alert_store.rs" <<'EOF'
pub const DDL: &str = r#"
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ
;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS acknowledged_by TEXT
;
"#;
SELECT_ACTIVE
SELECT_HISTORY
impl PostgresAlertStore {
    pub async fn set_alert(&self) -> anyhow::Result<()> {
        match decide() {
            AlertTransition::Fired => {
                INSERT INTO alerts
                    ON CONFLICT (dedup_key) DO UPDATE SET
                        acknowledged_at = NULL,
                        acknowledged_by = NULL
                Ok(AlertTransition::Fired)
            }
        }
    }
    pub async fn ack(&self) -> anyhow::Result<()> { Ok(()) }
}
EOF
  cat >"${work}/root/crates/cog-gateway/src/route_user.rs" <<'EOF'
        let acknowledged = "acknowledge_alert";
EOF
}

selects() {
  python3 - "${work}/root/crates/cog-observability/src/alert_store.rs" "$1" <<'PY'
import sys
p, which = sys.argv[1], sys.argv[2]
src = open(p, encoding="utf-8").read()
sel = (
    '        "SELECT id, rule, dedup_key, severity, state, message, labels, \\\n'
    "             fired_at, resolved_at, updated_at, last_seen_at, \\\n"
    '             acknowledged_at, acknowledged_by FROM alerts \\\n'
)
src = src.replace("SELECT_ACTIVE" if which == "a" else "SELECT_HISTORY", sel)
open(p, "w", encoding="utf-8").write(src)
PY
}

fixture
selects a
selects b
# acknowledge_alert 那句在网关 lib.rs 里（真树如此），夹具里同样放一份。
printf 'async fn h() { source.acknowledge_alert(&k, &u).await }\n' \
  >>"${work}/root/crates/cog-gateway/src/lib.rs"

run_gate() { bash "${work}/root/deploy/scripts/check-web-alert-panel.sh" "${work}/root" 2>&1; }

# 0) 夹具绿
out="$(run_gate)" || fail "夹具应当绿，实际红：${out}"
grep -q "^ok：" <<<"${out}" || fail "绿时没有报出判词：${out}"

STORE="crates/cog-observability/src/alert_store.rs"
APP="web/src/App.tsx"
HOOK="web/src/hooks/useAlerts.ts"
GW="crates/cog-gateway/src/lib.rs"

# 1) 面板不再接确认回调：列表还在，按钮那一半没了
python3 - "${work}/root/${APP}" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
old = " acknowledge={alerts.acknowledge}"
assert old in s, "夹具里没有可剪的那一段，用例失效"
open(p, "w", encoding="utf-8").write(s.replace(old, "", 1))
PY
out="$(run_gate)" && fail "面板不接确认回调时应当红，实际绿：${out}"
grep -q "没有一格渲染出来的组件同时接到 alerts= 与 acknowledge=" <<<"${out}" \
  || fail "没报出列表与确认没落在同一屏：${out}"

# 2) 两处 SELECT 少一列：确认写了，读的那一面看不见
fixture
selects a
python3 - "${work}/root/crates/cog-observability/src/alert_store.rs" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
old = ("             fired_at, resolved_at, updated_at, last_seen_at, \\\n"
       "             acknowledged_at, acknowledged_by FROM alerts \\\n")
assert old in s, "夹具里没有第二处 SELECT，用例失效"
open(p, "w", encoding="utf-8").write(s.replace(old, "             fired_at FROM alerts \\\n", 1))
PY
printf 'async fn h() { source.acknowledge_alert(&k, &u).await }\n' \
  >>"${work}/root/${GW}"
out="$(run_gate)" && fail "SELECT 少一列时应当红，实际绿：${out}"
grep -q "清单的 SELECT 里没有 acknowledged_at" <<<"${out}" \
  || fail "没报出读的那一面丢了标记：${out}"

# 3) 新 firing 边不清标记：上次发作的确认盖在新一次上
fixture
selects a
selects b
printf 'async fn h() { source.acknowledge_alert(&k, &u).await }\n' \
  >>"${work}/root/${GW}"
python3 - "${work}/root/${STORE}" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
old = "                        acknowledged_at = NULL,\n                        acknowledged_by = NULL\n"
assert old in s, "夹具里没有清标记那一段，用例失效"
open(p, "w", encoding="utf-8").write(s.replace(old, "", 1))
PY
out="$(run_gate)" && fail "重新 firing 不清标记时应当红，实际绿：${out}"
grep -q "新 firing 边没有清掉 acknowledged_at" <<<"${out}" \
  || fail "没报出旧确认会盖在新一次上：${out}"

# 4) 网关没登记路由：界面点下去只有 404
fixture
selects a
selects b
printf 'async fn h() { source.acknowledge_alert(&k, &u).await }\n' \
  >>"${work}/root/${GW}"
python3 - "${work}/root/${GW}" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
old = '        .route("/api/v1/alerts/{id}/ack", post(alerts_ack_handler))\n'
assert old in s, "夹具里没有路由登记那一行，用例失效"
open(p, "w", encoding="utf-8").write(s.replace(old, "", 1))
PY
out="$(run_gate)" && fail "没登记确认路由时应当红，实际绿：${out}"
grep -q "没有登记 /api/v1/alerts/{id}/ack" <<<"${out}" \
  || fail "没报出界面点下去只有 404：${out}"

# 5) 确认函数没人调用：路由与面板都在，点下去什么也不发生
fixture
selects a
selects b
printf 'async fn h() { source.acknowledge_alert(&k, &u).await }\n' \
  >>"${work}/root/${GW}"
python3 - "${work}/root/${HOOK}" <<'PY'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-8").read()
old = "const recorded = await acknowledgeAlert(id);"
assert old in s, "夹具里没有确认调用点，用例失效"
open(p, "w", encoding="utf-8").write(s.replace(old, "const recorded = { acknowledged_at: '' };", 1))
PY
out="$(run_gate)" && fail "确认函数没人调用时应当红，实际绿：${out}"
grep -q "没有任何一处调用 acknowledgeAlert" <<<"${out}" \
  || fail "没报出确认没有活的消费者：${out}"

# 6) 真树：夹具全绿而真树红，说明这条门只在测自己
out="$(bash "${checker}" "${repo}" 2>&1)" || fail "真树应当绿，实际红：${out}"
grep -q "^ok：" <<<"${out}" || fail "真树绿时没有报出判词：${out}"

echo "OK: check-web-alert-panel 定向测试通过（1 绿 + 5 红，各点名一处；真树绿）"
