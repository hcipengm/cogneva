#!/usr/bin/env bash
# check-live-frames-have-producers.sh 的判据：面折进来的帧要有生产者，而**门自己不能失明**。
#
# 第二种是这里的主要题目。这条门曾经把「文件里第一处 `#[cfg(test)]` 到文件尾」当成测试区，
# 在「测试总在文件末尾」的写法下成立；一旦某个文件在**生产代码中间**放一个测试专用的条目，
# 那一条以下的生产代码就全落进测试区，门对它们失明——判词说「四个面没有一个生产者」，
# 而生产代码一行没动。2026-11-11 实测过一次（`5534632b` 的 CI），代价是一轮流水线与一次
# 误判方向。所以夹具把测试条目放在构造点**上面**，门必须仍找得到生产者。
#
# 两段都先确认改动落到了文件上：「改动没生效」与「门没抓住」在读数上是同一个样子。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
checker="${repo}/deploy/scripts/check-live-frames-have-producers.sh"
fail() { echo "FAIL: $*" >&2; exit 1; }

[ -x "${checker}" ] || fail "not executable: ${checker}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ── 夹具：一个面吃一帧，它的生产者在测试条目之后 ──
fixture() {
  rm -rf "${work}/web" "${work}/crates"
  mkdir -p "${work}/web/src/store" "${work}/crates/cog-agent/src"
  cat > "${work}/web/src/store/liveAgents.ts" <<'TS'
export function fold(event: { type: string }) {
  switch (event.type) {
    case 'agent_start':
      return event;
    default:
      return null;
  }
}
TS
  cat > "${work}/crates/cog-agent/src/runtime.rs" <<'RS'
impl AgentRuntime {
    pub fn new() -> Self {
        Self::with_observable()
    }
}

/// 测试专用的缝，放在生产代码中间——正是让旧判据失明的那一种位置。
#[cfg(test)]
impl AgentRuntime {
    pub(crate) fn new_with_observable() -> Self {
        Self::with_observable()
    }
}

impl AgentRuntime {
    fn with_observable() -> Self {
        self.emit_event(AgentEvent::AgentStart { agent_id });
        Self {}
    }
}
RS
}

fixture
if ! out="$(bash "${checker}" "${work}" 2>&1)"; then
  printf '%s\n' "${out}" >&2
  fail "测试条目之后的构造点没有被读到：门按「第一处 #[cfg(test)] 到文件尾」判，这一段生产代码对它失明了"
fi
grep -q 'crates/cog-agent/src/runtime.rs:' <<<"${out}" \
  || fail "门放行了，却没有点名那个构造点的位置：${out}"

# ── 把构造点删掉，同一个夹具必须红 ──
before="$(grep -c 'AgentEvent::AgentStart' "${work}/crates/cog-agent/src/runtime.rs")"
sed -i '/AgentEvent::AgentStart/d' "${work}/crates/cog-agent/src/runtime.rs"
after="$(grep -c 'AgentEvent::AgentStart' "${work}/crates/cog-agent/src/runtime.rs" || true)"
[ "${before}" -eq 1 ] && [ "${after}" -eq 0 ] \
  || fail "改动没落到文件上：删除前 ${before} 处，删除后 ${after} 处"
if bash "${checker}" "${work}" >/dev/null 2>&1; then
  fail "构造点删掉了，门还是绿的：它判的已经不是「有没有生产者」"
fi

# ── 真树：夹具全绿而真树红，说明这条门只是在测自己 ──
bash "${checker}" "${repo}" >/dev/null || fail "真树在这条门上不绿"

echo "ok   - 测试条目不会让门失明；删掉生产者它会红；真树绿"
