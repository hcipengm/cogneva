#!/bin/bash
# 宿主臂：在同一台机器上直接跑 cogneva eval-table，而不是起 Pod 跑。
#
# 为什么要有这一支：评测 Pod 里两个执行面都缺——容器面（buildah 在 Pod 的 overlayfs 上
# 叠不了 overlay，退到 vfs 之后还会撞 ApplyLayer 的权限），Pod 面（Pod 里没有 kubectl，
# service account 也建不了 Pod）。同一份代码在宿主上两面都在：`/usr/bin/buildah` 直接可跑，
# `sudo -n kubectl auth can-i create pods` 是 yes。所以「swe-bench-pro 与 toolathlon 两列
# 到底能判出分吗」这个问题只能在宿主上回答。
#
# 登记跑法不许改；这里只解四件 Pod 里有、宿主上没有的东西：
#   1. 配置：宿主没有 /etc/cogneva，从 configmap `cogneva-json` 导出。里面的
#      `base_url` 写的是服务名 `cogneva-security-gateway`，宿主的解析器不认识集群 DNS，
#      所以每次按 Service 当前的 ClusterIP 改写一次——不写死 IP，Service 重建了也不会读到旧值。
#   2. 二进制：共享工作树里有别的会话未提交的改动，直接 cargo build 会把它编进去。所以
#      默认从一个指定 rev 的干净 worktree 里取 release 产物。产物要落到 $HOME 下而不是
#      /tmp：干净 worktree 建在 /tmp 里，一次清理就把它连同 150 MB 的二进制一起带走，
#      而症状要等到下一次真跑才出现（报的是「没有可执行的二进制」，读起来像命令写错了）。
#   3. 路径：Pod 里是 /data 与 /eval，宿主上是 benchmarks 根与 eval-runs 目录。
#   4. 镜像源：宿主上没有 `~/.config/containers/auth.json`，默认的 `ghcr.io` 匿名取
#      manifest 就是 401；本地 store 里那几份镜像能跑，是因为它们是以别的主机名拉进来的，
#      而 ref 带主机名，`buildah from` 按 ref 名对不上就不会复用。所以凡是要跑本地没有的
#      实例，得给 `COGNEVA_EVAL_IMAGE_MIRROR` 一个匿名可读的镜像源（进程环境变量，
#      下面 exec 时会原样继承）。不给也不是静默失败：缺镜像的格子会 errored。
#
# 用法：eval-host-arm.sh [eval-table 的全部参数]，例如
#   eval-host-arm.sh --benchmarks swe-bench-pro --scaffolds gepa --seeds 0 \
#                    --concurrency-slot 1 --tools platform --out $HOME/eval-runs/x.md
#
# 可覆盖的环境变量：BIN / DATA_ROOT / RUNS / CONFIG / SRC（导出后未改写的原样配置，
# 缺省为 `<CONFIG>.src.json`） / NAMESPACE / SERVICE / SUBSET / LOCK（互斥锁文件） /
# KUBECTL_CMD（测试桩，给了就不探测本机 kubectl） / REFRESH_CONFIG=1（强制重新导出）。
set -u

NAMESPACE=${NAMESPACE:-cogneva}
SERVICE=${SERVICE:-cogneva-security-gateway}
DATA_ROOT=${DATA_ROOT:-/srv/cogneva/benchmarks}
RUNS=${RUNS:-$HOME/eval-runs}
CONFIG=${CONFIG:-$RUNS/.host-arm/cogneva.json}
SUBSET=${SUBSET:-$RUNS/seed20261011_n40.tsv}
BIN=${BIN:-$RUNS/.host-arm/bin/cogneva}
# 同一把 store 只许一个写者：锁文件跟着本跑活到退出。
LOCK=${LOCK:-${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/eval-host-arm.lock}
# 测试用桩：给了就直接用它，不再探测本机的 kubectl。
KUBECTL_CMD=${KUBECTL_CMD:-}

die() { printf 'eval-host-arm: %s\n' "$*" >&2; exit 2; }

kubectl_cmd() {
  if sudo -n kubectl version >/dev/null 2>&1; then printf 'sudo -n kubectl'
  elif kubectl version >/dev/null 2>&1; then printf 'kubectl'
  else printf ''; fi
}

[ -x "$BIN" ] || die "没有可执行的二进制：$BIN（先在一个干净 worktree 里 cargo build --release --bin cogneva）"
[ -d "$DATA_ROOT" ] || die "数据根不在：$DATA_ROOT"
[ -f "$RUNS/pins.tsv" ] || die "pins 不在：$RUNS/pins.tsv"

# buildah 的 store 是本机一把锁。别的写者（另一个评测跑、一次 pull、别人的构建）在跑时，
# 这一跑会卡在 `buildah from` 上**不报错、不超时、不退出**，checkpoint 一行不落，而现场
# 只剩一句 `cogneva starting:`。实测过一次：并发起的 pull 把 40 例那跑卡了十几分钟。
#
# 两层，缺一不可：
#   * `pgrep` 快照只看得见**已经在跑**的写者（构建、pull 这些不走本脚本的写者）；
#   * `flock` 是本脚本之间的互斥——快照管不了「两条臂同时起跑、彼此都还看不见对方」，
#     实测过一次：两条臂起跑那一刻 store 都是空的，两边都放行，跑到一半对撞，其中一条
#     静默停了 27 分钟（CPU 只用 25 秒、checkpoint 一字节没动）。锁的 fd 一直开到下面的
#     `exec`，与本跑同寿命，进程退出（含被杀）由内核释放。
if [ "${SKIP_BUILDAH_GUARD:-0}" != 1 ]; then
  others=$(pgrep -af 'buildah (pull|from|bud|run)' 2>/dev/null | head -3)
  if [ -n "$others" ]; then
    printf 'eval-host-arm: 这台机器上已经有 buildah 写者在跑，store 锁会让本次静默停摆：\n%s\n' "$others" >&2
    die "等它们结束再跑；确知可共存时用 SKIP_BUILDAH_GUARD=1 跳过这条检查"
  fi
fi
# 锁不在 `SKIP_BUILDAH_GUARD` 里面。那个开关的意思是「我知道宿主上那些 buildah 写者
# 可以共存」，它管不到本脚本之间——两条臂同时起跑时快照看不见对方，只有锁拦得住，
# 而它们往往相隔几十秒才撞上。开关把锁一起跳过，等于把唯一有效的那层关掉：实测
# （2026-10-19，门内并发跑别的套件，宿主上有 buildah）跳过开关的臂根本没走到改写配置，
# 测试却把「以 2 退出」读成了锁在起作用。跳过快照，锁照留。
mkdir -p "$(dirname "$LOCK")" || die "建不了锁目录：$(dirname "$LOCK")"
exec 9>"$LOCK" || die "打不开锁文件：$LOCK"
flock -n 9 || die "另一条 eval-host-arm 正持有 $LOCK：同一把 store 同时只许一个写者。等它退出再跑（这条不受 SKIP_BUILDAH_GUARD 影响，跳过它拦的正是重复写者）"

if [ -n "$KUBECTL_CMD" ]; then KUBECTL="$KUBECTL_CMD"; else KUBECTL="$(kubectl_cmd)"; fi
[ -n "$KUBECTL" ] || die "这台机器上没有可用的 kubectl（拿不到 Service 的 ClusterIP，也就改不了配置里的网关地址）"

ip=$($KUBECTL -n "$NAMESPACE" get svc "$SERVICE" -o jsonpath='{.spec.clusterIP}' 2>/dev/null)
case "$ip" in
  "" | None) die "读不到 $SERVICE 的 ClusterIP" ;;
esac

# 配置只在需要时重新导出：configmap 变了要跟上，但没必要每跑一次就覆盖一份正在被读的文件。
# **导出的这份必须保持原样**（`base_url` 里仍是服务名）：它是每次改写的地基。
# 就地改写会让「上次写进去的 IP」变成下一次的地基，服务重建换了 ClusterIP 之后
# 改写逻辑认不出旧 IP，臂会拿着一个不存在的地址去跑——症状是不报错地连不上或挂住。
mkdir -p "$(dirname "$CONFIG")"
SRC=${SRC:-$CONFIG.src.json}
if [ ! -f "$SRC" ] || [ "${REFRESH_CONFIG:-0}" = 1 ]; then
  $KUBECTL -n "$NAMESPACE" get cm cogneva-json -o jsonpath='{.data.cogneva\.json}' >"$SRC.tmp" \
    || die "导出 configmap cogneva-json 失败"
  mv "$SRC.tmp" "$SRC"
fi

# 服务名 → ClusterIP。只在 `base_url` 上改，别处的服务名（注释、说明）原样留着。
# 每次都从 `$SRC` 重新派生 `$CONFIG`，所以「上一次的 IP 还在不在里面」不需要判：
# 地基里只有服务名，改写永远以它为准。写临时文件再 rename，读的一方看不到半份配置。
python3 - "$SRC" "$CONFIG" "$ip" <<'PY' || die "改写 base_url 失败"
import json, os, sys
src, dst, ip = sys.argv[1], sys.argv[2], sys.argv[3]
with open(src, encoding='utf-8') as f:
    doc = json.load(f)
backends = (doc.get('llm_routing') or {}).get('backends') or []
if not backends:
    sys.exit('llm_routing.backends 是空的，这份配置不是评测要的那份')
changed = 0
for backend in backends:
    base = backend.get('base_url') or ''
    if 'cogneva-security-gateway' in base:
        backend['base_url'] = base.replace('cogneva-security-gateway', ip)
        changed += 1
if not changed:
    sys.exit(f'{src} 里没有任何 backend 指向服务名 cogneva-security-gateway；'
             '它可能是一份已经被改写过的配置——用 REFRESH_CONFIG=1 重新导出')
tmp = dst + '.tmp'
with open(tmp, 'w', encoding='utf-8') as f:
    json.dump(doc, f, ensure_ascii=False, indent=2)
os.replace(tmp, dst)
PY

exec env COGNEVA_CONFIG_PATH="$CONFIG" "$BIN" eval-table \
  --pins "$RUNS/pins.tsv" \
  --data-root "$DATA_ROOT" \
  --subset "$SUBSET" \
  --backbone via-security-gateway@1 \
  --judge via-security-gateway@1 \
  "$@"
