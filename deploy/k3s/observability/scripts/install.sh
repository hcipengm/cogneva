#!/usr/bin/env bash
# Cogneva 可观测性栈一键安装脚本
#
# 用法:
#   ./install.sh                  # 默认：small 档指标栈 + Loki/ClickHouse 两个日志与
#                                 #        时序明细后端（适配 4C/7.5G 单节点，本机）
#   PROFILE=full ./install.sh     # 指标栈换成全量档（alertmanager 等），面向多节点生产
#   BACKENDS=0 ./install.sh       # 不装 Loki / ClickHouse，只要指标栈
#
# PROFILE 只决定指标栈（kube-prometheus-stack）用缩配还是全量 values；
# 日志与时序明细后端是否安装由 BACKENDS 独立控制，两者互不牵连。

set -euo pipefail

PROFILE="${PROFILE:-small}"
BACKENDS="${BACKENDS:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="${SCRIPT_DIR}/../helm"
MANIFESTS_DIR="${SCRIPT_DIR}/../manifests"
NAMESPACE="monitoring"
GRAFANA_PW_FILE="${HOME}/.cogneva/grafana-admin-password"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── 前置检查 ─────────────────────────────────────────────────────
check_prerequisites() {
    log_info "检查前置依赖..."

    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl 未安装，请先安装 K3s 并配置 kubeconfig"
        exit 1
    fi

    if ! command -v helm &> /dev/null; then
        log_error "helm 未安装，请执行: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
        exit 1
    fi

    if ! kubectl cluster-info &> /dev/null; then
        log_error "无法连接 K3s 集群，请检查 kubeconfig"
        exit 1
    fi

    log_info "前置依赖检查通过 ✓"
}

# ─── 添加 Helm 仓库 ───────────────────────────────────────────────
add_helm_repos() {
    log_info "添加 Helm 仓库..."
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
    helm repo add jaegertracing https://jaegertracing.github.io/helm-charts 2>/dev/null || true
    helm repo update
    log_info "Helm 仓库更新完成 ✓"
}

# ─── ClickHouse 凭证（只能在清单之前）───────────────────────────
# 仓库零可用密钥：首次安装生成强随机密码存 monitoring/clickhouse-credentials，
# 再镜像一份到 cogneva/clickhouse-password 供安全网关连接（跨命名空间不能在
# Pod env 里直接引用 Secret，只能各存一份，值同源）。已存在则保留不动。
ensure_clickhouse_credentials() {
    log_info "准备 ClickHouse 凭证..."

    if ! kubectl -n "${NAMESPACE}" get secret clickhouse-credentials >/dev/null 2>&1; then
        local pw
        pw="$(openssl rand -hex 24)"
        create_secret_from_value clickhouse-credentials password "${pw}"
        log_info "clickhouse-credentials 已生成随机密码"
    else
        log_info "clickhouse-credentials 已存在，保留不动"
    fi
}

# 用值建 Secret，值不经过 argv（--from-literal 会把密码暴露在宿主的 ps 里）。
# 非敏感的伴生键经 extra 透传给 kubectl（形如 --from-literal=admin-user=admin）。
create_secret_from_value() {
    local name="$1" key="$2" value="$3"
    shift 3
    local dir
    dir="$(mktemp -d)"
    chmod 700 "${dir}"
    ( umask 077; printf '%s' "${value}" > "${dir}/${key}" )
    kubectl -n "${NAMESPACE}" create secret generic "${name}" \
        --from-file="${key}=${dir}/${key}" "$@" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    rm -rf "${dir}"
}

# ─── Grafana 管理员凭证 ───────────────────────────────────────────
# 清单里不声明这个 Secret：写字面量等于把密码提交进仓库（历史清不掉），写空值
# 则每次 apply 都把已生效的密码清掉。所以安装时生成一次、已存在则保留，密码落
# 一份 600 的本地文件，日志只给路径。轮换用 scripts/rotate-grafana-password.sh。
ensure_grafana_admin_credentials() {
    log_info "准备 Grafana 管理员凭证..."

    if kubectl -n "${NAMESPACE}" get secret grafana-admin-credentials >/dev/null 2>&1; then
        log_info "grafana-admin-credentials 已存在，保留不动"
        return
    fi

    local pw
    pw="$(openssl rand -base64 20)"
    create_secret_from_value grafana-admin-credentials admin-password "${pw}" --from-literal=admin-user=admin
    mkdir -p "$(dirname "${GRAFANA_PW_FILE}")"
    ( umask 077; printf '%s\n' "${pw}" > "${GRAFANA_PW_FILE}" )
    log_info "grafana-admin-credentials 已生成随机密码，写入 ${GRAFANA_PW_FILE}（权限 600）"
}

# ─── 清单交付处置（与集群内收敛循环共用一张表）─────────────────────
# 哪些清单永不交付、哪些由 BACKENDS 开关决定，写在
# `manifests/delivery-dispositions.txt` 里。首次安装（本脚本）与周期收敛
# （部署器 Pod）读同一份：两处各留一份名单必然会分叉，分叉的样子是
# 「装的时候跳过、收敛的时候照做」，互相拆台。
DISPOSITIONS_FILE="${MANIFESTS_DIR}/delivery-dispositions.txt"

# 打出该文件的处置（`exempt`/`backends`）；没登记则打空并返回 1。
# 没登记不是跳过理由——调用方按「交付」处理，这是刻意的失效方向。
disposition_of() {
    local want="$1" name disp reason
    [ -f "${DISPOSITIONS_FILE}" ] || return 1
    while read -r name disp reason; do
        case "${name}" in ''|'#'*) continue ;; esac
        if [ "${name}" = "${want}" ]; then
            printf '%s' "${disp}"
            return 0
        fi
    done < "${DISPOSITIONS_FILE}"
    return 1
}

# 需要 monitoring.coreos.com CRD 的清单：CRD 由 chart 安装，先应用会直接报
# "no matches for kind"，所以这几个只能在 chart 之后。按**文件内容的 kind**
# 判，不按名单——名单会漏掉新加的那个 ServiceMonitor。
needs_monitoring_crds() {
    grep -qE '^[[:space:]]*kind:[[:space:]]*(ServiceMonitor|PodMonitor)[[:space:]]*$' "$1"
}

# 该文件这一轮该不该交付：打印跳过理由，空表示交付。
#
# 未登记 → 交付。这是刻意的失效方向：漏登记的结果是它被应用（看得见），反
# 方向是静默不交付，而那正是这条路要修的缺陷。覆盖方向由 CI 门禁兜（目录里
# 每个 yaml 都必须在这里有一行），所以漏登记进不了仓库，运行时这一步只是兜底。
skip_reason() {
    local base="$1" disp
    disp="$(disposition_of "${base}")" || disp=""
    case "${disp}" in
        ''|deliver)
            return 0 ;;
        exempt)
            printf '%s' "已登记为永不交付（见 delivery-dispositions.txt）" ;;
        backends)
            if [ "${BACKENDS}" != "1" ]; then
                printf '%s' "BACKENDS=0"
            fi ;;
    esac
}

# 处置表自身的检查：取值只认那两个，指向的文件必须真在。
# 放在部署之前跑一次，而不是在 skip_reason 里 `exit`——那个函数是在命令替换
# 里调的，`exit` 只会结束子 shell，表写错会退化成"静默按交付处理"。
validate_dispositions() {
    local name disp reason
    [ -f "${DISPOSITIONS_FILE}" ] || { log_error "缺少 ${DISPOSITIONS_FILE}"; exit 1; }
    while read -r name disp reason; do
        case "${name}" in ''|'#'*) continue ;; esac
        case "${disp}" in
            deliver|exempt|backends) ;;
            *)
                log_error "${DISPOSITIONS_FILE##*/}: ${name} 的处置值非法: ${disp:-（空）}"
                exit 1
                ;;
        esac
        if [ ! -f "${MANIFESTS_DIR}/${name}" ]; then
            log_error "${DISPOSITIONS_FILE##*/}: ${name} 指向的文件不存在"
            exit 1
        fi
    done < "${DISPOSITIONS_FILE}"

    # 反向覆盖：目录里有、表里没有的清单。这里只警告不拦——记账的小疏漏不该
    # 挡住监控装机（按「交付」处理，方向是安全的），拦的责任在 CI 门禁。
    local f
    for f in "${MANIFESTS_DIR}"/*.yaml; do
        if ! disposition_of "$(basename "$f")" >/dev/null; then
            log_warn "${DISPOSITIONS_FILE##*/}: $(basename "$f") 没有登记处置，按交付处理"
        fi
    done
}

deploy_manifests() {
    log_info "部署 K8s 基础资源 (Namespace / Secret / ServiceMonitor / Dashboard)..."

    # 命名空间先于一切：下面的凭证准备要在它里面建 Secret，首次安装时它还
    # 不存在——若等遍历清单时才创建，凭证那一步会在空命名空间上失败。
    log_info "应用: 01-namespace.yaml"
    kubectl apply -f "${MANIFESTS_DIR}/01-namespace.yaml"

    # 清单里没有 Grafana 的那个 Secret：它必须由这里在 apply 之前备好
    # （kube-prometheus-stack 的 grafana 从 existingSecret 读）。
    ensure_grafana_admin_credentials

    for f in "${MANIFESTS_DIR}"/*.yaml; do
        local base reason
        base="$(basename "$f")"
        # 01 已在上面应用（凭证准备依赖它）。
        [ "${base}" = "01-namespace.yaml" ] && continue
        reason="$(skip_reason "${base}")"
        if [ -n "${reason}" ]; then
            log_warn "跳过: ${base}（${reason}）"
            continue
        fi
        # 凭证只能在清单之前备好：ClickHouse 从 secretKeyRef 读密码。
        [ "${base}" = "09-clickhouse.yaml" ] && ensure_clickhouse_credentials
        if needs_monitoring_crds "$f"; then
            log_info "延后: ${base}（等待 chart 安装 CRD）"
            continue
        fi
        log_info "应用: ${base}"
        kubectl apply -f "$f"
    done

    log_info "基础资源部署完成 ✓"
}

# ─── chart 安装之后才能应用的清单 ─────────────────────────────────
deploy_crd_dependent_manifests() {
    for f in "${MANIFESTS_DIR}"/*.yaml; do
        local base reason
        base="$(basename "$f")"
        needs_monitoring_crds "$f" || continue
        reason="$(skip_reason "${base}")"
        if [ -n "${reason}" ]; then
            log_warn "跳过: ${base}（${reason}）"
            continue
        fi
        log_info "应用: ${base}"
        kubectl apply -f "$f"
    done
}

# ─── 把 ClickHouse 密码镜像给安全网关命名空间 ─────────────────────
# 网关从 cogneva-secrets/clickhouse-password 读，值必须与 ClickHouse 服务端一致。
mirror_clickhouse_password() {
    kubectl -n cogneva get ns >/dev/null 2>&1 || { log_warn "cogneva 命名空间不存在，跳过密码镜像"; return; }
    local pw_b64
    pw_b64="$(kubectl -n "${NAMESPACE}" get secret clickhouse-credentials -o jsonpath='{.data.password}')"
    kubectl -n cogneva get secret cogneva-secrets >/dev/null 2>&1 \
        || kubectl -n cogneva create secret generic cogneva-secrets
    kubectl -n cogneva patch secret cogneva-secrets --type=json \
        -p="[{\"op\":\"add\",\"path\":\"/data/clickhouse-password\",\"value\":\"${pw_b64}\"}]" >/dev/null 2>&1 \
        || kubectl -n cogneva patch secret cogneva-secrets --type=merge \
            -p="{\"data\":{\"clickhouse-password\":\"${pw_b64}\"}}" >/dev/null
    log_info "ClickHouse 密码已镜像到 cogneva-secrets"
}

# ─── 部署 kube-prometheus-stack ───────────────────────────────────
deploy_prometheus_stack() {
    if [ "${PROFILE}" = "full" ]; then
        VALUES_FILE="${HELM_DIR}/kube-prometheus-stack-values.yaml"
        log_info "部署 kube-prometheus-stack 全量档（含 alertmanager 双副本）..."
    else
        VALUES_FILE="${HELM_DIR}/kube-prometheus-stack-values-small.yaml"
        log_info "部署 kube-prometheus-stack 缩配档（Prometheus + Grafana + node-exporter + kube-state-metrics）..."
    fi

    helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
        --namespace "${NAMESPACE}" \
        --create-namespace \
        --values "${VALUES_FILE}" \
        --wait \
        --timeout 600s

    log_info "kube-prometheus-stack 部署完成 ✓"
}

# ─── 部署 Jaeger ──────────────────────────────────────────────────
deploy_jaeger() {
    log_info "部署 Jaeger (链路追踪)..."

    helm upgrade --install jaeger jaegertracing/jaeger \
        --namespace "${NAMESPACE}" \
        --values "${HELM_DIR}/jaeger-values.yaml" \
        --wait \
        --timeout 300s

    log_info "Jaeger 部署完成 ✓"
}

# ─── 验证部署 ─────────────────────────────────────────────────────
verify_deployment() {
    log_info "验证部署状态..."

    kubectl wait --for=condition=Ready pods --all -n "${NAMESPACE}" --timeout=300s

    echo ""
    log_info "=== 部署状态 ==="
    kubectl get pods -n "${NAMESPACE}"

    echo ""
    log_info "=== 服务访问地址 ==="
    echo "  Prometheus:   https://prometheus.sf-network.local"
    echo "  Grafana:      https://grafana.sf-network.local    (admin / 密码见 ${GRAFANA_PW_FILE})"
    echo "  Alertmanager: https://alertmanager.sf-network.local"
    echo "  Jaeger:       https://jaeger.sf-network.local"
    echo ""
    log_warn "请先配置 DNS 或 /etc/hosts 指向 Ingress IP"
    echo "  <K3s-Node-IP>  prometheus.sf-network.local grafana.sf-network.local alertmanager.sf-network.local jaeger.sf-network.local"
    echo ""
    log_warn "Grafana 管理员密码由安装时随机生成（${GRAFANA_PW_FILE}），不写在清单里；对外暴露前用 rotate-grafana-password.sh 轮换一次。"
}

# ─── 主流程 ───────────────────────────────────────────────────────
main() {
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SF-Network 可观测性栈 — K3s 生产环境部署"
    echo "  覆盖 16 项 DevOps 组件"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    check_prerequisites
    # 交付处置表先自检：表写错就在这里断，别等它退化成"静默按交付处理"。
    validate_dispositions
    add_helm_repos
    deploy_manifests
    deploy_prometheus_stack
    # ServiceMonitor 依赖 chart 带来的 CRD，只能排在 chart 之后。
    deploy_crd_dependent_manifests
    # Loki / ClickHouse 清单已在 deploy_manifests 应用；这里把 ClickHouse 密码同步给
    # 网关命名空间，网关据此连 ClickHouse 写时序明细
    # （securityGateway.observability.clickhouse）。
    if [ "${BACKENDS}" = "1" ]; then
        mirror_clickhouse_password
    else
        log_info "BACKENDS=0：跳过日志与时序明细后端（Loki / ClickHouse）"
    fi
    if [ "${PROFILE}" = "full" ]; then
        deploy_jaeger
    fi
    verify_deployment

    echo ""
    log_info "全部部署完成！"
}

main "$@"
