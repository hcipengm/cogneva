#!/usr/bin/env bash
# Cogneva 可观测性栈一键安装脚本
#
# 用法:
#   ./install.sh            # 默认 small 档：prometheus+grafana+node-exporter+kube-state-metrics，
#                           # 适配 4C/7.5G 单节点（本机）
#   PROFILE=full ./install.sh   # 全量档：加 alertmanager+loki+jaeger，面向多节点生产

set -euo pipefail

PROFILE="${PROFILE:-small}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_DIR="${SCRIPT_DIR}/../helm"
MANIFESTS_DIR="${SCRIPT_DIR}/../manifests"
NAMESPACE="monitoring"

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
    helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
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
        kubectl -n "${NAMESPACE}" create secret generic clickhouse-credentials \
            --from-literal=password="${pw}" >/dev/null
        log_info "clickhouse-credentials 已生成随机密码"
    else
        log_info "clickhouse-credentials 已存在，保留不动"
    fi
}

# ─── 部署基础 Manifests ───────────────────────────────────────────
deploy_manifests() {
    log_info "部署 K8s 基础资源 (Namespace / Secrets / ServiceMonitor / Dashboard)..."

    # 不应用的文件及原因：
    #   02-networkpolicy — 首条策略对 monitoring 全体 Pod 做 ingress 默认拒绝，
    #     但放行来源按 Pod 标签匹配 ingress controller；hostNetwork 模式的
    #     ingress-nginx 源地址是节点 IP，匹配不上，会把反代流量全拦下。
    #     生产形态（controller 非 hostNetwork）再启用。
    #   05-podmonitor — 与 04-servicemonitor 二选一的替代方案，避免双份抓取。
    #   09-clickhouse — 只在 full 档部署（时序明细后端，见 deploy_clickhouse）。
    for f in "${MANIFESTS_DIR}"/*.yaml; do
        case "$(basename "$f")" in
            02-networkpolicy.yaml|05-podmonitor-cogneva.yaml)
                log_warn "跳过: $(basename "$f")"
                continue ;;
            09-clickhouse.yaml)
                if [ "${PROFILE}" = "full" ]; then
                    ensure_clickhouse_credentials
                    log_info "应用: $(basename "$f")"
                    kubectl apply -f "$f"
                else
                    log_warn "跳过: $(basename "$f")（缩配档不部署时序明细后端）"
                fi
                continue ;;
        esac
        log_info "应用: $(basename "$f")"
        kubectl apply -f "$f"
    done

    log_info "基础资源部署完成 ✓"
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

# ─── 部署 Loki ────────────────────────────────────────────────────
deploy_loki() {
    log_info "部署 Loki + Promtail (日志聚合)..."

    helm upgrade --install loki grafana/loki-stack \
        --namespace "${NAMESPACE}" \
        --values "${HELM_DIR}/loki-values.yaml" \
        --wait \
        --timeout 300s

    log_info "Loki 部署完成 ✓"
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
    echo "  Grafana:      https://grafana.sf-network.local    (admin / 查看 03-grafana-secrets.yaml)"
    echo "  Alertmanager: https://alertmanager.sf-network.local"
    echo "  Jaeger:       https://jaeger.sf-network.local"
    echo ""
    log_warn "请先配置 DNS 或 /etc/hosts 指向 Ingress IP"
    echo "  <K3s-Node-IP>  prometheus.sf-network.local grafana.sf-network.local alertmanager.sf-network.local jaeger.sf-network.local"
    echo ""
    log_warn "生产环境请修改 03-grafana-secrets.yaml 中的默认密码后再部署！"
}

# ─── 主流程 ───────────────────────────────────────────────────────
main() {
    echo "═══════════════════════════════════════════════════════════════"
    echo "  SF-Network 可观测性栈 — K3s 生产环境部署"
    echo "  覆盖 16 项 DevOps 组件"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""

    check_prerequisites
    add_helm_repos
    deploy_manifests
    deploy_prometheus_stack
    if [ "${PROFILE}" = "full" ]; then
        deploy_loki
        deploy_jaeger
        # ClickHouse 清单已在 deploy_manifests 应用；这里把密码同步给网关命名空间，
        # 网关据此连 ClickHouse 写时序明细（securityGateway.observability.clickhouse）。
        mirror_clickhouse_password
    else
        log_info "缩配档跳过 Loki / ClickHouse / Jaeger（日志、时序明细与链路追踪生产档再上）"
    fi
    verify_deployment

    echo ""
    log_info "全部部署完成！"
}

main "$@"
