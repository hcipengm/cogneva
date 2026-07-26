#!/usr/bin/env bash
# SF-Network 可观测性栈 — K3s 生产环境一键安装脚本
# 部署全部 16 项 DevOps 组件
#
# 用法:
#   chmod +x install.sh
#   ./install.sh

set -euo pipefail

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

# ─── 部署基础 Manifests ───────────────────────────────────────────
deploy_manifests() {
    log_info "部署 K8s 基础资源 (Namespace / NetworkPolicy / Secrets / ServiceMonitor)..."

    # 按文件名排序顺序应用
    for f in "${MANIFESTS_DIR}"/*.yaml; do
        log_info "应用: $(basename "$f")"
        kubectl apply -f "$f"
    done

    log_info "基础资源部署完成 ✓"
}

# ─── 部署 kube-prometheus-stack ───────────────────────────────────
deploy_prometheus_stack() {
    log_info "部署 kube-prometheus-stack (Prometheus + Grafana + Alertmanager + node-exporter + kube-state-metrics)..."

    helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
        --namespace "${NAMESPACE}" \
        --create-namespace \
        --values "${HELM_DIR}/kube-prometheus-stack-values.yaml" \
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
    deploy_loki
    deploy_jaeger
    verify_deployment

    echo ""
    log_info "全部部署完成！"
}

main "$@"
