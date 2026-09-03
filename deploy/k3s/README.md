# deploy/k3s —— K3s 静态清单（parity 基线 + GitOps 运行时消费）

## 维护者：应用拓扑在哪改（单一数据源）

**应用工作负载拓扑的唯一权威源是 Helm chart `deploy/helm/cogneva/`**：主应用、
安全网关、进化 Pod、沙箱执行器、buildah DaemonSet、nats/postgres/redis/qdrant、
NetworkPolicy、Ingress 都在 chart templates 里定义，环境差异（containerd socket、
StorageClass、git-remote 模式、kubectl 二进制路径）走 values。三种部署形态由
profile values 表达：`deploy/helm/cogneva/profiles/k3s-single.yaml`、
`k3s-multi.yaml`、`k8s-standard.yaml`。

本目录的静态清单**不是**权威源，不能手改后不回写 chart。它保留两个消费方：

1. **bootstrap 的 apply 物不直接是本目录**——bootstrap 消费的是 chart 经 CI
   预渲染的 `deploy/rendered/<profile>/`（`bash deploy/scripts/render-deploy.sh`
   生成，CI 新鲜度门禁防漂移），按环境探测自动选 profile。
2. **集群内 GitOps 拉取端**（cog-reflection）在进化流水线里 apply 克隆仓库中的
   本目录文件（如 `cogneva-json-configmap.yaml`、`evolution-deployment.yaml`），
   换版脚本 `swap-image.sh` 也直接操作本目录清单。

因此本目录必须与 chart 的 k3s profile **字段级对齐**，由 parity 门禁强制
（CI deploy-parity job 同名脚本）：

```bash
bash deploy/scripts/check-deploy-parity.sh
# 对比 kubectl kustomize deploy/k3s 与 chart k3s profile 渲染结果：
# 资源集合 + 每个工作负载的 env/卷/挂载/端口/ServiceAccount 必须全对齐
# （38 个资源基线），差异即失败。
```

**改动拓扑的正确顺序**：改 chart templates / values.yaml → 跑
`render-deploy.sh` 重新生成 `deploy/rendered/` → 同步本目录对应静态清单 →
`check-deploy-parity.sh` 绿。三者同提一个 commit。

## 不在本目录的两类东西

- **构建/发布链清单在 `deploy/k8s/image-distributor.yaml`**：镜像服务 Pod + Service +
  分发器 DaemonSet 三段。该文件被 bootstrap 编译期内嵌
  （`crates/bootstrap/src/main.rs` 的 `include_str!`），多节点引导时按网络模式替换
  `__BUSYBOX_IMAGE__` 后 apply。不要移动或改名这个文件，移动会破坏 bootstrap 编译。
- **监控栈在 `observability/`**：kube-prometheus-stack 的 helm values（缩配版
  `helm/kube-prometheus-stack-values-small.yaml` 已在本机部署，商用全量版同目录保留）
  与配套清单；`scripts/install.sh` 支持 `PROFILE=small|full` 双档。
  loki/jaeger 本轮未部署，生产档再上。

## 集群入口

- `ingress-nginx-values.yaml`：ingress-nginx controller 参数（hostNetwork 直绑节点
  80/443，默认 ingressClass）。k3s 以 `--disable=traefik` 安装，入口统一走它。
- `ingress.yaml`：主应用兜底路由（8080）。无公网 IP 场景（家用主机）下，隧道出口
  直接打节点 80/443。
- 监控组件入口（grafana/prometheus 按域名）在
  `observability/manifests/07-ingress-monitoring.yaml`。

## 换版与分发

- 单机换二进制镜像：`swap-image.sh`（四个业务 Deployment 全量 set image 滚动）。
- 多节点镜像分发：`deploy/scripts/distribute-image.sh`（镜像服务 Pod 承载 tar 包，
  分发器 DaemonSet 每节点导入）。
