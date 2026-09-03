# deploy/k3s —— 现行集群清单

本目录是当前 k3s 集群的权威清单来源，`kubectl apply` 直接可用。

## 维护者：应用拓扑在哪改（单一数据源）

本目录是**应用工作负载拓扑的唯一权威来源**：主应用、安全网关、进化 Pod、沙箱执行器、
buildah DaemonSet、nats/postgres/redis/qdrant、NetworkPolicy、Ingress 都定义在这里。
其余部署路径全部复用本目录，**不要在别处另写一份应用 Deployment/StatefulSet**：

- `deploy/k8s/`（标准 K8s 生产）是 kustomize **overlay**：`resources: [../k3s]` 复用本目录
  全套拓扑，只 patch 环境差异（PVC 回落集群默认 StorageClass、buildah 的 containerd
  socket 改 `/run/containerd`）。加环境差异用 overlay patch，不要往 `deploy/k8s/` 放独立应用清单。
- `deploy/kustomize/`（base + overlays/dev|prod）同样 `resources: [../../k3s]` 指向本目录，
  只叠加副本数 / 镜像 tag 变体。

**唯一的平行定义是 Helm Chart** `deploy/helm/cogneva/templates/`：它不引用本目录，而是
独立模板化了同一套拓扑。因此**在本目录新增或改动一个工作负载时，必须同步改 Helm
templates 与 `deploy/helm/cogneva/values.yaml`**（开关、资源、镜像等参数走 values），
否则 Helm 路径会与 kustomize 路径能力分叉。改完用下面两条命令自检工作负载集合一致：

```bash
kubectl kustomize deploy/k8s | grep -E '^kind:' | sort | uniq -c   # kustomize 侧
helm template deploy/helm/cogneva | grep -E '^kind:' | sort | uniq -c  # helm 侧
# 两者的 DaemonSet/Deployment/StatefulSet 数量应一致（1 DS / 6 Deploy / 2 STS）
```

kustomize overlay 改容器挂载路径时注意：strategic-merge 中 `volumes` 按 `name` 合并，
而 `volumeMounts` 按 **`mountPath`** 合并（不是 name）——改挂载点路径要先 `$patch: delete`
旧 mountPath 再补新值，否则会追加出重复挂载（见 `deploy/k8s/kustomization.yaml` 内注释）。

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
