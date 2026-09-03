# deploy/k3s —— 现行集群清单

本目录是当前 k3s 集群的权威清单来源，`kubectl apply` 直接可用。

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
