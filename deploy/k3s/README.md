# deploy/k3s —— 现行集群清单

本目录是当前 k3s 集群的权威清单来源，`kubectl apply` 直接可用。

## 不在本目录的两类东西

- **构建/发布链清单在 `deploy/k8s/image-distributor.yaml`**：镜像服务 Pod + Service +
  分发器 DaemonSet 三段。该文件被 bootstrap 编译期内嵌
  （`crates/bootstrap/src/main.rs` 的 `include_str!`），多节点引导时按网络模式替换
  `__BUSYBOX_IMAGE__` 后 apply。不要移动或改名这个文件，移动会破坏 bootstrap 编译。
- **监控栈在 `observability/`**：kube-prometheus-stack + loki + jaeger 的 helm values
  与配套清单，当前集群未部署（配置面向较大规格环境，本机部署前需先缩小资源参数）。

## 换版与分发

- 单机换二进制镜像：`swap-image.sh`（四个业务 Deployment 全量 set image 滚动）。
- 多节点镜像分发：`deploy/scripts/distribute-image.sh`（镜像服务 Pod 承载 tar 包，
  分发器 DaemonSet 每节点导入）。
