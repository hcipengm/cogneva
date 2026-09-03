# deploy/k8s — 标准 K8s 生产路径

## 应用拓扑：kustomize overlay（与 K3s 同源）

应用工作负载（主应用、安全网关、进化 Pod、沙箱执行器、buildah DaemonSet、
nats / postgres / redis / qdrant、NetworkPolicy、Ingress）**不在本目录重复定义**。
`kustomization.yaml` 是一个生产 overlay，`resources: [../k3s]` 直接复用
`deploy/k3s/` 的完整拓扑（单一数据源，能力与 K3s 路径完全对齐），只叠加标准
K8s 与 K3s 的环境差异：

- 不创建 K3s 专有的 `cogneva-local-retain` StorageClass（其 provisioner
  `rancher.io/local-path` 仅 K3s 自带）；
- 进化源码/数据卷、nats 卷移除写死的 `storageClassName`，**回落集群默认
  StorageClass**（Longhorn / Ceph / EBS 任意，不绑定具体厂商）；
- buildah DaemonSet 的 containerd socket 宿主路径改为标准路径 `/run/containerd`
  （K3s 是 `/run/k3s/containerd`；k0s 等其他发行版按实际调整 overlay patch）。

overlay 用 labelSelector 按语义标签（`app.kubernetes.io/component`）选择要适配的
PVC / DaemonSet，base 中同角色新增资源会自动被覆盖，无需逐个按名字维护。

## 部署步骤

```bash
# 1) 前置：集群有可用的默认 StorageClass（生产常用 Longhorn）
#    安装 Longhorn 后把它设为默认：
kubectl patch storageclass longhorn -p \
  '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

# 2) 初始化内部密钥（通用脚本，仅依赖 kubectl；自动随机生成，幂等）
bash deploy/k3s/init-secrets.sh

# 3) 部署全套应用拓扑（安全网关/进化/沙盒/buildah/数据面）
kubectl apply -k deploy/k8s/

# 4) 平台 token / LLM 上游：首次打开 WebUI 经配置向导写入（只注入安全网关）
```

监控栈（Prometheus / Grafana / Loki / Jaeger / Alertmanager）见 `monitoring/`，
按需单独 `kubectl apply`。

## 一键路径（bootstrap 使用）

bootstrap 的 K8s 分支同样**复用 `deploy/k3s/` 应用清单**（应用集群无关），
apply 前按集群环境自动适配——无 local-path provisioner 时 PVC 回落集群默认
StorageClass，CN 模式公开镜像统一走 daocloud 前缀。

1. **镜像分发器** `image-distributor.yaml`（本目录，bootstrap 内嵌为模板）：
   镜像服务 Pod + 每节点 DaemonSet（用宿主 `ctr` 把镜像导入各节点 containerd），
   常设保留。多节点集群里单节点 `ctr import` 覆盖不到其他节点，必须逐节点导入。
2. **生产仓库供给**：设置 `COGNEVA_IMAGE_REGISTRY` 后 bootstrap 跳过逐节点
   分发，镜像引用由运维自行对齐仓库地址。

## 多节点集群从哪来

- **空白多机**：`COGNEVA_CLUSTER_NODES="user@ip2,user@ip3"`（SSH 免密可达），
  bootstrap 在本机装 K3s server、向各节点推送安装 agent。K3s 是 CNCF 认证
  K8s，多节点 K3s 即多节点 K8s。
- **已有集群**：kubectl 可连通即可，分支按节点数自动判定。

## 增量升级（新版本镜像分发）

```bash
deploy/scripts/build-release-image.sh            # 构建新版本镜像
deploy/scripts/distribute-image.sh dist/cogneva-image-vX.Y.Z-linux-x86_64.tar.gz
```

脚本把 tar 注入分发服务 Pod → rollout restart 分发器（全节点重新导入新 tag）
→ 滚动重启应用。`SKIP_RESTART=1` 可只分发不重启。

## 分发排障

分发器 DaemonSet 日志：`kubectl -n cogneva logs ds/cogneva-image-distributor`

常见失败：
- `宿主 ctr 二进制未找到`：节点 /usr/bin、/usr/local/bin 下没有 ctr 或 k3s
- `containerd socket 未找到`：节点容器运行时不是 containerd（如 CRI-O），
  本分发器暂不支持
- `wget 超时`：分发服务 Pod 未 Ready（`kubectl -n cogneva get pod cogneva-image-server`）

## 本目录文件分类

- `kustomization.yaml`：**生产 overlay**（应用全套复用 `../k3s`），`kubectl apply -k` 用它。
- 基础设施参考（集群级，按需单独 apply，不被 overlay 引用）：
  `longhorn.yaml`、`ingress-nginx.yaml`、`metallb.yaml`、`cert-manager.yaml`、
  `argocd.yaml`、`velero.yaml`、`monitoring/`。
- `image-distributor.yaml`：镜像分发器（bootstrap 内嵌模板，勿删）。
- `meilisearch.yaml`：可选的全文搜索组件参考（核心拓扑不依赖它）。
- `cogneva.service`：裸机 systemd 参考（不走容器编排时使用）。

> 早期版本本目录曾有 cogneva-deployment / postgres / redis / qdrant /
> llm-admin-rbac 等与 `deploy/k3s/` 重复且残缺的应用清单（缺网关/进化/沙盒），
> 已删除——应用拓扑统一由 kustomize overlay（或 Helm chart `deploy/helm/cogneva`）
> 提供，不再维护第二份。

## 密钥与凭证

- 应用清单**不 apply 任何 Secret**，仓库不含可用密码。部署前先运行
  `bash deploy/k3s/init-secrets.sh`（自动随机生成 PostgreSQL/Redis/内部签名
  密钥并创建 cogneva-secrets，幂等、不覆盖已有值）；生产也可用 Vault /
  Sealed Secrets 等外部管理器注入同名键。参考键结构见 `../k3s/examples/secret.yaml`。
- 主应用**零带外凭证**：不挂 OPENAI/ANTHROPIC/GitHub/Gitee 任何 token，LLM 与
  代码平台凭证只注入安全网关，主应用/沙盒经网关代理访问。
