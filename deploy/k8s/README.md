# deploy/k8s — 标准 K8s 生产参考

## 应用拓扑不在本目录

应用工作负载（主应用、安全网关、进化 Pod、沙箱执行器、buildah DaemonSet、
nats / postgres / redis / qdrant、NetworkPolicy、Ingress）**不在本目录定义**。
拓扑唯一权威源是 Helm chart `deploy/helm/cogneva/`，标准 K8s 的环境差异由
profile `deploy/helm/cogneva/profiles/k8s-standard.yaml` 表达：

- 不创建 K3s 专有的 `cogneva-local-retain` StorageClass（其 provisioner
  `rancher.io/local-path` 仅 K3s 自带）；
- 进化源码/数据卷、nats、git-remote 各卷不写 `storageClassName`，**回落集群默认
  StorageClass**（Longhorn / Ceph / EBS 任意，不绑定具体厂商）；
- buildah DaemonSet 的 containerd socket 宿主路径为标准路径 `/run/containerd`
  （K3s 是 `/run/k3s/containerd`；k0s 等其他发行版按实际 `--set
  buildah.containerdSocket=...` 或拷贝 profile 修改）；
- git-remote 中央裸仓库走 PVC 模式（多节点可挂载），不挂 K3s 专属宿主路径；
- 主应用 GitOps 拉取端不挂宿主 kubectl 二进制（标准节点上无 `/usr/local/bin/k3s`）。

## 部署步骤

标准 K8s 上**直接跑元启动**即可——它探测到非 K3s 发行版会校验默认 StorageClass
并自动选择 `k8s-standard` profile，无需手动三选一：

```bash
bash bootstrap.sh          # 入口脚本；Windows 用 bootstrap.ps1
```

前置条件：集群必须有可用的默认 StorageClass（生产常用 Longhorn；安装后设为默认）。
元启动会在运行时硬校验这一点，缺默认 SC 直接报错提示先装 Longhorn，而不是静默挂出
无法绑定的 PVC：

```bash
kubectl patch storageclass longhorn -p \
  '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

元启动内部的两种投递机制（同源同能力，无需人工执行；CI / 自动化流水线可直接复用）：

```bash
# 机制 1：预渲染清单——chart 在 CI 渲染好的独立 YAML，kubectl 直接 apply
#         （元启动 apply 前自动跑 init-secrets.sh 随机生成内部密钥，幂等不覆盖）
kubectl apply -f deploy/rendered/k8s-standard/

# 机制 2：Helm——本机有 helm 时装 chart（元启动在复用集群上自动选择，缺 helm 自动装）
helm install cogneva deploy/helm/cogneva \
  -f deploy/helm/cogneva/profiles/k8s-standard.yaml

# 平台 token / LLM 上游：首次打开 WebUI 经配置向导写入（只注入安全网关）
```

> `deploy/rendered/<profile>/` 是 chart 的 CI 渲染产物（随仓库提交），改 chart 后
> 跑 `bash deploy/scripts/render-deploy.sh` 重新生成，CI 新鲜度门禁防漂移。

监控栈（Prometheus / Grafana / Loki / Jaeger / Alertmanager）见 `monitoring/`，
按需单独 `kubectl apply`。

## 一键路径（bootstrap 使用）

bootstrap 按集群环境自动适配：检测发行版（K3s 节点标签 / `/run/k3s`）、节点数、
默认 StorageClass，从 `deploy/rendered/` 选对应 profile 目录 apply；apply 前自动
跑 `deploy/scripts/init-secrets.sh`；CN 模式公开镜像统一走 daocloud 前缀。

1. **镜像分发器** `image-distributor.yaml`（本目录，bootstrap 内嵌为模板）：
   镜像服务 Pod + 每节点 DaemonSet（用宿主 `ctr` 把镜像导入各节点 containerd），
   常设保留。多节点集群里单节点 `ctr import` 覆盖不到其他节点，必须逐节点导入。
2. **生产仓库供给**：设置 `COGNEVA_IMAGE_REGISTRY` 后 bootstrap 跳过逐节点
   分发，镜像引用由运维自行对齐仓库地址。

## 多节点集群从哪来

- **空白多机**：`COGNEVA_CLUSTER_NODES="user@ip2,user@ip3"`（SSH 免密可达），
  bootstrap 在本机装 K3s server、向各节点推送安装 agent。K3s 是 CNCF 认证
  K8s，多节点 K3s 即多节点 K8s。
- **已有集群**：kubectl 可连通即可，profile 按发行版与节点数自动判定。

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

- 基础设施参考（集群级，按需单独 apply）：
  `longhorn.yaml`、`ingress-nginx.yaml`、`metallb.yaml`、`cert-manager.yaml`、
  `argocd.yaml`、`velero.yaml`、`monitoring/`。
- `image-distributor.yaml`：镜像分发器（bootstrap 内嵌模板，勿删）。
- `meilisearch.yaml`：可选的全文搜索组件参考（核心拓扑不依赖它）。
- `cogneva.service`：裸机 systemd 参考（不走容器编排时使用）。

> 历史：本目录曾有 cogneva-deployment / postgres / redis / qdrant /
> llm-admin-rbac 等与 `deploy/k3s/` 重复且残缺的应用清单（缺网关/进化/沙盒），
> 已删除；也曾有一个复用 `deploy/k3s/` 的 kustomize overlay 来表达标准 K8s 差异，
> 在 chart 成为拓扑唯一权威源后由 `profiles/k8s-standard.yaml` +
> `deploy/rendered/k8s-standard/` 取代，一并删除——环境差异只在 chart values
> 维护一份，不再有第二份会腐烂的适配清单。

## 密钥与凭证

- 应用清单**不 apply 任何 Secret**，仓库不含可用密码。部署前由
  `bash deploy/scripts/init-secrets.sh` 自动随机生成 PostgreSQL/Redis/内部签名
  密钥并创建 cogneva-secrets（幂等、不覆盖已有值；bootstrap 会自动调用）；
  Helm 路径在 `helm install` 时自动随机生成；生产也可用 Vault / Sealed Secrets
  等外部管理器注入同名键。参考键结构见 `../k3s/examples/secret.yaml`。
- 主应用**零带外凭证**：不挂 OPENAI/ANTHROPIC/GitHub/Gitee 任何 token，LLM 与
  代码平台凭证只注入安全网关，主应用/沙盒经网关代理访问。
