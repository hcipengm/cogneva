# Cogneva Deployment Guide

Cross-platform deployment instructions for Linux (systemd), macOS (launchd), and Windows Service.

---

## 先看清：用户路径只有一条，目录不是按路径分的

**正常使用只跑元启动**（仓库根 `bootstrap.sh` / Windows `bootstrap.ps1`），全程无人
值守：裸机它自动装 K3s，已有集群它直接复用，然后自动选材料、选投递方式（见下文
Kubernetes 节）。用户不需要在 K3s / 标准 K8s / Helm 之间做选择——那些是**内部材料
形态**，不是用户路径。

所以 `deploy/` 下的文件夹是**按"权威源 / 产物 / 参考 / 工具 / 非容器部署"分类组织
的，不是一条路径一个文件夹**——不要用"几条部署路径就该有几个文件夹"去数它。早期
文档曾把"裸机自举 + K3s / 标准 K8s / Helm 三条集群路径"讲成 4 条并列路径，那套说法
已废弃：K8s 场景下用户路径只有元启动 1 条（内部 2 种投递机制），`systemd`/`launchd`
则是"完全不用 K8s、直接在宿主机跑二进制"的另一种传统部署，与 K8s 路线互斥。当前 7
个文件夹各管一件事：

| 目录 | 做什么 / 实现什么功能 | 角色与消费方 |
|---|---|---|
| **`helm/cogneva/`** | **应用拓扑唯一权威源（Helm chart）**：`values.yaml` 默认参数、`profiles/` 三种形态（`k3s-single`/`k3s-multi`/`k8s-standard`）、`templates/` 全部 K8s 资源、`files/` ConfigMap 原文（cogneva.json、prompts） | **唯一手改入口**；改应用拓扑只改这里 |
| **`rendered/<profile>/`** | chart 的 **CI 预渲染产物**（38/39/38 个 standalone YAML），由 `scripts/render-deploy.sh` 生成、随仓库提交 | 元启动 **apply 投递**消费；CI 新鲜度门禁防漂移；**不要手改** |
| **`k3s/`** | **K3s 静态清单**：完整应用拓扑的静态 YAML + `observability/` 监控栈（Prometheus/Grafana/Loki/Jaeger 的 helm values 与装卸脚本）+ `examples/`、`swap-image.sh`、`sync-git-remote.sh` | **不是权威源**：是 chart k3s profile 的 **parity 基线**，兼集群内自进化 GitOps 拉取端的**运行时消费物**（cog-reflection 运行时 apply 克隆仓库里的这些文件）；由 `scripts/check-deploy-parity.sh` 字段级门禁对齐 |
| **`k8s/`** | **标准 K8s 的基础设施参考清单**（不是应用拓扑）：`longhorn`/`argocd`/`cert-manager`/`ingress-nginx`/`metallb`/`velero`/`monitoring/` 集群级组件按需单独 apply；`image-distributor.yaml` 多节点镜像分发器（**元启动 `include_str!` 内嵌模板**）；`meilisearch.yaml` 可选搜索参考；`cogneva.service` systemd 参考副本 | 标准 K8s 的**应用拓扑不在这里**——走 chart 的 `k8s-standard` profile（渲染在 `rendered/k8s-standard/`）；本目录只有集群周边设施 |
| **`scripts/`** | 部署工具脚本（见下表） | CI 与元启动调用 |
| **`systemd/`** | **Linux 裸机（不用 K8s）传统部署**的 systemd unit，直接把二进制跑成宿主服务 | 非容器路线 |
| **`launchd/`** | **macOS 裸机传统部署**的 launchd plist | 非容器路线（Windows 服务用 `sc.exe`，命令见后文，无独立文件夹） |

`scripts/` 内各脚本：

| 脚本 | 功能 |
|---|---|
| `render-deploy.sh` | 用 `helm template -f profiles/<p>.yaml` 把 chart 渲染进 `rendered/<profile>/`；`--check` 做 CI 新鲜度门禁（重渲染有 diff 即红） |
| `check-deploy-parity.sh` | chart k3s profile 渲染 vs `k3s/` 静态清单的**字段级 parity** 门禁（38 资源基线，env/卷/挂载/端口/SA 全比对），CI 强制 |
| `init-secrets.sh` | 安装时**随机生成**内部密钥（pg/redis/内部签名）并创建 Secret；幂等不覆盖已有值；元启动 apply 前自动跑 |
| `distribute-image.sh` | 多节点镜像增量升级：把镜像 tar 分发到全部节点并滚动重启 |
| `build-release-image.sh` | release 预构建运行时镜像的**本机单源**构建（产物 tar.gz + sha256） |
| `verify-firecracker.sh` | Firecracker 微虚拟机沙盒的真机端到端验证 |

### 关于 `k3s/` `k8s/` `helm/` 三个文件夹（不是三条可选路径）

- **`helm/` 是权威源**，`rendered/` 是它渲染出来的产物，`k3s/` 是它在 K3s 形态下的
  静态基线 + 自进化运行时消费物——三者是"源 → 产物 → 基线/消费物"的关系，不是三选一。
- **`k8s/` 不放应用清单**，只放标准 K8s 集群周边的基础设施参考；标准 K8s 部署应用走
  chart 的 `k8s-standard` profile。
- **`deploy/kustomize/` 已删除**：那是早期用 kustomize overlay（base + dev/prod、
  以及 `deploy/k8s/kustomization.yaml` 复用 `../k3s` 的生产 overlay）做拓扑复用的
  尝试；单一数据源收敛后，环境差异全部下沉为 chart 的 profile values，overlay 整体
  退出（2026-09-04，commit 3f6ca0b）。现在看到任何"kustomize overlay / dev/prod
  变体"的说法都是历史，改拓扑统一改 chart。

---

## Kubernetes

**正常使用只需跑元启动**（仓库根 `bootstrap.sh` / Windows `bootstrap.ps1`）：
它探测集群发行版（K3s / 标准 K8s）、节点数、默认 StorageClass，自动选 profile
并完成密钥初始化与部署，不需要在 Helm / 静态清单之间手动选择。投递方式也自动
决定：元启动自建的集群走预渲染清单 `kubectl apply`（零 helm 依赖）；复用的既有
生产集群自动 `helm install`（release 可被 ArgoCD 接管）——本机没有 helm 时元启动
自行安装（CN 走华为云镜像、海外走 get.helm.sh，多候选自动换源），装不上才回落
apply；已有 release 自动 `helm upgrade`，已 apply 部署的保持 apply 不换轨。

元启动内部使用的两种投递机制（同源同能力，无需人工执行；CI / 自动化流水线可直接
复用这两条等价命令）：

```bash
# 机制 1：预渲染清单（无需本机装 helm；CI 从 chart 渲染、随仓库提交）
bash deploy/scripts/init-secrets.sh                     # 幂等随机生成内部密钥
kubectl apply -f deploy/rendered/k3s-single/            # 或 k3s-multi / k8s-standard

# 机制 2：Helm 直接安装（本机有 helm；元启动缺 helm 时自动安装）
helm install cogneva deploy/helm/cogneva \
  -f deploy/helm/cogneva/profiles/k3s-single.yaml       # 或 k3s-multi / k8s-standard
```

三种 profile：`k3s-single`（单节点 K3s，local-path 存储、hostPath git-remote、
挂宿主 k3s 二进制）、`k3s-multi`（多节点 K3s，git-remote 走 PVC）、
`k8s-standard`（标准 K8s：不建 K3s 专有 StorageClass、PVC 跟随集群默认 SC、
containerd socket `/run/containerd`、不挂宿主 kubectl；前置：集群须有默认
StorageClass，如 Longhorn）。渲染检查：`helm template cogneva deploy/helm/cogneva
-f deploy/helm/cogneva/profiles/<profile>.yaml`。

关键 values：`backends.{postgres,redis,qdrant,nats}.enabled` 控制是否随 chart 部署后端（禁用即使用外部服务）；`evolution.enabled` 控制自进化 worker，`evolution.gitRemote.mode`（`hostPath` 单节点 / `pvc` 多节点）控制中央 bare 仓库供给；`sandboxExecutor.enabled` / `buildah.enabled` 控制沙盒执行器与节点镜像构建 DaemonSet；`buildah.containerdSocket` 适配发行版（K3s 为 `/run/k3s/containerd`，标准 containerd 为 `/run/containerd`）；`storage.localRetainClass.create`（K3s 专有 Retain StorageClass，标准 K8s 置 false）与 `storage.evolution`/`storage.retain` 控制各卷存储类（留空跟随集群默认 SC）；`gitops.kubectlBin.enabled`+`gitops.kubectlBin.hostPath` 控制主应用 GitOps 拉取端的 Pod 内 kubectl（K3s 挂宿主 k3s 二进制，节点无可用二进制时置 false）；`webhook.nodePort` 为平台 webhook 入口；`ingress.className`（`nginx` 默认 / `traefik` 自动附带 WebSocket Middleware）；`networkPolicy.enabled` 控制沙盒出站隔离。内部密钥留空即安装时自动随机生成；预渲染清单路径用 `secrets.create=false`，密钥改由 init-secrets.sh 生成（bootstrap 自动调用）。

> **维护者注意**：应用拓扑唯一权威源是 Helm chart（`deploy/helm/cogneva/`）。
> `deploy/rendered/<profile>/` 是 chart 的 CI 渲染产物（`bash
> deploy/scripts/render-deploy.sh` 重新生成，CI 新鲜度门禁防漂移）；
> `deploy/k3s/` 静态清单是 parity 基线兼集群内 GitOps 拉取端的运行时消费物，
> 必须与 chart k3s profile 字段级对齐，由 CI 与 `deploy/scripts/check-deploy-parity.sh`
> 强制（38 资源基线）。改拓扑的顺序见 `deploy/k3s/README.md`。

---

## Linux (systemd)

### Install

```bash
# 1. Copy binary
sudo mkdir -p /opt/cogneva
sudo cp target/release/cogneva /opt/cogneva/
sudo chmod +x /opt/cogneva/cogneva

# 2. Create directories
sudo mkdir -p /var/lib/cogneva-data /var/log/cogneva /run/cogneva /etc/cogneva

# 3. Install service unit
sudo cp deploy/systemd/cogneva.service /etc/systemd/system/
sudo systemctl daemon-reload

# 4. Start & enable
sudo systemctl start cogneva
sudo systemctl enable cogneva
```

### Uninstall

```bash
sudo systemctl stop cogneva
sudo systemctl disable cogneva
sudo rm /etc/systemd/system/cogneva.service
sudo systemctl daemon-reload
```

### Operations

```bash
sudo systemctl status cogneva
sudo systemctl restart cogneva
sudo systemctl stop cogneva
sudo journalctl -u cogneva -f
```

---

## macOS (launchd)

### Install

```bash
# 1. Copy binary
sudo mkdir -p /Applications/cogneva
sudo cp target/release/cogneva /Applications/cogneva/

# 2. Create directories
mkdir -p ~/Library/Application\ Support/cogneva-data
mkdir -p ~/Library/Logs/cogneva
mkdir -p ~/Library/Preferences/cogneva

# 3. Edit plist (replace $USER with your username) and install
sed "s/\$USER/$(whoami)/g" deploy/launchd/cogneva.plist > /tmp/cogneva.plist
cp /tmp/cogneva.plist ~/Library/LaunchAgents/com.cogneva.cogneva.plist

# 4. Load
launchctl load ~/Library/LaunchAgents/com.cogneva.cogneva.plist
launchctl start com.cogneva.cogneva
```

### Uninstall

```bash
launchctl stop com.cogneva.cogneva
launchctl unload ~/Library/LaunchAgents/com.cogneva.cogneva.plist
rm ~/Library/LaunchAgents/com.cogneva.cogneva.plist
```

### Operations

```bash
launchctl list | grep cogneva
launchctl start com.cogneva.cogneva
launchctl stop com.cogneva.cogneva
launchctl unload ~/Library/LaunchAgents/com.cogneva.cogneva.plist
launchctl load ~/Library/LaunchAgents/com.cogneva.cogneva.plist
tail -f ~/Library/Logs/cogneva/cogneva.log
```

---

## Windows Service

### Install

Open PowerShell as **Administrator**:

```powershell
# 1. Create directories
New-Item -ItemType Directory -Force -Path "C:\ProgramData\cogneva"
New-Item -ItemType Directory -Force -Path "C:\ProgramData\cogneva-data"
New-Item -ItemType Directory -Force -Path "C:\ProgramData\cogneva\logs"

# 2. Copy binary
Copy-Item target\release\cogneva.exe C:\ProgramData\cogneva\

# 3. Create service
sc.exe create cogneva binPath= "C:\ProgramData\cogneva\cogneva.exe --service" start= auto

# 4. Start service
sc.exe start cogneva
```

### Uninstall

```powershell
sc.exe stop cogneva
sc.exe delete cogneva
```

### Operations

```powershell
# Status
sc.exe query cogneva

# Start / Stop / Restart
sc.exe start cogneva
sc.exe stop cogneva

# View logs (if logging to file is configured)
Get-Content "C:\ProgramData\cogneva\logs\cogneva.log" -Tail 50 -Wait

# Windows Event Viewer
# Applications and Services Logs -> cogneva
```

---

## Environment Variables (All Platforms)

| Variable | Description | Default |
|----------|-------------|---------|
| `SF_CONFIG_PATH` | Base config file path | Platform-specific |
| `SF_DATA_DIR` | Persistent data directory | Platform-specific |
| `SF_LOG_LEVEL` | Log level (trace/debug/info/warn/error) | `info` |
| `SF_PID_FILE` | PID file path | Platform-specific |
| `COGNEVA_ENV` | Environment suffix (e.g. `staging`) | `development` |
| `SF_REDIS_URL` | Redis connection URL | `redis://127.0.0.1:6379` |
