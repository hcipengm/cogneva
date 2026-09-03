# Cogneva Deployment Guide

Cross-platform deployment instructions for Linux (systemd), macOS (launchd), and Windows Service.

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
