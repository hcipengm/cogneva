# Cogneva Deployment Guide

Cross-platform deployment instructions for Linux (systemd), macOS (launchd), and Windows Service.

---

## Kubernetes（Helm / Kustomize）

### Helm（推荐）

```bash
# 渲染检查
helm template cogneva deploy/helm/cogneva

# 安装（开发默认值）
helm install cogneva deploy/helm/cogneva

# 生产：覆盖镜像与口令
helm install cogneva deploy/helm/cogneva \
  --set image.tag=0.1.39 \
  --set secrets.pgPassword=<strong-password> \
  --set gateway.replicas=2
```

关键 values：`backends.{postgres,redis,qdrant,nats}.enabled` 控制是否随 chart 部署后端（禁用即使用外部服务）；`evolution.enabled` 控制自进化 worker，`evolution.gitRemote.mode`（`hostPath` 单节点 / `pvc` 多节点）控制中央 bare 仓库供给；`sandboxExecutor.enabled` / `buildah.enabled` 控制沙盒执行器与节点镜像构建 DaemonSet；`buildah.containerdSocket` 适配发行版（K3s 为 `/run/k3s/containerd`，标准 containerd 为 `/run/containerd`）；`storage.localRetainClass.create`（K3s 专有 Retain StorageClass，标准 K8s 置 false）与 `storage.evolution`/`storage.retain` 控制各卷存储类（留空跟随集群默认 SC）；`gitops.kubectlBin.enabled`+`gitops.kubectlBin.hostPath` 控制主应用 GitOps 拉取端的 Pod 内 kubectl（K3s 挂宿主 k3s 二进制，节点无可用二进制时置 false）；`webhook.nodePort` 为平台 webhook 入口；`ingress.className`（`nginx` 默认 / `traefik` 自动附带 WebSocket Middleware）；`networkPolicy.enabled` 控制沙盒出站隔离。内部密钥留空即安装时自动随机生成；渲染清单 apply 的场景（如引导器）用 `--set secrets.create=false`，密钥改由 init-secrets.sh 生成。

> **维护者注意**：应用拓扑正在收敛为 Helm chart 单一权威源（`deploy/helm/cogneva/`），`deploy/k3s/` 静态清单是引导器消费的渲染产物。chart 的 k3s profile 渲染结果与 `deploy/k3s/` 必须资源集合与工作负载字段（env/卷/挂载/端口/SA）全对齐——CI 的 Deploy Manifest Parity 任务与 `deploy/scripts/check-deploy-parity.sh` 强制校验，改任一侧后本地必须跑通该脚本。详见 `deploy/k3s/README.md` 的"应用拓扑在哪改"。

### Kustomize

```bash
# 直接应用原始 k3s 清单
kubectl apply -k deploy/k3s

# overlay：dev（单副本 + dev 镜像 tag）/ prod（3 副本 + 固定发布 tag）
kubectl apply -k deploy/kustomize/overlays/dev
kubectl apply -k deploy/kustomize/overlays/prod
```

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
