# Cogneva Deployment Guide

Cross-platform deployment instructions for Linux (systemd), macOS (launchd), and Windows Service.

---

## 先看清：用户路径只有一条，目录不是按路径分的

**正常使用只跑元启动**（仓库根 `bootstrap.sh` / Windows `bootstrap.ps1`），全程无人
值守：裸机它自动装 K3s，已有集群它直接复用，然后自动选材料、选投递方式（见下文
Kubernetes 节）。用户不需要在 K3s / 标准 K8s / Helm 之间做选择——那些是**内部材料
形态**，不是用户路径。

所以 `deploy/` 下的文件夹是**按"权威源 / 产物 / 参考 / 工具 / 非容器部署"分类组织
的，不是一条路径一个文件夹**，不要用"几条部署路径就该有几个文件夹"去数它。K8s
场景下用户路径只有元启动 1 条（内部 2 种投递机制）；`systemd`/`launchd` 则是"完全
不用 K8s、直接在宿主机跑二进制"的传统部署，与 K8s 路线互斥。各文件夹职责如下：

| 目录 | 做什么 / 实现什么功能 | 角色与消费方 |
|---|---|---|
| **`helm/cogneva/`** | **应用拓扑唯一权威源（Helm chart）**：`values.yaml` 默认参数、`profiles/` 三种形态（`k3s-single`/`k3s-multi`/`k8s-standard`）、`templates/` 全部 K8s 资源、`files/` ConfigMap 原文（cogneva.json、prompts） | **唯一手改入口**；改应用拓扑只改这里 |
| **`rendered/<profile>/`** | chart 的 **CI 预渲染产物**（38/39/38 个 standalone YAML），由 `scripts/render-deploy.sh` 生成、随仓库提交 | 元启动 **apply 投递**消费；CI 新鲜度门禁防漂移；**不要手改** |
| **`k3s/`** | **K3s 静态清单**：完整应用拓扑的静态 YAML + `observability/` 监控栈（Prometheus/Grafana/Loki/Jaeger 的 helm values 与装卸脚本）+ `examples/`、`swap-image.sh`、`sync-git-remote.sh` | **不是权威源**：是 chart k3s profile 的 **parity 基线**，兼集群内自进化 GitOps 拉取端的**运行时消费物**（cog-reflection 运行时 apply 克隆仓库里的这些文件）；由 `scripts/check-deploy-parity.sh` 字段级门禁对齐 |
| **`k8s/`** | **标准 K8s 的基础设施参考清单**（不是应用拓扑）：`longhorn`/`argocd`/`cert-manager`/`ingress-nginx`/`metallb`/`velero`/`monitoring/` 集群级组件按需单独 apply；`image-distributor.yaml` 多节点镜像分发器（**元启动 `include_str!` 内嵌模板**）；`meilisearch.yaml` 可选搜索参考；`cogneva.service` systemd 参考副本 | 标准 K8s 的**应用拓扑不在这里**——走 chart 的 `k8s-standard` profile（渲染在 `rendered/k8s-standard/`）；本目录只有集群周边设施 |
| **`site-overlays/`** | **安装期私有事实的 kustomize overlay**（当前一处：`host-documents/`——宿主文档整理的 scope 挂载）。scope 的宿主路径是某台机器自己的事实，而 `k3s/` 会被集群内 GitOps 循环 apply 到每台机器上、挂载又是 `type: Directory`（路径不对 Pod 起不来），所以它不能待在基座里；与已删除的 `deploy/kustomize/`（按环境复制拓扑）不同，这里不复制拓扑，只有一条无处可写的私有事实 | 运维按需 `kubectl kustomize deploy/site-overlays/host-documents \| kubectl apply -f -`；与 chart 的字段级对齐由 `scripts/tests/host-documents-wiring.test.sh` 断言 |
| **`scripts/`** | 部署工具脚本（见下表） | CI 与元启动调用 |
| **`systemd/`** | **Linux 裸机（不用 K8s）传统部署**的 systemd unit，直接把二进制跑成宿主服务 | 非容器路线 |
| **`launchd/`** | **macOS 裸机传统部署**的 launchd plist | 非容器路线（Windows 服务用 `sc.exe`，命令见后文，无独立文件夹） |

`scripts/` 内各脚本：

| 脚本 | 功能 |
|---|---|
| `render-deploy.sh` | 用 `helm template -f profiles/<p>.yaml` 把 chart 渲染进 `rendered/<profile>/`；`--check` 做 CI 新鲜度门禁（重渲染有 diff 即红） |
| `check-deploy-parity.sh` | chart k3s profile 渲染 vs `k3s/` 静态清单的**字段级 parity** 门禁：资源集合、每工作负载的 Pod 模板标签与容器内外全部字段、Service/NetworkPolicy/Ingress/StorageClass/Namespace/ServiceAccount/CronJob 的整份对象、治理对象与 ConfigMap 的整份 spec（安装来源标签除外）。比了多少资源、多少类由脚本自己报（资源随拓扑增减，别在文档里钉死），CI 强制 |
| `check-pg-sharing-processes.sh` | Postgres 连接分摊门禁：渲染产物里 `COGNEVA_PG_SHARING_PROCESSES` 必须 ≥ 带 `COGNEVA_DATABASE_URL` 的常驻 Deployment 副本数之和 + 1（滚动影子）。这个数决定每个进程把服务器连接上限摊成每池几条；少算的那份平时是余量、滚动时被新 Pod 当成自己的，实测新 Pod 报 `pool timed out while waiting for an open connection`。读渲染产物而不是模板，表达式写对没生效也会红。CI 强制 |
| `check-config-checksum-wiring.sh` | 配置内容与 Pod 身份的接线门禁：Pod 模板引用**同一份产物交付的**任一 ConfigMap（配置面从产物里读，不写名单；目前是 `cogneva-json` / `cogneva-prompts` / `cogneva-evolution-config` / `cogneva-config` / `searxng-settings`）的工作负载必须带 `checksum/config`（64 位 sha256），否则只改配置的升级不会滚动——配置换了、Pod 还是旧的，而渲染、parity、Pod 状态三面全绿。读同一组配置的消费者指纹必须相同（按「读到的配置有交集」分族，不同配置各自哈希不同的文档），形态之间不能全等（全等＝表达式被换成写死的字面量）。第二段真渲染两次、只改一个只进 env 配置的键，由两次渲染比出哪几份 ConfigMap 变了，要求它们的消费者指纹跟着变——否则把表达式改回只哈希卷里那份，上面几条仍全绿，而运维配好通知地址、升级成功后 Pod 一个都不换。CI 强制 |
| `check-gate-carriers.py` | 门禁的**载体**：`scripts/` 下每个 `check-*` 都要有东西真的跑它——CI 作业点名、`install-*.sh` 定时器点名、别的脚本调用、或 `tests/*.test.sh` 点名（CI 用 glob 跑那些目录），四类都从树里推，不在这里写名单。四类都没有的门禁必须在自己头部的注释块里写一行 `# carrier: <谁跑它>`，否则红并点名。反向也判：CI 点名了树里没有的 `check-*` 报「陈旧的登记」。为什么需要它：门禁是一个目录、跑它们的是另外几份名单，中间没有任何东西比较——新加一个门禁忘了登记，它不会红，因为从不执行，而「从不执行」在 CI 上与「一直绿」一模一样。CI 强制 |
| `check-cross-layer-reconcile.sh` | 资源账三层对账读数：配额记账的 `used` ≤ 节点 `allocatable`；每节点 `Σanon` + 该节点 file 的 7 天最低 < `MemTotal`（并列 PSI full）；准入余量 `hard - used` 装得下部署器下一次滚动 Job 的声明上限（`used` 含运行时自建的 Pod，渲染期门禁看不见）。某层读不到报 2 而不是报绿。定时载体见 `install-cross-layer-reconcile.sh` |
| `check-container-headroom.sh` | 容器内存余量读数：窗口内峰值 RSS 对声明上限（逐容器，默认红线 0.85），加窗口内 `OOMKilled` 终止记录。判的是**单个容器**上「实际要过的量 vs 它的上限」——集群总量全绿时它仍可以是红的。定时载体见 `install-container-headroom.sh` |
| `install-host-units.sh` | 一次装完本仓的宿主单元。**成员从树里枚举**（`deploy/scripts/install-*.sh`，每个在自己头部声明一行 `# host-install: user\|root`；缺这行、写两行、写错值都退出码 2 并点名那个文件），不靠谁记得住那几个名字。它把**自己**也装成 `cogneva-host-units.timer`（每半小时一拍），所以从跑完那一刻起，树里新增一个读数、改了一个排期、换了一条路径，最迟半小时后宿主自己跟上——不需要人回来重跑。`--list` 只印计划；`COGNEVA_INSTALL_RENDER_ONLY=1` 只渲染、不碰 systemd（测试与「同刻撞车」门靠它读排期） |
| `init-secrets.sh` | 安装时**随机生成**内部密钥（pg/redis/内部签名）并创建 Secret；幂等不覆盖已有值；元启动 apply 前自动跑 |
| `distribute-image.sh` | 多节点镜像增量升级：把镜像 tar 分发到全部节点并滚动重启（重启面与换版脚本同源，见下一行；`--list-workloads` 只印集合） |
| `platform-image-workloads.sh` | 换镜像面**唯一的**判定：从 `k3s/` 清单读出哪些工作负载跑着本项目镜像（Deployment / DaemonSet / StatefulSet 的常驻容器；initContainer 与 CronJob 不算——前者随所属工作负载换、后者每次起 Pod 按浮动签拉）。换版与分发两个脚本都调这一份，各自抄名单时漏掉的那一项不会红，只会安静地继续跑旧二进制。空集合/解析失败报 2（判不了该滚谁），不退化成一次静默的空滚动 |
| `build-release-image.sh` | release 预构建运行时镜像的**本机单源**构建（产物 tar.gz + sha256） |
| `verify-firecracker.sh` | Firecracker 微虚拟机沙盒的真机端到端验证 |

### 关于 `k3s/` `k8s/` `helm/` 三个文件夹（不是三条可选路径）

- **`helm/` 是权威源**，`rendered/` 是它渲染出来的产物，`k3s/` 是它在 K3s 形态下的
  静态基线 + 自进化运行时消费物——三者是"源 → 产物 → 基线/消费物"的关系，不是三选一。
- **`k8s/` 不放应用清单**，只放标准 K8s 集群周边的基础设施参考；标准 K8s 部署应用走
  chart 的 `k8s-standard` profile。
- **环境差异全部通过 chart 的 profile values 表达**（containerd socket、存储类、
  git-remote 模式、kubectl 二进制等），不维护 kustomize overlay 或第二份应用清单；
  改应用拓扑统一改 `helm/cogneva/`。

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

关键 values：`backends.{postgres,redis,qdrant,nats,meilisearch}.enabled` 控制是否随 chart 部署后端（禁用即使用外部服务）；`evolution.enabled` 控制自进化 worker，`evolution.gitRemote.mode`（`hostPath` 单节点 / `pvc` 多节点）控制中央 bare 仓库供给；`sandboxExecutor.enabled` / `buildah.enabled` 控制沙盒执行器与节点镜像构建 DaemonSet；`buildah.containerdSocket` 适配发行版（K3s 为 `/run/k3s/containerd`，标准 containerd 为 `/run/containerd`）；`storage.localRetainClass.create`（K3s 专有 Retain StorageClass，标准 K8s 置 false）与 `storage.evolution`/`storage.retain` 控制各卷存储类（留空跟随集群默认 SC）；`gitops.kubectlBin.enabled`+`gitops.kubectlBin.hostPath` 控制主应用 GitOps 拉取端的 Pod 内 kubectl（K3s 挂宿主 k3s 二进制，节点无可用二进制时置 false）；`webhook.nodePort` 为平台 webhook 入口；`ingress.className`（values 默认 `nginx` 是中立默认值；K3s 两个 profile 覆盖为 `traefik`——K3s 自带 traefik 并把它标成默认 IngressClass；取 `traefik` 时自动附带 WebSocket/安全头 Middleware）；`networkPolicy.enabled` 控制沙盒出站隔离。内部密钥留空即安装时自动随机生成；预渲染清单路径用 `secrets.create=false`，密钥改由 init-secrets.sh 生成（bootstrap 自动调用）。

> **维护者注意**：应用拓扑唯一权威源是 Helm chart（`deploy/helm/cogneva/`）。
> `deploy/rendered/<profile>/` 是 chart 的 CI 渲染产物（`bash
> deploy/scripts/render-deploy.sh` 重新生成，CI 新鲜度门禁防漂移）；
> `deploy/k3s/` 静态清单是 parity 基线兼集群内 GitOps 拉取端的运行时消费物，
> 必须与 chart k3s profile 字段级对齐，由 CI 与 `deploy/scripts/check-deploy-parity.sh`
> 强制（覆盖多少资源与资源类型以脚本自报为准）。改拓扑的顺序见 `deploy/k3s/README.md`。

### 升级与回滚

升级就是重跑安装那一步：apply 投递的集群重新 `kubectl apply -f deploy/rendered/<profile>/`，
helm 投递的集群 `helm upgrade cogneva deploy/helm/cogneva -f ...`。两条路都幂等。

回滚（apply 投递）：

```bash
bash deploy/scripts/rollback.sh --to <上一版 rev>              # 只出计划，不碰集群
bash deploy/scripts/rollback.sh --to <上一版 rev> --apply --wait
```

单位是**那一版的 `deploy/rendered/<profile>/`**，也就是升级时吃进去的那份清单：
它把拓扑送回那一版，不动数据（PVC、库里的行），也不改仓库。它拒绝往前/旁支的版本
（那是变更，走评审），也拒绝那一版没有清单目录、或目录里没有 yaml（apply 空目录会
退出 0，读起来像回滚成功）。默认只打印计划，写集群要显式 `--apply`。

从 apply 投递迁到 helm 托管时，现网对象此前没有 helm 归属三件套，`helm upgrade` 会在
归属校验上整单拒收。先用 `helm upgrade cogneva deploy/helm/cogneva -f
deploy/helm/cogneva/profiles/<profile>.yaml --dry-run=server` 看缺哪些对象，确认它们都是
这个 chart 渲染的之后，再用 `--take-ownership`（helm ≥ 4）接管——接管意味着这些对象
从此由该 release 管理。


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

### Offsite copy (restic)

The backup CronJob writes packages to a local-path volume and, if `backup.offsiteHostPath`
is set, to a second directory **on the same host**. Neither survives the machine. This
unit pushes that directory to an encrypted restic repository on another machine:

```bash
sudo deploy/scripts/install-backup-offsite.sh \
    s3:https://your-bucket.example/cogneva /root/offsite-secrets /var/lib/cogneva-offsite
```

`/root/offsite-secrets` holds the plaintext `restic-password` (and `aws-access-key-id` /
`aws-secret-access-key` for S3-compatible backends). The installer encrypts each into
`/etc/cogneva/credentials/*.cred` with `systemd-creds`, deletes the plaintext, and installs
`cogneva-backup-offsite.{service,timer}`. At runtime the repository password exists only in
`$CREDENTIALS_DIRECTORY` (a tmpfs owned by the process) — never on disk, never in the
package, never in a Pod. Each credential directive in the unit carries its own
`# cred:` line — the permission the source file must have, who can rotate it, and why
it may stay on the host at all. `deploy/scripts/check-host-credential-surface.sh`
reddens when a directive has no such line (or a line has no directive), and CI runs it.

The bucket key should be write-only for objects it did not create and should not carry
`DeleteObject`: a compromised host must not be able to erase the copies that outlive it.

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
