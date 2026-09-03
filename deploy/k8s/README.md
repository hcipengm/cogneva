# deploy/k8s — K8s 分支说明

## 一键路径（bootstrap 使用）

K8s 分支的一键部署**不直接使用本目录的应用清单**，而是：

1. **镜像分发器** `image-distributor.yaml`（本目录，bootstrap 内嵌为模板）：
   镜像服务 Pod + 每节点 DaemonSet（用宿主 `ctr` 把镜像导入各节点 containerd），
   常设保留。多节点集群里单节点 `ctr import` 覆盖不到其他节点，必须逐节点导入。
2. **应用清单复用 `deploy/k3s/`**：应用本身是集群无关的；bootstrap apply 前
   按集群环境自动适配——无 local-path provisioner 时 PVC 回落集群默认
   StorageClass，CN 模式公开镜像统一走 daocloud 前缀。

## 多节点集群从哪来

- **空白多机**：`COGNEVA_CLUSTER_NODES="user@ip2,user@ip3"`（SSH 免密可达），
  bootstrap 在本机装 K3s server、向各节点推送安装 agent。K3s 是 CNCF 认证
  K8s，多节点 K3s 即多节点 K8s。
- **已有集群**：kubectl 可连通即可，分支按节点数自动判定。
- **生产仓库供给**：设置 `COGNEVA_IMAGE_REGISTRY` 后 bootstrap 跳过逐节点
  分发，镜像引用由运维自行对齐仓库地址。

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

## 本目录其余文件（生产周边参考，不进一键路径）

argocd / cert-manager / ingress-nginx / longhorn / metallb / velero /
monitoring / meilisearch / postgres / redis / qdrant：面向域名化、持久化、
GitOps 化的生产部署参考，需要按真实环境调整后再用。
cogneva-deployment.yaml 已对齐当前现实（namespace/探针/secret 键名与
deploy/k3s 一致，数据卷换 Longhorn PVC，附 ClusterIP Service），可作
多副本生产部署的起点。cogneva.service 为裸机 systemd 参考（不走容器编排
时使用），二进制 /opt/cogneva/bin/cogneva、配置 /etc/cogneva。

## 密钥与凭证（生产参考清单）

- 本目录清单**不 apply 任何 Secret**，仓库不含可用密码。部署前先运行
  `bash deploy/k3s/init-secrets.sh`（脚本通用、仅依赖 kubectl，与 kustomize
  无关）：自动随机生成 PostgreSQL/Redis/内部签名密钥并创建 cogneva-secrets，
  幂等、不覆盖已有值；生产也可用 Vault/Sealed Secrets 等外部管理器注入同名键。
  参考键结构见 `../k3s/examples/secret.yaml`。
- postgres / redis 已统一引用 cogneva-secrets 的 `pg-password` / `redis-password`，
  不再单独维护 postgres-secrets。
- 主应用**零带外凭证**：不挂 OPENAI/ANTHROPIC/GitHub/Gitee 任何 token，LLM 与
  代码平台凭证只注入安全网关，主应用经 `COGNEVA_GITHUB_API_BASE` /
  `COGNEVA_GITEE_API_BASE` / `COGNEVA_GIT_PROXY_BASE` 走网关代理。本参考骨架
  未含安全网关与进化沙盒，完整生产拓扑请用 Helm chart（`deploy/helm/cogneva`）。
