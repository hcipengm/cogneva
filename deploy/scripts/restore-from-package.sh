#!/usr/bin/env bash
# 换机/重装恢复：把 `cogneva backup` 产出的 tar.zst 包恢复到目标集群。
#
# 前置：本机 kubectl 已指向目标集群；包文件在本机。顺序是身份先行——
# 先 apply 包内 Secret（init-secrets.sh 幂等跳过既有 key，实例指纹保持），
# 再要求集群已完成正常安装（bootstrap/helm，表结构由迁移建好），
# 最后用一次性 Job 把数据灌回去并滚动重启业务部署。
#
# 用法：deploy/scripts/restore-from-package.sh <cogneva-backup-*.tar.zst> [命名空间]
set -euo pipefail

PKG="${1:-}"
NS="${2:-cogneva}"
if [ -z "$PKG" ] || [ ! -f "$PKG" ]; then
    echo "用法: $0 <cogneva-backup-*.tar.zst> [命名空间，默认 cogneva]" >&2
    exit 2
fi
command -v kubectl >/dev/null || { echo "缺少 kubectl" >&2; exit 2; }

BASE="$(basename "$PKG")"
STAGE_POD="cogneva-restore-stage"
JOB_NAME="cogneva-restore-$(date +%s)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
tar --zstd -xf "$PKG" -C "$TMP"
# 包内顶层目录名 = 包名去掉 .tar.zst 后缀。
PKG_DIR="$TMP/${BASE%.tar.zst}"
[ -f "$PKG_DIR/manifest.json" ] || { echo "包内缺少 manifest.json，不是有效备份包" >&2; exit 1; }
echo "==> 包清单："
cat "$PKG_DIR/manifest.json"

echo "==> 1/4 创建命名空间并恢复实例身份（Secret 先行，init-secrets.sh 幂等跳过）"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$PKG_DIR/secrets/cogneva-secrets.yaml"

echo "==> 2/4 等待正常安装就绪（postgres / qdrant）"
if ! kubectl -n "$NS" get deployment cogneva >/dev/null 2>&1; then
    echo "命名空间 $NS 内没有 cogneva 部署——先跑 bootstrap/helm 完成正常安装，再执行本脚本" >&2
    exit 1
fi
kubectl -n "$NS" rollout status deployment/postgres --timeout=300s
kubectl -n "$NS" rollout status deployment/qdrant --timeout=300s

# 用运行中的业务镜像做恢复载体：集群内必然拉得到，版本与部署面一致。
IMAGE="$(kubectl -n "$NS" get deployment cogneva -o jsonpath='{.spec.template.spec.containers[0].image}')"
echo "==> 3/4 上传包到备份卷并跑恢复 Job（镜像 $IMAGE）"

# 先起一个只挂备份卷的暂存 Pod 把包 kubectl cp 进去；恢复 Job 再从卷上读。
kubectl -n "$NS" delete pod "$STAGE_POD" --ignore-not-found --wait=true
kubectl -n "$NS" run "$STAGE_POD" --restart=Never --image="$IMAGE" \
    --overrides='{
      "spec": {
        "containers": [{
          "name": "'"$STAGE_POD"'",
          "image": "'"$IMAGE"'",
          "command": ["/bin/sh", "-c", "sleep 3600"],
          "volumeMounts": [{"name": "backup", "mountPath": "/backups"}]
        }],
        "volumes": [{"name": "backup", "persistentVolumeClaim": {"claimName": "cogneva-backup-pvc"}}]
      }
    }'
kubectl -n "$NS" wait --for=condition=Ready "pod/$STAGE_POD" --timeout=180s
kubectl -n "$NS" cp "$PKG" "$STAGE_POD:/backups/$BASE"
kubectl -n "$NS" delete pod "$STAGE_POD" --wait=true

kubectl -n "$NS" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: $NS
  labels:
    app.kubernetes.io/name: cogneva
    app.kubernetes.io/component: restore
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: restore
          image: $IMAGE
          args: ["restore", "/backups/$BASE"]
          env:
            - name: PG_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: cogneva-secrets
                  key: pg-password
            - name: COGNEVA_DATABASE_URL
              value: "postgres://cog_user:\$(PG_PASSWORD)@postgres.$NS.svc.cluster.local:5432/cogneva"
            - name: COGNEVA_QDRANT_HTTP_URL
              value: "http://qdrant.$NS.svc.cluster.local:6333"
          volumeMounts:
            - name: data
              mountPath: /var/lib/cogneva-data
            - name: backup
              mountPath: /backups
              readOnly: true
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: cogneva-data-pvc
        - name: backup
          persistentVolumeClaim:
            claimName: cogneva-backup-pvc
EOF
kubectl -n "$NS" wait --for=condition=Complete "job/$JOB_NAME" --timeout=1800s || {
    echo "==> 恢复 Job 失败，日志：" >&2
    kubectl -n "$NS" logs "job/$JOB_NAME" >&2
    exit 1
}
kubectl -n "$NS" logs "job/$JOB_NAME" | tail -20

echo "==> 4/4 滚动重启业务部署，让内存态对齐持久层"
for d in cogneva cogneva-evolution cogneva-sandbox-executor cogneva-security-gateway; do
    kubectl -n "$NS" rollout restart "deployment/$d"
done
for d in cogneva cogneva-evolution cogneva-sandbox-executor cogneva-security-gateway; do
    kubectl -n "$NS" rollout status "deployment/$d" --timeout=300s
done
echo "==> 恢复完成"
