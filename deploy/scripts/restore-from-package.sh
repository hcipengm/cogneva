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
# 按包内文件逐个 apply，不写死对象名：包里有几片密钥对象就恢复几片。写死一份
# 会让另一份（带外凭证：平台 token、git 身份私钥）静默留在包里没被恢复，而集群
# 看起来一切正常——直到需要推送代码或访问上游时才发现身份是空的。
for f in "$PKG_DIR"/secrets/*.yaml; do
    [ -f "$f" ] || { echo "包内没有密钥清单（$PKG_DIR/secrets/*.yaml）" >&2; exit 1; }
    echo "    apply $(basename "$f")"
    kubectl apply -f "$f"
done

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

# 产物层写回：包里按数据根带了策略库与决策统计，恢复 Job 就必须每个根都挂得到。
# 少挂一个根，恢复端会因为「包里有这个根、给不出目标路径」直接失败——这是有意
# 的：静默少写一份，换机后那套进化状态就是空的，而回执说成功。
EVOLUTION_ENV=""
EVOLUTION_MOUNTS=""
EVOLUTION_VOLUMES=""
EVOLUTION_AFFINITY=""
if kubectl -n "$NS" get pvc cogneva-evolution-data-pvc >/dev/null 2>&1; then
    EVOLUTION_ENV='            - name: COGNEVA_RESTORE_EXTRA_DATA_ROOTS
              value: "evolution=/var/lib/cogneva-evolution-data"'
    EVOLUTION_MOUNTS='            - name: evolution-data
              mountPath: /var/lib/cogneva-evolution-data'
    EVOLUTION_VOLUMES='        - name: evolution-data
          persistentVolumeClaim:
            claimName: cogneva-evolution-data-pvc'
    # 进化卷是 RWO：只有与持有它的 Pod 落在同一节点才挂得上。进化 Pod 不在场时
    # 加这条会让 Job 永远调度不上（required 亲和找不到目标），所以只在场才钉。
    if kubectl -n "$NS" get pods \
        -l app.kubernetes.io/name=cogneva-evolution,app.kubernetes.io/component=evolution \
        -o name | grep -q .; then
        EVOLUTION_AFFINITY='      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app.kubernetes.io/name: cogneva-evolution
                  app.kubernetes.io/component: evolution
              topologyKey: kubernetes.io/hostname'
    fi
fi

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
$EVOLUTION_AFFINITY
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
$EVOLUTION_ENV
          volumeMounts:
            - name: data
              mountPath: /var/lib/cogneva-data
$EVOLUTION_MOUNTS
            - name: backup
              mountPath: /backups
              readOnly: true
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: cogneva-data-pvc
$EVOLUTION_VOLUMES
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
# The set is read from the cluster rather than transcribed: what the restore gives
# back is the persistent layer, and the deployments that have to replay in-memory
# state are the ones running an image this project publishes -- a set that grows.
# A transcribed list keeps working when it grows and skips the new deployment in
# silence: the restore reports success while that deployment still holds the state
# it had before. The repository name comes from the same $IMAGE read above (digest
# and tag stripped), so the registry prefix and the rev tag follow the live cluster
# instead of being maintained here.
# rollout-set: start
IMAGE_REPO="$(printf '%s' "$IMAGE" | sed -E 's/@.*$//; s/:[^/]*$//')"
APP_DEPLOYMENTS=()
while IFS=$'\t' read -r name images; do
    [ -n "$name" ] || continue
    read -ra imgs <<<"$images"
    for img in "${imgs[@]}"; do
        repo="$(printf '%s' "$img" | sed -E 's/@.*$//; s/:[^/]*$//')"
        if [ "$repo" = "$IMAGE_REPO" ]; then
            APP_DEPLOYMENTS+=("$name")
            break
        fi
    done
done < <(kubectl -n "$NS" get deploy \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.template.spec.containers[*]}{.image}{" "}{end}{"\n"}{end}')
if [ "${#APP_DEPLOYMENTS[@]}" -eq 0 ]; then
    echo "No deployment runs this project's image ($IMAGE_REPO); the restore would leave every" \
        "in-memory state unreplayed" >&2
    exit 1
fi
# rollout-set: end
for d in "${APP_DEPLOYMENTS[@]}"; do
    kubectl -n "$NS" rollout restart "deployment/$d"
done
for d in "${APP_DEPLOYMENTS[@]}"; do
    kubectl -n "$NS" rollout status "deployment/$d" --timeout=300s
done
echo "==> 恢复完成"
