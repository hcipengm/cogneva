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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PKG="${1:-}"
NS="${2:-cogneva}"
if [ -z "$PKG" ] || [ ! -f "$PKG" ]; then
    echo "用法: $0 <cogneva-backup-*.tar.zst> [命名空间，默认 cogneva]" >&2
    exit 2
fi
command -v kubectl >/dev/null || { echo "缺少 kubectl" >&2; exit 2; }

BASE="$(basename "$PKG")"
STAGE_POD="cogneva-restore-stage"
# 调用方可以把名字定下来（空白机恢复要把 Job 名写进回执、事后按名字取日志）；
# 没给就按时刻取一个。
JOB_NAME="${COGNEVA_RESTORE_JOB_NAME:-cogneva-restore-$(date +%s)}"

TMP="$(mktemp -d)"
# 写者缩下去之后，任何一步失败都不能悄悄把它们停在 0 副本上：那是一条「装是装完了，
# 但服务全没起」的结局，而现场只看到脚本退出码非 0。恢复失败时**故意**不放行（半装
# 的数据不该被拿去服务），但必须把「现在是停着的、怎么放」喊出来。
APP_REPLICAS=()
RESUMED=0
cleanup() {
    local rc=$?
    rm -rf "$TMP"
    if [ "$rc" -ne 0 ] && [ "${#APP_REPLICAS[@]}" -gt 0 ] && [ "$RESUMED" -ne 1 ]; then
        echo "==> 业务部署仍是 0 副本（恢复没走完，故意不放行）。原副本数：${APP_REPLICAS[*]}" >&2
        echo "    修好后重跑本脚本即可；要手工放行：kubectl -n $NS scale --replicas=<数> deployment/<名字>" >&2
    fi
}
trap cleanup EXIT
tar --zstd -xf "$PKG" -C "$TMP"
# 包内顶层目录名 = 包名去掉 .tar.zst 后缀。
PKG_DIR="$TMP/${BASE%.tar.zst}"
[ -f "$PKG_DIR/manifest.json" ] || { echo "包内缺少 manifest.json，不是有效备份包" >&2; exit 1; }
echo "==> 包清单："
cat "$PKG_DIR/manifest.json"
# 出包那一版的源码 rev。代次判据用它，不用镜像 tag —— tag 是派生的，可以改。
PKG_REV="$(sed -n 's/^[[:space:]]*"revision":[[:space:]]*"\([^"]*\)".*/\1/p' "$PKG_DIR/manifest.json" | head -1)"

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

# 写者必须先后退。恢复写的是持久层，而读同一批数据的进程此刻还在跑：写者不停，
# 恢复回去的字节与它们手里那份内存态就没有先后关系，恢复「成功」之后它们继续按
# 旧状态写同一张表；进化 Pod 还握着策略库所在的卷，恢复写进去的版本文件可能被它
# 下一轮覆盖。所以先把这些部署缩到 0，等 Pod 真的没了再动数据。
#
# 集合只算一次：下面缩容、放行都用上面那一份，不在两处各读一遍集群（两处会漂）。
# quiesce: start
APP_REPLICAS=()
quiesce_writers() {
    local d r sel
    for d in "${APP_DEPLOYMENTS[@]}"; do
        r="$(kubectl -n "$NS" get deploy "$d" -o jsonpath='{.spec.replicas}')"
        APP_REPLICAS+=("$d=${r:-1}")
        kubectl -n "$NS" scale --replicas=0 "deployment/$d" >/dev/null
    done
    # 缩容是异步的：desired 变 0 时 Pod 还在正常退出，此刻开恢复会和它们抢同一块卷。
    # 按 Deployment 自己的 selector 等 Pod 全消失；卷是 RWO，最后一个读者走了才挂得上。
    for d in "${APP_DEPLOYMENTS[@]}"; do
        sel="$(kubectl -n "$NS" get deploy "$d" \
            -o jsonpath='{range $k,$v := .spec.selector.matchLabels}{$k}={$v},{end}')"
        sel="${sel%,}"
        if kubectl -n "$NS" get pods -l "$sel" -o name 2>/dev/null | grep -q .; then
            kubectl -n "$NS" wait --for=delete pod -l "$sel" --timeout=300s >/dev/null
        fi
    done
    # 走到这里＝每个部署的 Pod 都等没了（等超时会被 set -e 带走），所以这一行是
    # 读数而不是声明。原副本数一并印出来：回执与日志里能对上「待会儿要放回几个」。
    echo "    writers_running=0 originals: ${APP_REPLICAS[*]}"
}
resume_writers() {
    local pair d r
    for pair in "${APP_REPLICAS[@]}"; do
        d="${pair%%=*}"
        r="${pair##*=}"
        kubectl -n "$NS" scale --replicas="$r" "deployment/$d" >/dev/null
    done
    # 从 0 起回来的 Pod 是新的，本身就把内存态重放了一遍，不再额外 restart。
    for pair in "${APP_REPLICAS[@]}"; do
        d="${pair%%=*}"
        kubectl -n "$NS" rollout status "deployment/$d" --timeout=300s
    done
    RESUMED=1
}
# quiesce: end

echo "==> 3/4 停写者、上传包到备份卷并跑恢复 Job（镜像 $IMAGE）"
quiesce_writers

# 先起一个只挂备份卷的暂存 Pod 把包 kubectl cp 进去；恢复 Job 再从卷上读。
kubectl -n "$NS" delete pod "$STAGE_POD" --ignore-not-found --wait=true
# 暂存 Pod 以备份/演练同一个专用 uid 写包：kubectl cp 用容器里的 tar 解包，
# 解出来的文件属主就是容器 uid。以 root 跑的话包会以 root 0600 落盘，下一场
# 演练（非 root）既读不到它，又会把「属主不是自己」判红——恢复路径自己制造
# 一次误报。init 先把目录交给这个 uid，否则 0700 的目录连写都不允许。
kubectl -n "$NS" run "$STAGE_POD" --restart=Never --image="$IMAGE" \
    --overrides='{
      "spec": {
        "securityContext": {"runAsNonRoot": true, "runAsUser": 1001, "runAsGroup": 1001},
        "initContainers": [{
          "name": "own-backups",
          "image": "'"$IMAGE"'",
          "command": ["/bin/sh", "-c", "chown -R 1001:1001 /backups"],
          "securityContext": {"runAsNonRoot": false, "runAsUser": 0},
          "volumeMounts": [{"name": "backup", "mountPath": "/backups"}]
        }],
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
# 恢复 Job 跑的就是这份镜像，所以代次比对要问它自己，而不是问 tag：tag 是派生
# 的，可以改；二进制里 baked 的那个 rev 才是代码状态。问不到就跳过，退回显式接受。
IMAGE_REV="$(kubectl -n "$NS" exec "$STAGE_POD" -- /opt/cogneva/cogneva --version 2>/dev/null \
    | sed -n 's/.*(rev \(.*\))$/\1/p' | tr -d '[:space:]' || true)"
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

# 进化 bare 仓的落点：跟着目标部署自己的声明走（pvc / hostPath 两种模式）。
# 包里那份 bundle 由恢复 Job 种回去，且必须发生在写者重新启动之前——它此刻
# 正被停在 0 副本上。
GIT_ENV=""
GIT_MOUNTS=""
GIT_VOLUMES=""
GIT_VOLUME_JSON="$(kubectl -n "$NS" get deployment cogneva-evolution \
    -o jsonpath='{.spec.template.spec.volumes[?(@.name=="git-remote")]}' 2>/dev/null || true)"
if printf '%s' "$GIT_VOLUME_JSON" | grep -q '"persistentVolumeClaim"'; then
    GIT_VOLUMES='        - name: git-remote
          persistentVolumeClaim:
            claimName: cogneva-git-remote-pvc'
elif printf '%s' "$GIT_VOLUME_JSON" | grep -q '"hostPath"'; then
    GIT_HOST_PATH="$(printf '%s' "$GIT_VOLUME_JSON" | python3 -c \
        'import json,sys; print(json.load(sys.stdin)["hostPath"]["path"])' 2>/dev/null || true)"
    if [ -n "$GIT_HOST_PATH" ]; then
        GIT_VOLUMES="        - name: git-remote
          hostPath:
            path: \"$GIT_HOST_PATH\""
    fi
fi
if [ -n "$GIT_VOLUMES" ]; then
    GIT_ENV='            - name: COGNEVA_RESTORE_GIT_REMOTE_DIR
              value: "/git-remote"'
    GIT_MOUNTS='            - name: git-remote
              mountPath: /git-remote'
fi

# 代次：包 rev 必须被恢复镜像的 rev 包含，不是字符串相等。仓库在宿主上，所以
# 「包含」这件事只能在这里用 git 回答；证明的两个 rev 一起交给 Job，它核对证明
# 说的正是眼前的包与自己。包更新（或宿主上没有那段历史）时证明不出来，仍旧
# 退回显式接受，并在下面说清楚为什么。
#
# 抽成函数是为了单独跑得动：这段唯一的产物是「交给 Job 的两条 env」，证明不出来时
# 产物为空、只印原因。产物走 stdout、人读的话走 stderr，判据才钉得住方向——
# 否则「向前放了、向后也放」在输出里长得一样。
# ancestry: start
ancestry_proof_env() { # 读 PKG_REV / IMAGE_REV / ROOT：能证明包含关系就打印 env；证明不出来返回 1
    [ -n "$PKG_REV" ] && [ -n "$IMAGE_REV" ] && [ "$PKG_REV" != "$IMAGE_REV" ] || return 0
    if command -v git >/dev/null 2>&1 \
        && git -C "$ROOT" cat-file -e "${IMAGE_REV}^{commit}" 2>/dev/null \
        && git -C "$ROOT" merge-base --is-ancestor "$PKG_REV" "$IMAGE_REV" 2>/dev/null; then
        echo "    代次：包 rev $PKG_REV 是恢复镜像 $IMAGE_REV 的祖先，按前进方向安装" >&2
        printf '            - name: COGNEVA_RESTORE_ANCESTRY_PACKAGE_REV\n              value: "%s"\n            - name: COGNEVA_RESTORE_ANCESTRY_SOURCE_REV\n              value: "%s"\n' \
            "$PKG_REV" "$IMAGE_REV"
        return 0
    fi
    echo "    代次：证明不了包 rev $PKG_REV 是恢复镜像 $IMAGE_REV 的祖先（源码更旧或宿主上没有那段历史）；要恢复请显式设 COGNEVA_RESTORE_ACCEPT_REV_MISMATCH=1" >&2
    return 1
}
# ancestry: end
ANCESTRY_ENV="$(ancestry_proof_env)" || true

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
      # 恢复与备份、演练跑在同一个专用非 root uid 上。恢复读的正是备份写的包，
      # 三处 uid 一致，「包只有属主可读」才是一句真话；恢复端若还是 root，
      # A8 在演练那边绿了而换机这一条路照样谁都能读。
      securityContext:
        runAsNonRoot: true
        runAsUser: 1001
        runAsGroup: 1001
      # 0700 的卷目录对非属主连遍历都不给，先把属主交给主容器的 uid。
      initContainers:
        - name: own-backups
          image: $IMAGE
          command: ["/bin/sh", "-c", "chown -R 1001:1001 /backups"]
          securityContext:
            runAsNonRoot: false
            runAsUser: 0
          volumeMounts:
            - name: backup
              mountPath: /backups
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
            # 出包那一代的镜像引用，只作对照：同一个 rev 可以有多个构建产物，
            # 拿它当身份会拦住合法恢复。
            - name: COGNEVA_IMAGE_REF
              value: "$IMAGE"
            # 跨代恢复要显式接受。默认 0：包与二进制不是一个 rev 就停，让属主
            # 自己说「我知道，装吧」，而不是默认滑过去。
            - name: COGNEVA_RESTORE_ACCEPT_REV_MISMATCH
              value: "${COGNEVA_RESTORE_ACCEPT_REV_MISMATCH:-0}"
$ANCESTRY_ENV
$GIT_ENV
$EVOLUTION_ENV
          volumeMounts:
            - name: data
              mountPath: /var/lib/cogneva-data
$EVOLUTION_MOUNTS
$GIT_MOUNTS
            - name: backup
              mountPath: /backups
              readOnly: true
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: cogneva-data-pvc
$EVOLUTION_VOLUMES
$GIT_VOLUMES
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

echo "==> 4/4 放行业务部署，让它们从持久层重放内存态"
resume_writers
echo "==> 恢复完成"
