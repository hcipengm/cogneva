#!/usr/bin/env bash
#
# 把重排模型的 ONNX 权重拉到加载器会去找的那个目录里，来源是任何 HuggingFace
# 兼容的镜像。
#
# 为什么这件事需要一个产出方：加载器（fastembed → hf-hub）是用缓存目录构造 API 的，
# 而那个构造把 endpoint 写死——重排这条路不吃 HF_ENDPOINT（embedding 那条吃）。
# 于是到了不了 huggingface.co 的网络里，没有任何运行期开关能改掉它，权重必须在进程
# 起来之前就位；在此之前仓库里没有任何东西产出过这个目录。
#
# 写出的布局就是 hf-hub 自己下载时写的那个，所以之后网络可用时进程直接命中这些
# 文件，一个字节也不下载：
#
#   <dest>/models--<org>--<name>/blobs/<etag>                 文件字节
#   <dest>/models--<org>--<name>/refs/main                    快照对应的 commit
#   <dest>/models--<org>--<name>/snapshots/<commit>/<文件>     软链指向上面的 blob
#
# refs/main 里**不带换行**：读它的那侧不 trim，读出来什么就拼进快照路径；多一个换行
# 等于把快照目录名写成一个不存在的名字，命中失败会静默退回网络。
#
# blob 以 hub 报的 etag 命名，且每个下载先按 etag 校验再安装：被截断或被改过的字节
# 装不进去。etag 有两种算法——大文件是 LFS 对象、etag 是内容的 sha256；小文件是
# git blob、etag 是该 blob 的 sha1（`sha1("blob <长度>\0" + 内容)`）。两种都校验，
# 因为"有时校验"等于没校验。etag 从 hub 那条响应上取（`x-linked-etag`，取不到才退
# 到 `etag`）：跟随重定向之后的那条来自 CDN，它的 etag 是缓存对象自己的哈希，
# 实测与内容哈希不是一回事。
#
# 可重复执行：已经在该在的名字下的文件重算哈希、不重下；下载中的体先落在 `.part`，
# 断线后再跑会续传。
#
# 已经就位但哈希对不上的文件**不覆盖**，只报错退出：名字来自 hub 的 etag，内容对不上
# 说明库里那份不是它自称的那份，而这可能是磁盘坏道、也可能是有人换过模型。重下确实能
# 自愈，但也会把"它曾经是什么"一起抹掉；运维删掉那一个文件再跑一次是几秒钟的事。
#
# 用法：
#   deploy/scripts/fetch-reranker-weights.sh --dest /srv/cogneva/models/fastembed
#   deploy/scripts/fetch-reranker-weights.sh --endpoint https://hf-mirror.com --dest ...
#
# 目录按 hf-hub 的缓存布局写，fastembed 直接认：不给 cache_dir 时它读环境变量
# FASTEMBED_CACHE_DIR。部署里现在没有任何东西加载这个模型，执行器也没有这份挂载——
# 本地那一站经实测不成立，读数在 crates/cog-memory/tests/reranker_boundary.rs。这个
# 脚本留着的理由就是那次测量能重跑：权重得有出处，而 2.3 GiB 里那一堆哈希、refs/main
# 不带换行这些细节手抄必错。

set -euo pipefail

readonly REPO_DEFAULT="rozgo/bge-reranker-v2-m3"
readonly DEST_DEFAULT="/srv/cogneva/models/fastembed"
readonly ENDPOINT_DEFAULT="https://hf-mirror.com"

# 加载器会打开的文件，一个不多一个不少：
#   model.onnx / model.onnx.data  —— 计算图与权重（`RerankerModel::BGERerankerV2M3`
#                                    的 model_file 与 additional_files）
#   tokenizer.json、config.json、special_tokens_map.json、tokenizer_config.json
#                                  —— 分词器与模型配置，四条缺一不可
# 这份清单的完备性不是靠读代码保证的，是靠一份离线加载测试钉住的：清单少了哪条，
# 那次加载就失败。
readonly NEEDED_FILES=(
  config.json
  model.onnx
  model.onnx.data
  special_tokens_map.json
  tokenizer.json
  tokenizer_config.json
)

die() { echo "错误：$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
用法：fetch-reranker-weights.sh [--repo <owner/name>] [--dest <目录>] [--endpoint <URL>]

  --repo      权重所属的仓库，缺省 rozgo/bge-reranker-v2-m3
  --dest      写出的缓存目录，缺省 /srv/cogneva/models/fastembed
  --endpoint  镜像地址，缺省取 $HF_ENDPOINT，没有则 https://hf-mirror.com
  -h, --help  这段文字

写出的是 hf-hub 自己的缓存布局（blobs/ + refs/main + snapshots/<commit>/），
可重复执行：已就位的重算哈希不重下，下载中断的续传。
EOF
}

repo="$REPO_DEFAULT"
dest="$DEST_DEFAULT"
endpoint="${HF_ENDPOINT:-$ENDPOINT_DEFAULT}"

while [ $# -gt 0 ]; do
  case "$1" in
    --repo) repo="${2:?--repo 需要一个值}"; shift 2 ;;
    --dest) dest="${2:?--dest 需要一个值}"; shift 2 ;;
    --endpoint) endpoint="${2:?--endpoint 需要一个值}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) die "不认识的参数：$1（--help 看用法）" ;;
  esac
done

command -v curl >/dev/null || die "缺少依赖：curl"
command -v python3 >/dev/null || die "缺少依赖：python3"
command -v sha256sum >/dev/null || die "缺少依赖：sha256sum"
command -v sha1sum >/dev/null || die "缺少依赖：sha1sum"

endpoint="${endpoint%/}"
repo_dir="models--${repo//\//--}"
root="$dest/$repo_dir"

# 仓库清单：commit 与每个文件的字节数。字节数是校验的一端——哈希对不上时它把
# "下少了"和"下错了"分开报。
listing="$(
  curl -fsSL --retry 3 --retry-delay 2 "$endpoint/api/models/$repo?blobs=true" |
    python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d["sha"])
for s in d["siblings"]:
    lfs = s.get("lfs") or {}
    print("{}\t{}".format(s["rfilename"], lfs.get("size", s.get("size", 0))))
'
)" || die "取仓库清单失败：$endpoint/api/models/$repo"

commit="$(printf '%s\n' "$listing" | head -1)"
[ -n "$commit" ] || die "仓库清单里没有 commit"
size_of() {
  printf '%s\n' "$listing" | awk -F'\t' -v f="$1" '$1 == f { print $2; exit }'
}

# hub 那条响应上的 etag（不跟随重定向）：跟随之后拿到的是 CDN 那条，它的 etag 不是
# 内容哈希。HEAD 不被支持时退到只取一个字节的 GET，同样不跟随。
etag_of() {
  local file="$1" headers etag
  headers="$(curl -fsSI -m 60 "$endpoint/$repo/resolve/$commit/$file" 2>/dev/null || true)"
  if ! printf '%s' "$headers" | grep -qi '^x-linked-etag:\|^etag:'; then
    headers="$(curl -fsS -m 60 -r 0-0 -D - -o /dev/null "$endpoint/$repo/resolve/$commit/$file" 2>/dev/null || true)"
  fi
  etag="$(
    printf '%s' "$headers" |
      tr -d '\r' |
      grep -i '^x-linked-etag:' |
      head -1 |
      sed 's/^[^:]*:[[:space:]]*//; s/"//g'
  )"
  if [ -z "$etag" ]; then
    etag="$(
      printf '%s' "$headers" |
        tr -d '\r' |
        grep -i '^etag:' |
        tail -1 |
        sed 's/^[^:]*:[[:space:]]*//; s/"//g'
    )"
  fi
  printf '%s' "$etag"
}

# 按名字所声明的算法校验，不是"算一个哈希看一眼"：64 位十六进制是内容的 sha256，
# 40 位是 git blob 的 sha1，其余长度没有可校验的声明，直接拒绝而不是放过。
verify_file() {
  local file="$1" expected="$2" actual
  case "${#expected}" in
    64) actual="$(sha256sum "$file" | cut -d' ' -f1)" ;;
    40)
      local size
      size="$(stat -c%s "$file")"
      actual="$( { printf 'blob %s\0' "$size"; cat "$file"; } | sha1sum | cut -d' ' -f1)"
      ;;
    *) die "etag 既不是 sha256 也不是 git blob sha1，无法校验：$expected" ;;
  esac
  [ "$actual" = "$expected" ] ||
    die "校验失败：$file 的哈希是 $actual，etag 声明的是 $expected"
}

mkdir -p "$root/blobs" "$root/refs" "$root/snapshots/$commit"

echo "仓库 $repo @ $commit → $root"
for file in "${NEEDED_FILES[@]}"; do
  expected_size="$(size_of "$file")"
  if [ -z "$expected_size" ] || [ "$expected_size" = "0" ]; then
    die "仓库清单里没有 $file（或它没有字节数），权重集与仓库对不上"
  fi

  # 已经装过的文件，它的 etag 就写在快照软链上（软链的目标就是 blobs/<etag>）。
  # 先读本地再问 hub：一次完整的重跑因此只花一次清单请求，不比"每个文件发一次
  # HEAD"差在别处——那 6 次往返问的是同一件已经写在盘上的事。
  link="$root/snapshots/$commit/$file"
  etag=""
  if [ -L "$link" ]; then
    etag="$(readlink "$link" | sed 's|.*/||')"
  fi

  if [ -n "$etag" ] && [ -f "$root/blobs/$etag" ]; then
    verify_file "$root/blobs/$etag" "$etag"
    echo "  已就位  $file  $(numfmt --to=iec "$expected_size")"
  else
    # 只有没装过（软链不在，或它指向的 blob 已被删）才问 hub 要 etag。
    [ -n "$etag" ] || etag="$(etag_of "$file")"
    [ -n "$etag" ] || die "$file 没有 etag，无法校验下载（hub 那条响应上既无 x-linked-etag 也无 etag）"
    blob="$root/blobs/$etag"
    part="$blob.part"
    # 续传：目标字节数已知，断点接着下；下完先按 etag 校验再改名，半截的体进不了
    # blobs/。
    if [ -f "$part" ]; then
      echo "  续传    $file  $(numfmt --to=iec "$(stat -c%s "$part")") / $(numfmt --to=iec "$expected_size")"
    else
      echo "  下载    $file  $(numfmt --to=iec "$expected_size")"
    fi
    curl -fSL --retry 5 --retry-delay 3 -C - --no-progress-meter \
      "$endpoint/$repo/resolve/$commit/$file" -o "$part" ||
      die "$file 下载失败（已下的字节留在 $part，重跑会续传）"

    got_size="$(stat -c%s "$part")"
    [ "$got_size" = "$expected_size" ] ||
      die "$file 字节数不对：下到 $got_size，仓库清单说 $expected_size"
    verify_file "$part" "$etag"
    mv -f "$part" "$blob"
  fi

  # 软链用相对路径（hf-hub 自己也是这么写的），整棵目录能整体搬走。
  ln -sfn "../../blobs/$etag" "$link"
done

printf '%s' "$commit" >"$root/refs/main"

echo "完成：$root"
echo "  refs/main = $(cat "$root/refs/main")"
echo "  重跑那份测量：FASTEMBED_CACHE_DIR=$dest \\"
echo "    cargo test -p cog-memory --test reranker_boundary -- --ignored --nocapture"
