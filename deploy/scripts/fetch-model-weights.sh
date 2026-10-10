#!/usr/bin/env bash
#
# 把本地模型（fastembed 加载的那几个）的 ONNX 权重拉到加载器会去找的那个目录里。
#
# 为什么这件事需要一个产出方：加载器（fastembed → hf-hub）是用缓存目录构造 API 的，
# 命中缓存就不发任何请求；到了不了 model hub 的网络里，权重必须在进程起来之前就位，
# 否则那次拉取**既不成功也不失败**，plugin init 挂到 liveness 杀 Pod。
#
# 写出的布局就是 hf-hub 自己下载时写的那个，之后网络可用时进程直接命中这些文件，一个
# 字节也不下载：
#
#   <dest>/models--<org>--<name>/blobs/<哈希>                  文件字节
#   <dest>/models--<org>--<name>/refs/main                     快照对应的 commit
#   <dest>/models--<org>--<name>/snapshots/<commit>/<文件>      软链指向上面的 blob
#
# refs/main 里**不带换行**：读它的那侧（hf-hub 的 CacheRepo::get）不 trim，读出来什么就
# 拼进快照路径；多一个换行等于把快照目录名写成一个不存在的名字，命中失败会静默退回网络。
#
# blob 以 hub 报的哈希命名，且每个下载先按它校验再安装：被截断或被改过的字节装不进去。
# 哈希有两种算法——大文件是 LFS 对象、哈希是内容的 sha256；小文件是 git blob、哈希是该
# blob 的 sha1（`sha1("blob <长度>\0" + 内容)`）。两种都校验，因为"有时校验"等于没校验。
# 名字多少位就按多少位校验：64 位十六进制当 sha256，40 位当 git blob sha1，其余长度没有
# 可校验的声明，直接拒绝而不是放过。
#
# 可重复执行：已经在该在的名字下的文件重算哈希、不重下；下载中的体先落在 `.part`，断线
# 后再跑会续传。
#
# 已经就位但哈希对不上的文件**不覆盖**，只报错退出：名字来自 hub 报的哈希，内容对不上
# 说明库里那份不是它自称的那份，而这可能是磁盘坏道、也可能是有人换过模型。重下确实能
# 自愈，但也会把"它曾经是什么"一起抹掉；运维删掉那一个文件再跑一次是几秒钟的事。
#
# 两个来源，一份布局。`--source` 决定从哪读清单与字节：
#
#   hf         HuggingFace 形状：清单在 `/api/models/<repo>?blobs=true`，每个文件的哈希要
#              再向 `/resolve/<commit>/<file>` 发一次 HEAD 拿（`x-linked-etag`，取不到才退
#              到 `etag`；跟随重定向之后的那条来自 CDN，它的 etag 是缓存对象自己的哈希，
#              实测与内容哈希不是一回事，所以不跟随）。
#   modelscope ModelScope 形状：清单在 `/api/v1/models/<repo>/repo/files?Revision=&Recursive=True`，
#              每条自带 `Sha256`，不用再发 HEAD。本机实测（2026-10-10）huggingface.co 与
#              hf-mirror.com 都连不上（DNS 通、连接失败），modelscope.cn 通——BAAI/bge-m3
#              因此走这条。
#
# 每个模型要哪些文件，由下面的 catalog 一处说了算：清单的完备性不是靠读代码保证的，是靠
# 一份离线加载测试钉住的（重排 `crates/cog-memory/tests/reranker_boundary.rs`、嵌入
# `crates/cog-memory/tests/embedding_boundary.rs`）——清单少了哪条，那次加载就失败。
#
# 用法：
#   deploy/scripts/fetch-model-weights.sh --model reranker --dest /srv/cogneva/models/fastembed
#   deploy/scripts/fetch-model-weights.sh --model bge-m3   --dest /srv/cogneva/models/fastembed
#   deploy/scripts/fetch-model-weights.sh --model bge-m3 --endpoint https://www.modelscope.cn ...

set -euo pipefail

readonly DEST_DEFAULT="/srv/cogneva/models/fastembed"

die() { echo "错误：$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
用法：fetch-model-weights.sh [--model <reranker|bge-m3>] [--dest <目录>] [--endpoint <URL>]
                            [--source <hf|modelscope>] [--repo <owner/name>] [--revision <rev>]
                            [--print-files]

  --model      要抓哪个模型的权重，缺省 reranker；可用的见下面
  --dest       写出的缓存目录，缺省 /srv/cogneva/models/fastembed
  --endpoint   镜像地址，缺省取 $HF_ENDPOINT，没有则按来源取默认
  --source     覆盖该模型缺省的 hub 形状（hf / modelscope）
  --repo       覆盖该模型缺省的仓库
  --revision   清单查询用的 revision，覆盖缺省（hf 是 main，modelscope 是 master）
  --print-files 只打印该模型要抓的文件清单（一行一个），不联网、不写盘
  -h, --help   这段文字

模型与它们缺省的来源：
  reranker   rozgo/bge-reranker-v2-m3   hf         （~2.2 GiB）
  bge-m3     BAAI/bge-m3                modelscope （~2.2 GiB，一份权重喂 dense 与 sparse 两个 session）

写出的是 hf-hub 自己的缓存布局（blobs/ + refs/main + snapshots/<commit>/），
可重复执行：已就位的重算哈希不重下，下载中断的续传。
EOF
}

# 每个模型一行：来源、仓库、清单查询用的 revision、加载器会打开的文件。
# 大文件排在最后：断线重跑时先落地的是那些小的，续传的往返花在真正大头上。
catalog() {
  case "$1" in
    reranker)
      source="hf"
      repo="rozgo/bge-reranker-v2-m3"
      revision="main"
      # 计算图与权重（RerankerModel::BGERerankerV2M3 的 model_file 与 additional_files），
      # 外加分词器与模型配置四条，缺一不可。
      files=(config.json model.onnx model.onnx.data special_tokens_map.json tokenizer.json tokenizer_config.json)
      ;;
    bge-m3)
      source="modelscope"
      repo="BAAI/bge-m3"
      revision="master"
      # EmbeddingModel::BGEM3 的 model_file 是 onnx/model.onnx，additional_files 是
      # onnx/model.onnx_data 与 onnx/Constant_7_attr__value（计算图旁边的常量张量）；
      # 分词器那侧另外打开仓库根下的四条。加载器建 dense 与 sparse 两个 session，而
      # SparseModel::BGEM3 指的就是同一批文件（sparse 的分类头编译在 crate 里），所以
      # 这七条是**两个 session 合起来**要打开的全部文件，抓这一份就够。
      files=(config.json special_tokens_map.json tokenizer_config.json tokenizer.json onnx/Constant_7_attr__value onnx/model.onnx onnx/model.onnx_data)
      ;;
    *) die "不认识的 --model：$1（可用的：reranker、bge-m3）" ;;
  esac
}

model="reranker"
dest="$DEST_DEFAULT"
endpoint=""
source_override=""
repo_override=""
revision_override=""
print_files=""

while [ $# -gt 0 ]; do
  case "$1" in
    --model) model="${2:?--model 需要一个值}"; shift 2 ;;
    --dest) dest="${2:?--dest 需要一个值}"; shift 2 ;;
    --endpoint) endpoint="${2:?--endpoint 需要一个值}"; shift 2 ;;
    --source) source_override="${2:?--source 需要一个值}"; shift 2 ;;
    --repo) repo_override="${2:?--repo 需要一个值}"; shift 2 ;;
    --revision) revision_override="${2:?--revision 需要一个值}"; shift 2 ;;
    --print-files) print_files="1"; shift ;;
    -h | --help) usage; exit 0 ;;
    *) die "不认识的参数：$1（--help 看用法）" ;;
  esac
done

catalog "$model"
[ -n "$source_override" ] && source="$source_override"
[ -n "$repo_override" ] && repo="$repo_override"
[ -n "$revision_override" ] && revision="$revision_override"
[ "$source" = "hf" ] || [ "$source" = "modelscope" ] ||
  die "不认识的 --source：$source（可用的：hf、modelscope）"

if [ -n "$print_files" ]; then
  printf '%s\n' "${files[@]}"
  exit 0
fi

# 来源的缺省 endpoint：hf 那条本机实测连不上，缺省仍按惯例指向 hf-mirror（别的网络里它
# 是通的），实际能不能到由调用者用 --endpoint 决定。
if [ -z "$endpoint" ]; then
  if [ "$source" = "hf" ]; then
    endpoint="${HF_ENDPOINT:-https://hf-mirror.com}"
  else
    endpoint="${HF_ENDPOINT:-https://www.modelscope.cn}"
  fi
fi

command -v curl >/dev/null || die "缺少依赖：curl"
command -v python3 >/dev/null || die "缺少依赖：python3"
command -v sha256sum >/dev/null || die "缺少依赖：sha256sum"
command -v sha1sum >/dev/null || die "缺少依赖：sha1sum"

endpoint="${endpoint%/}"
repo_dir="models--${repo//\//--}"
root="$dest/$repo_dir"

# 清单打成统一的形状：每行 `路径 \t 字节数 [\t 哈希 \t revision]`，另有一行 `commit \t <sha>`
# 打头。两个来源在这里各解析各的，下面的逻辑只认这个形状。
if [ "$source" = "hf" ]; then
  listing="$(
    curl -fsSL --retry 3 --retry-delay 2 "$endpoint/api/models/$repo?blobs=true" |
      python3 -c '
import json, sys
d = json.load(sys.stdin)
print("commit\t{}".format(d["sha"]))
for s in d["siblings"]:
    lfs = s.get("lfs") or {}
    print("{}\t{}".format(s["rfilename"], lfs.get("size", s.get("size", 0))))
'
  )" || die "取仓库清单失败：$endpoint/api/models/$repo"
else
  listing="$(
    curl -fsSL --retry 3 --retry-delay 2 \
      "$endpoint/api/v1/models/$repo/repo/files?Revision=$revision&Recursive=True" |
      python3 -c '
import json, sys
for f in (json.load(sys.stdin).get("Data") or {}).get("Files") or []:
    if f.get("Type") == "blob":
        print("{}\t{}\t{}\t{}".format(
            f.get("Path", ""), f.get("Size", 0), f.get("Sha256", ""), f.get("Revision", "")))
'
  )" || die "取仓库清单失败：$endpoint/api/v1/models/$repo"
fi

field_of() {
  printf '%s\n' "$listing" | awk -F'\t' -v f="$1" -v n="$2" '$1 == f { print $n; exit }'
}

# 清单里每个必需文件都得在，且都得指向同一个 commit——一半来自旧 commit、一半来自新 commit
# 的快照目录没有意义，宁可在这里停下。
commit=""
for file in "${files[@]}"; do
  size="$(field_of "$file" 2)"
  [ -n "$size" ] && [ "$size" != "0" ] ||
    die "仓库清单里没有 $file（或它没有字节数），权重集与仓库对不上"
  if [ "$source" = "hf" ]; then
    # hf 的清单不带 per-file revision，commit 是那条响应自己的 sha。
    [ -n "$commit" ] || commit="$(printf '%s\n' "$listing" | awk -F'\t' '$1 == "commit" { print $2; exit }')"
    [ -n "$commit" ] || die "仓库清单里没有 commit，无法拼成快照"
  else
    rev="$(field_of "$file" 4)"
    [ -n "$rev" ] || die "仓库清单里没有 $file（或它不是 blob），权重集与仓库对不上"
    if [ -z "$commit" ]; then
      commit="$rev"
    elif [ "$commit" != "$rev" ]; then
      die "清单里的文件来自不同 revision（$commit 与 $rev），无法拼成一个快照"
    fi
  fi
done

# 按名字所声明的算法校验，不是"算一个哈希看一眼"：64 位十六进制是内容的 sha256，40 位是
# git blob 的 sha1，其余长度没有可校验的声明，直接拒绝而不是放过。
verify_file() {
  local file="$1" expected="$2" actual
  case "${#expected}" in
    64) actual="$(sha256sum "$file" | cut -d' ' -f1)" ;;
    40)
      local size
      size="$(stat -c%s "$file")"
      actual="$( { printf 'blob %s\0' "$size"; cat "$file"; } | sha1sum | cut -d' ' -f1)"
      ;;
    *) die "hub 报的哈希既不是 sha256 也不是 git blob sha1，无法校验：$expected" ;;
  esac
  [ "$actual" = "$expected" ] ||
    die "校验失败：$file 的哈希是 $actual，清单声明的是 $expected"
}

# hub 那条响应上的 etag（不跟随重定向）：跟随之后拿到的是 CDN 那条，它的 etag 不是内容哈希。
# HEAD 不被支持时退到只取一个字节的 GET，同样不跟随。
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

# 该在的名字：hf 要从 hub 现问，modelscope 清单里自带。
declared_sha_of() {
  if [ "$source" = "hf" ]; then
    etag_of "$1"
  else
    field_of "$1" 3
  fi
}

download_url() {
  if [ "$source" = "hf" ]; then
    printf '%s/%s/resolve/%s/%s' "$endpoint" "$repo" "$commit" "$1"
  else
    printf '%s/api/v1/models/%s/repo?Revision=%s&FilePath=%s' "$endpoint" "$repo" "$commit" "$1"
  fi
}

# 快照软链相对 blobs/ 的写法：软链在 snapshots/<commit>/ 下，路径里每多一层目录就多退一级。
# 顶层文件退两级，onnx/<x> 退三级。
rel_target() {
  local file="$1" dir="${1%/*}" ups="../.."
  if [ "$dir" != "$file" ]; then
    local IFS='/' n=0
    for _ in $dir; do n=$((n + 1)); done
    for _ in $(seq 1 "$n"); do ups="$ups/.."; done
  fi
  printf '%s/blobs' "$ups"
}

mkdir -p "$root/blobs" "$root/refs"

echo "模型 $model（$source）仓库 $repo @ $commit → $root"
for file in "${files[@]}"; do
  expected_size="$(field_of "$file" 2)"
  link="$root/snapshots/$commit/$file"
  mkdir -p "$(dirname "$link")"

  # 已经装过的文件，它该在的名字就写在快照软链上（软链的目标就是 blobs/<哈希>）。先读本地
  # 再问 hub：一次完整的重跑因此只花一次清单请求，不比"每个文件发一次 HEAD"差在别处。
  sha=""
  if [ -L "$link" ]; then
    sha="$(readlink "$link" | sed 's|.*/||')"
  fi

  if [ -n "$sha" ] && [ -f "$root/blobs/$sha" ]; then
    verify_file "$root/blobs/$sha" "$sha"
    echo "  已就位  $file  $(numfmt --to=iec "$expected_size")"
  else
    # 只有没装过（软链不在，或它指向的 blob 已被删）才向 hub 要哈希。
    [ -n "$sha" ] || sha="$(declared_sha_of "$file")"
    [ -n "$sha" ] || die "$file 没有哈希，无法校验下载（hub 那条响应上既无 x-linked-etag 也无 etag）"
    blob="$root/blobs/$sha"
    part="$blob.part"
    # 续传：目标字节数已知，断点接着下；下完先按声明的哈希校验再改名，半截的体进不了 blobs/。
    if [ -f "$part" ]; then
      echo "  续传    $file  $(numfmt --to=iec "$(stat -c%s "$part")") / $(numfmt --to=iec "$expected_size")"
    else
      echo "  下载    $file  $(numfmt --to=iec "$expected_size")"
    fi
    curl -fSL --retry 5 --retry-delay 3 -C - --no-progress-meter "$(download_url "$file")" -o "$part" ||
      die "$file 下载失败（已下的字节留在 $part，重跑会续传）"

    got_size="$(stat -c%s "$part")"
    [ "$got_size" = "$expected_size" ] ||
      die "$file 字节数不对：下到 $got_size，仓库清单说 $expected_size"
    verify_file "$part" "$sha"
    mv -f "$part" "$blob"
  fi

  ln -sfn "$(rel_target "$file")/$sha" "$link"
done

printf '%s' "$commit" >"$root/refs/main"

echo "完成：$root"
echo "  refs/main = $(cat "$root/refs/main")"
case "$model" in
  reranker)
    echo "  重跑那份测量：FASTEMBED_CACHE_DIR=$dest \\"
    echo "    cargo test -p cog-memory --test reranker_boundary -- --ignored --nocapture"
    ;;
  bge-m3)
    echo "  离线加载测试：FASTEMBED_CACHE_DIR=$dest \\"
    echo "    cargo test -p cog-memory --test embedding_boundary -- --ignored --nocapture"
    ;;
esac
