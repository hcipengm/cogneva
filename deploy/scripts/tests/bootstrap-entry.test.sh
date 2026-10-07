#!/usr/bin/env bash
# bootstrap.sh 的判据测试：受限网络分类、apt 源改写、权限结算。
# 全程不联网——PATH 前置假 curl / 假 sudo / 假 id，注入"通/不通"与"是不是 root"。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
FAKE="$(mktemp -d)"
FAKE2="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$FAKE2"' EXIT

fails=0
check() { # check <描述> <期望> <实际>
    if [ "$2" = "$3" ]; then
        echo "ok   - $1"
    else
        echo "FAIL - $1: 期望 [$2] 实际 [$3]"
        fails=$((fails + 1))
    fi
}

# 假 curl：按 $BLOCKED（空格分隔的 URL 子串）决定回 000（不通）还是 200。
# 只回状态码，正好是 probe_reachable 用 -w '%{http_code}' 取的那一项。
cat > "$FAKE/curl" <<'EOF'
#!/bin/sh
url=""
for a in "$@"; do
    case "$a" in http*) url="$a" ;; esac
done
for h in ${BLOCKED:-}; do
    case "$url" in *"$h"*) echo 000; exit 0 ;; esac
done
echo 200
EOF
# 假 sudo：模拟已配置免密 sudo（`sudo -n true` 成功）
cat > "$FAKE/sudo" <<'EOF'
#!/bin/sh
[ "${1:-}" = "-n" ] && shift
exec "$@"
EOF
# 假 id：模拟非 root 用户
cat > "$FAKE/id" <<'EOF'
#!/bin/sh
[ "${1:-}" = "-u" ] && { echo 1000; exit 0; }
exec /usr/bin/id "$@"
EOF
chmod +x "$FAKE"/*
cp "$FAKE/id" "$FAKE2/id"
cp "$FAKE/curl" "$FAKE2/curl"

ORIG_PATH="$PATH"
export PATH="$FAKE:$PATH"

COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 . "$ROOT/bootstrap.sh"
set +eu

# ---------- 受限网络判据 ----------
export BLOCKED=""
detect_restricted_net >/dev/null 2>&1
check "全部信号可达 → 开放网络" 0 "$CN_MIRROR"

export BLOCKED="registry-1.docker.io"
# 直接调用而不是 $(...) 捕获：命令替换会开子 shell，函数里算出的 CN_MIRROR 出不来
detect_restricted_net > "$FAKE/out" 2>&1
out="$(cat "$FAKE/out")"
check "单条信号不可达 → 受限网络" 1 "$CN_MIRROR"
case "$out" in
    *registry-1.docker.io*) echo "ok   - 受限判定点名了不可达的信号" ;;
    *) echo "FAIL - 受限判定未点名不可达信号: $out"; fails=$((fails + 1)) ;;
esac

# B1 的真实事故形态：rustup 分发域可达（旧判据的唯一探针），GitHub raw 被墙。
export BLOCKED="raw.githubusercontent.com"
detect_restricted_net >/dev/null 2>&1
check "rustup 可达但 GitHub raw 被墙 → 仍判受限" 1 "$CN_MIRROR"

export BLOCKED=""
COGNEVA_CN_MIRROR=1 detect_restricted_net >/dev/null 2>&1
check "COGNEVA_CN_MIRROR=1 强制受限（跳过探测）" 1 "$CN_MIRROR"
export BLOCKED="registry-1.docker.io"
COGNEVA_CN_MIRROR=0 detect_restricted_net >/dev/null 2>&1
check "COGNEVA_CN_MIRROR=0 强制开放（不因不可达改判）" 0 "$CN_MIRROR"
unset BLOCKED COGNEVA_CN_MIRROR

# ---------- apt 源改写（纯函数） ----------
MIRROR="https://mirrors.tuna.tsinghua.edu.cn/ubuntu"
check "legacy 一行式换源" \
    "deb $MIRROR noble main" \
    "$(printf '%s\n' 'deb http://archive.ubuntu.com/ubuntu noble main' | rewrite_apt_sources "$MIRROR")"
check "deb822 URIs 换源" \
    "URIs: $MIRROR" \
    "$(printf '%s\n' 'URIs: http://archive.ubuntu.com/ubuntu' | rewrite_apt_sources "$MIRROR")"
check "security 主机也换" \
    "deb $MIRROR noble-security main" \
    "$(printf '%s\n' 'deb http://security.ubuntu.com/ubuntu noble-security main' | rewrite_apt_sources "$MIRROR")"
# 第三方 PPA / 内网源不能被顺手改掉
check "非官方源原样保留" \
    "deb https://ppa.example.com/ubuntu noble main" \
    "$(printf '%s\n' 'deb https://ppa.example.com/ubuntu noble main' | rewrite_apt_sources "$MIRROR")"
DMIRROR="https://mirrors.ustc.edu.cn/debian"
check "debian-security 走 -security 后缀" \
    "deb $DMIRROR-security bookworm-security main" \
    "$(printf '%s\n' 'deb http://security.debian.org/debian-security bookworm-security main' | rewrite_apt_sources "$DMIRROR")"

# ---------- git 选路（实测优先 + 策略表兜底） ----------
# 假 git：ls-remote 的成败与耗时由 GIT_TEST_* 注入，clone 只记一次调用。
# 这样测的是真实的候选次序判据，而不是把脚本里的常量抄一遍。
cat > "$FAKE/git" <<'EOF'
#!/bin/sh
case "$1" in
    ls-remote)
        url=""
        for a in "$@"; do
            case "$a" in git@*|ssh://*|http*) url="$a" ;; esac
        done
        case "$url" in
            git@*|ssh://*)
                ms="${GIT_TEST_SSH_MS:-}"
                [ "${GIT_TEST_SSH_OK:-1}" = "1" ] || exit 128
                ;;
            *)
                ms="${GIT_TEST_HTTPS_MS:-}"
                [ "${GIT_TEST_HTTPS_OK:-1}" = "1" ] || exit 128
                ;;
        esac
        [ -n "$ms" ] && sleep "$ms"
        exit 0
        ;;
    clone)
        echo "clone $*" >> "${GIT_TEST_LOG:-/dev/null}"
        exit "${GIT_TEST_CLONE_RC:-0}"
        ;;
esac
exit 0
EOF
chmod +x "$FAKE/git"

KEYDIR="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$FAKE2" "$KEYDIR"' EXIT
: > "$KEYDIR/id_ed25519"
SSH_URL="git@github.com:hcipengm/cogneva.git"

# 候选取序（空格分隔），断言用全序列：只断言首选会把"回落次序丢了"漏过去
cands() { git_candidates 2>/dev/null | tr '\n' ' ' | sed 's/ $//'; }

# 密钥一律显式给路径：不给就落到 ~/.cogneva/.ssh/id_ed25519，那台机器上有没有
# 部署密钥会决定判据方向，测试不能依赖它。
export COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519"

# 开放网络 + 有密钥：都通、HTTPS 更快 → HTTPS 优先（与加选路之前一致）
export CN_MIRROR=0 GIT_TEST_SSH_MS=0.2 GIT_TEST_HTTPS_MS=0.05
check "开放网络实测 HTTPS 更快 → HTTPS 优先" \
    "$REPO_URL $SSH_URL $GITEE_REPO_URL" "$(cands)"

# 实测压过画像：开放网络本该 HTTPS 优先，但实测 SSH 快就用 SSH
export CN_MIRROR=0 GIT_TEST_SSH_MS=0.05 GIT_TEST_HTTPS_MS=0.2
check "实测 SSH 更快 → SSH 优先（区域只是画像，不是测量）" \
    "$SSH_URL $REPO_URL $GITEE_REPO_URL" "$(cands)"

# 受限网络 + SSH 不通：实测给结论 → HTTPS 优先，SSH 留作兜底而不是被删掉
export CN_MIRROR=1 GIT_TEST_SSH_OK=0 GIT_TEST_HTTPS_MS=0.05
check "SSH 不可达 → HTTPS 优先（SSH 仍在候选里兜底）" \
    "$REPO_URL $SSH_URL $GITEE_REPO_URL" "$(cands)"

# 两条都测不到：实测没有信息量 → 退回策略表（受限网络 SSH 优先）
export CN_MIRROR=1 GIT_TEST_SSH_OK=0 GIT_TEST_HTTPS_OK=0
check "两条都没测通 → 按策略表（受限网络 SSH 优先）" \
    "$SSH_URL $REPO_URL $GITEE_REPO_URL" "$(cands)"

# 没有部署密钥：SSH 不进候选（结构上不可用，不是排后面）
export CN_MIRROR=1 COGNEVA_GIT_SSH_KEY="$KEYDIR/absent"
check "无部署密钥 → 候选里只有 HTTPS（即便 SSH 测得通）" \
    "$REPO_URL $GITEE_REPO_URL" "$(cands)"
export COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519"
unset GIT_TEST_SSH_MS GIT_TEST_HTTPS_MS GIT_TEST_SSH_OK GIT_TEST_HTTPS_OK CN_MIRROR

# 首选失败必须真的轮到下一个，而不是停在原地（早先的写法里第二个源永远试不到）。
# 这里跑的是真的 fetch_source，所以三件事缺一不可：
#   COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 —— 否则 source 本文件会**执行整个安装流程**
#     （测试里跑装机，且装到一半退出、后面的 fetch_source 根本不执行）；
#   COGNEVA_HOME 指向临时目录 —— 空机器模式把源码落在 $DEFAULT_HOME/src，
#     不指走就会去看真实仓库在不在，命中"源码已存在"直接返回、一个 clone 都不发；
#   cd 到临时目录 —— 克隆后执行 ./bootstrap.sh 的入口靠 `dirname $0` 认当前仓库，
#     从仓库根跑会命中"使用当前仓库"。
: > "$FAKE/clone.log"
(
    cd "$FAKE" || exit 1
    PATH="$FAKE:$ORIG_PATH" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 COGNEVA_HOME="$FAKE/home" \
        COGNEVA_GIT_SSH_KEY="$KEYDIR/id_ed25519" \
        GIT_TEST_LOG="$FAKE/clone.log" GIT_TEST_CLONE_RC=1 \
        /bin/sh -c ". '$ROOT/bootstrap.sh' >/dev/null 2>&1; fetch_source" >/dev/null 2>&1
)
check "首选远端失败 → 逐个试完全部候选" 3 "$(wc -l < "$FAKE/clone.log" | tr -d ' ')"
rm -rf "${FAKE:?}/home" "${FAKE:?}/clone.log"

# ---------- 权限结算 ----------
ensure_privileges >/dev/null 2>&1
check "非 root + 免密 sudo → 用 sudo 提权" "sudo" "$SUDO"

# 没有 sudo 可用时必须当场退出并说清要什么权限，而不是跑到深处 EACCES
err="$(PATH="$FAKE2" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 /bin/sh -c \
    ". '$ROOT/bootstrap.sh' >/dev/null 2>&1; ensure_privileges" 2>&1)"
rc=$?
check "非 root 且无 sudo → 非零退出" 1 "$rc"
case "$err" in
    *"需要 root"*) echo "ok   - 缺权限时报错指明需要 root" ;;
    *) echo "FAIL - 缺权限报错未指明原因: $err"; fails=$((fails + 1)) ;;
esac

# ---------- 入口命令：取不到脚本必须报错，不许静默成功 ----------
# 旧写法 `(A || B) | sh` 在两条腿都失败时把**空输入**递给 sh，而 sh 读空 stdin 以 0
# 退出：命令报成功、机器上什么都没装（macOS/WSL 路径还会接着打印「完成！」并打开
# 一个指向空服务的浏览器；实测旧写法两腿都失败时 rc=0、零输出）。
# 这里把**每一处载体的原文**抽出来逐条跑，而不是重新拼一份命令——重新拼只能证明我
# 抄得对，证明不了载体本身带着这道判据。
ENTRY="$(mktemp -d)"
trap 'rm -rf "$FAKE" "$FAKE2" "$KEYDIR" "$ENTRY"' EXIT

# 假 curl：按 URL 认腿、回放该腿正文；ENTRY_FAKE_FAIL=all|<leg> 时按失败退出
# （真 curl 带 -f，HTTP 错误同样是非零退出，这里用 22 对齐）。
cat > "$ENTRY/curl" <<'EOF'
#!/bin/sh
url=""
for a in "$@"; do
    case "$a" in http*) url="$a" ;; esac
done
case "$url" in
    *gitee.com*) leg=gitee ;;
    *raw.githubusercontent.com*) leg=github ;;
    *) echo "假 curl 收到未知 URL: $url" >&2; exit 2 ;;
esac
case "${ENTRY_FAKE_FAIL:-}" in
    all|"$leg") exit 22 ;;
esac
cat "$(dirname "$0")/$leg.body"
EOF
chmod +x "$ENTRY/curl"

# 两条腿回放**不同的**标记：这样「首选失败真的轮到兜底」是可断言的事实。
# 正文是可执行的 sh（与真脚本同形），并回显 COGNEVA_CN_MIRROR——强制模式能不能透到
# 真正的引导器也一并量了。
cat > "$ENTRY/github.body" <<'EOF'
#!/bin/sh
printf 'ENTRY-RAN github\n'
printf 'ENTRY-CN=%s\n' "${COGNEVA_CN_MIRROR-unset}"
EOF
cat > "$ENTRY/gitee.raw" <<'EOF'
#!/bin/sh
printf 'ENTRY-RAN gitee\n'
printf 'ENTRY-CN=%s\n' "${COGNEVA_CN_MIRROR-unset}"
EOF
# Gitee 腿走 API contents：JSON 进、base64 "content" 出、decode。假正文必须是同一形状，
# 否则测的是我编的管道，不是真命令的解码路径。
printf '{"content":"%s"}\n' "$(base64 < "$ENTRY/gitee.raw" | tr -d '\n')" > "$ENTRY/gitee.body"
rm -f "$ENTRY/gitee.raw"

# 载体清单。标签格式 `<来源>:<型号>:<首选腿>`。
# 分母按**来源**给（两个 README 各两处安装片段、ps1 两个字符串、bootstrap.sh 两个
# 常量）：只断言总数的话，删掉一处、别处加一处，总数照样对得上。
entry_carriers() {
    printf 'bootstrap.sh:INTL:github\t%s\n' "$ENTRY_CMD_INTL"
    printf 'bootstrap.sh:CN:gitee\t%s\n' "$ENTRY_CMD_CN"
    for f in README.md README.zh-CN.md; do
        grep -nF 'src="$(curl -fsSL -m 15 https://raw.githubusercontent.com/hcipengm/cogneva/main/bootstrap.sh' "$ROOT/$f" |
            while IFS=: read -r ln text; do
                printf '%s:%s:github\t%s\n' "$f" "$ln" "$text"
            done
    done
    # PowerShell 单引号串里的 '' 是一个转义出来的 '；还原成 sh 看到的文本再比。
    sed -n "s/^\$EntryCmdIntl *= *'\(.*\)'\$/bootstrap.ps1:Intl:github\t\1/p" "$ROOT/bootstrap.ps1" | sed "s/''/'/g"
    sed -n "s/^\$EntryCmdCn *= *'\(.*\)'\$/bootstrap.ps1:Cn:gitee\t\1/p" "$ROOT/bootstrap.ps1" | sed "s/''/'/g"
}

ok_or() { # ok_or <描述> <必须出现的串> <输出>
    case "$3" in
        *"$2"*) echo "ok   - $1" ;;
        *) echo "FAIL - $1: 输出里没有 [$2]: $3"; fails=$((fails + 1)) ;;
    esac
}
not_or() { # not_or <描述> <不许出现的串> <输出>
    case "$3" in
        *"$2"*) echo "FAIL - $1: 输出里不该有 [$2]: $3"; fails=$((fails + 1)) ;;
        *) echo "ok   - $1" ;;
    esac
}

declare -A seen=()
n_carriers=0
while IFS=$'\t' read -r label cmd; do
    [ -n "${cmd:-}" ] || continue
    src="${label%%:*}"
    first="${label##*:}"
    second=gitee
    [ "$first" = "gitee" ] && second=github
    n_carriers=$((n_carriers + 1))
    seen[$src]=$(( ${seen[$src]:-0} + 1 ))

    up="$(PATH="$ENTRY:$ORIG_PATH" sh -c "$cmd" 2>&1)"; rc=$?
    check "[$label] 两条腿都在 → 退出码 0" 0 "$rc"
    ok_or "[$label] 两条腿都在 → 取到的正文真的被执行" "ENTRY-RAN $first" "$up"

    # 首选腿失败必须真的轮到兜底腿（旧写法里这一步靠 `||` 在工作，不能顺手改坏）
    fb="$(PATH="$ENTRY:$ORIG_PATH" ENTRY_FAKE_FAIL="$first" sh -c "$cmd" 2>&1)"
    ok_or "[$label] 首选腿失败 → 真的轮到兜底腿" "ENTRY-RAN $second" "$fb"

    # 主体判据：两条腿都失败 ⇒ 非零退出 + 说明原因 + 什么都没执行
    down="$(PATH="$ENTRY:$ORIG_PATH" ENTRY_FAKE_FAIL=all sh -c "$cmd" 2>&1)"; rc=$?
    check "[$label] 两条腿都失败 → 非零退出（旧写法这里是 0）" 1 "$rc"
    not_or "[$label] 两条腿都失败 → 没有执行任何东西" "ENTRY-RAN" "$down"
    ok_or "[$label] 两条腿都失败 → 报错说明原因" "入口脚本取不到" "$down"

    # 强制模式的前缀写法必须透到真正的引导器：入口命令改成变量赋值开头后，
    # `COGNEVA_CN_MIRROR=1 <一键命令>` 的前缀不再落在管道上（实测会丢）。
    forced="$(PATH="$ENTRY:$ORIG_PATH" sh -c "COGNEVA_CN_MIRROR=1 $cmd" 2>&1)"
    ok_or "[$label] COGNEVA_CN_MIRROR=1 前缀仍透到引导器" "ENTRY-CN=1" "$forced"
done < <(entry_carriers)

for src in bootstrap.sh bootstrap.ps1 README.md README.zh-CN.md; do
    check "载体清单：$src 的入口命令处数" 2 "${seen[$src]:-0}"
done
check "载体清单：载体总数（4 个来源 × 2 处）" 8 "$n_carriers"

# 同侧载体必须逐字相同。一条命令散在 4 个文件 8 个地方、没有判据盯着，就一定会漂：
# 单改 README 的 `-m 15`、单改 ps1 的腿序，都不会有任何东西变红。
check "INTL 侧只有一个版本的命令文本" 1 "$(entry_carriers | awk -F'\t' '$1 ~ /:github$/ {print $2}' | sort -u | wc -l | tr -d ' ')"
check "CN 侧只有一个版本的命令文本" 1 "$(entry_carriers | awk -F'\t' '$1 ~ /:gitee$/ {print $2}' | sort -u | wc -l | tr -d ' ')"

# ---------- ensure_rust：取不到 rustup 脚本不许读成「装好了」 ----------
# 与入口命令同一形状：`curl … | sh -s` 的退出码是 sh 的，而 sh 读空 stdin 以 0 退出。
# 跑的是 bootstrap.sh 自己那份函数（文件在上面已经 source 过），PATH 只留假 curl 与
# 一个 sh —— **没有 cargo**，否则函数会在第一行提前 return，这一整段就成了空跑
# （判据没有题目时是空的，不是绿的）。
RF="$FAKE/rust"
mkdir -p "$RF"
cat > "$RF/curl" <<'EOF'
#!/bin/sh
case "${RUSTUP_FAKE:-ok}" in
    fail) exit 22 ;;
    empty) exit 0 ;;
    *)
        # 取到的「脚本」：只打一行记号、写掉末尾那次 `.` 要 source 的那个文件
        echo 'echo RUSTUP-RAN'
        echo 'printf true > "$HOME/.cargo/env"'
        exit 0 ;;
esac
EOF
chmod +x "$RF/curl"
ln -sf /bin/sh "$RF/sh"

rust_home() { mktemp -d; }

run_ensure_rust() { # run_ensure_rust <假法> <HOME>
    HOME="$2" PATH="$RF" CN_MIRROR=0 RUSTUP_FAKE="$1" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 \
        /bin/sh -c '. "$1/bootstrap.sh" >/dev/null 2>&1; ensure_rust' _ "$ROOT" 2>&1
    echo "rc=$?"
}

h1="$(rust_home)"; out_fail="$(run_ensure_rust fail "$h1")"
ok_or "ensure_rust：取不到 → 说出是哪一步取不到" "rustup 安装脚本取不到" "$out_fail"
not_or "ensure_rust：取不到 → 失败不许落在 cargo/env 这个后果上" "cargo/env" "$out_fail"
ok_or "ensure_rust：取不到 → 非零退出" "rc=1" "$out_fail"

h2="$(rust_home)"; out_empty="$(run_ensure_rust empty "$h2")"
ok_or "ensure_rust：回 200 但正文为空 → 同样当场报「取不到」" "rustup 安装脚本取不到" "$out_empty"
ok_or "ensure_rust：回 200 但正文为空 → 非零退出" "rc=1" "$out_empty"

h3="$(rust_home)"; mkdir -p "$h3/.cargo"; out_ok="$(run_ensure_rust ok "$h3")"
ok_or "ensure_rust：取到 → 正文真的被执行" "RUSTUP-RAN" "$out_ok"
ok_or "ensure_rust：取到 → 正常退出" "rc=0" "$out_ok"
check "ensure_rust：取到 → 末尾那次 source 有东西可读" "yes" "$([ -f "$h3/.cargo/env" ] && echo yes || echo no)"

# 对照（旧写法必须命中这条判据）：`curl … | sh -s` 的管道退出码在空输入下是 0，
# 而失败只会以 `. "$HOME/.cargo/env"` 这个**后果**的形式出现。
h4="$(rust_home)"; old_rc="$(HOME="$h4" PATH="$RF" RUSTUP_FAKE=fail /bin/sh -c \
    'curl --proto "=https" --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal' 2>/dev/null; echo $?)"
check "对照：旧写法的管道退出码（空输入 ⇒ 0）" 0 "$old_rc"
h5="$(rust_home)"; old_out="$(HOME="$h5" PATH="$RF" RUSTUP_FAKE=fail /bin/sh -c \
    'curl --proto "=https" --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"' 2>&1)"
ok_or "对照：旧写法的失败只说后果（cargo/env 打不开）" "cargo/env" "$old_out"
not_or "对照：旧写法里没有一句提到取不到" "取不到" "$old_out"

# ---------- macOS 路径：VM 内失败不许被读成「完成」 ----------
# 假 limactl 把 `shell` 之后的参数原样执行 —— 于是这条判据跑的是**真的**入口命令，
# 而不是我编造的一个「VM 返回 1」。
cat > "$ENTRY/limactl" <<'EOF'
#!/bin/sh
case "${1:-}" in
    --version) echo "limactl version 0.0.0-fake"; exit 0 ;;
    list)
        case "${2:-}" in
            -q) echo cogneva ;;
            *)  echo "cogneva Running" ;;
        esac
        exit 0
        ;;
    shell) shift 3; exec "$@" ;;
esac
exit 0
EOF
chmod +x "$ENTRY/limactl"

macos_run() { # $1 = ENTRY_FAKE_FAIL
    PATH="$ENTRY:$ORIG_PATH" ENTRY_FAKE_FAIL="$1" COGNEVA_CN_MIRROR=0 \
        COGNEVA_HOME="$ENTRY/home" COGNEVA_BOOTSTRAP_SOURCE_ONLY=1 \
        /bin/sh -c ". '$ROOT/bootstrap.sh' >/dev/null 2>&1; macos_bootstrap" 2>&1
}

mac_ok="$(macos_run "")"; rc=$?
check "macOS 路径：VM 内成功 → 退出码 0" 0 "$rc"
ok_or "macOS 路径：VM 内成功 → 报完成" "完成！" "$mac_ok"

mac_bad="$(macos_run all)"; rc=$?
check "macOS 路径：VM 内取不到脚本 → 非零退出" 1 "$rc"
not_or "macOS 路径：VM 内取不到脚本 → 不许报完成" "完成！" "$mac_bad"
ok_or "macOS 路径：VM 内取不到脚本 → 说明未完成" "VM 内引导未完成" "$mac_bad"

# ---------- bootstrap.ps1 的投递结构：CI 对 ps1 只做语法检查 ----------
# 上面那份载体清单钉的是 `$EntryCmd*` 的**文本**。投递方式那一半没有任何判据看着：
# 入口命令是当 stdin 喂给 WSL 里的 sh、还是当 argv 传（wsl.exe 会重建命令行，引号往返
# 在 Windows 侧不可测，错了也是静默的）、那次调用的退出码有没有当场结算、强制模式是
# 跟着 stdin 一起 export 还是写成一个会被丢掉的 `VAR=…` 前缀。这几条都是**行为**，
# PSParser 只看语法子树，全改动完也不会红。所以下面拿真文件过一遍，再拿三个变异体
# 各拒一次——判据要能拒，拒不了的只是描述，而这三处恰好都踩在 D36/D37 那条线上。
ps1_delivery_fails() { # 读 ps1 文本，逐条报「哪一条不过」；全过则无输出
    awk '
        { l[NR] = $0 }
        END {
            for (i = 1; i <= NR; i++) {
                # 投递行：以 `-- sh` 结尾（argv 写法会带 -c "…"，不以它结尾）
                if (l[i] ~ /\| wsl\.exe -d Ubuntu -u root -- sh$/) dl = i
                if (l[i] ~ /sh -c[ ]*"\$entry/) argv = 1
            }
            if (!dl) print "no-stdin-delivery"
            else {
                # 喂进去的必须是那个变量，不是别的什么
                if (l[dl] !~ /\$entry" \| wsl\.exe/) print "entry-not-in-stdin-payload"
                # 强制模式在 payload 里自己占一行（入口命令以赋值开头，前缀会丢）
                if (l[dl] !~ /^"export COGNEVA_CN_MIRROR=\$cn`n\$entry"/) print "forced-mode-not-exported"
                # 这一次调用的退出码当场结算，不能交给后面的步骤去撞
                if (l[dl + 1] !~ /LASTEXITCODE/) print "exit-code-not-checked"
            }
            if (argv) print "entry-passed-as-argv"
        }
    '
}

PS1="$ROOT/bootstrap.ps1"
check "ps1 投递结构：真文件的五条都过" "" "$(ps1_delivery_fails < "$PS1")"

ps1_mutant() { # ps1_mutant <说明> <期望被拒在哪一条> <变异：awk 字面替换 from> <to>
    MUT_FROM="$3" MUT_TO="$4" awk '
        BEGIN { from = ENVIRON["MUT_FROM"]; to = ENVIRON["MUT_TO"] }
        { i = index($0, from); if (i) $0 = substr($0, 1, i - 1) to substr($0, i + length(from)); print }
    ' "$PS1" > "$FAKE/mut.ps1"
    if cmp -s "$PS1" "$FAKE/mut.ps1"; then
        # 变异没生效 ⇒ 这条「必须被拒」是空跑，当成失败报出去
        echo "FAIL - 变异体：$1 → 变异没生效（原串在文件里找不到）"
        fails=$((fails + 1))
        return
    fi
    got="$(ps1_delivery_fails < "$FAKE/mut.ps1")"
    case "$got" in
        *"$2"*) echo "ok   - 变异体：$1 → 被拒在 [「$2」]" ;;
        *) echo "FAIL - 变异体：$1 → 期望拒在 [$2]，实际 [${got:-（没被拒）}]"; fails=$((fails + 1)) ;;
    esac
}
# 变异体按行生成的那种（删掉退出码检查那行）单独来，不能走字面替换
grep -v 'WSL 内引导失败' "$PS1" > "$FAKE/mut.ps1"
if cmp -s "$PS1" "$FAKE/mut.ps1"; then
    echo "FAIL - 变异体：删掉退出码检查 → 变异没生效"
    fails=$((fails + 1))
else
    got="$(ps1_delivery_fails < "$FAKE/mut.ps1")"
    case "$got" in
        *exit-code-not-checked*) echo "ok   - 变异体：删掉退出码检查 → 被拒在 [「exit-code-not-checked」]" ;;
        *) echo "FAIL - 变异体：删掉退出码检查 → 期望拒在 [exit-code-not-checked]，实际 [${got:-（没被拒）}]"; fails=$((fails + 1)) ;;
    esac
fi
ps1_mutant "入口命令改当 argv 传（wsl.exe 会重建命令行）" no-stdin-delivery \
    '$entry" | wsl.exe -d Ubuntu -u root -- sh' \
    '$entry" | wsl.exe -d Ubuntu -u root -- sh -c "$entry"'
ps1_mutant "强制模式改写成会被丢掉的 VAR=… 前缀" forced-mode-not-exported \
    '"export COGNEVA_CN_MIRROR=$cn`n$entry" | wsl.exe' \
    '"COGNEVA_CN_MIRROR=$cn $entry" | wsl.exe'

export PATH="$ORIG_PATH"
if [ "$fails" -ne 0 ]; then
    echo "bootstrap 入口判据测试失败: $fails"
    exit 1
fi
echo "bootstrap 入口判据测试全部通过"
