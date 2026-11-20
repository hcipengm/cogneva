#!/usr/bin/env bash
# 判这一笔要用谁的名字署名：应当是**本会话那位数字员工**的名字。
#
# 为什么在提交门槛上问：仓库的 `user.name` 只有一份，挂在共享的 `.git/config`。
# 一棵树上有好几个会话同时提交时，谁最后一次写了那份 config，之后所有人的提交就都署
# 那个名字——2026-10-27 实测六笔提交署成 `t <t@example.com>`，来源是一次夹具测试把共享
# config 写成了自己的夹具身份，而真正的写者无迹可寻；属主 2026-11-07 定：提交要署
# **提交者自己**的名字。名字不是写死的一份映射，而是会话自己的记录：
# `~/.claude/sessions/<pid>.json` 里的 `name`（pid 取自 `CLAUDE_PID`，`sessionId`
# 与 `CLAUDE_CODE_SESSION_ID` 对得上才算）。那条记录由宿主在拉起会话时写，正是给
# 「这个动作是谁做的」用的同一个来源。
#
# 判的是什么：`git var GIT_AUTHOR_IDENT` / `GIT_COMMITTER_IDENT` 的**名字**。
# 只判名字不判邮箱：邮箱是同一身份的附属写法，钉死它等于把一处约定抄成两份。
#
# 判不了就不拦：没有 `CLAUDE_CODE_SESSION_ID`（不是 Claude 会话发起的提交，例如
# 部署器、定时脚本、平台自己的落地），记录文件不在 / pid 不活 / 解析不出名字——
# 每种情况印一行具名理由后放行。「这台机器答不了」不是这一笔的判词，而沉默的放行会
# 让人以为判过了，所以理由必须印出来。唯一例外是没有那两个环境变量：那时这条判据
# 根本不适用，一片静默才是对的。
#
# 退出码：0 放行（含判不了）；1 署名与本次会话的数字员工不符；3 参数/环境用不了。
set -u

sessions_dir="${COGNEVA_SESSION_DIR:-$HOME/.claude/sessions}"

not_judged() {
    printf '作者署名: 这一条今天判不了，不拦 —— %s\n' "$1" >&2
    exit 0
}

# 身份的第一来源是**显式交进来的名字**，其次才是 Claude Code 的会话记录。
#
# 为什么要有第一来源：平台的智能体不在 Claude Code 里跑，它没有 `CLAUDE_*` 环境变量、
# 盘上也没有 `<pid>.json`。只认会话记录，这道门对它们永远是「判不了」，等于不存在。
# 交名字的方式是一个环境变量——调用它的那一侧本来就知道自己是谁（任务、进程、actor），
# 这比让门去别处猜可靠。
if [ -n "${COGNEVA_AGENT_NAME:-}" ]; then
    employee="${COGNEVA_AGENT_NAME}"
    sid=""
    pid=""
else
    sid="${CLAUDE_CODE_SESSION_ID:-}"
    pid="${CLAUDE_PID:-}"
    [ -n "$sid" ] || exit 0
fi

record=""
if [ -n "${sid:-}" ] && [ -n "${pid:-}" ] && [ -r "$sessions_dir/$pid.json" ]; then
    record="$sessions_dir/$pid.json"
fi

if [ -z "${employee:-}" ] && [ -z "$record" ]; then
    # 没有 CLAUDE_PID（或文件不在）：退到按 sessionId 反查。只认**还活着**的进程，
    # 因为被 resume 过的会话会在盘上留下多份同 sessionId 的记录，死掉的那几份是旧名字。
    shopt -s nullglob
    live=()
    for f in "$sessions_dir"/*.json; do
        grep -qF "\"sessionId\":\"$sid\"" "$f" 2>/dev/null || continue
        p="$(sed -n 's/.*"pid":\([0-9]\{1,\}\).*/\1/p' "$f" | head -1)"
        [ -n "$p" ] || continue
        kill -0 "$p" 2>/dev/null || continue
        live+=("$f")
    done
    shopt -u nullglob
    case "${#live[@]}" in
    0) not_judged "没有找到会话 $sid 的记录（$sessions_dir）" ;;
    1) record="${live[0]}" ;;
    *) not_judged "会话 $sid 对应 ${#live[@]} 份活着的记录，认不出是哪一份" ;;
    esac
fi

# 冒号后的空格必须可选：宿主写的是紧凑 JSON，而任何一次「顺手格式化」都会让原来那条
# `"sessionId":"…"` 的 sed 读成空串。读成空之后这道门走的是「判不了 → 放行」，也就是
# 一个格式化差异就能让它静默失效——2026-11-20 实测发生过一次。
if [ -z "${employee:-}" ]; then
    [ -n "$record" ] || not_judged "会话 $sid 没有对应的盘上记录"
    got_sid="$(sed -n 's/.*"sessionId"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record" | head -1)"
    [ "$got_sid" = "$sid" ] ||
        not_judged "$record 记的是会话 ${got_sid:-（空）}，不是 $sid"
    employee="$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record" | head -1)"
    [ -n "$employee" ] || not_judged "$record 里没有 name 字段"
fi

repo="$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --show-toplevel 2>/dev/null)" ||
    not_judged "不在 git 工作树里"
[ -n "$repo" ] || not_judged "不在 git 工作树里"

# 只管**登记过**的项目仓：这条规矩要防的是「几个会话共写同一份 .git/config」，
# 而哪些仓会被这么写，宿主有一份正本（`install.sh project` 写的登记表）。夹具仓、
# 一次性快照、别人的临时克隆都不在表上——它们各自只属于一个进程，不存在「谁最后
# 写 config 谁给所有人改名」这回事，拿这条去判它们只会把测试判红。
# 工作树按 common dir 认：登记的是仓根，worktree 的 common dir 指向它的 .git。
# 登记表不存在 ⇒ 这台机器没有「登记过的项目仓」，这条判据在这里不适用，静默放行。
projects="${COGNEVA_PROJECTS_TSV:-$HOME/.claude/cogneva-projects.tsv}"
[ -f "$projects" ] || exit 0
common="$(env -u GIT_DIR -u GIT_WORK_TREE git rev-parse --git-common-dir 2>/dev/null)" ||
    not_judged "读不出这个仓的 common dir"
case "$common" in
/*) ;;
*) common="$(cd "$common" 2>/dev/null && pwd)" || not_judged "读不出这个仓的 common dir" ;;
esac
case "$common" in
*/.git) common="${common%/.git}" ;;
esac
registered=0
while IFS=$'\t' read -r _name dir; do
    [ -n "${dir:-}" ] || continue
    dir="${dir%/}"
    case "$common/" in
    "$dir"/*) registered=1; break ;;
    esac
done <"$projects"
[ "$registered" -eq 1 ] || exit 0

ident_name() { # $1 = GIT_AUTHOR_IDENT | GIT_COMMITTER_IDENT
    git var "$1" 2>/dev/null | sed 's/ <.*//'
}

author="$(ident_name GIT_AUTHOR_IDENT)"
committer="$(ident_name GIT_COMMITTER_IDENT)"
[ -n "$author" ] || not_judged "读不出 GIT_AUTHOR_IDENT"

# 署谁的名字，看这是谁在提交（属主 2026-11-08）：本机上由人驱动、`kind` 记作
# `interactive` 的 Claude Code 会话，署它自己的账号 `hcipengm`；cogneva 平台自己拉起
# 的数字员工会话，署会话记录里的员工名。两种都不是的（部署器、定时脚本）根本没有
# 会话记录，前面已经走「不适用」那一支。
#
# 为什么名字不直接取会话记录的 `name`：宿主给交互会话分配的名字（`cogneva-b6` 这类）
# 与平台员工同名同形，署上去之后历史里看不出这一笔是人让 Claude Code 做的，还是平台
# 自己做的——2026-11-08 属主点名要这两种分开。
#
# 数字员工的写法里有 `#`（`Luna#1822ab0e`）。名字带 `#` 的一律按员工判，不管记录写着
# 什么 kind：cogneva 自己发布的提交必须署那个员工名，这是属主同一句话的后半截。
kind=""
[ -n "$record" ] &&
    kind="$(sed -n 's/.*"kind":"\([^"]*\)".*/\1/p' "$record" | head -1)"
if [ "$kind" = interactive ] && [ "${employee#*#}" = "$employee" ]; then
    expected="hcipengm"
    email="hcipengm@users.noreply.github.com"
    signer="Claude Code 会话（属主 2026-11-08：以 hcipengm 署名）"
else
    expected="$employee"
    email="$(printf '%s' "$employee" | tr '[:upper:]' '[:lower:]' | tr -d '#')@cogneva.ai"
    signer="本会话的数字员工"
fi

if [ "$author" = "$expected" ] && [ "$committer" = "$expected" ]; then
    exit 0
fi

printf '作者署名: 这一笔会署成「%s」/「%s」，而%s应该署「%s」。\n' \
    "${author:-（空）}" "${committer:-（空）}" "$signer" "$expected" >&2
cat >&2 <<EOF

属主 2026-11-07：提交署提交者自己的名字，不署共享 config 里那个。
属主 2026-11-08：Claude Code 署 hcipengm，cogneva 的数字员工署各自的员工名。
这一笔该署「$expected」，依据是：${record:+会话记录 $record（pid $pid，sessionId $sid，kind ${kind:-（无）}）}${record:-显式环境变量 COGNEVA_AGENT_NAME（平台智能体没有会话记录）}

工作树归你一个人用时，一次设好（只改这棵工作树，不动共享 config）：
  git config extensions.worktreeConfig true
  git config --worktree user.name '$expected'
  git config --worktree user.email '$email'

几棵工作树共用一个身份时，改用环境变量（只对这一笔生效）：
  GIT_AUTHOR_NAME='$expected' GIT_AUTHOR_EMAIL='$email' \\
  GIT_COMMITTER_NAME='$expected' GIT_COMMITTER_EMAIL='$email' git commit ...

要照常提交（例如这条判据本身在修）：\`git commit --no-verify\`，并在信息里写清为什么。
EOF
exit 1
