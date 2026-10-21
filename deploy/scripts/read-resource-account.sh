#!/usr/bin/env bash
# 读资源账的事件那一半：按 kind / outcome / subject 分账，并把「谁写的」摆出来。
#
# 为什么需要这一面：资源账有一族读数（`cogneva_resource_events{kind,outcome}`、
# `_pending`、`_oldest_unfolded_age_seconds`、`_dropped_total`），但它们只按
# kind/outcome 切。这一类账最容易出的毛病不在计数上：**某个 kind 的行数在涨、pending 为 0、
# 消费者在跑，而它记的全是记录者自己造的那堵墙**——计数面全绿，账里却没有一件外面的
# 事。所以这一面读 **subject 分布**，并把每个 subject 归一分档：
#
#   self_raised —— 平台自己的控制律造的墙（`build_gate.*`、`volume_capacity.*`，以及
#                  `build_gate` 四个墙名 `<asker>:busy|waited_out|maintenance_hold|queue_ahead`
#                  ——同一道闸门的四种关法，按整条 subject 只认 `busy` 会把另外三种
#                  读成「外面又多了一堵墙」）；
#   external    —— 别的东西造的墙（`mainline advance:quota|limitrange|podsecurity` 等）。
#
# **external=0 不是判词**：那堵墙可能只是还没拒过这一次。「配额没满过」与「配额拒绝没
# 接上」在这一行上同形，所以这一面只报数、不报绿。要判接线，看树内那几条断言（构建闸门
# 记过账、拒绝事件带着墙与作业），以及下一次真被拒时这一面能不能读出那行。
#
# 只读。每行一个 key=value，供人直接看或前后比对。
# 退出码只表可读性：0 读得到（判词是什么另说）、3 读不到（PG Pod 不在 / 查询失败 /
# 取回是空的 / 输出认不得）。「一行都没有」是读得到的结论，不是读不到。
#
# 用法：
#   bash deploy/scripts/read-resource-account.sh                 # 从集群里的 PG 读
#   COGNEVA_RESOURCE_EVENTS_FILE=rows.tsv bash ...               # 离线：读一份导出的查询结果
#
# 环境变量：COGNEVA_NS（默认 cogneva）、COGNEVA_KUBECTL（默认 kubectl）、
# COGNEVA_PG_POD（默认 postgres-0）。
set -uo pipefail

NS="${COGNEVA_NS:-cogneva}"
KUBECTL="${COGNEVA_KUBECTL:-kubectl}"
PG_POD="${COGNEVA_PG_POD:-postgres-0}"

# 三节一张表：1TOT（总量与折叠进度）、2KIND（kind×outcome）、3SUBJ（kind×subject）。
# 六列对齐是为了能走一次 union，少一次进 Pod。
SQL="
with e as (select * from resource_events)
select '1TOT' as tag,
       (select count(*)::text from e) as x1,
       (select count(*)::text from e where folded_at is null) as x2,
       coalesce((select min(occurred_at)::text from e where folded_at is null), '') as x3,
       '' as x4, '' as x5
union all
select '2KIND', kind, outcome, count(*)::text,
       min(occurred_at)::text, max(occurred_at)::text
from e group by kind, outcome
union all
select '3SUBJ', kind, subject, count(*)::text,
       min(occurred_at)::text, max(occurred_at)::text
from e group by kind, subject
order by 1, 2, 3
"

src="${COGNEVA_RESOURCE_EVENTS_FILE:-}"
if [ -n "$src" ]; then
  if [ ! -r "$src" ]; then
    echo "resource_account_source=file:$src"
    echo "resource_account_verdict=unreadable (给定的查询结果文件读不了；这不是「一行都没有」)"
    exit 3
  fi
  echo "resource_account_source=file:$src"
  rows="$(cat "$src")"
else
  echo "resource_account_source=pod:$PG_POD"
  # 空结果有两个原因，而它们要的修法相反：查询根本没跑成（kubectl 读不到 kubeconfig、
  # 连不上 API server、psql 拒登），和查询跑成了、表里确实没有行。只看 `rows` 这两个同形，
  # 而「读不到」这个判词对两边都成立，于是读者会去查账，其实该修的是调用方式——
  # 2026-10-21 实测：不带 COGNEVA_KUBECTL 时 kubectl 拿不到 kubeconfig，判词是
  # unreadable，账其实是好的（467 行）。所以 stderr 不再扔掉，读不到时把前几行附在后面。
  err_file="$(mktemp)"
  rows="$($KUBECTL -n "$NS" exec "$PG_POD" -- sh -c \
    'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F "$(printf "\t")" -c "$0"' \
    "$SQL" 2>"$err_file")"
  if [ -z "$rows" ]; then
    echo "resource_account_verdict=unreadable (PG Pod 在也可能，但查询取回来是空的：读不到，不是空账)"
    if [ -s "$err_file" ]; then
      echo "resource_account_error=$(head -3 "$err_file" | tr '\n' ' ')"
    else
      echo "resource_account_error=<empty: 查询跑通了却一行都没取回>"
    fi
    rm -f "$err_file"
    exit 3
  fi
  rm -f "$err_file"
fi

# 平台自己的控制律造的墙。这里是**档案**不是判据：档位只影响这一面把 subject 归到哪一栏，
# 不影响任何退出码。
is_self_raised() {
  case "$1" in
    build_gate.*|volume_capacity.*) return 0 ;;
    # 闸门的四种关法，形状是 `<asker>:<wall>`。只认这个账当初先见到的那一种，
    # 会把另外三种归到「别人造的墙」——而那一种读法正好让账显得比实际健康。
    *:busy|*:waited_out|*:maintenance_hold|*:queue_ahead) return 0 ;;
    *) return 1 ;;
  esac
}

tot_seen=""
declare -A kind_subjects=()
while IFS=$'\t' read -r tag a b c d e; do
  [ -n "${tag:-}" ] || continue
  case "$tag" in
    *TOT)
      tot_seen=1
      echo "resource_events_total=$a"
      echo "resource_events_pending=$b"
      if [ -n "$c" ]; then
        echo "resource_events_oldest_unfolded=$c"
        if secs="$(date -u -d "$c" +%s 2>/dev/null)"; then
          echo "resource_events_oldest_unfolded_age_seconds=$(( $(date -u +%s) - secs ))"
        fi
      else
        echo "resource_events_oldest_unfolded=<none>"
      fi
      ;;
    *KIND)
      echo "kind=$a outcome=$b rows=$c first=$d last=$e"
      ;;
    *SUBJ)
      echo "subject kind=$a subject=\"$b\" rows=$c first=$d last=$e"
      kind_subjects["$a"]+="$b"$'\n'
      ;;
  esac
done <<<"$rows"

if [ -z "$tot_seen" ]; then
  echo "resource_account_verdict=unreadable (查询回来了但没有 TOT 节：这不是「空账」，是这一面认不得的输出)"
  exit 3
fi

# 每个 kind 的覆盖：几个 subject、几堵是平台自己造的、几堵是外面的。
for k in $(printf '%s\n' "${!kind_subjects[@]}" | sort -u); do
  [ -n "$k" ] || continue
  self=0; ext=0
  while IFS= read -r s; do
    [ -n "$s" ] || continue
    if is_self_raised "$s"; then self=$((self + 1)); else ext=$((ext + 1)); echo "external kind=$k subject=\"$s\""; fi
  done <<<"${kind_subjects[$k]}"
  echo "coverage kind=$k subjects=$((self + ext)) self_raised=$self external=$ext"
done

echo "resource_account_verdict=readable"
