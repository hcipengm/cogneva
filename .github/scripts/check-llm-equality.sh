#!/usr/bin/env bash
# LLM 平等门：业务侧的清单与配置里不许出现**上游身份**。
#
# 规则（项目 CLAUDE.md）：真上游只在网关的上游池（`llm-upstreams`），业务侧配置与部署
# 零厂商名、零厂商 env。它没有门之前是一条散文，而散文在这件事上特别不管用：一条清单
# 里多一个厂商端点不会让任何东西失败，它只是让「换上游」这件事从网关的一个字段变成
# 一次全树搜索——读数面照常绿，直到有人真去换。
#
# **为什么判据不是一张厂商名清单**：厂商名里最容易被误伤的是那两个协议方言词。
# 业务侧必须用两种线上方言之一发请求，而这方言除了「定义它的厂商名」没有别的名字，
# `api_style` 声明的正是**协议风格**（`openai` / `anthropic`），configmap 自己的注释
# 就这么写。真上游只住在网关，任何厂商的上游都能被任一风格前置 ⇒ 方言词既不泄露上游
# 身份、也不泄露厂商凭证。把它算厂商名，等于禁止平台声明自己说哪种方言——那是过度。
# 所以判据锚在**带身份的形状**上，而不是裸词上：凭证 env 名、端点、域名、模型品牌名。
# 这四类形状里，方言词一个都占不到，于是「协议不算厂商名」是**判据的构造**给的，
# 不是靠一张豁免表维持的——豁免表会随方言词增删而漂，构造不会。
#
# 判据（逐条，命中即红，判词指到行）：
#   1. 厂商凭证 env：形如 `<VENDOR>_API_KEY` 的环境变量名。
#   2. 非网关的 `/v1` 端点：`/v1` 是 LLM 的线上接口面，业务侧只允许指网关。
#   3. 厂商域名：闭集（见下面的 VENDOR_DOMAINS）。
#   4. 厂商身份词：闭集（见下面的 VENDOR_TOKENS），只收**不会与英文常用词撞**的那些。
#
# 扫的是**产物**：渲染产物 + chart + GitOps 静态清单 + 示例配置。不扫 `crates/`——
# 网关自己那侧合法地提到上游（协议探测、池条目 schema），那条线划在网关的进程边界上，
# 不划在树的位置上。
#
# 用法：bash check-llm-equality.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"

# 业务侧配置与部署所在的路径。渲染产物在里面（它是 chart 的输出），但四个都扫：
# 只扫产物会漏掉「改了 chart 还没重渲染」的那一拍，只扫 chart 会漏掉静态清单。
SCANNED_PATHS=(deploy/rendered deploy/k3s deploy/helm/cogneva cogneva.example.json)

# 唯一允许被业务侧点名的 LLM 端点宿主。
ALLOWED_ENDPOINT_HOST='cogneva-security-gateway'

# 厂商域名闭集。收的是**域名**，不是厂商名——域名没有歧义。
VENDOR_DOMAINS='(openai|anthropic|deepseek|moonshot|mistral|cohere|perplexity|siliconflow|openrouter|together|googleapis|generativelanguage|dashscope|aliyuncs|bigmodel|volces|volcengine|baidubce|bytedance|zhipu|minimaxi|stepfun|sensetime|baichuan)\.(com|cn|ai|xyz|org|net)'

# 厂商身份词闭集。刻意**不收** `together`/`google`/`ark`/`spark` 这类英文常用词：
# 一个会在正常散文里出现的词做判据，第一天就红在一个不是问题的东西上，而红了没人信
# 的门最后会被删掉。这类词的真实身份载体（域名、凭证 env、模型品牌）已经被上面三条
# 覆盖——型号名那一列就是它们在这里的替身。
VENDOR_TOKENS='moonshot|kimi|deepseek|dashscope|qwen|tongyi|zhipu|chatglm|baichuan|minimax|stepfun|sensetime|volcengine|volces|doubao|ernie|hunyuan|siliconflow|openrouter|cohere|perplexity|mistral|llama|claude|gemini|grok'

log() { printf '%s\n' "$*"; }

hits=0

# 报一条判据。参数：判据名；已经筛好的 `文件:行:内容` 行走 stdin。
# 用进程替换而不是管道调用它：管道会把函数放进子壳，`hits` 的自增就丢了，
# 于是门在有命中时仍然退出 0 —— 一条永远绿的门。
report() {
  local name="$1" body
  body="$(cat)"
  [ -n "${body}" ] || return 0
  log "${name}："
  printf '%s\n' "${body}"
  hits=1
}

git_grep() { git -C "$ROOT" grep -n "$@" -- "${SCANNED_PATHS[@]}" || true; }

main() {
  # 1. 厂商凭证 env 名。结构判据：不管厂商叫什么都拦得住。
  report "厂商凭证 env（<VENDOR>_API_KEY 形态的环境变量名）" \
    < <(git_grep -E '[A-Z][A-Z0-9_]*_API_KEY')

  # 2. 非网关的 /v1 端点。先把 /v1 URL 抽出来（不带文件名就分不出哪一行），再筛掉网关。
  report "非网关的 /v1 端点（业务侧只允许指 ${ALLOWED_ENDPOINT_HOST}）" \
    < <(git_grep -HoE 'https?://[A-Za-z0-9._:-]+/v1' | grep -v -- "://${ALLOWED_ENDPOINT_HOST}[:/]")

  # 3. 厂商域名闭集。
  report "厂商域名" < <(git_grep -iE "${VENDOR_DOMAINS}")

  # 4. 厂商身份词闭集。
  report "厂商身份词" < <(git_grep -iwE "${VENDOR_TOKENS}")

  if [ "${hits}" -ne 0 ]; then
    log ""
    log "LLM 平等门：以上命中把上游身份带进了业务侧的清单/配置。真上游属于网关的上游池"
    log "（${ALLOWED_ENDPOINT_HOST} 后面的那一份），业务侧只声明协议方言。"
    return 1
  fi
  log "LLM 平等门：业务侧清单/配置里没有厂商身份面（方言词 openai/anthropic 是协议名，不算）。"
}

main
