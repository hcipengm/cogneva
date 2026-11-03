#!/usr/bin/env bash
# 跑一条 CI 步骤，红的时候把「哪一条测试红了」写成注解。
#
# 为什么不是让每一步直接写 `run: cargo test …`：红了的步骤只会得到 runner 自己那句
# `Process completed with exit code 101`，它说的是这一步结束了，不是它在哪里结束的；
# 而作业日志要 token 才读得到，于是没有 token 的读者——人、看 CI 状态的程序、按失败
# 去派活的平台——拿到的都只有「套件红了」。实测 2026-11-03：`Test` 在两版上红，落点
# 在 `-- --ignored` 的活库那几步，日志尾巴是 Postgres 的服务容器拆除，从头到尾没有
# 一行说出是哪个测试；同一天主套件那一格反而说得出，因为它自己把失败块写进注解。
#
# 注解不用 token，所以这一步把主套件那一格的形状搬到每一条活库步骤上：同一条命令
# 被同一套 grep 判词读一遍，红的时候把条数与上下文写进一条 `::error::`。
#
# 一条注解，不是一行一条：GitHub 每个步骤最多留十条 error 注解，多出来的静默丢弃，
# 于是「十条」既可能是全部也可能只是一部分，读者分不出。整块塞进一条新行转义过的
# 注解，第一行是条数。
#
# 判词里 `##[error]` 不算：runner 用它收尾（退出码、被取消），两种都说「失败了」，
# 都不说在哪里；服务容器的拆除也会打 `ERROR:`。空的尾巴也不冒充——认不出标记时
# 仍然发注解，带上退出码与最后几行，因为「说不出是哪条」与「什么都没红」是两件事。
#
# 跑法：bash deploy/scripts/run-a-ci-step.sh <命令…>
# 退出码原样透传，不吞任何一个：这一步判的是同一条命令，不是它的摘要。
set -uo pipefail

if [ "$#" -eq 0 ]; then
    printf '::error::run-a-ci-step: 没有给命令\n'
    exit 2
fi

log="$(mktemp "${TMPDIR:-/tmp}/ci-step.XXXXXX.log")"
ann="$(mktemp "${TMPDIR:-/tmp}/ci-step.XXXXXX.ann")"
trap 'rm -f "${log}" "${ann}"' EXIT

# `tee` 保住作业日志里的原样输出（有人仍然会去读它），`PIPESTATUS[0]` 保住命令自己的
# 退出码——不经 pipefail 也得是它。
"$@" 2>&1 | tee "${log}"
rc="${PIPESTATUS[0]}"

if [ "${rc}" -ne 0 ]; then
    failed="$(grep -cE '^test .* FAILED$' "${log}" || true)"
    {
        printf '%s\n' "$* -> exit ${rc}; ${failed} failing test(s)"
        grep --no-group-separator -A 4 \
            -E "^(test .* FAILED|failures:|test result: FAILED|error(\[E[0-9]+\])?:)|panicked at" \
            "${log}" 2>/dev/null | grep -v '^[[:space:]]*$' | head -400
        # 一个标记都没认出来时，尾巴是这一格唯一有的东西：拿它顶，别拿空顶。
        if ! grep -qE "^(test .* FAILED|failures:|test result: FAILED|error(\[E[0-9]+\])?:)|panicked at" "${log}" 2>/dev/null; then
            printf '%s\n' '--- no marker recognised; last lines of the step ---'
            tail -n 40 "${log}"
        fi
    } >"${ann}"
    body="$(sed -e 's/%/%25/g' -e 's/\r/%0D/g' -e ':a;N;$!ba;s/\n/%0A/g' "${ann}")"
    printf '::error::%s\n' "${body}"
fi

exit "${rc}"
