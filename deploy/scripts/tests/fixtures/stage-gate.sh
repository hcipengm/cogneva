#!/usr/bin/env bash
# Stages the pre-push gate, together with every file it sources, into a fixture tree.
#
# Usage (after sourcing): stage_gate <repo root> <fixture root>
#
# The gate is not one file any more: it sources `lib/build-slot.sh`, which is the
# client for the host's build slots. A fixture that copies only the script gets a
# tree where the gate cannot even name its own dependency -- the failure reads as
# `line 51: ...: No such file or directory` above an assertion that never mentions
# it, and the fixture's real question (does the Test leg hand the changed crate and
# the assembly crate to cargo?) is answered by an accident of staging.
#
# That happened on 2026-11-09 across four fixtures at once, when the slot client was
# added. The copies now come from here, so the next file the gate sources is added
# in one place and every fixture follows; a fixture that still copied whole
# directories by hand would drift back to the tree it used to test.
stage_gate() { # stage_gate <repo root> <fixture root>
    local repo="$1" dest="$2"
    mkdir -p "${dest}/deploy/scripts/lib"
    cp "${repo}/deploy/scripts/check-before-push.sh" "${dest}/deploy/scripts/check-before-push.sh"
    # The whole directory, not the one file known today: what the gate sources is a
    # fact about the gate, and a fixture that had to list it would be asserting on
    # its own copy of that fact.
    cp "${repo}/deploy/scripts/lib/"*.sh "${dest}/deploy/scripts/lib/"
    # The gate also runs some of its siblings by path rather than sourcing them.
    # A fixture that staged only the gate answered their absence as RED steps
    # ("python3: can't open file ..."), which is a staging accident wearing a
    # failing verdict -- and it was the new case that found it, not the tree.
    # A shell sibling the gate starts calling is added here the same day; the
    # Python ones go by glob, so a new one is staged without anyone remembering
    # this file.
    cp "${repo}/deploy/scripts/check-format.sh" "${dest}/deploy/scripts/check-format.sh"
    # 2026-11-20：门的 CI 回放跑 `check-commit-attribution-ci.sh`（推的范围的提交信息
    # 不带 AI 署名），它再按路径调 `check-commit-attribution.sh`。夹具里缺任一份，那一格
    # 都红成 `No such file or directory`——又一次「staging 事故穿着判词的衣服」。
    cp "${repo}/deploy/scripts/check-commit-attribution-ci.sh" "${dest}/deploy/scripts/check-commit-attribution-ci.sh"
    cp "${repo}/deploy/scripts/check-commit-attribution.sh" "${dest}/deploy/scripts/check-commit-attribution.sh"
    # 2026-11-20：私有标识那一面（判私有仓的名字不许出现在这棵树里）也是按路径调的同级
    # 脚本；夹具里缺它，那一格红成 `No such file or directory`。
    cp "${repo}/deploy/scripts/check-private-names.sh" "${dest}/deploy/scripts/check-private-names.sh"
    cp "${repo}/deploy/scripts/"*.py "${dest}/deploy/scripts/" 2>/dev/null || true
}
