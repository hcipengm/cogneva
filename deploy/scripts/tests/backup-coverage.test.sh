#!/usr/bin/env bash
# principle: feedback_a_census_needs_a_source_outside_itself
# check-backup-coverage.sh 的两半各有一份测试。前半（claim 覆盖）在这个门刚写出来
# 时就有，后半（数据根一级子目录逐项分类）是补上的：它的普查面来自代码、配置、
# 渲染物与本机实盘，**不是**登记表自己——登记表自己当普查面的判据恒真。
#
# 用例：
#   1. 真树 -> 绿，且报出普查数与已分类数（不是空跑）；
#   2. 代码里新出现一个数据根子目录而没有登记 -> 红，点名它；
#   3. 登记行没有 why -> 红（只写名字的条目等于把问题留给下一个人）；
#   4. 登记行的 evidence 指向不存在的文件 -> 红（指针防锈）；
#   5. 登记行的 class 不在闭集里 -> 红；
#   6. 一级段写成常量、后面再拼一层（`data_dir.join(EVAL_SUBDIR).join("datasets")`）
#      -> 按常量值判一级段，第二个 join 的名字不得进普查面；
#   7. 常量指向一个没登记的子目录 -> 红，点名解析出来的目录而不是常量名。
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../../.." && pwd)"
ledger="deploy/data-root-subdirs.tsv"
fail() { echo "FAIL: $*"; exit 1; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
command -v jq >/dev/null || { echo "缺少依赖：jq" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 门禁会读渲染物、配置、代码三处；副本里三处都要有，但可以很小——普查只认
# 「文本里有没有 `<数据根>/<段>`」与「代码里有没有由 data_dir 拼出来的段」。
mirror() {
  rm -rf "${work}/root"
  mkdir -p "${work}/root/deploy/rendered/k3s-single" \
           "${work}/root/deploy/helm/cogneva/files" \
           "${work}/root/deploy/scripts" \
           "${work}/root/crates/demo/src"
  cp "${repo}/deploy/scripts/check-backup-coverage.sh" "${work}/root/deploy/scripts/"
  cp "${repo}/${ledger}" "${work}/root/${ledger}"
  cp "${repo}/deploy/helm/cogneva/files/cogneva.json" "${work}/root/deploy/helm/cogneva/files/"
  cat > "${work}/root/deploy/rendered/k3s-single/app.yaml" <<'YAML'
kind: Deployment
metadata: {name: demo}
spec:
  template:
    spec:
      containers: [{name: app, volumeMounts: [{name: v, mountPath: /var/lib/cogneva-data}]}]
      volumes: [{name: v, persistentVolumeClaim: {claimName: cogneva-data-pvc}}]
YAML
  cat > "${work}/root/deploy/rendered/k3s-single/backup.yaml" <<'YAML'
kind: CronJob
metadata: {name: cogneva-backup}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers: [{name: backup, volumeMounts: [{name: b, mountPath: /var/lib/cogneva-data}]}]
          volumes: [{name: b, persistentVolumeClaim: {claimName: cogneva-data-pvc}}]
YAML
  cat > "${work}/root/crates/demo/src/lib.rs" <<'RUST'
pub fn dir() -> std::path::PathBuf {
    let data_dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_default();
    std::path::PathBuf::from(&data_dir).join("storage")
}
RUST
  # 登记表的 evidence 是指针，门禁核它在不在树上；副本里得把那些文件也摆出来，
  # 否则用例 1 会把「副本里没有源码」读成「指针锈了」。
  while read -r ev; do
    [ -n "${ev}" ] || continue
    mkdir -p "${work}/root/$(dirname "${ev}")"
    cp "${repo}/${ev}" "${work}/root/${ev}"
  done < <(awk -F'\t' '!/^#/ && !/^subdir\t/ && NF==6 {print $6}' "${repo}/${ledger}")
}
mirror

run_gate() { bash "${work}/root/deploy/scripts/check-backup-coverage.sh" "${work}/root" 2>&1; }

# 1) 真树绿
out="$(run_gate)" || fail "真树应当绿，实际红：${out}"
grep -q "已分类" <<<"${out}" || fail "绿时没有报出已分类条数与普查数：${out}"

# 2) 代码里新出现一个子目录而没有登记
mirror
cat >> "${work}/root/crates/demo/src/lib.rs" <<'RUST'
pub fn other() -> std::path::PathBuf {
    let data_dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_default();
    std::path::PathBuf::from(&data_dir).join("brand-new-tree")
}
RUST
out="$(run_gate)" && fail "未分类的数据根子目录应当红，实际绿：${out}"
grep -q "brand-new-tree" <<<"${out}" || fail "红时没有点名那个子目录：${out}"

# 3) 登记行没有 why
mirror
printf 'ghost-tree\tmain\tdiscardable\t没有 why 的一行\t\tcrates/demo/src/lib.rs\n' \
    >> "${work}/root/${ledger}"
out="$(run_gate)" && fail "没有 why 的登记行应当红，实际绿：${out}"
grep -q "没有 why" <<<"${out}" || fail "红时没有说是缺 why：${out}"

# 4) evidence 指向不存在的文件
mirror
printf 'ghost-tree\tmain\tdiscardable\t有个东西\t因为它是派生的\tcrates/demo/src/missing.rs\n' \
    >> "${work}/root/${ledger}"
out="$(run_gate)" && fail "evidence 锈了应当红，实际绿：${out}"
grep -q "missing.rs" <<<"${out}" || fail "红时没有点名那个指针：${out}"

# 5) class 不在闭集里
mirror
printf 'ghost-tree\tmain\tmaybe\t有个东西\t因为它可能是派生的\tcrates/demo/src/lib.rs\n' \
    >> "${work}/root/${ledger}"
out="$(run_gate)" && fail "class 不在闭集里应当红，实际绿：${out}"
grep -q "maybe" <<<"${out}" || fail "红时没有点名那个 class：${out}"

# 6) 一级段是常量、二级目录是字面量
mirror
cat > "${work}/root/crates/demo/src/nested.rs" <<'RUST'
const EVAL_SUBDIR: &str = "eval";

pub fn datasets_dir(data_dir: &str) -> std::path::PathBuf {
    std::path::Path::new(data_dir).join(EVAL_SUBDIR).join("datasets")
}
RUST
out="$(run_gate)" || fail "常量拼出的一级段应当按常量值（eval，已登记）判，实际红：${out}"
grep -q "datasets" <<<"${out}" && fail "把二级目录 datasets 当成了一级段：${out}"

# 7) 常量指向没登记的子目录：红要点名目录，不是常量名
mirror
cat > "${work}/root/crates/demo/src/nested.rs" <<'RUST'
const NEW_SUBDIR: &str = "const-tree";

pub fn new_dir(data_dir: &str) -> std::path::PathBuf {
    std::path::Path::new(data_dir).join(NEW_SUBDIR)
}
RUST
out="$(run_gate)" && fail "常量指向的未登记子目录应当红，实际绿：${out}"
grep -q "const-tree" <<<"${out}" || fail "红时没有点名常量解析出来的目录：${out}"
grep -q "NEW_SUBDIR" <<<"${out}" && fail "把常量名当成了目录名：${out}"

echo "backup-coverage: 7 个用例全绿"
