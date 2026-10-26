//! 集群内 registry 的两条访问端点：部署面声明，代码不拼。
//!
//! chart 从 `clusterRegistry.port` / `clusterRegistry.nodePort` 渲染出
//! `COGNEVA_MAINLINE_DEPLOYER_REGISTRY`（Pod 内 buildah 用的 Service DNS 端点）与
//! `COGNEVA_MAINLINE_DEPLOYER_LOCAL_REGISTRY`（节点上 kubelet 拉镜像用的
//! `localhost:<NodePort>`）。这里只负责读那份声明。
//!
//! 以前这里按 `cogneva-registry.<ns>.svc.cluster.local:5000` 拼过一个兜底值。values
//! 里对同一个端口写着：Service、containerPort、部署器拼的 Service DNS、network-policy
//! 四处各写 5000，就有一条路径会静默指向不存在的端口——那段代码正是第五处，而它的
//! 症状是 push 连不上，读起来像「buildah 坏了」。没声明就是没声明：调用方报错，
//! 不拿约定补位。

/// 集群内 registry 的 Service 端点（`host:port`，不带 scheme）。
pub fn declared() -> Option<String> {
    read("COGNEVA_MAINLINE_DEPLOYER_REGISTRY")
}

/// 节点侧引用同一个 registry 的端点（`localhost:<NodePort>`）。
pub fn declared_local() -> Option<String> {
    read("COGNEVA_MAINLINE_DEPLOYER_LOCAL_REGISTRY")
}

fn read(key: &str) -> Option<String> {
    std::env::var(key).ok().as_deref().and_then(clean)
}

/// 声明值去掉空白与尾斜杠；空声明等于没声明——`Some("")` 拼出来的 `"/cogneva:tag"`
/// 在 push 时是「仓库名不合法」，读不出是配置缺了一项。
fn clean(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_declaration_is_no_declaration() {
        assert_eq!(clean(""), None);
        assert_eq!(clean("   "), None);
        assert_eq!(clean(" / "), None, "只有尾斜杠也算空");
    }

    #[test]
    fn a_declared_endpoint_keeps_its_value_without_the_trailing_slash() {
        assert_eq!(
            clean(" cogneva-registry.cogneva.svc.cluster.local:5000/ "),
            Some("cogneva-registry.cogneva.svc.cluster.local:5000".to_string())
        );
        assert_eq!(
            clean("localhost:30500"),
            Some("localhost:30500".to_string())
        );
    }
}
