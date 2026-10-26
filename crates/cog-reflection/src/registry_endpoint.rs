//! 集群内 registry 的约定端点。
//!
//! 部署面才是这个地址的属主：chart 把 `clusterRegistry.port` 渲染进
//! `COGNEVA_MAINLINE_DEPLOYER_REGISTRY`，凡声明了它的进程都用声明值。这里留下的
//! 是**没声明时**的那条约定——registry 由安装脚本起在平台自己的命名空间里，
//! Service 名与端口不随 release 变。
//!
//! 之所以收成一个函数：这个格式串此前在三处各写了一遍（推送端点、卷占用走查、
//! 晋级镜像），三份副本里改一份就有一处会对着关了的端口推，而那一处的症状是
//! 构建失败，读起来像「buildah 坏了」。同一个事实只留一处，改的时候只有一个地方
//! 要改，找的时候也只有一个地方要看。

/// 集群内 registry 的端点（`host:port`，不带 scheme）。
///
/// `namespace` 是平台自己安装所在的命名空间，不是调用方随手写的常量：跨命名空间
/// 的两份安装各有自己的 registry，写死一个命名空间就会让另一份推回第一份的仓库。
pub fn in_cluster(namespace: &str) -> String {
    format!("cogneva-registry.{namespace}.svc.cluster.local:5000")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_endpoint_follows_the_namespace_it_is_given() {
        assert_eq!(
            in_cluster("cogneva"),
            "cogneva-registry.cogneva.svc.cluster.local:5000"
        );
        assert_eq!(
            in_cluster("other"),
            "cogneva-registry.other.svc.cluster.local:5000"
        );
    }
}
