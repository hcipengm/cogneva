//! 「哪个面上的东西被改了」——REST 侧那族帧的出处。
//!
//! 编排器发的是智能体与任务的帧。平台其余的面——设置、文件、存储、变更流水、治理
//! 名册——只在有人**写**的时候变，而一次写过去在总线上不留任何痕迹。于是这些面的
//! 读数只能靠定时重读：屏幕什么时候动，取决于读者自己那个定时器，而不是取决于平台
//! 上发生了什么。一次写写下之后，读者要等到下一拍才知道。
//!
//! 这里补的就是那一步：**一次成功的写**在总线上落一帧，只说它落在哪个前缀下。
//! 帧里不带这次写的内容——内容仍然由服务端现算，读的一方重读快照。把写的内容也
//! 发一遍，账就有两本了。
//!
//! 为什么放在协议边缘：前缀是路径的语法，不是业务语义。判断因此是一个纯函数
//! （`resource_domain`），它的输入只有方法与路径，输出只有前缀名。
//!
//! 为什么只有写、只有 2xx：一次被拒的写什么都没改，叫醒所有面只是让它们重读同一份
//! 字节；一次 `GET` 更是连写都没有。

use axum::{extract::Request, extract::State, middleware::Next, response::Response};
use std::sync::Arc;

/// API 版本根。前缀从它后面第一段开始算。
pub const API_ROOT: &str = "/api/v1/";

/// 不该叫醒任何面的前缀。
///
/// `auth` 下面全是登录、刷新、换票：它们按构造高频（每个令牌一次），而且改的不是
/// 哪个面上的数据——把 `auth` 放进来，等于给整棵树加了一个按令牌频率抽的鞭子。
const SILENT: &[&str] = &["auth"];

/// 是不是一次写。
fn is_write(method: &str) -> bool {
    matches!(method, "POST" | "PUT" | "PATCH" | "DELETE")
}

/// 一次写落在哪个前缀下；不是写、或者不在 API 根下，就是 `None`。
///
/// 只认第一段路径，且只认普通标识符：`/api/v1/settings/llm` 与
/// `/api/v1/settings` 是同一条面，读者订的是那一条面，不是某一个端点。端点级别的
/// 粒度会让每个面都得背下自己读过的每一条路径，忘一条就少一次重读。
pub fn resource_domain<'a>(method: &str, path: &'a str) -> Option<&'a str> {
    if !is_write(method) {
        return None;
    }
    let rest = path.strip_prefix(API_ROOT)?;
    let segment = rest.split('/').next()?;
    if segment.is_empty() || SILENT.contains(&segment) {
        return None;
    }
    let plain = segment
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
    if !plain {
        return None;
    }
    Some(segment)
}

/// 写成功之后在总线上落一帧。
///
/// 位置在 `next.run()` 之后：那时写已经落地，读者收到帧立刻重读，读到的是写完之后
/// 的那一份。放在之前，读者会读到写之前的快照，然后要等下一次触发才追上——比轮询
/// 还糟，因为它看起来像是「写没生效」。
pub async fn publish_resource_frame(
    State(state): State<Arc<crate::GatewayState>>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let domain = resource_domain(&method, &path).map(str::to_owned);

    let response = next.run(req).await;

    if let Some(domain) = domain {
        if response.status().is_success() {
            // 没有接收者（没有连接）不是错误：帧的消费者是界面上打开着的面，
            // 一个都没有的时候这一帧本来就无处可去。
            let _ = state.event_tx.send(cog_core::AgentEvent::ResourceChanged {
                domain,
                timestamp: chrono::Utc::now(),
            });
        }
    }

    response
}

#[cfg(test)]
mod tests {
    use super::resource_domain;

    #[test]
    fn a_write_names_the_prefix_it_landed_under() {
        assert_eq!(
            resource_domain("POST", "/api/v1/settings/llm"),
            Some("settings")
        );
        assert_eq!(
            resource_domain("DELETE", "/api/v1/files/a/b.txt"),
            Some("files")
        );
        assert_eq!(
            resource_domain("PATCH", "/api/v1/governance/faces/13"),
            Some("governance")
        );
    }

    #[test]
    fn a_read_changes_nothing_so_it_wakes_nobody() {
        for method in ["GET", "HEAD", "OPTIONS"] {
            assert_eq!(
                resource_domain(method, "/api/v1/settings/llm"),
                None,
                "{method} 不是写，叫醒面只会让它们重读同一份字节"
            );
        }
    }

    #[test]
    fn sign_in_churn_is_not_a_face_changing() {
        // 每个令牌一次刷新；放进来就等于按令牌频率抽整棵树。
        assert_eq!(resource_domain("POST", "/api/v1/auth/refresh"), None);
        assert_eq!(resource_domain("POST", "/api/v1/auth/login"), None);
    }

    #[test]
    fn paths_outside_the_api_root_are_not_faces() {
        assert_eq!(resource_domain("POST", "/health"), None);
        assert_eq!(resource_domain("POST", "/api/v2/settings"), None);
        assert_eq!(resource_domain("POST", "/api/v1/"), None);
        assert_eq!(resource_domain("POST", "/api/v1"), None);
    }

    #[test]
    fn a_dynamic_first_segment_is_not_a_prefix() {
        // 前缀必须是固定的一段，否则每个 id 都成了一条独立的面。
        assert_eq!(resource_domain("POST", "/api/v1/{id}/x"), None);
        assert_eq!(resource_domain("POST", "/api/v1//x"), None);
    }
}
