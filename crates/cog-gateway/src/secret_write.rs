//! 凭证 Secret 的写入面：只有安全网关进程执行 patch。
//!
//! 主应用的管理面（LLM 向导、贡献通道、GitHub App）继续接收用户输入、做校验与
//! 读回，但**不再自己 patch Secret**——它把要写的字段转发给安全网关的内部端点，
//! 由持有 Secret 写权限的那个进程落盘并滚动自己。
//!
//! 边界在 RBAC 上：对象级约束只认 ServiceAccount，主应用多握一个可写对象名，
//! 等于把「代持带外凭证」这件事复制一份到零凭证的进程里。所以这里搬的不是代码
//! 位置，而是**执行写入的主体**。
//!
//! 两侧共用既有的进程间 HMAC（`COGNEVA_WEBHOOK_INTERNAL_SECRET`）：两个进程本来
//! 就共享这一条内部信任边界，再生成一把新键只是把同一个边界抄成两份。

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 安全网关上的内部端点；挂在 LLM 通道（8081），主应用已经能到达那个地址。
pub(crate) const SECRET_PATCH_PATH: &str = "/internal/secret-patch";
/// 内部请求的签名头，与 webhook 转发同一个形状（`sha256=<hex>`）。
pub(crate) const SIGNATURE_HEADER: &str = "x-cogneva-signature-256";

pub(crate) fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

pub(crate) fn signature_matches(secret: &str, body: &[u8], header: &str) -> bool {
    let Some(hex_sig) = header.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(expected) = hex::decode(hex_sig) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

/// 写入之后滚动动作的结局。
///
/// 「已写但没滚」不是失败：值已经落盘，只是消费者（在环境变量里读它）要等下一次
/// 重启才看得见。把它当失败会把一次成功的写入报成「丢了」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Restart {
    NotRequested,
    Rolled,
    Failed(String),
}

impl Restart {
    pub(crate) fn rolled(&self) -> bool {
        matches!(self, Self::Rolled)
    }
}

/// 网关侧：把一份字段落进 Secret，必要时滚动自己。
///
/// `b64 = true` 走 base64 那支——`null` 在这支里表示删键；`false` 走 stringData
/// 那支，值原样写入。`Err` 只表示**写入**失败；滚动失败放在返回值里，因为那笔
/// 写入是成功的，调用方要能把两句分开报。
pub(crate) async fn apply(fields: Value, b64: bool, restart: bool) -> Result<Restart, String> {
    let kube = crate::llm_admin::KubeClient::in_cluster()?;
    if b64 {
        crate::secret_layout::patch_fields_b64(&kube, fields).await?;
    } else {
        crate::secret_layout::patch_fields(&kube, fields).await?;
    }
    if !restart {
        return Ok(Restart::NotRequested);
    }
    Ok(match kube.restart_gateway().await {
        Ok(()) => Restart::Rolled,
        Err(e) => Restart::Failed(e),
    })
}

/// 主应用侧：把写入请求转发给安全网关。
///
/// 地址默认取集群内服务名；`COGNEVA_SECURITY_GATEWAY_BASE` 可覆盖（本地联调）。
/// 没配内部凭证就拒绝——宁可让向导报错，也不退回本进程直接 patch：那会悄悄
/// 把权限面又打开，而且没人看得出来今天写的是哪一条路。
pub(crate) async fn forward(fields: Value, b64: bool, restart: bool) -> Result<Restart, String> {
    let base = std::env::var("COGNEVA_SECURITY_GATEWAY_BASE")
        .unwrap_or_else(|_| "http://cogneva-security-gateway:8081".to_string());
    let secret = std::env::var("COGNEVA_WEBHOOK_INTERNAL_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "主应用未配置内部转发凭证（COGNEVA_WEBHOOK_INTERNAL_SECRET），无法把 Secret 写入交给安全网关"
                .to_string()
        })?;
    let body = serde_json::to_vec(&serde_json::json!({
        "fields": fields,
        "b64": b64,
        "restart": restart,
    }))
    .map_err(|e| format!("写入请求序列化失败: {e}"))?;
    let request = cog_core::HttpRequest::post(format!(
        "{}{}",
        base.trim_end_matches('/'),
        SECRET_PATCH_PATH
    ))
    .header("Content-Type", "application/json")
    .header(SIGNATURE_HEADER, sign(&secret, &body))
    .body(body);
    // 出站走本进程装配进来的那一份客户端：这里的请求和本模块其他网关调用同属
    // 一次短交换，超时、连接池、代理不应该由这个函数自钉一套——自己 `Client::new()`
    // 造出来的那份没有超时，上游不应答就永远挂着，而调用方看不出用的是哪一份设置。
    let resp = crate::contribution_admin::http_client()?
        .execute(request)
        .await
        .map_err(|e| format!("转发安全网关失败: {e}"))?;
    if !resp.is_success() {
        return Err(format!(
            "安全网关返回 {}: {}",
            resp.status,
            resp.text().unwrap_or_default()
        ));
    }
    let body: Value = resp
        .json()
        .map_err(|e| format!("安全网关响应解析失败: {e}"))?;
    Ok(match body.get("restart").and_then(Value::as_str) {
        Some("rolled") => Restart::Rolled,
        Some("not-requested") => Restart::NotRequested,
        Some("failed") => Restart::Failed(
            body.get("message")
                .and_then(Value::as_str)
                .unwrap_or("未知原因")
                .to_string(),
        ),
        _ => Restart::NotRequested,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_round_trips_and_rejects_tampering() {
        let secret = "s3cret";
        let body = br#"{"fields":{}}"#;
        let sig = sign(secret, body);
        assert!(signature_matches(secret, body, &sig));
        // 改一个字节就不认：这条断言是「内部端点只收持有同一把凭证的进程」的
        // 可执行形式。
        assert!(!signature_matches(secret, br#"{"fields":{"x":1}}"#, &sig));
        assert!(!signature_matches("other", body, &sig));
        // 形状不对的头（不是 sha256=、不是十六进制）一律不认，不是 panic。
        assert!(!signature_matches(secret, body, "sha256=zz"));
        assert!(!signature_matches(secret, body, "bearer x"));
    }
}
