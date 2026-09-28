//! 机器人报文的签名：向安全网关借一对 `(timestamp, sign)`。
//!
//! 钉钉与飞书的机器人可以对每条报文验签，密钥是平台发给这个机器人的那把。业务侧
//! **不持有**这把密钥——主应用 Pod 零带外凭证是硬约束，而配置面根本没有它的投递键
//! （`config_loader` 的 env 映射里只有地址三条），所以「配了 secret 就能签」在部署面上
//! 是**不可达**的，不是没配。密钥住在安全网关，业务侧要的是它的产物：一次签名。
//!
//! 规则本身在契约层（`cog_core::contract::platform_sign`），两边读同一份——两份实现
//! 必然分叉，而分叉的症状是平台回一句「签名校验失败」，从外面看像对端的问题。
//!
//! 这里只做三件事：把请求打给网关、把网关的具名拒绝与传输故障**分开**、给调用方一个
//! 不用自己拼口径的入口（[`signature_for`]）。

use std::sync::Arc;

use cog_core::contract::platform_sign::{
    PlatformOutlet, PlatformSignature, SignRefusal, SignRequest, SIGN_PATH,
};
use cog_core::HttpClient;

/// 向签名者要一次签名失败的原因。
///
/// 拒绝（网关说得出原因）与故障（说不出）分开：前者的处置人可能是运维（去投递密钥）
/// 或我们（改出口名），后者是网关不可达或答了看不懂的东西。两者都不能让调用方
/// **猜一个未签名的报文发出去**——那会把「签名没拿到」变成平台那边的「签名校验失败」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignError {
    /// 签名者具名拒绝（原因过边界时是短码，见 [`SignRefusal`]）。
    Refused(SignRefusal),
    /// 没拿到答复：连接失败、DNS、TLS、超时。
    Unreachable(String),
    /// 拿到了答复，但不是一份能读的签名。
    Malformed(String),
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(refusal) => {
                write!(f, "签名者拒绝：{}（{}）", refusal.code(), refusal.hint())
            }
            Self::Unreachable(detail) => write!(f, "签名者不可达：{detail}"),
            Self::Malformed(detail) => write!(f, "签名者的答复读不懂：{detail}"),
        }
    }
}

/// 一次签名的来源。业务侧只依赖这个特征，不依赖网关——网关只是当下唯一的实现。
///
/// 带 `Debug`：dispatcher 自己 `derive(Debug)`，而它要持有签名者。
#[async_trait::async_trait]
pub trait NotificationSigner: Send + Sync + std::fmt::Debug {
    /// 按出口要一对签名。时间戳由签名者取，调用方不能指定。
    async fn sign(&self, outlet: PlatformOutlet) -> Result<PlatformSignature, SignError>;
}

/// 安全网关上的签名者。
#[derive(Debug)]
pub struct GatewaySigner {
    http_client: Arc<dyn HttpClient>,
    /// 签名端点基址（部署面注入，与 `COGNEVA_GITHUB_API_BASE` 同形）。
    base: String,
}

impl GatewaySigner {
    pub fn new(http_client: Arc<dyn HttpClient>, base: impl Into<String>) -> Self {
        Self {
            http_client,
            base: base.into(),
        }
    }
}

#[async_trait::async_trait]
impl NotificationSigner for GatewaySigner {
    async fn sign(&self, outlet: PlatformOutlet) -> Result<PlatformSignature, SignError> {
        // 请求体只说「哪个出口」：不说密钥（那正是拿不到的东西），也不说时间戳
        // （签名与它必须同源，复用旧时间戳会得到平台报文层拒收）。
        let body = serde_json::to_vec(&SignRequest {
            outlet: outlet.as_str().to_string(),
        })
        .map_err(|e| SignError::Malformed(format!("请求体序列化失败：{e}")))?;
        let url = format!("{}{SIGN_PATH}", self.base.trim_end_matches('/'));
        let req = cog_core::HttpRequest::post(url)
            .header("Content-Type", "application/json")
            .body(body);

        let resp = self
            .http_client
            .execute(req)
            .await
            .map_err(|e| SignError::Unreachable(e.to_string()))?;

        if resp.is_success() {
            return serde_json::from_slice::<PlatformSignature>(&resp.body)
                .map_err(|e| SignError::Malformed(format!("签名报文解析失败：{e}")));
        }

        // 非 2xx：优先读那句具名拒绝。读不出来就按「答了但答的不是我们要的」处理，
        // 不按不可达——网关是通的，它只是说了别的话。
        let refusal = serde_json::from_slice::<SignRefusalBody>(&resp.body)
            .ok()
            .and_then(|b| parse_refusal(&b.refusal));
        Err(match refusal {
            Some(refusal) => SignError::Refused(refusal),
            None => SignError::Malformed(format!("HTTP {}", resp.status)),
        })
    }
}

/// 网关那句具名拒绝的读法。短码过边界，取值域由 [`SignRefusal::ALL`] 界定。
#[derive(Debug, serde::Deserialize)]
struct SignRefusalBody {
    refusal: String,
}

/// 短码 → 取值。表外的短码不猜。
fn parse_refusal(code: &str) -> Option<SignRefusal> {
    SignRefusal::ALL.into_iter().find(|r| r.code() == code)
}

/// 一次「要签名」的尝试，落成三个互斥的结局。
#[derive(Debug)]
pub enum SignAttempt {
    /// 拿到了签名。
    Signed(PlatformSignature),
    /// 这次不该签名，或者没有可签的密钥：按未签名报文发。
    Unsigned,
    /// 该签而签不成：**不要发**，把原因报上去。
    Failed(SignError),
}

/// 出口报文要不要签名、能不能签成的唯一判据。
///
/// 三档的含义与处置人各不同：
///
/// - 没有签名者（部署面没配签名端点）：按未签名报文发——这是这个出口今天的样子，
///   也是「机器人用关键词安全、不需要签名」的合法形态。
/// - 网关说**它没有这个出口的密钥**：同样按未签名报文发。这一档是**按出口**的，
///   所以「只给钉钉配了签名」不会把飞书的投递弄没——两个出口各有各的密钥，
///   缺哪个只影响哪个。
/// - 其余（网关不可达、答复读不懂、出口名不在表里）：**不发**。签名没拿到还发出去，
///   等于把我们的问题伪装成平台的「签名校验失败」，那正是这套读数里最难与「已送达」
///   分开的一类失败。
pub async fn signature_for(
    signer: &Option<Arc<dyn NotificationSigner>>,
    outlet: PlatformOutlet,
) -> SignAttempt {
    let Some(signer) = signer else {
        return SignAttempt::Unsigned;
    };
    match signer.sign(outlet).await {
        Ok(signature) => SignAttempt::Signed(signature),
        Err(SignError::Refused(SignRefusal::NotConfigured)) => SignAttempt::Unsigned,
        Err(e) => SignAttempt::Failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{HttpRequest, HttpResponse, SFError};
    use std::sync::Mutex;

    /// 只回答一次、把发出的请求记下来的替身。
    #[derive(Debug)]
    struct StubClient {
        reply: Mutex<Option<Result<HttpResponse, String>>>,
        sent: Mutex<Vec<HttpRequest>>,
    }

    impl StubClient {
        fn answering(status: u16, body: &str) -> Arc<Self> {
            Arc::new(Self {
                reply: Mutex::new(Some(Ok(HttpResponse {
                    status,
                    headers: Default::default(),
                    body: body.as_bytes().to_vec(),
                }))),
                sent: Mutex::new(Vec::new()),
            })
        }

        fn recording(self: &Arc<Self>) -> Arc<dyn HttpClient> {
            self.clone() as Arc<dyn HttpClient>
        }

        fn last_url(&self) -> String {
            self.sent.lock().unwrap().last().unwrap().url.clone()
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for StubClient {
        async fn execute(&self, req: HttpRequest) -> cog_core::SFResult<HttpResponse> {
            self.sent.lock().unwrap().push(req);
            match self.reply.lock().unwrap().take() {
                Some(Ok(resp)) => Ok(resp),
                Some(Err(e)) => Err(SFError::IO(e)),
                None => Err(SFError::IO("替身只答一次".into())),
            }
        }
    }

    #[tokio::test]
    async fn a_signature_is_read_off_the_gateways_answer() {
        let client = StubClient::answering(200, r#"{"timestamp":"1700000000123","sign":"abc="}"#);
        let signer = GatewaySigner::new(client.recording(), "http://gateway:8081/");
        let signature = signer.sign(PlatformOutlet::DingTalk).await.expect("签名");
        assert_eq!(signature.timestamp, "1700000000123");
        assert_eq!(signature.sign, "abc=");
        // 基址末尾的斜杠与路径之间的拼接只出一种写法。
        assert_eq!(
            client.last_url(),
            "http://gateway:8081/v1/notification/sign"
        );
    }

    #[tokio::test]
    async fn a_named_refusal_stays_named_across_the_boundary() {
        let client = StubClient::answering(503, r#"{"refusal":"not_configured"}"#);
        let signer = GatewaySigner::new(client.recording(), "http://gateway:8081");
        let err = signer.sign(PlatformOutlet::Feishu).await.expect_err("拒绝");
        assert_eq!(err, SignError::Refused(SignRefusal::NotConfigured));
    }

    #[tokio::test]
    async fn an_unreadable_answer_is_not_reported_as_unreachable() {
        let client = StubClient::answering(500, "not json at all");
        let signer = GatewaySigner::new(client.recording(), "http://gateway:8081");
        let err = signer
            .sign(PlatformOutlet::DingTalk)
            .await
            .expect_err("故障");
        assert!(
            matches!(err, SignError::Malformed(_)),
            "答复读不懂不是不可达：网关通着，它只是说了别的话（{err:?}）"
        );
    }

    /// 三档的**方向**各自钉一条：没有签名者发未签名报文；网关说没有密钥也发未签名
    /// 报文；其余一律不发。反过来（拿不到签名仍然发出去）会把我们的故障伪装成平台的
    /// 「签名校验失败」。
    #[tokio::test]
    async fn only_a_named_missing_key_falls_back_to_an_unsigned_message() {
        assert!(matches!(
            signature_for(&None, PlatformOutlet::DingTalk).await,
            SignAttempt::Unsigned
        ));

        let missing = StubClient::answering(503, r#"{"refusal":"not_configured"}"#);
        let signer: Option<Arc<dyn NotificationSigner>> = Some(Arc::new(GatewaySigner::new(
            missing.recording(),
            "http://gateway:8081",
        )));
        assert!(matches!(
            signature_for(&signer, PlatformOutlet::DingTalk).await,
            SignAttempt::Unsigned
        ));

        let broken = StubClient::answering(502, "");
        let signer: Option<Arc<dyn NotificationSigner>> = Some(Arc::new(GatewaySigner::new(
            broken.recording(),
            "http://gateway:8081",
        )));
        assert!(matches!(
            signature_for(&signer, PlatformOutlet::Feishu).await,
            SignAttempt::Failed(SignError::Malformed(_))
        ));
    }
}
