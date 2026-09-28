//! 平台机器人出口的签名规格。
//!
//! 四个通知出口里两个要签名（钉钉、飞书），企微与通用 webhook 不要。签名是出口
//! 自己的契约，两个平台的口径只差一处：钉钉把 `timestamp` 与 `sign` 放进 URL 查询串、
//! 时间戳是**毫秒**；飞书把它们放进请求体、时间戳是**秒**。待签文本两处同形
//! （`{timestamp}\n{secret}`），密钥都是那把平台 secret。
//!
//! **这条规则只有这一份。** 业务侧不再自己算签名——那要求它手上有一把平台密钥，
//! 正是「零带外凭证」要避免的；它向安全网关要一个 `(timestamp, sign)`，网关用同
//! 一份规则、同一把密钥算出来。规则放契约 crate 而不是任一侧，是因为两边真的都要
//! 用它：只要存在两份实现，钉钉或飞书改一次口径就会只改一边，而症状是平台回一句
//! 「签名校验失败」——从外面看像对端的问题，不像我们的两份实现分了叉。
//!
//! 与入站验签（`cog-gateway` 的 `verify_github_signature`）不对称是刻意的：验签只有
//! 网关一道门在用，签名有两个进程在用。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 一次签名的产物，两个出口各自的书写口径都在里面。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformSignature {
    /// 平台口径的时间戳字符串（钉钉毫秒、飞书秒）。
    pub timestamp: String,
    /// `base64(HMAC-SHA256(secret, "{timestamp}\n{secret}"))`。
    pub sign: String,
}

/// 需要签名的出口。
///
/// 取值范围而不是自由字符串：签名口径按平台而异，一个不在这张表里的名字没有口径
/// 可套，只能**具名**拒绝。出口名的字面量与 `cog-notification` 的出口清单同源，
/// 有门禁钉住两边一致（否则这里多一个名字、那边少一个出口，签名请求会永远落在
/// 没人认的名字上）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformOutlet {
    DingTalk,
    Feishu,
}

impl PlatformOutlet {
    /// 全部需要签名的出口。它的长度就是签名面的宽度。
    pub const ALL: [PlatformOutlet; 2] = [Self::DingTalk, Self::Feishu];

    /// 出口名，与通知侧出口清单里的字面量相同。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DingTalk => "dingtalk",
            Self::Feishu => "feishu",
        }
    }

    /// 按出口名解析；不在表里返回 `None`，调用方据此拒绝而不是猜一个口径。
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.as_str() == name)
    }

    /// 这个出口的密钥该从哪个环境变量读。
    ///
    /// 名字按出口生成而不是各处手写：密钥的投递面只有一处（安全网关进程），
    /// 名字散开写就多了一处会漂的地方。
    pub const fn secret_env(self) -> &'static str {
        match self {
            Self::DingTalk => DINGTALK_SECRET_ENV,
            Self::Feishu => FEISHU_SECRET_ENV,
        }
    }
}

/// 钉钉机器人签名密钥的投递键（只注入安全网关进程，业务 Pod 零持有）。
pub const DINGTALK_SECRET_ENV: &str = "COGNEVA_NOTIFICATION_DINGTALK_SECRET";
/// 飞书机器人签名密钥的投递键，同钉钉。
pub const FEISHU_SECRET_ENV: &str = "COGNEVA_NOTIFICATION_FEISHU_SECRET";

/// 业务侧要签名时打的路径。
///
/// 挂在安全网关的**业务借用面**上，与 `/github`、`/gitee`、`/v1/oauth/*` 同一条：
/// 那一条面就是「业务要什么给什么」的借用通道，代签是它长出的第一条**出站**面
/// （前几条都是替业务去外部读或换）。
pub const SIGN_PATH: &str = "/v1/notification/sign";

/// 业务侧读到的签名端点（部署面注入，与 `COGNEVA_GITHUB_API_BASE` 同形）。
pub const SIGN_BASE_ENV: &str = "COGNEVA_NOTIFICATION_SIGN_BASE";

/// 签名请求体：只说「哪个出口」。
///
/// 不说密钥（那正是不能给的东西），也不说时间戳：签名与时间戳必须同源。让调用方
/// 自己选时间戳、再由签名者签，等于把"签的是哪一刻"交给调用方——它复用一次旧时间戳
/// 就会得到平台报文层的拒收，而那正是这套读数里最难与"已送达"分开的一类失败。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignRequest {
    pub outlet: String,
}

/// 签名者拒绝一次请求的原因。
///
/// 有界取值：它要过进程边界（进响应体、进日志、进告警标签），自由文本会把「对端
/// 拒绝」变成「我们解析不了」。两个取值的处置人不同——一个该去配密钥，一个该去改
/// 出口名。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignRefusal {
    /// 问的出口不在这张表里。
    UnknownOutlet,
    /// 签名者手上没有这个出口的密钥。
    NotConfigured,
}

impl SignRefusal {
    /// 过边界的短码。
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownOutlet => "unknown_outlet",
            Self::NotConfigured => "not_configured",
        }
    }

    /// 给读到这条拒绝的人一句话，说清该动哪里。
    pub const fn hint(self) -> &'static str {
        match self {
            Self::UnknownOutlet => "这个出口没有签名口径，名字也没有出口清单认它",
            Self::NotConfigured => "签名者没有持有这个出口的密钥，把密钥投递进安全网关进程",
        }
    }

    /// 全部取值。拒绝一侧的词汇也走闭集，理由同出口表。
    pub const ALL: [SignRefusal; 2] = [Self::UnknownOutlet, Self::NotConfigured];
}

/// 按出口口径算一次签名。
///
/// `now` 由调用方给：签名者用它自己的时钟，业务侧那条路（不经网关的部署）用进程
/// 时钟，两条路都不接受"调用方送来的时间戳"。
pub fn platform_signature(
    outlet: PlatformOutlet,
    secret: &str,
    now: DateTime<Utc>,
) -> PlatformSignature {
    let timestamp = match outlet {
        PlatformOutlet::DingTalk => now.timestamp_millis().to_string(),
        PlatformOutlet::Feishu => now.timestamp().to_string(),
    };
    let sign = hmac_sha256_base64(secret, &format!("{timestamp}\n{secret}"));
    PlatformSignature { timestamp, sign }
}

/// `base64(HMAC-SHA256(secret, data))`，两个平台同一种写法。
fn hmac_sha256_base64(secret: &str, data: &str) -> String {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 期望值由**独立的实现**算出来（Python `hmac` + `base64`），不是本模块自己
    /// 再算一遍：自己算自己只会证明代码与它自己一致，证明不了它符合平台的口径。
    /// 密钥与时间戳都取定值，于是这条断言是可复算的。
    #[test]
    fn the_two_outlets_sign_the_platform_string_with_their_own_timestamp_unit() {
        let now = Utc.timestamp_opt(1_700_000_000, 123_000_000).unwrap();
        let secret = "s3cr3t";

        let ding = platform_signature(PlatformOutlet::DingTalk, secret, now);
        assert_eq!(ding.timestamp, "1700000000123");
        assert_eq!(ding.sign, "dBCxFZ3qAByqt+EpHqakZ98Qk0JGKXD8ok6hScCRMY8=");

        let fei = platform_signature(PlatformOutlet::Feishu, secret, now);
        assert_eq!(fei.timestamp, "1700000000");
        assert_eq!(fei.sign, "APRjwSrmu2gntY/NUIZCw/i74wzW+CEcuSp2qg2XrSQ=");
    }

    #[test]
    fn an_outlet_name_round_trips_and_a_name_outside_the_table_is_not_guessed() {
        for outlet in PlatformOutlet::ALL {
            assert_eq!(PlatformOutlet::from_name(outlet.as_str()), Some(outlet));
        }
        assert_eq!(PlatformOutlet::from_name("wechat-work"), None);
        assert_eq!(PlatformOutlet::from_name(""), None);
        assert_eq!(PlatformOutlet::from_name("DingTalk"), None);
    }

    #[test]
    fn every_refusal_has_a_distinct_bounded_code() {
        let codes: std::collections::BTreeSet<&str> =
            SignRefusal::ALL.iter().map(|r| r.code()).collect();
        assert_eq!(codes.len(), SignRefusal::ALL.len());
    }

    #[test]
    fn the_secret_env_names_are_distinct_per_outlet() {
        let names: std::collections::BTreeSet<&str> =
            PlatformOutlet::ALL.iter().map(|o| o.secret_env()).collect();
        assert_eq!(names.len(), PlatformOutlet::ALL.len());
    }
}
