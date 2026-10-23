//!Protocol DTOs and traits — A2A / MCP abstractions so the gateway never
//!depends on `cog-protocol` concrete types.

use serde::{Deserialize, Serialize};

// ─── A2A Agent Card DTOs ──────────────────────────────────────────────────

/// A2A Agent Card — describes an agent's capabilities and endpoints.
///
/// 一个字段集要收得下**已发布的两个版本**的名片，因为对端可能是任何一个：
/// 0.3 的名片用一个 `url` 指明端点，1.0 的名片把它换成 `supportedInterfaces`
/// 列表（同一个端点可以有多个版本）。1.0 还要求 `protocolVersion`、
/// `defaultInputModes`/`defaultOutputModes`，并把能力旗写成 camelCase。
/// 只有 `authentication` 是更早草稿留下的形状，已从 0.3 起被 `securitySchemes`
/// 取代；它留在这里只为读得动旧对端，不是我们要发的东西。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    /// 0.3 名片上的端点地址。1.0 名片没有这个字段，读出来是空串——
    /// 用 [`Self::endpoint`] 取，不要直接读它。
    #[serde(default)]
    pub url: String,
    #[serde(default, rename = "protocolVersion")]
    pub protocol_version: Option<String>,
    pub version: String,
    pub capabilities: AgentCapabilities,
    #[serde(default, rename = "defaultInputModes")]
    pub default_input_modes: Vec<String>,
    #[serde(default, rename = "defaultOutputModes")]
    pub default_output_modes: Vec<String>,
    #[serde(default)]
    pub skills: Vec<AgentSkill>,
    /// 0.3 的名片用 `securitySchemes`；更早的草稿用 `authentication`。同一个
    /// 事实两个字段名，收得下两个才算收得下「一张名片」。
    #[serde(default, rename = "securitySchemes")]
    pub security_schemes: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<AgentAuthentication>,
    /// 1.0 名片上的接口列表。条目自带 `protocolVersion`，所以这里也读得出
    /// 对端会哪几版。
    #[serde(default, rename = "supportedInterfaces")]
    pub supported_interfaces: Vec<AgentInterface>,
}

impl AgentCard {
    /// 名片给对端的那个地址。
    ///
    /// 0.3 写在 `url`，1.0 写在 `supportedInterfaces[0].url`（第一条是首选）。
    /// 两处都没有 ⇒ `None`：一张说不出自己在哪的名片，不能靠猜把请求发出去。
    pub fn endpoint(&self) -> Option<&str> {
        if !self.url.is_empty() {
            return Some(self.url.as_str());
        }
        self.supported_interfaces
            .first()
            .map(|interface| interface.url.as_str())
    }

    /// 这张名片代表的协议版本。
    ///
    /// 0.3 的 `protocolVersion` 是顶层字段；1.0 把它挪进每个接口条目。1.0 名片
    /// 没有顶层版本时，取第一条接口的版本——那正是规范里「第一条是首选」的意思。
    pub fn protocol_version(&self) -> Option<A2aVersion> {
        self.protocol_version
            .as_deref()
            .and_then(A2aVersion::parse)
            .or_else(|| {
                self.supported_interfaces
                    .iter()
                    .find_map(|interface| A2aVersion::parse(&interface.protocol_version))
            })
    }
}

/// One entry of a 1.0 agent card's interface list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInterface {
    pub url: String,
    #[serde(default, rename = "protocolBinding")]
    pub protocol_binding: String,
    #[serde(default, rename = "protocolVersion")]
    pub protocol_version: String,
}

/// Capabilities advertised by an A2A agent.
///
/// 三个开关在两版里都是可选的：缺一个不等于名片无效，等于对端没说这一项，
/// 读作 `false`。把缺字段当解析失败，会让一张少写一行的合法名片在客户端那里
/// 变成「对端坏了」——而真正发生的事只是它对某一项没表态。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AgentCapabilities {
    pub streaming: bool,
    pub push_notifications: bool,
    pub state_transition_history: bool,
}

/// A skill exposed by an A2A agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSkill {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub examples: Vec<String>,
}

/// Authentication schemes supported by an A2A agent (pre-0.3 card shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAuthentication {
    pub schemes: Vec<String>,
    pub credentials: Option<serde_json::Value>,
}

/// A2A 协议版本。这张表只有两格，因为只有两格有真实的在网对端：0.3 是所有
/// 已发布 SDK 的默认（规范规定空的 `A2A-Version` 就按 0.3 解释），1.0 是当前
/// 版本。更早的 0.1 草稿没有任何只发那个形状的在网客户端，且没有任何名片字段
/// 能宣告它，所以不在这里——不宣告又不服务的版本，放进来只多一条没人走的路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aVersion {
    V03,
    V10,
}

impl A2aVersion {
    /// `Major.Minor` 两段才算数（patch 不参与兼容判断）。
    pub fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        match raw {
            "0.3" => Some(Self::V03),
            "1.0" => Some(Self::V10),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::V03 => "0.3",
            Self::V10 => "1.0",
        }
    }
}

// ─── A2A Task DTOs ────────────────────────────────────────────────────────

// 这一族与上面的名片同源、同理由：A2A 的 HTTP 面由 gateway 服务，而 gateway 不许
// 依赖 `cog-protocol` 的具体类型。所以模型与两版线上形状的编解码都住在这里，
// 服务端与客户端读同一份。
//
// 两版线上形状差得不小：0.3 用 `kind` 当判别字段、字段 camelCase、状态是小写
// 短横线；1.0 取消了 `kind`（成员名本身就是判别）、状态是大写 `TASK_STATE_*`、
// 流帧包在 `statusUpdate`/`artifactUpdate` 里。**内部只有一套模型**，两版只是
// 同一行任务的两种渲染——两套模型会让「同一个任务在两个对端眼里」迟早各自漂。
//
// `A2aTaskStatus` 带前缀，是因为本 crate 根上已经有一个给自家任务存储用的
// `TaskStatus`，而两者不是同一套状态：那个是调度器的，这个是 A2A 线上的。同一个名字
// 挂两套状态，就是让读者把一套读成另一套。

/// A2A Task — 从一条 message 到终态的生命周期（版本无关的内部模型）。
#[derive(Debug, Clone)]
pub struct A2aTask {
    pub id: String,
    /// 任务所属的上下文。本平台一条 message 建一个任务，没有跨任务会话，
    /// 所以上下文就是任务自己——等于「这个任务独占一个上下文」，语义成立。
    pub context_id: String,
    pub status: A2aTaskStatus,
    /// 状态附带的一句说明（规范 `TaskStatus.message` 的文本内容）。
    pub status_message: Option<String>,
    /// 状态记录的时点（规范 `TaskStatus.timestamp`）。
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub artifacts: Vec<TaskArtifact>,
    /// 这个任务交换过的 message。本平台不落 message 历史，正常是空的；
    /// 客户端发消息时用它承载要说的话。
    pub history: Vec<TaskMessage>,
}

/// A2A 线上的一条任务状态。闭集与 A2A 规范逐字一致，不是本平台调度器那一套。
/// `Rejected`/`AuthRequired`/`Unknown` 本平台不产出，但**对端可能发给我们**——
/// 解析不认识的线上值时退成 `Unknown` 而不是报错，与规范给的那一档同名。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2aTaskStatus {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Canceled,
    Failed,
    Rejected,
    AuthRequired,
    Unknown,
}

impl A2aTaskStatus {
    /// 这个状态在指定版本的线上怎么拼。
    pub fn wire(self, version: A2aVersion) -> &'static str {
        match version {
            A2aVersion::V03 => match self {
                Self::Submitted => "submitted",
                Self::Working => "working",
                Self::InputRequired => "input-required",
                Self::Completed => "completed",
                Self::Canceled => "canceled",
                Self::Failed => "failed",
                Self::Rejected => "rejected",
                Self::AuthRequired => "auth-required",
                Self::Unknown => "unknown",
            },
            A2aVersion::V10 => match self {
                Self::Submitted => "TASK_STATE_SUBMITTED",
                Self::Working => "TASK_STATE_WORKING",
                Self::InputRequired => "TASK_STATE_INPUT_REQUIRED",
                Self::Completed => "TASK_STATE_COMPLETED",
                Self::Canceled => "TASK_STATE_CANCELED",
                Self::Failed => "TASK_STATE_FAILED",
                Self::Rejected => "TASK_STATE_REJECTED",
                Self::AuthRequired => "TASK_STATE_AUTH_REQUIRED",
                Self::Unknown => "TASK_STATE_UNSPECIFIED",
            },
        }
    }

    /// 读线上值。两版的拼法都认（入站按规范允许的兼容期收旧形状），
    /// 认不出的一律 `Unknown`——一个对端新加的枚举值不该让整条任务读不出来。
    pub fn parse(raw: &str) -> Self {
        match raw {
            "submitted" | "TASK_STATE_SUBMITTED" => Self::Submitted,
            "working" | "TASK_STATE_WORKING" => Self::Working,
            "input-required" | "TASK_STATE_INPUT_REQUIRED" => Self::InputRequired,
            "completed" | "TASK_STATE_COMPLETED" => Self::Completed,
            "canceled" | "TASK_STATE_CANCELED" => Self::Canceled,
            "failed" | "TASK_STATE_FAILED" => Self::Failed,
            "rejected" | "TASK_STATE_REJECTED" => Self::Rejected,
            "auth-required" | "TASK_STATE_AUTH_REQUIRED" => Self::AuthRequired,
            _ => Self::Unknown,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Rejected
        )
    }
}

/// 一条 message：谁说的 + 若干 part。
#[derive(Debug, Clone)]
pub struct TaskMessage {
    pub message_id: String,
    pub role: String,
    pub parts: Vec<MessagePart>,
}

impl TaskMessage {
    /// 0.3：`{kind:"message", messageId, role, parts}`；1.0 没有 `kind`。
    pub fn to_wire(&self, version: A2aVersion) -> serde_json::Value {
        // 1.0 的 role 是 proto 枚举的 JSON 名（`ROLE_USER`/`ROLE_AGENT`），0.3 是
        // 小写的两个词。内部统一存小写，出站时按版本拼。
        let role = match (version, self.role.as_str()) {
            (A2aVersion::V10, "user") => "ROLE_USER",
            (A2aVersion::V10, "agent") => "ROLE_AGENT",
            (_, other) => other,
        };
        let mut message = serde_json::json!({
            "messageId": self.message_id,
            "role": role,
            "parts": self.parts.iter().map(|part| part.to_wire(version)).collect::<Vec<_>>(),
        });
        if version == A2aVersion::V03 {
            message["kind"] = serde_json::json!("message");
        }
        message
    }

    pub fn from_wire(message: &serde_json::Value) -> Result<Self, String> {
        let role = message
            .get("role")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("agent")
            .to_string();
        // 1.0 的 role 是大写线名 `ROLE_AGENT`/`ROLE_USER`，内部统一成小写。
        let role = match role.as_str() {
            "ROLE_USER" => "user".to_string(),
            "ROLE_AGENT" => "agent".to_string(),
            other => other.to_string(),
        };
        let parts = message
            .get("parts")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "the message carries no `parts` array".to_string())?
            .iter()
            .filter_map(|part| MessagePart::from_wire(part).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            message_id: message
                .get("messageId")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            role,
            parts,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessagePart {
    Text {
        text: String,
    },
    File {
        name: String,
        mime_type: String,
        bytes: String,
    },
}

impl MessagePart {
    /// 0.3 用 `kind` 判别、文件嵌在 `file` 对象里；1.0 拿成员名判别、文件字段
    /// 摊平（`raw`/`filename`/`mediaType`）。
    pub fn to_wire(&self, version: A2aVersion) -> serde_json::Value {
        match (self, version) {
            (Self::Text { text }, A2aVersion::V03) => {
                serde_json::json!({"kind": "text", "text": text})
            }
            (Self::Text { text }, A2aVersion::V10) => serde_json::json!({"text": text}),
            (
                Self::File {
                    name,
                    mime_type,
                    bytes,
                },
                A2aVersion::V03,
            ) => serde_json::json!({
                "kind": "file",
                "file": {"name": name, "mimeType": mime_type, "fileWithBytes": bytes},
            }),
            (
                Self::File {
                    name,
                    mime_type,
                    bytes,
                },
                A2aVersion::V10,
            ) => serde_json::json!({
                "raw": bytes,
                "filename": name,
                "mediaType": mime_type,
            }),
        }
    }

    /// 读一个 part。`kind`（0.3）、`type`（更早草稿）与「成员名即类型」（1.0）
    /// 三种写法都认；一个字段名的差异不该让一份照规范写的 message 变成「没有文本」。
    pub fn from_wire(part: &serde_json::Value) -> Result<Option<Self>, String> {
        let kind = part
            .get("kind")
            .or_else(|| part.get("type"))
            .and_then(serde_json::Value::as_str);
        let is_text = kind == Some("text") || (kind.is_none() && part.get("text").is_some());
        if is_text {
            return Ok(part
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(|text| Self::Text {
                    text: text.to_string(),
                }));
        }
        if kind == Some("file") || part.get("file").is_some() || part.get("raw").is_some() {
            let file = part.get("file").unwrap_or(part);
            let field = |names: &[&str]| {
                names
                    .iter()
                    .find_map(|name| file.get(*name).and_then(serde_json::Value::as_str))
                    .unwrap_or_default()
                    .to_string()
            };
            return Ok(Some(Self::File {
                name: field(&["name", "filename"]),
                mime_type: field(&["mimeType", "mediaType"]),
                bytes: field(&["fileWithBytes", "raw"]),
            }));
        }
        Ok(None)
    }
}

/// 一条产物：若干 part，加它在任务里的身份。
#[derive(Debug, Clone)]
pub struct TaskArtifact {
    pub artifact_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub parts: Vec<MessagePart>,
}

impl TaskArtifact {
    /// 两个版本的这个对象字段名一致（`artifactId`/`name`/`parts`），差别只在
    /// part 的形状，所以只按 part 分版本。
    pub fn to_wire(&self, version: A2aVersion) -> serde_json::Value {
        let mut artifact = serde_json::json!({
            "artifactId": self.artifact_id,
            "parts": self.parts.iter().map(|part| part.to_wire(version)).collect::<Vec<_>>(),
        });
        if let Some(name) = &self.name {
            artifact["name"] = serde_json::json!(name);
        }
        if let Some(description) = &self.description {
            artifact["description"] = serde_json::json!(description);
        }
        artifact
    }

    pub fn from_wire(artifact: &serde_json::Value) -> Result<Self, String> {
        let artifact_id = artifact
            .get("artifactId")
            .and_then(serde_json::Value::as_str)
            // 更早草稿用它自己的 `name` 与位置当身份；读得动旧对端比挑剔字段名重要。
            .or_else(|| artifact.get("name").and_then(serde_json::Value::as_str))
            .unwrap_or_default()
            .to_string();
        let parts = artifact
            .get("parts")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "the artifact carries no `parts` array".to_string())?
            .iter()
            .filter_map(|part| MessagePart::from_wire(part).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            artifact_id,
            name: artifact
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            description: artifact
                .get("description")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            parts,
        })
    }
}

impl A2aTask {
    /// 把这一行任务渲染成指定版本的线上形状。
    pub fn to_wire(&self, version: A2aVersion) -> serde_json::Value {
        let mut status = serde_json::json!({
            "state": self.status.wire(version),
            "timestamp": self
                .updated_at
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        });
        if let Some(note) = &self.status_message {
            status["message"] = TaskMessage {
                message_id: format!("{}-status", self.id),
                role: "agent".into(),
                parts: vec![MessagePart::Text { text: note.clone() }],
            }
            .to_wire(version);
        }
        let mut task = serde_json::json!({
            "id": self.id,
            "contextId": self.context_id,
            "status": status,
            "artifacts": self
                .artifacts
                .iter()
                .map(|artifact| artifact.to_wire(version))
                .collect::<Vec<_>>(),
        });
        if version == A2aVersion::V03 {
            task["kind"] = serde_json::json!("task");
        }
        if !self.history.is_empty() {
            task["history"] = serde_json::json!(self
                .history
                .iter()
                .map(|message| message.to_wire(version))
                .collect::<Vec<_>>());
        }
        task
    }

    /// 读一条任务。两版形状都认；认不出的状态退 `Unknown`，缺 `contextId`
    /// 时用任务 id 补（老对端没这个字段，而「任务自成上下文」正是本平台的模型）。
    pub fn from_wire(task: &serde_json::Value) -> Result<Self, String> {
        let id = task
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "the task carries no `id`".to_string())?
            .to_string();
        let (status, status_message, updated_at) = parse_status(task.get("status"))?;
        let artifacts = task
            .get("artifacts")
            .and_then(serde_json::Value::as_array)
            .map(|artifacts| {
                artifacts
                    .iter()
                    .map(TaskArtifact::from_wire)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        let history = task
            .get("history")
            .or_else(|| task.get("messages"))
            .and_then(serde_json::Value::as_array)
            .map(|messages| {
                messages
                    .iter()
                    .map(TaskMessage::from_wire)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        let context_id = task
            .get("contextId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| id.clone());
        Ok(Self {
            id,
            context_id,
            status,
            status_message,
            updated_at,
            artifacts,
            history,
        })
    }
}

/// 读 `status`：规范是对象 `{state, message?, timestamp?}`；本平台早先对外发过
/// 一个字符串（`"submitted"`），一并读得动——那个形状的对端还在外面跑。
fn parse_status(
    status: Option<&serde_json::Value>,
) -> Result<(A2aTaskStatus, Option<String>, chrono::DateTime<chrono::Utc>), String> {
    let Some(status) = status else {
        return Ok((A2aTaskStatus::Unknown, None, chrono::Utc::now()));
    };
    if let Some(raw) = status.as_str() {
        return Ok((A2aTaskStatus::parse(raw), None, chrono::Utc::now()));
    }
    let state = status
        .get("state")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "the task status carries no `state`".to_string())?;
    let message = status.get("message").and_then(|message| {
        let text = message
            .get("parts")?
            .as_array()?
            .iter()
            .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        (!text.is_empty()).then_some(text)
    });
    let updated_at = status
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|parsed| parsed.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);
    Ok((A2aTaskStatus::parse(state), message, updated_at))
}

// ─── MCP Client trait ─────────────────────────────────────────────────────

/// Trait abstracting an MCP (Model Context Protocol) client.
#[async_trait::async_trait]
pub trait McpClient: Send + Sync {
    /// List tools exposed by the MCP server.
    async fn list_tools(&self) -> crate::SFResult<Vec<serde_json::Value>>;

    /// Call a tool exposed by the MCP server and return its result.
    ///
    /// Both ends of this call are untyped JSON, for the same reason
    /// [`Self::list_tools`] is: the concrete request/result types belong to the
    /// protocol crate, and a caller holding this trait object must be able to
    /// make the call without depending on that crate. A trait that stops at
    /// listing tools makes the call unreachable for exactly those callers —
    /// they can see which tools exist and cannot run one.
    ///
    /// `arguments` is the tool's argument object; an implementation is expected
    /// to reject a non-object rather than guess.
    async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> crate::SFResult<serde_json::Value>;
}
