//! 网关 git 身份自举。
//!
//! 网关是整条链路唯一的 git 出口：沙盒零凭证、进化 Pod 零凭证，它们到上游的
//! 每一次 fetch/push 都经过这里。所以**身份必须在网关上自持**——但装机时没有
//! 人愿意先生成密钥、再去平台后台登记一遍。这一步要自己做：
//!
//! - **手上有 token**：生成本机密钥对 → 用 token 把它登记成**仓库部署密钥**
//!   （deploy key，写权限；可单独吊销，不像账号级 key 会连带授出账号下所有
//!   仓库）→ 私钥写回 Secret → 滚动网关。零人工。
//! - **没有 token**：生成密钥对并把**公钥**与状态写回 Secret（状态 `pending`），
//!   WebUI 向导展示公钥并要求输入 token；输入后走上面那一支。人工也可以自己把
//!   公钥加到仓库——那样不需要谁声明"我加好了"，本模块用一次真实 SSH 握手自证，
//!   通了才晋级。
//!
//! **判定归证据**：状态 `ready` 只在（a）token 支登记成功、或（b）一次真实
//! `ls-remote` 握手成功之后才写。不因为"密钥文件在"就认为能用——文件在而密钥
//! 没登记，SSH 每次都会失败。
//!
//! **待确认的私钥不写进生效键位**：生成后先放在 `git-ssh-private-key-pending`，
//! 只有确认可用才晋级到 `git-ssh-private-key`。生效键位是挂载进容器的那个，
//! 未确认的密钥放进它就等于让 git 兜底通道以一把没人认识的密钥上线，而它只会
//! 在真的该兜底时才发现融不进去。
//!
//! **状态存在 Secret 里**：装机期间进程可能重启多次，进程内状态每次归零。归零的
//! 后果很具体：待确认的密钥被重新生成，于是使用者刚粘到平台上的那把公钥又对不
//! 上了。
//!
//! **公钥从私钥导出，不照抄 Secret 里那行**：集群手里只有私钥，而 OpenSSH 私钥
//! 自带公钥 blob（外面那一段就是），所以"这对密钥的公钥是什么"是可以读出来的
//! 事实。Secret 里那行可以与私钥不是同一对——人工换过密钥而没换那行、或那行来自
//! 更早一代——那时面板显示、重新登记、指纹比对全都在描述一把集群里并不存在的
//! 密钥，而唯一的症状是 SSH 每次都被拒。写下来的那半必须能被手里这半验证。

use std::path::{Path, PathBuf};

use base64::Engine as _;
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::contribution_admin::generate_ssh_keypair;
use crate::llm_admin::KubeClient;

/// 生效私钥的键位。与容器里的挂载点一一对应，见 `git_mirror::DEFAULT_SSH_KEY`。
pub(crate) const SECRET_SSH_PRIVATE_KEY: &str = "git-ssh-private-key";
/// 已生成但尚未确认可用的私钥。
pub(crate) const SECRET_SSH_PENDING_KEY: &str = "git-ssh-private-key-pending";
/// 公钥（给人看、给平台登记用）。公钥不是秘密，但只写在 Secret 里，
/// 不经日志以外的通道外发。
pub(crate) const SECRET_SSH_PUBLIC_KEY: &str = "git-ssh-public-key";
/// 身份状态：`ready` 表示已验证可用。
pub(crate) const SECRET_IDENTITY_STATE: &str = "git-identity-state";
/// 最近一次"为什么还没就绪"的一句话理由，给人看（面板提示 + 排查）。
///
/// 理由落盘而不是只打日志：装机期间进程会重启多次，日志随容器一起没了，而
/// "到底卡在哪一步"正是面板要显示、人要照做的那件事（缺 token / token 权限不够 /
/// 没配仓库 / 公钥还没登记）。
const SECRET_IDENTITY_NOTE: &str = "git-identity-note";
const SECRET_GITHUB_TOKEN: &str = "github-token";

pub(crate) const STATE_READY: &str = "ready";

/// 真实上游 API 地址。**不能**取 `COGNEVA_GITHUB_API_BASE`：那个变量让业务 Pod
/// 把请求发回网关（凭证只留在网关）。网关就是出口，照它取会变成回环。
const GITHUB_API: &str = "https://api.github.com";
/// 平台侧登记时的密钥标题：同一把密钥重复登记会返回"已存在"，
/// 而不同标题会堆出一串同名含义的 key，标题固定下来便于人在后台辨认。
const GITHUB_KEY_TITLE: &str = "cogneva-gateway";

/// 自举的部署配置。
#[derive(Debug, Clone)]
pub(crate) struct IdentityConfig {
    /// 登记部署密钥的目标仓库 `owner/name`。空 = 未配置，自动登记这一支不可用。
    pub repo: String,
    /// 生效私钥的路径（与 git 兜底通道共用同一份）。
    pub key_path: PathBuf,
    /// SSH 远端前缀，用于自证握手。
    pub ssh_base: String,
    /// git 可执行文件（与镜像刷新同源，见 `git_mirror::git_bin_from_env`）。
    pub git_bin: PathBuf,
    /// 待确认状态的重试节拍。
    pub retry_secs: u64,
}

impl IdentityConfig {
    pub(crate) fn from_env() -> Self {
        let nonempty = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
        Self {
            repo: nonempty("COGNEVA_GATEWAY_GIT_IDENTITY_REPO")
                .unwrap_or_default()
                .trim()
                .to_string(),
            key_path: nonempty("COGNEVA_GATEWAY_GIT_SSH_KEY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(crate::git_mirror::DEFAULT_SSH_KEY)),
            ssh_base: nonempty("COGNEVA_GATEWAY_GIT_SSH_BASE")
                .unwrap_or_else(|| crate::git_mirror::DEFAULT_SSH_BASE.to_string()),
            git_bin: crate::git_mirror::git_bin_from_env(),
            // 下界 60s：节拍读成 1 会让"等人输入 token"变成每秒一次 SSH 握手。
            retry_secs: nonempty("COGNEVA_GATEWAY_GIT_IDENTITY_RETRY_SECS")
                .and_then(|v| v.parse().ok())
                .filter(|v| *v >= 60)
                .unwrap_or(600),
        }
    }
}

/// 自举的下一步。纯函数：判据只有四个观测，与网络、时钟无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// 身份已生效：不碰网络也不碰 Secret。
    Settled,
    /// 有密钥但没确认可用，且有 token：登记它。
    Register,
    /// 有密钥但没确认可用，且没 token：只能等人。
    AwaitHuman,
    /// 连密钥都还没有，有 token：生成 + 登记。
    GenerateThenRegister,
    /// 连密钥都还没有，没 token：生成 + 等人。
    GenerateThenAwaitHuman,
}

/// 判据表。`state` 是 Secret 里记的身份状态，`has_live_key` 是生效键位有没有私钥，
/// `has_candidate_key` 是待确认键位有没有私钥，`pair_consistent` 是记下来的公钥与
/// 生效私钥是不是同一对（读不出任一半时为真）。
///
/// 生效 + `ready` 直接短路：这是稳态，不该每次启动都去握一次手。**但 `ready` 与
/// 密钥成对是两件事**：状态位说"验证过"，而"验证过的那把密钥"只有公钥也与私钥
/// 对得上才成立。对不上时不短路——此时重新登记能把那半纠正回来，而短路会让面板
/// 与 git 兜底通道长期描述一把集群里不存在的密钥。
pub(crate) fn next_step(
    state: Option<&str>,
    has_live_key: bool,
    has_candidate_key: bool,
    has_token: bool,
    pair_consistent: bool,
) -> Step {
    if has_live_key && state == Some(STATE_READY) && pair_consistent {
        return Step::Settled;
    }
    match (has_live_key || has_candidate_key, has_token) {
        (true, true) => Step::Register,
        (true, false) => Step::AwaitHuman,
        (false, true) => Step::GenerateThenRegister,
        (false, false) => Step::GenerateThenAwaitHuman,
    }
}

/// 一次自举的结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// 身份已生效，什么都没做。
    Settled,
    /// 登记成功并已滚动网关（新私钥要等重启才被读进兜底通道）。
    Registered,
    /// 人工登记的公钥被一次真实握手证实，已晋级并滚动网关。
    Adopted,
    /// 缺一份能登记的凭证：需要人给 token（或自己把公钥加到仓库）。
    AwaitingToken(String),
    /// 这份资源不归本进程管（不在集群里、或没有 Secret 权限）。
    NotOwner(String),
}

impl Outcome {
    /// 这一轮之后还需不需要再看一次。等人输入要轮询，其余都可以收工。
    pub(crate) fn keeps_waiting(&self) -> bool {
        matches!(self, Outcome::AwaitingToken(_))
    }
}

/// 公钥指纹：`SHA256:` + base64(sha256(密钥 blob))（去 padding），与
/// `ssh-keygen -lf` 同款。日志里报指纹而不是整行公钥，是为了让"这把密钥变了吗"
/// 一眼可答，且不必把公钥刷满日志。
pub(crate) fn fingerprint(public_line: &str) -> Option<String> {
    let blob = key_blob(public_line)?;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(blob)
        .ok()?;
    let digest = Sha256::digest(raw);
    Some(format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD
            .encode(digest)
            .trim_end_matches('=')
    ))
}

/// 公钥行里的密钥材料（base64 那一段），不带算法名与注释。指纹与"两半是不是一对"
/// 都只看这一段：注释是给人认的，改注释不该让同一把密钥看起来换了一把。
fn key_blob(public_line: &str) -> Option<&str> {
    public_line.split_whitespace().nth(1)
}

/// OpenSSH v1 私钥容器开头的那串魔数。
const OPENSSH_KEY_MAGIC: &[u8] = b"openssh-key-v1\0";

/// 从 OpenSSH v1 私钥里读出它自带的公钥，渲染成 `<算法> <blob>`。
///
/// 读的是容器结构本身：魔数 → ciphername → kdfname → kdfoptions → 密钥条数 →
/// 公钥段（一个 length-prefixed blob）。公钥段就在明文里，所以这一步不需要解密、
/// 不需要派生，也不碰私钥材料。
///
/// **不支持或读不出就给 `None`**，绝不猜：这是个"能读出来就对一对"的检查，不是
/// 判生死的门槛——读不出时下面所有判定都放行（见 `Facts::pair_consistent`）。
pub(crate) fn public_line_of(private_key: &str) -> Option<String> {
    /// 一个 length-prefixed 段，返回 (内容, 剩余)。
    fn take(raw: &[u8]) -> Option<(&[u8], &[u8])> {
        let len = u32::from_be_bytes(raw.get(..4)?.try_into().ok()?) as usize;
        let end = 4usize.checked_add(len)?;
        Some((raw.get(4..end)?, raw.get(end..)?))
    }

    let body: String = private_key
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(body.trim())
        .ok()?;
    let rest = raw.strip_prefix(OPENSSH_KEY_MAGIC)?;
    let (_, rest) = take(rest)?; // ciphername
    let (_, rest) = take(rest)?; // kdfname
    let (_, rest) = take(rest)?; // kdf options
    let count = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
    if count != 1 {
        return None;
    }
    let (blob, _) = take(rest.get(4..)?)?;
    let (algo, _) = take(blob)?;
    let algo = std::str::from_utf8(algo).ok()?;
    Some(format!(
        "{algo} {}",
        base64::engine::general_purpose::STANDARD.encode(blob)
    ))
}

/// 两半是不是一对：记下来的公钥行与生效私钥里自带的那半比。
///
/// **读不出任一半时为真**。这是"两个读数互相矛盾"的判据，而缺席不是矛盾：私钥是
/// 别人给的别的格式、公钥行只有算法名、Secret 里干脆没有那一行——这些都只是没读到
/// 证据，不该把一份本来能用的身份判成坏的（那正是"没拿到证据"与"拿到反证"的区别）。
fn pair_matches(live_key: Option<&str>, recorded_line: Option<&str>) -> bool {
    match (
        live_key.and_then(public_line_of),
        recorded_line.and_then(key_blob),
    ) {
        (Some(derived), Some(recorded)) => key_blob(&derived) == Some(recorded),
        _ => true,
    }
}

/// 公钥与私钥对不上时的理由。面板显示的就是自举写下的这一句，两处共用同一个串，
/// 免得"人看到的"与"日志里写的"变成两句话。
pub(crate) const NOTE_KEY_PAIR_MISMATCH: &str =
    "Secret 里的公钥与生效私钥不是同一对；已按私钥导出的公钥为准，将重新自证或登记";

/// 登记部署密钥的应答判决。
///
/// **201 与 422 都算成功**：422 是"这把密钥已经登记过了"，重跑自举时会撞上，
/// 把它当失败会让一次成功的状态每次重启都被判成坏。其余按可读原因分流——
/// 401/403 是权限不够（要仓库管理权限，普通 read 权限的 token 登记不了），
/// 404 通常也是权限（无权看到的仓库与不存在的仓库在 API 上同形）。
pub(crate) fn registration_verdict(status: u16) -> Result<(), String> {
    match status {
        201 | 422 => Ok(()),
        401 => Err("token 无效或已过期（HTTP 401），无法登记部署密钥".to_string()),
        403 => Err(
            "token 缺少仓库管理权限（HTTP 403），登记部署密钥需要该仓库的 admin 权限".to_string(),
        ),
        404 => {
            Err("仓库不存在，或 token 看不到它（HTTP 404），请核对仓库名与 token 权限".to_string())
        }
        other => Err(format!("登记部署密钥返回 HTTP {other}")),
    }
}

/// 状态接口回的「git 身份」块。公钥可以外发（它本来就是公开材料），私钥绝不回读
/// ——面板要的只是"配好了没有、公钥是什么、下一步该谁动手"。
///
/// 状态词与自举模块共用同一套取值（`ready` / `pending` / `absent`），面板不必自己
/// 猜"私钥在但状态位没有"算什么：那正是**未确认**，不是已生效。
pub(crate) fn status_block(secret: &serde_json::Value) -> serde_json::Value {
    let engine = base64::engine::general_purpose::STANDARD;
    let field = |key: &str| {
        secret
            .get("data")
            .and_then(|d| d.get(key))
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .and_then(|b64| engine.decode(b64).ok())
            .and_then(|bytes| String::from_utf8(bytes).ok())
    };
    let live_key = field(SECRET_SSH_PRIVATE_KEY);
    let has_live = live_key.is_some();
    let recorded_line = field(SECRET_SSH_PUBLIC_KEY);
    // 展示的公钥从生效私钥导出，只有导不出时才退回 Secret 里那行：人要拿去平台登记、
    // 要照着比指纹的，必须是手里这把密钥对应的那半。
    let public_line = live_key
        .as_deref()
        .and_then(public_line_of)
        .or_else(|| recorded_line.clone());
    let pair_consistent = pair_matches(live_key.as_deref(), recorded_line.as_deref());
    let note = field(SECRET_IDENTITY_NOTE);
    let state = match field(SECRET_IDENTITY_STATE).as_deref() {
        Some(STATE_READY) if has_live && pair_consistent => STATE_READY,
        // 私钥在但没有状态位：**未确认**。把它读成 ready 会让面板对着一把
        // 还没登记成功的密钥说"配好了"。状态位在、但公钥不是这一对的，同理：
        // 那句 ready 描述的不是手里这把密钥。
        _ if has_live || field(SECRET_SSH_PENDING_KEY).is_some() => "pending",
        _ => "absent",
    };
    let has_token = field(SECRET_GITHUB_TOKEN).is_some();
    json!({
        "state": state,
        "public_key": public_line,
        "fingerprint": public_line.as_deref().and_then(fingerprint),
        "token_present": has_token,
        "repo_configured": !IdentityConfig::from_env().repo.is_empty(),
        // 下一步该谁动手：有 token 的安装会自己走完，没有就需要人给一份。
        "needs_token": state != STATE_READY && !has_token,
        // 卡在哪一步。ready 时不该有理由，也不显示上一次的旧理由。两半对不上是唯一
        // 一个不必等自举写下理由就该当场说清的状态：面板判它 pending，理由得同时到位。
        "note": if !pair_consistent {
            Some(NOTE_KEY_PAIR_MISMATCH.to_string())
        } else if state == STATE_READY {
            None
        } else {
            note
        },
    })
}

/// Secret 里读出来的身份事实。
#[derive(Clone)]
struct Facts {
    state: Option<String>,
    live_key: Option<String>,
    candidate_key: Option<String>,
    public_line: Option<String>,
    token: Option<String>,
    /// 上一次写下的"为什么还没就绪"。与这一轮的结论相同就不再写——等人输入时
    /// 每轮都重复写同一个理由，只会在 Secret 上堆出无穷的版本。
    note: Option<String>,
}

impl Facts {
    fn from_secret(secret: &serde_json::Value) -> Self {
        let field = |key: &str| {
            secret
                .get("data")
                .and_then(|d| d.get(key))
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .and_then(|b64| {
                    base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .ok()
                        .and_then(|bytes| String::from_utf8(bytes).ok())
                })
        };
        Self {
            state: field(SECRET_IDENTITY_STATE),
            live_key: field(SECRET_SSH_PRIVATE_KEY),
            candidate_key: field(SECRET_SSH_PENDING_KEY),
            public_line: field(SECRET_SSH_PUBLIC_KEY),
            token: field(SECRET_GITHUB_TOKEN),
            note: field(SECRET_IDENTITY_NOTE),
        }
    }

    /// 生效私钥里自带的那半公钥。登记、比对、展示都用它。
    fn derived_public_line(&self) -> Option<String> {
        self.live_key.as_deref().and_then(public_line_of)
    }

    /// 记下来的公钥与生效私钥是不是同一对（读不出任一半时为真）。
    fn pair_consistent(&self) -> bool {
        pair_matches(self.live_key.as_deref(), self.public_line.as_deref())
    }

    fn step(&self) -> Step {
        next_step(
            self.state.as_deref(),
            self.live_key.is_some(),
            self.candidate_key.is_some(),
            self.token.is_some(),
            self.pair_consistent(),
        )
    }
}

/// 一次自举。幂等：任何一步已经做过就不再做第二遍。
pub(crate) async fn ensure_git_identity(config: &IdentityConfig) -> Outcome {
    let kube = match KubeClient::in_cluster() {
        Ok(kube) => kube,
        Err(e) => return Outcome::NotOwner(format!("不在集群内运行（{e}）")),
    };
    let path = crate::contribution_admin::secret_api_path(kube.namespace());
    let secret = match kube.get_json(&path).await {
        Ok(secret) => secret,
        Err(e) => return Outcome::NotOwner(format!("读取 {path} 失败（{e}）")),
    };
    let facts = Facts::from_secret(&secret);
    let step = facts.step();
    info!(?step, "git 身份自举：本轮判据");

    let outcome = match step {
        Step::Settled => Outcome::Settled,
        // 「有密钥但没确认」与「还没有密钥」的差别只在密钥从哪来：确认过程一致，
        // 所以先把候选密钥备齐，再走同一条"先自证、再登记"的路。
        Step::Register | Step::AwaitHuman => {
            let key = facts
                .live_key
                .as_ref()
                .or(facts.candidate_key.as_ref())
                .cloned()
                .unwrap_or_default();
            confirm_or_prompt(&kube, config, &facts, &key).await
        }
        Step::GenerateThenRegister | Step::GenerateThenAwaitHuman => {
            let keypair = generate_ssh_keypair("cogneva-gateway");
            info!(
                fingerprint = %fingerprint(&keypair.public_line).unwrap_or_default(),
                "git 身份自举：生成密钥对（尚未确认可用）"
            );
            if let Err(e) = patch_secret(
                &kube,
                json!({
                    SECRET_SSH_PENDING_KEY: keypair.private_pem,
                    SECRET_SSH_PUBLIC_KEY: keypair.public_line,
                    SECRET_IDENTITY_STATE: "pending",
                }),
            )
            .await
            {
                return Outcome::NotOwner(format!("写入待确认密钥失败（{e}）"));
            }
            // 复制的理由：本轮新生成的公钥要带进确认流程，而下面的"写理由"还要
            // 拿原有的那一份做比较（新旧理由相同就不重复写）。
            let mut facts = facts.clone();
            facts.public_line = Some(keypair.public_line.clone());
            confirm_or_prompt(&kube, config, &facts, &keypair.private_pem).await
        }
    };
    // 停在等人这一步时把理由落盘：面板要显示、人要知道下一步该做什么。
    if let Outcome::AwaitingToken(reason) = &outcome {
        if facts.note.as_deref() != Some(reason.as_str()) {
            if let Err(e) = patch_secret(&kube, json!({ SECRET_IDENTITY_NOTE: reason })).await {
                warn!("git 身份自举：写入待办理由失败: {e}");
            }
        }
    }
    outcome
}

/// 自证优先于登记：一次真实握手比任何声明都硬。
///
/// 顺序刻意是"先自证、后登记"——已经能用的身份不该再往平台写一把新 key，
/// 那既多一次需要管理员权限的调用，也会在仓库里堆出两把同义密钥。
async fn confirm_or_prompt(
    kube: &KubeClient,
    config: &IdentityConfig,
    facts: &Facts,
    private_key: &str,
) -> Outcome {
    let derived = facts.derived_public_line();
    if let (Some(derived), Some(recorded)) = (derived.as_deref(), facts.public_line.as_deref()) {
        if key_blob(derived) != key_blob(recorded) {
            warn!(
                recorded = %fingerprint(recorded).unwrap_or_default(),
                derived_from_key = %fingerprint(derived).unwrap_or_default(),
                "git 身份自举：Secret 里的公钥与生效私钥不是同一对，按私钥导出的那半为准"
            );
        }
    }
    let Some(public_line) = derived.or_else(|| facts.public_line.clone()) else {
        // 生效键位里有一把密钥，但既读不出它自带的公钥、Secret 里也没有一行：
        // 无法登记它，也无法把它讲给人听。只能重造一把。
        warn!("git 身份自举：私钥存在但读不出对应公钥，重造密钥对");
        let keypair = generate_ssh_keypair("cogneva-gateway");
        return register_and_promote(
            kube,
            config,
            facts,
            &keypair.private_pem,
            &keypair.public_line,
        )
        .await;
    };

    if verify_ssh(config, private_key).await {
        return match promote(kube, config, private_key, &public_line).await {
            Ok(()) => {
                info!(fingerprint = %fingerprint(&public_line).unwrap_or_default(),
                      "git 身份自举：SSH 握手自证通过，身份已生效");
                Outcome::Adopted
            }
            Err(e) => Outcome::NotOwner(format!("晋级密钥失败（{e}）")),
        };
    }

    if facts.token.is_some() {
        return register_and_promote(kube, config, facts, private_key, &public_line).await;
    }

    let reason = if config.repo.is_empty() {
        "未配置目标仓库（COGNEVA_GATEWAY_GIT_IDENTITY_REPO），无法自动登记".to_string()
    } else if !facts.pair_consistent() {
        // 对不上时登记的会是私钥导出的那半（上面已经选了它），所以这里要说清
        // 发生了什么，而不是笼统地说"公钥还没登记"。
        NOTE_KEY_PAIR_MISMATCH.to_string()
    } else {
        "SSH 握手未通过（公钥尚未登记到上游仓库）".to_string()
    };
    warn!(
        fingerprint = %fingerprint(&public_line).unwrap_or_default(),
        public_key = %public_line,
        "git 身份自举：需要人工介入——在 WebUI 输入平台 token（将自动登记并滚动网关），\
         或把上面这行公钥加到仓库部署密钥后本网关会自证接管；原因: {reason}"
    );
    Outcome::AwaitingToken(reason)
}

/// 登记 + 自证 + 晋级。登记只是把公钥交给平台；`ready` 仍然只由一次真实握手写。
async fn register_and_promote(
    kube: &KubeClient,
    config: &IdentityConfig,
    facts: &Facts,
    private_key: &str,
    public_line: &str,
) -> Outcome {
    let Some(token) = facts.token.clone() else {
        return Outcome::AwaitingToken("没有可用的平台 token".to_string());
    };
    if config.repo.is_empty() {
        warn!("git 身份自举：未配置目标仓库，跳过自动登记（公钥已在日志与 WebUI 中给出）");
        return Outcome::AwaitingToken("未配置目标仓库".to_string());
    }
    if let Err(reason) = register_deploy_key(&token, &config.repo, public_line).await {
        warn!(repo = %config.repo, "git 身份自举：登记部署密钥失败: {reason}");
        warn!(
            public_key = %public_line,
            "git 身份自举：可把上面这行公钥手工加为仓库部署密钥，本网关会自证接管"
        );
        return Outcome::AwaitingToken(reason);
    }
    // 登记成功也要再握一次手才晋级。写进平台的那次调用只说"平台收下了这个请求"，
    // 而且**422 有两种含义**（已存在的密钥、无效的密钥），都被判成功；把它当成
    // "身份已生效"，等于用一个 HTTP 状态码顶替一次真实的认证。握手是同一件事的
    // 直接读数，成了才写 `ready`。
    if !verify_ssh(config, private_key).await {
        let reason = "部署密钥已登记，但 SSH 握手仍未通过（密钥未在仓库生效）".to_string();
        warn!(repo = %config.repo, public_key = %public_line, "{reason}");
        return Outcome::AwaitingToken(reason);
    }
    match promote(kube, config, private_key, public_line).await {
        Ok(()) => {
            info!(repo = %config.repo, fingerprint = %fingerprint(public_line).unwrap_or_default(),
                  "git 身份自举：部署密钥已登记，身份生效");
            Outcome::Registered
        }
        Err(e) => Outcome::NotOwner(format!("晋级密钥失败（{e}）")),
    }
}

/// 晋级：候选私钥 → 生效键位 + 状态 `ready`，并滚动网关让兜底通道读进新私钥。
///
/// 滚动只在生效键位的**文件还不存在**时做：文件已存在说明本进程启动时就带着它，
/// 这次只是补状态位，重启是白重启。
async fn promote(
    kube: &KubeClient,
    config: &IdentityConfig,
    private_key: &str,
    public_line: &str,
) -> Result<(), String> {
    patch_secret(
        kube,
        json!({
            SECRET_SSH_PRIVATE_KEY: private_key,
            SECRET_SSH_PUBLIC_KEY: public_line,
            SECRET_IDENTITY_STATE: STATE_READY,
            // 晋级后待确认键位就没有意义了；留着它只会让人以为还有一份候选。
            SECRET_SSH_PENDING_KEY: serde_json::Value::Null,
            // 上一次"卡在哪"的理由同理：身份已经生效，留着它只会让人以为还卡着。
            SECRET_IDENTITY_NOTE: serde_json::Value::Null,
        }),
    )
    .await?;
    if config.key_path.is_file() {
        return Ok(());
    }
    info!(
        key = %config.key_path.display(),
        "git 身份自举：滚动安全网关以装载新私钥"
    );
    kube.restart_gateway().await
}

/// 以明文写 Secret。写入用 `data`（base64）而不是 `stringData`：同一份 patch 里
/// 还要把待确认键位置空，两类字段混用会让"删掉的那个到底生效没有"变得不可读。
async fn patch_secret(kube: &KubeClient, string_fields: serde_json::Value) -> Result<(), String> {
    let engine = base64::engine::general_purpose::STANDARD;
    let mut data = serde_json::Map::new();
    if let Some(map) = string_fields.as_object() {
        for (key, value) in map {
            match value {
                serde_json::Value::String(text) => {
                    data.insert(key.clone(), json!(engine.encode(text)));
                }
                _ => {
                    data.insert(key.clone(), serde_json::Value::Null);
                }
            }
        }
    }
    kube.patch(
        &crate::contribution_admin::secret_api_path(kube.namespace()),
        json!({ "data": serde_json::Value::Object(data) }),
    )
    .await
}

/// 用给定的私钥做一次**只读** SSH 握手：`ls-remote` 不改变上游任何状态，
/// 却把所有要验的东西都走了一遍（DNS、22 端口、密钥被接受、仓库可读）。
async fn verify_ssh(config: &IdentityConfig, private_key: &str) -> bool {
    // 没配仓库就没有可自证的对象：这时候去连一个拼不出来的地址只是浪费一次超时。
    if config.repo.is_empty() || private_key.is_empty() {
        return false;
    }
    let Ok(dir) = temp_key_file(private_key) else {
        return false;
    };
    let url = format!("{}{}.git", config.ssh_base, config.repo);
    let ssh = format!(
        "ssh -i {} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o BatchMode=yes \
         -o ConnectTimeout=10",
        dir.path().display()
    );
    // 这条 git 走的是"起进程 + 进程组 + 一次硬超时"那条共用路径：`ssh` 的
    // `ConnectTimeout` 只管到建连为止，建上之后对端不吐字节就是**没有边界的
    // 等待**——而这里是自举链路的自证步骤，卡住就意味着状态位永远停在待确认。
    let (child, mut guard) = match crate::git_mirror::spawn_git_group(
        &config.git_bin,
        &["ls-remote", "--exit-code", &url, "HEAD"],
        Some(&ssh),
        std::process::Stdio::piped(),
        std::process::Stdio::piped(),
    ) {
        Ok(pair) => pair,
        Err(e) => {
            info!(target: "git_identity", "SSH 自证无法执行: {e}");
            return false;
        }
    };

    let out = tokio::time::timeout(SSH_PROBE_TIMEOUT, child.wait_with_output()).await;
    match out {
        Ok(Ok(out)) if out.status.success() => {
            // 进程已经归位、号可以被复用：守卫到此为止。留在后面 disarm，
            // 会变成对一个可能已经属于别人的进程组发 SIGKILL。
            guard.disarm();
            true
        }
        Ok(Ok(out)) => {
            guard.disarm();
            let stderr = String::from_utf8_lossy(&out.stderr);
            info!(target: "git_identity", "SSH 自证未通过: {}", stderr.trim());
            false
        }
        Ok(Err(e)) => {
            info!(target: "git_identity", "SSH 自证无法执行: {e}");
            false
        }
        // 超时：守卫随作用域被丢弃，整条 `git → sh → ssh` 一起走。
        Err(_) => {
            info!(
                target: "git_identity",
                "SSH 自证超时（{}s），已杀进程组",
                SSH_PROBE_TIMEOUT.as_secs()
            );
            false
        }
    }
}

/// SSH 自证握手的硬超时。与选路探测同量级（那是一条 `ls-remote` 的一次往返），
/// 但要留出"密钥被接受、仓库可读"这一段真实往返的余量。
const SSH_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// 把私钥写到临时文件（0600）供 git 使用。私钥不进命令行、不进环境变量，
/// 只以一个权限受限的临时文件存在，用完随 TempDir 一起消失。
fn temp_key_file(private_key: &str) -> Result<tempfile::TempDir, String> {
    let dir = tempfile::Builder::new()
        .prefix("cogneva-git-identity-")
        .tempdir()
        .map_err(|e| format!("创建临时目录失败: {e}"))?;
    let path = dir.path().join("id_ed25519");
    std::fs::write(&path, private_key).map_err(|e| format!("写临时私钥失败: {e}"))?;
    set_owner_only(&path)?;
    Ok(dir)
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("设置私钥权限失败: {e}"))
}

#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> Result<(), String> {
    Ok(())
}

/// 登记为**仓库部署密钥**（写权限）。用 deploy key 而不是账号级 key：
/// 它只授出这一个仓库，且能单独吊销——网关被攻破时的影响面就是这个仓库，
/// 而不是 token 持有人名下的全部仓库。
async fn register_deploy_key(token: &str, repo: &str, public_line: &str) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;
    let resp = client
        .post(format!("{GITHUB_API}/repos/{repo}/keys"))
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "cogneva-gateway")
        .json(&json!({
            "title": GITHUB_KEY_TITLE,
            "key": public_line,
            // 写权限：landing 要把变更推回上游，只读密钥会让推送在远端被拒。
            "read_only": false,
        }))
        .send()
        .await
        .map_err(|e| format!("请求 GitHub 失败: {e}"))?;
    let status = resp.status().as_u16();
    if let Err(reason) = registration_verdict(status) {
        let body = resp.text().await.unwrap_or_default();
        let snippet: String = body.chars().take(200).collect();
        return Err(format!("{reason}（响应: {snippet}）"));
    }
    Ok(())
}

/// 后台自举任务。**只由能读写 Secret 的进程运行**（属主判定复用贡献通道那一份：
/// 同一份资源、同一个权限面）。
///
/// 停在"等人输入"时按 `retry_secs` 轮询，其它结论都收工——登记与晋级都已经
/// 写进 Secret 且滚了网关，本进程即将被换掉，继续跑没有意义。
pub fn spawn_git_identity_bootstrap(
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let config = IdentityConfig::from_env();
        if !crate::contribution_admin::is_contribution_secret_owner().await {
            info!("git 身份自举未启动：本进程读不到 cogneva-secrets（非属主）");
            return;
        }
        loop {
            let outcome = tokio::select! {
                outcome = ensure_git_identity(&config) => outcome,
                _ = shutdown.wait() => {
                    info!("git 身份自举收到停机信号，退出");
                    return;
                }
            };
            info!(?outcome, "git 身份自举：本轮结论");
            if !outcome.keeps_waiting() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(config.retry_secs)) => {}
                _ = shutdown.wait() => {
                    info!("git 身份自举收到停机信号，退出");
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_settled_identity_is_left_alone() {
        // 常态：生效键位有私钥 + 状态 ready + 两半成对 → 既不握手也不写 Secret
        assert_eq!(
            next_step(Some(STATE_READY), true, false, false, true),
            Step::Settled
        );
        assert_eq!(
            next_step(Some(STATE_READY), true, false, true, true),
            Step::Settled
        );
    }

    #[test]
    fn a_pair_that_does_not_match_is_not_left_alone() {
        // 状态位说 ready、私钥也在，但记下来的公钥不是这一对的。此时那句 ready
        // 描述的不是手里这把密钥，而面板与重新登记都会照着错的那半来。
        assert_eq!(
            next_step(Some(STATE_READY), true, false, true, false),
            Step::Register,
            "有 token 时重新登记能把公钥那半纠正回来"
        );
        assert_eq!(
            next_step(Some(STATE_READY), true, false, false, false),
            Step::AwaitHuman,
            "没有 token 时只能等人，不能当成稳态收工"
        );
    }

    #[test]
    fn a_key_without_evidence_needs_confirmation() {
        // 私钥在但状态不是 ready（首装、或人工刚登记）：先自证，自证不过才登记
        assert_eq!(next_step(None, true, false, true, true), Step::Register);
        assert_eq!(
            next_step(Some("pending"), true, false, false, true),
            Step::AwaitHuman
        );
        // 贡献通道写入的私钥没有状态位：同样落到"待确认"，不会被当成已生效
        assert_eq!(next_step(None, true, false, false, true), Step::AwaitHuman);
    }

    #[test]
    fn a_fresh_machine_generates_first() {
        assert_eq!(
            next_step(None, false, false, true, true),
            Step::GenerateThenRegister
        );
        assert_eq!(
            next_step(None, false, false, false, true),
            Step::GenerateThenAwaitHuman
        );
        // 待确认键位上有密钥就不必再生成一对：重造会把人工刚粘好的公钥作废
        assert_eq!(
            next_step(Some("pending"), false, true, false, true),
            Step::AwaitHuman
        );
        assert_eq!(
            next_step(Some("pending"), false, true, true, true),
            Step::Register
        );
    }

    #[test]
    fn the_public_half_is_read_out_of_the_private_key() {
        let keypair = generate_ssh_keypair("cogneva-gateway");
        let derived = public_line_of(&keypair.private_pem).expect("OpenSSH v1 私钥自带公钥那一段");
        assert_eq!(key_blob(&derived), key_blob(&keypair.public_line));
        assert_eq!(fingerprint(&derived), fingerprint(&keypair.public_line));
        // 导出行不带注释（私钥里那半段只有算法名与 blob）——比对只看 blob，正是
        // 为了不让注释差别看起来像换了一把密钥
        assert!(derived.starts_with("ssh-ed25519 "));
        assert_eq!(derived.split_whitespace().count(), 2);

        // 读不出来就给 None，而不是编一行出来
        assert!(public_line_of("").is_none());
        assert!(public_line_of("not a key at all").is_none());
        // 别家格式（PEM）没有可读的公钥段
        assert!(
            public_line_of("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n")
                .is_none()
        );
        // 魔数对但后面截断：结构不完整同样不猜
        let truncated = base64::engine::general_purpose::STANDARD.encode(b"openssh-key-v1\0");
        assert!(public_line_of(&format!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n{truncated}\n-----END OPENSSH PRIVATE KEY-----\n"
        ))
        .is_none());
    }

    #[test]
    fn a_public_line_is_checked_against_the_key_that_must_use_it() {
        let keypair = generate_ssh_keypair("cogneva-gateway");
        let other = generate_ssh_keypair("cogneva-gateway");
        assert!(pair_matches(
            Some(&keypair.private_pem),
            Some(&keypair.public_line)
        ));
        // 注释不同不算换密钥
        let commented = format!(
            "ssh-ed25519 {} elsewhere",
            key_blob(&keypair.public_line).unwrap()
        );
        assert!(pair_matches(Some(&keypair.private_pem), Some(&commented)));
        assert!(!pair_matches(
            Some(&keypair.private_pem),
            Some(&other.public_line)
        ));
        // 读不出任一半是"没拿到证据"，不是反证：放行，否则一份能用的身份会被判坏
        assert!(pair_matches(None, Some(&other.public_line)));
        assert!(pair_matches(Some(&keypair.private_pem), None));
        assert!(pair_matches(Some("not a key"), Some(&other.public_line)));
    }

    #[test]
    fn registration_treats_already_present_as_success() {
        // 重跑自举会拿到 422（密钥已存在），把它当失败会让一次成功的状态被反复判坏
        assert!(registration_verdict(201).is_ok());
        assert!(registration_verdict(422).is_ok());
        let denied = registration_verdict(403).unwrap_err();
        assert!(denied.contains("admin"), "理由要指名缺哪种权限: {denied}");
        assert!(registration_verdict(401).unwrap_err().contains("token"));
        assert!(registration_verdict(404).unwrap_err().contains("仓库"));
        assert!(registration_verdict(500).is_err());
    }

    #[test]
    fn fingerprint_matches_ssh_keygen_shape() {
        // 已知向量（sha256 后 base64 去 padding），与 ssh-keygen -lf 的算法一致
        let line = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKq7Z9d9Zw1lGm0kQb1V8vX1Y0v7o1oK2m4kzZ3Q5w2R cogneva-gateway";
        assert_eq!(
            fingerprint(line).unwrap(),
            "SHA256:CBW+xw7Rqgj0DlIHUe6ktbnuLFG39NRuqAXbwUF5UdA"
        );
        // 注释不影响指纹（指纹只覆盖密钥 blob）
        let no_comment =
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKq7Z9d9Zw1lGm0kQb1V8vX1Y0v7o1oK2m4kzZ3Q5w2R";
        assert_eq!(fingerprint(line), fingerprint(no_comment));
        // 不是公钥行就该给 None，而不是编一个指纹出来
        assert!(fingerprint("").is_none());
        assert!(fingerprint("ssh-ed25519").is_none());
        assert!(fingerprint("ssh-ed25519 !!!not-base64!!! x").is_none());
    }

    #[test]
    fn the_status_block_separates_ready_from_pending() {
        let engine = base64::engine::general_purpose::STANDARD;
        let secret = |fields: Vec<(&str, &str)>| {
            let mut data = serde_json::Map::new();
            for (k, v) in fields {
                data.insert(k.into(), json!(engine.encode(v)));
            }
            json!({ "data": data })
        };

        // 私钥在但状态位没有：**未确认**，不是已生效——面板不能对着它说"配好了"
        let unconfirmed = status_block(&secret(vec![(SECRET_SSH_PRIVATE_KEY, "PRIVATE")]));
        assert_eq!(unconfirmed["state"], "pending");
        assert_eq!(unconfirmed["needs_token"], true);

        // 生效：公钥可外发、理由清空（上一次卡住的旧理由不该继续显示）
        let ready = status_block(&secret(vec![
            (SECRET_SSH_PRIVATE_KEY, "PRIVATE"),
            (SECRET_SSH_PUBLIC_KEY, "ssh-ed25519 AAAA x"),
            (SECRET_IDENTITY_STATE, STATE_READY),
            (SECRET_IDENTITY_NOTE, "旧理由"),
        ]));
        assert_eq!(ready["state"], STATE_READY);
        assert_eq!(ready["needs_token"], false);
        assert_eq!(ready["public_key"], "ssh-ed25519 AAAA x");
        assert!(ready["note"].is_null());

        // 等人输入令牌：理由要能读出来（面板照它说下一步该做什么）
        let waiting = status_block(&secret(vec![
            (SECRET_SSH_PENDING_KEY, "PRIVATE"),
            (SECRET_IDENTITY_STATE, "pending"),
            (
                SECRET_IDENTITY_NOTE,
                "SSH 握手未通过（公钥尚未登记到上游仓库）",
            ),
        ]));
        assert_eq!(waiting["state"], "pending");
        assert_eq!(waiting["needs_token"], true);
        assert!(waiting["note"].as_str().unwrap().contains("公钥尚未登记"));

        // 什么都没有：absent，而不是"没配好"；状态位单独存在也不算有身份——
        // 判据看的是**密钥在不在**（那才是自举能据以行动的事实）
        assert_eq!(status_block(&json!({}))["state"], "absent");
        assert_eq!(
            status_block(&secret(vec![(SECRET_IDENTITY_STATE, "pending")]))["state"],
            "absent"
        );
    }

    #[test]
    fn the_status_block_does_not_call_a_mismatched_pair_ready() {
        let engine = base64::engine::general_purpose::STANDARD;
        let live = generate_ssh_keypair("cogneva-gateway");
        let other = generate_ssh_keypair("cogneva-gateway");
        let secret = |fields: Vec<(&str, String)>| {
            let mut data = serde_json::Map::new();
            for (k, v) in fields {
                data.insert(k.into(), json!(engine.encode(v)));
            }
            json!({ "data": data })
        };

        let mismatched = status_block(&secret(vec![
            (SECRET_SSH_PRIVATE_KEY, live.private_pem.clone()),
            (SECRET_SSH_PUBLIC_KEY, other.public_line.clone()),
            (SECRET_IDENTITY_STATE, STATE_READY.to_string()),
        ]));
        assert_eq!(
            mismatched["state"], "pending",
            "状态位写的是 ready，但记下来的公钥不是手里这把密钥的——那就不是已生效"
        );
        assert_eq!(mismatched["note"], NOTE_KEY_PAIR_MISMATCH);
        // 摆给人和平台的公钥必须来自私钥：照着错的那半去登记，登记的还是错的
        assert_eq!(
            key_blob(mismatched["public_key"].as_str().unwrap()),
            key_blob(&live.public_line),
            "面板显示的必须是手里这把密钥的公钥"
        );
        assert_eq!(
            mismatched["fingerprint"],
            json!(fingerprint(&live.public_line).unwrap())
        );

        // 成对时照旧 ready、且不显示任何理由
        let consistent = status_block(&secret(vec![
            (SECRET_SSH_PRIVATE_KEY, live.private_pem.clone()),
            (SECRET_SSH_PUBLIC_KEY, live.public_line.clone()),
            (SECRET_IDENTITY_STATE, STATE_READY.to_string()),
            (SECRET_IDENTITY_NOTE, "旧理由".to_string()),
        ]));
        assert_eq!(consistent["state"], STATE_READY);
        assert!(consistent["note"].is_null());
    }

    #[test]
    fn facts_read_the_secret_fields() {
        let engine = base64::engine::general_purpose::STANDARD;
        let secret = json!({
            "data": {
                SECRET_SSH_PRIVATE_KEY: engine.encode("PRIVATE"),
                SECRET_SSH_PUBLIC_KEY: engine.encode("ssh-ed25519 AAAA x"),
                SECRET_IDENTITY_STATE: engine.encode(STATE_READY),
                SECRET_GITHUB_TOKEN: engine.encode(""),
            }
        });
        let facts = Facts::from_secret(&secret);
        assert_eq!(facts.state.as_deref(), Some(STATE_READY));
        assert_eq!(facts.live_key.as_deref(), Some("PRIVATE"));
        // 空字符串等于没有：写成空值的键不该被当成"有 token"
        assert!(facts.token.is_none());
        assert_eq!(facts.step(), Step::Settled);
    }
}
