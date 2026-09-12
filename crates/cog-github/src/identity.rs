//! 实例身份体系：机器指纹 + 人名池哈希分配 + `evol/<id>` 分支标识。
//!
//! 每个私版实例在首次进化时自动生成一个有辨识度的数字员工身份：
//! `Alice#a3f9d2c1` —— 人名按机器指纹从预设名册固定选取（同一台机器永远
//! 得到同一个名字），短指纹防冒充。身份持久化在私版配置里，用于：
//! - 实例进化分支 `evol/<branch_id>`（基线移植的工作分支）；
//! - 提交作者身份（git name/email）；
//! - PR 元数据标准块里的 bot 签名（贡献通道）。
//!
//! 本模块是纯确定性计算：指纹来自机器稳定标识的 SHA-256，人名来自指纹
//! 哈希取模，无随机、无网络。

use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tracing::warn;

/// 预设人名池。前四个是设计锚定名字（Alice/Ralph/Nova/Kai），其余为同风格
/// 短名扩充以降低撞名概率（撞名也由短指纹区分，不影响唯一性）。
pub const NAME_POOL: &[&str] = &[
    "Alice", "Ralph", "Nova", "Kai", "Vera", "Orion", "Sage", "Echo", "Iris", "Leo", "Mira",
    "Quinn", "Ada", "Felix", "Luna", "Max",
];

/// 一个私版实例的自治身份。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstanceIdentity {
    /// 人名池分配的名字，如 `Alice`。
    pub persona: String,
    /// 机器指纹完整值（SHA-256 hex，64 字符）。
    pub fingerprint: String,
    /// 指纹短码（前 8 字符 hex），展示与防冒充用。
    pub short: String,
    /// 完整展示句柄 `Alice#a3f9d2c1`。
    pub handle: String,
    /// 分支/文件系统安全的实例 id：`alice-a3f9d2c1`（evol 分支后缀）。
    pub branch_id: String,
    /// git 提交作者名。
    pub git_name: String,
    /// git 提交作者邮箱。
    pub git_email: String,
    /// 一句话自述（PR 资料/元数据用）。
    pub bio: String,
}

impl InstanceIdentity {
    /// 由机器指纹推导完整身份。纯函数：同一指纹永远得到同一身份。
    pub fn from_fingerprint(fingerprint: &str) -> Self {
        let digest = Sha256::digest(fingerprint.as_bytes());
        let selector = u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]));
        let persona = NAME_POOL[(selector as usize) % NAME_POOL.len()];

        let short = fingerprint.chars().take(8).collect::<String>();
        let handle = format!("{persona}#{short}");
        let branch_id = format!("{}-{}", persona.to_lowercase(), short);
        let git_name = handle.clone();
        let git_email = format!("{branch_id}@cogneva.ai");
        let bio = format!(
            "Autonomous Cogneva self-evolution instance ({handle}). \
             Changes proposed by this instance are verified in its own sandbox."
        );
        Self {
            persona: persona.into(),
            fingerprint: fingerprint.into(),
            short,
            handle,
            branch_id,
            git_name,
            git_email,
            bio,
        }
    }

    /// 采集本机稳定标识并生成身份。
    pub async fn generate() -> std::io::Result<Self> {
        Ok(Self::from_fingerprint(&machine_fingerprint().await?))
    }

    /// 把身份写回配置结构（调用方负责落盘持久化）。
    pub fn persist_to(&self, config: &mut crate::config::BotIdentityConfig) {
        config.persona = Some(self.persona.clone());
        config.fingerprint = Some(self.fingerprint.clone());
    }

    /// 从持久化配置恢复身份；配置里没有指纹时现场生成并回填。
    /// 人名始终由指纹规范重算，不信配置里的人名字段（防漂移/手改）。
    pub async fn load_or_generate(
        config: &mut crate::config::BotIdentityConfig,
    ) -> std::io::Result<Self> {
        match &config.fingerprint {
            Some(fp) if !fp.is_empty() => Ok(Self::from_fingerprint(fp)),
            _ => {
                let identity = Self::generate().await?;
                identity.persist_to(config);
                Ok(identity)
            }
        }
    }
}

/// 身份状态文件落点：`$COGNEVA_DATA_DIR/identity.json`，默认
/// `/var/lib/cogneva-data/identity.json`。
pub fn identity_state_path() -> PathBuf {
    let dir = std::env::var("COGNEVA_DATA_DIR").unwrap_or_else(|_| "/var/lib/cogneva-data".into());
    PathBuf::from(dir).join("identity.json")
}

/// 装入期指纹的环境变量名。集群里由 `cogneva-secrets` 的
/// `instance-fingerprint` 注入，安装时生成一次、之后幂等保留。
///
/// 为什么需要它：容器里采集不到 machine-id，指纹素材只剩 Pod 主机名与
/// veth MAC，两者都随 Pod 生命周期变化——身份于是只能靠"把结果写到持久卷"
/// 粘住，重装集群或换机器就换名。把指纹变成安装期提供的外部输入之后，身份
/// 不再依赖任何易失素材，可以随 Secret 备份、迁移，运维也能显式钉住。
pub const ENV_FINGERPRINT: &str = "COGNEVA_INSTANCE_FINGERPRINT";

/// 读取装入期指纹；未设置或全空白视为未提供。
pub fn fingerprint_from_env() -> Option<String> {
    std::env::var(ENV_FINGERPRINT)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 把身份写进状态文件，供其他 crate（如按 `evol/<id>` 命名的沙盒侧）读取。
/// 写失败只告警：身份本身仍然有效，只是跨进程共享的那份副本缺失。
async fn persist_state_file(identity: &InstanceIdentity) {
    let path = identity_state_path();
    let Ok(json) = serde_json::to_string_pretty(identity) else {
        return;
    };
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            warn!(path = %path.display(), error = %e, "identity state dir not writable");
            return;
        }
    }
    if let Err(e) = tokio::fs::write(&path, json).await {
        warn!(
            path = %path.display(),
            error = %e,
            "identity state file not writable; identity stays process-local"
        );
    }
}

/// 进程启动早期把装入期指纹落地为身份状态文件。
///
/// 必须在插件初始化之前调用：`evol/<id>` 分支名与实例工作树名由 cog-reflection
/// 直接读状态文件得出，而身份解析发生在 cog-github 插件里，两者启动顺序并无
/// 保证——冷启动时先读的一方会读到空文件，把分支名退化成 `local`。启动早期
/// 统一落一次盘，所有消费者看到的永远是同一份身份。
///
/// 未提供装入期指纹时返回 `None`，交由 [`resolve`] 走既有的状态文件/机器指纹
/// 路径（未接入 Secret 的单机场景行为不变）。
pub async fn seed_from_env() -> Option<InstanceIdentity> {
    let fingerprint = fingerprint_from_env()?;
    let identity = InstanceIdentity::from_fingerprint(&fingerprint);

    // 与既有状态文件不一致意味着这次启动会给实例换名：按实例命名的分支与
    // 工作树会变成孤儿。是运维显式提供新指纹的结果，但必须留下声音。
    if let Ok(text) = tokio::fs::read_to_string(identity_state_path()).await {
        if let Ok(previous) = serde_json::from_str::<InstanceIdentity>(&text) {
            if previous.fingerprint != identity.fingerprint {
                warn!(
                    previous = %previous.handle,
                    current = %identity.handle,
                    "provisioned fingerprint differs from the persisted identity; \
                     this instance is being renamed"
                );
            }
        }
    }

    persist_state_file(&identity).await;
    Some(identity)
}

/// 实例身份解析的统一入口（首次进化时自动生成）。
///
/// 优先级：内存配置指纹 → 装入期指纹（Secret 注入）→ 状态文件 → 现场采集
/// 机器指纹生成（并尽力回写状态文件与回填配置）。装入期指纹优先于状态文件：
/// 它是安装期提供的外部输入，重装带同一个 Secret、换机器带同一个 Secret 都能
/// 得到同一个身份，这正是"身份可迁移"的前提。机器标识全部缺失的极端环境退化
/// 为进程内临时身份（重启可能换名，仅保底不阻塞启动）。
pub async fn resolve(config: &mut crate::config::BotIdentityConfig) -> InstanceIdentity {
    if let Some(fp) = config.fingerprint.as_ref() {
        if !fp.is_empty() {
            return InstanceIdentity::from_fingerprint(fp);
        }
    }
    if let Some(fingerprint) = fingerprint_from_env() {
        let identity = InstanceIdentity::from_fingerprint(&fingerprint);
        identity.persist_to(config);
        persist_state_file(&identity).await;
        return identity;
    }
    let path = identity_state_path();
    if let Ok(text) = tokio::fs::read_to_string(&path).await {
        if let Ok(identity) = serde_json::from_str::<InstanceIdentity>(&text) {
            identity.persist_to(config);
            return identity;
        }
    }
    match InstanceIdentity::generate().await {
        Ok(identity) => {
            identity.persist_to(config);
            persist_state_file(&identity).await;
            identity
        }
        Err(e) => {
            warn!(
                error = %e,
                "machine fingerprint unavailable; using an ephemeral instance identity \
                 (may change after restart)"
            );
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let material = format!("ephemeral:{}:{}", std::process::id(), now);
            let digest = Sha256::digest(material.as_bytes());
            let identity = InstanceIdentity::from_fingerprint(&hex::encode(digest));
            identity.persist_to(config);
            identity
        }
    }
}

/// 采集机器稳定标识并哈希成指纹。
///
/// 来源（存在即采，逐项标注后哈希）：systemd machine-id、dbus machine-id、
/// 主机名、物理网卡 MAC。容器里 machine-id 通常随实例生命周期稳定；全都
/// 缺失才报错（裸机/K3s 节点至少有 machine-id 或主机名）。
pub async fn machine_fingerprint() -> std::io::Result<String> {
    let mut material = String::new();

    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            let value = content.trim();
            if !value.is_empty() {
                material.push_str(&format!("machine-id:{value}\n"));
            }
        }
    }

    if let Ok(hostname) = tokio::fs::read_to_string("/proc/sys/kernel/hostname").await {
        let value = hostname.trim();
        if !value.is_empty() {
            material.push_str(&format!("hostname:{value}\n"));
        }
    }

    // 网卡 MAC 兜底（排序保证确定性，跳过 lo 与空地址）。
    if let Ok(mut entries) = tokio::fs::read_dir("/sys/class/net").await {
        let mut ifaces: Vec<String> = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            ifaces.push(entry.file_name().to_string_lossy().into_owned());
        }
        ifaces.sort();
        for iface in ifaces {
            if iface == "lo" {
                continue;
            }
            let path = format!("/sys/class/net/{iface}/address");
            if let Ok(addr) = tokio::fs::read_to_string(&path).await {
                let addr = addr.trim();
                if !addr.is_empty() && addr != "00:00:00:00:00:00" {
                    material.push_str(&format!("mac:{iface}={addr}\n"));
                }
            }
        }
    }

    if material.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no stable machine identifiers found (machine-id, hostname, NIC addresses all absent)",
        ));
    }

    let digest = Sha256::digest(material.as_bytes());
    Ok(hex::encode(digest))
}

/// `COGNEVA_DATA_DIR` 是进程级环境变量，用串行锁保护所有依赖它的测试
/// （identity 与 cross_validation 的状态文件测试共用）。
#[cfg(test)]
pub(crate) static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_deterministic_for_same_fingerprint() {
        let a = InstanceIdentity::from_fingerprint("a3f9d2c1a3f9d2c1");
        let b = InstanceIdentity::from_fingerprint("a3f9d2c1a3f9d2c1");
        assert_eq!(a, b);
        assert_eq!(a.handle, b.handle);
    }

    #[test]
    fn different_fingerprints_yield_different_identities() {
        // 真实指纹是机器材料的 SHA-256 hex（64 字符）。
        let a = InstanceIdentity::from_fingerprint(
            "a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1",
        );
        let b = InstanceIdentity::from_fingerprint(
            "b81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9f",
        );
        assert_ne!(a.fingerprint, b.fingerprint);
        assert_eq!(a.short, "a3f9d2c1");
        assert_ne!(a.short, b.short);
        assert_ne!(a.branch_id, b.branch_id);
    }

    #[test]
    fn persona_comes_from_pool() {
        for seed in 0..64u32 {
            let id = InstanceIdentity::from_fingerprint(&format!("fp-{seed}"));
            assert!(
                NAME_POOL.contains(&id.persona.as_str()),
                "persona {} not in pool",
                id.persona
            );
        }
    }

    #[test]
    fn handle_and_branch_follow_format() {
        let id = InstanceIdentity::from_fingerprint("0123456789abcdef0123456789abcdef");
        // 句柄：人名 + # + 8 位短码。
        assert!(id.handle.starts_with(&id.persona));
        let short = id.handle.split('#').nth(1).unwrap();
        assert_eq!(short.len(), 8);
        assert!(short.chars().all(|c| c.is_ascii_hexdigit()));
        // 分支 id：全小写、只含 [a-z0-9-]，无 # / 点号（git ref 安全）。
        assert!(id
            .branch_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert!(!id.branch_id.contains('#'));
        assert!(id.branch_id.starts_with(&id.persona.to_lowercase()));
        assert_eq!(id.git_email, format!("{}@cogneva.ai", id.branch_id));
        assert!(id.bio.contains(&id.handle));
    }

    #[test]
    fn branch_id_is_usable_as_git_ref_suffix() {
        let id = InstanceIdentity::from_fingerprint("deadbeefcafebabe");
        let branch = format!("evol/{}", id.branch_id);
        // git check-ref-format 的核心禁则。
        for bad in [' ', '~', '^', ':', '?', '*', '[', '\\', '.', '#'] {
            assert!(!branch.contains(bad), "branch {branch} contains {bad}");
        }
        assert!(!branch.contains(".."));
        assert!(!branch.ends_with('/'));
    }

    #[tokio::test]
    async fn load_or_generate_fills_config_then_round_trips() {
        let mut config = crate::config::BotIdentityConfig::default();
        assert!(config.fingerprint.is_none());

        let first = InstanceIdentity::load_or_generate(&mut config)
            .await
            .unwrap();
        assert_eq!(
            config.fingerprint.as_deref(),
            Some(first.fingerprint.as_str())
        );
        assert_eq!(config.persona.as_deref(), Some(first.persona.as_str()));

        // 第二次：从持久化指纹恢复，身份完全一致（不重新生成）。
        let second = InstanceIdentity::load_or_generate(&mut config)
            .await
            .unwrap();
        assert_eq!(first, second);

        // 人手改了人名也不影响：人名由指纹规范重算。
        config.persona = Some("Tampered".into());
        let third = InstanceIdentity::load_or_generate(&mut config)
            .await
            .unwrap();
        assert_eq!(third.persona, first.persona);
    }

    #[tokio::test]
    async fn resolve_persists_state_file_and_reloads() {
        let _guard = ENV_LOCK.lock().await;
        let data_dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", data_dir.path());

        let mut config = crate::config::BotIdentityConfig::default();
        let first = resolve(&mut config).await;
        assert!(!config.fingerprint.as_ref().unwrap().is_empty());

        // 状态文件已写出。
        let state = identity_state_path();
        assert!(state.exists());

        // 新配置从状态文件恢复，身份一致。
        let mut fresh = crate::config::BotIdentityConfig::default();
        let second = resolve(&mut fresh).await;
        assert_eq!(first, second);

        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[tokio::test]
    async fn resolve_prefers_explicit_config_fingerprint() {
        let mut config = crate::config::BotIdentityConfig {
            fingerprint: Some(
                "a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1".into(),
            ),
            ..Default::default()
        };
        let id = resolve(&mut config).await;
        assert_eq!(id.short, "a3f9d2c1");
    }

    /// 装入期指纹是身份的规范来源：没有状态文件时，它必须先于机器指纹生效，
    /// 并把结果落盘给别的 crate 读。
    #[tokio::test]
    async fn resolve_prefers_provisioned_env_fingerprint() {
        let _guard = ENV_LOCK.lock().await;
        let data_dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", data_dir.path());
        let provisioned = "b81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9f";
        std::env::set_var(ENV_FINGERPRINT, provisioned);

        let mut config = crate::config::BotIdentityConfig::default();
        let id = resolve(&mut config).await;

        assert_eq!(id.fingerprint, provisioned);
        assert_eq!(id.short, "b81c0e9f");
        assert_eq!(config.fingerprint.as_deref(), Some(provisioned));
        let persisted: InstanceIdentity =
            serde_json::from_str(&std::fs::read_to_string(identity_state_path()).unwrap()).unwrap();
        assert_eq!(persisted, id);

        std::env::remove_var(ENV_FINGERPRINT);
        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    /// 启动早期播种：这是插件初始化之前唯一一次落盘，目的是让读状态文件的
    /// cog-reflection 与解析身份的 cog-github 看到同一份身份。
    #[tokio::test]
    async fn seed_from_env_writes_state_file() {
        let _guard = ENV_LOCK.lock().await;
        let data_dir = tempfile::tempdir().unwrap();
        std::env::set_var("COGNEVA_DATA_DIR", data_dir.path());

        std::env::remove_var(ENV_FINGERPRINT);
        assert!(
            seed_from_env().await.is_none(),
            "未提供装入期指纹时不得凭空造身份"
        );
        assert!(!identity_state_path().exists());

        let provisioned = "deadbeefcafebabedeadbeefcafebabedeadbeefcafebabedeadbeefcafebabe";
        std::env::set_var(ENV_FINGERPRINT, provisioned);
        let seeded = seed_from_env().await.expect("应当播种身份");
        assert_eq!(seeded.fingerprint, provisioned);

        let written: InstanceIdentity =
            serde_json::from_str(&std::fs::read_to_string(identity_state_path()).unwrap()).unwrap();
        assert_eq!(written, seeded);

        // 换一个指纹再播种：状态文件必须被改写（装入期指纹优先于陈旧文件）。
        let rotated = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        std::env::set_var(ENV_FINGERPRINT, rotated);
        let reseeded = seed_from_env().await.expect("应当播种身份");
        assert_ne!(reseeded.handle, seeded.handle);
        let written: InstanceIdentity =
            serde_json::from_str(&std::fs::read_to_string(identity_state_path()).unwrap()).unwrap();
        assert_eq!(written.fingerprint, rotated);

        std::env::remove_var(ENV_FINGERPRINT);
        std::env::remove_var("COGNEVA_DATA_DIR");
    }

    #[test]
    fn config_author_accessors_fall_back_to_static_identity() {
        let config = crate::config::BotIdentityConfig::default();
        assert_eq!(config.git_author_name(), "Cogneva Bot");
        assert_eq!(config.git_author_email(), "bot@cogneva.ai");
        assert!(config.instance().is_none());

        let with_persona = crate::config::BotIdentityConfig {
            fingerprint: Some(
                "a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1".into(),
            ),
            ..Default::default()
        };
        let instance = with_persona.instance().unwrap();
        assert_eq!(with_persona.git_author_name(), instance.git_name);
        assert!(with_persona.git_author_name().contains('#'));
        assert_eq!(with_persona.git_author_email(), instance.git_email);
    }

    #[tokio::test]
    async fn machine_fingerprint_is_stable_hex_on_this_host() {
        let fp = machine_fingerprint().await.unwrap();
        assert_eq!(fp.len(), 64);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
        // 同机二次采集一致。
        let fp2 = machine_fingerprint().await.unwrap();
        assert_eq!(fp, fp2);
    }
}
