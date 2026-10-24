//! 数字员工的名字：人名 ＋ 机器指纹。
//!
//! 注册表里的 `agent_id` 是这个名字，不是 Pod 名。理由不是好看：Pod 名每一
//! 次滚动都换，凡是按 `agent_id` 记下来的东西——谁在干这个任务、谁提交了这
//! 一条变更、哪个单元占着这个面——都会跟着换成一个新单元。读的人看到的是
//! "上一批员工全走了、又来了一批没见过的"，而实际上只是同一个槽位换了个 Pod。
//! 名字的输入里只有安装期指纹（跨重装可携带）、进程分组与槽位号，这三样都不
//! 随滚动变化，所以同一个槽位滚回来还是同一个名字。
//!
//! 指纹取短码（8 位 hex）当尾巴，是为了让两个同名的实例仍然可以分辨：人名池
//! 只有十几个名字，撞名是常态，尾巴把"重名"和"同名"分开。

use sha2::{Digest, Sha256};

/// 人名池只有一份，在 `cog-core`：注册表里两种员工（常驻槽位与按任务起的
/// 作战小队）必须从同一份名册取名，否则同一个人在两处读成两个名字。
pub use cog_core::NAME_POOL;

/// 安装期指纹的环境变量名。由 Secret 注入，跨重装可携带；缺失时退回进程自身
/// 的装配标识（Pod 名 / 主机名），那时名字只能稳定到下一次滚动。
pub const ENV_FINGERPRINT: &str = "COGNEVA_INSTANCE_FINGERPRINT";

/// 进程分组名。主应用与进化两个进程在同一台机器上各跑一个 worker 池，槽位号
/// 相同；分组名进名字的哈希材料，两个池才不会同名。分组名是清单里的字面量，
/// 所以它稳定。
pub const ENV_GROUP: &str = "COGNEVA_AGENT_GROUP";

/// 名字的兜底分组：清单没给分组名时用进程自身的装配标识，保证唯一但会随
/// 滚动变化（`stable` 随之为 false）。
const FALLBACK_GROUP: &str = "local";

/// 一个数字员工的名字。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentName {
    /// 人名，如 `Luna`。
    pub persona: String,
    /// 指纹短码，8 位 hex。
    pub short: String,
    /// 展示句柄 `Luna#1822ab0e`。
    pub handle: String,
    /// 注册表与消息面用的 id `luna-1822ab0e`：全小写、只含 `[a-z0-9-]`，
    /// 可以直接当 Redis stream 的 key 与消费者组名。
    pub id: String,
    /// 名字的来源是否跨重启稳定：只有指纹存在时为 true。README 意义上的
    /// "同一个员工"，判据就是这一位。
    pub stable: bool,
}

impl AgentName {
    /// 这个员工在 git 作者 / 文档署名里用的邮箱。
    pub fn email(&self) -> String {
        format!("{}@cogneva.ai", self.id)
    }
}

/// 由“机器指纹 ＋ 分组 ＋ 槽位”推出名字。纯函数：同样的三项永远得到同样的
/// 名字。
///
/// 三项都进哈希，是因为少了任何一项都会撞：只按槽位分，两个池的 0 号同名；
/// 只按指纹分，同一台机器上的 N 个员工同名；只按分组分，同一分组里每个槽位
/// 同名。
pub fn name_for(seed: &str, group: &str, slot: usize) -> AgentName {
    let material = format!("agent\nslot:{slot}\ngroup:{group}\nseed:{seed}\n");
    let digest = Sha256::digest(material.as_bytes());

    // 人名取头 8 字节，短码取接下来 4 字节：两段不重叠，名字与短码不互相泄漏。
    let selector = u64::from_be_bytes(digest[0..8].try_into().unwrap_or([0; 8]));
    let persona = NAME_POOL[(selector as usize) % NAME_POOL.len()];
    let short = hex::encode(&digest[8..12]);

    AgentName {
        persona: persona.to_string(),
        handle: format!("{persona}#{short}"),
        id: format!("{}-{}", persona.to_lowercase(), short),
        short,
        stable: true,
    }
}

/// 进程跑在哪个分组里。
///
/// 清单里给了 `COGNEVA_AGENT_GROUP` 就用它——那是稳定的字面量。没给时退回
/// 进程自身的装配标识（Pod 名 / 主机名）：唯一性保住了，稳定性没有了，调用方
/// 据此把名字标成不稳定，日志里要说得出来。
pub fn group_name() -> String {
    std::env::var(ENV_GROUP)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(agent_instance)
}

/// 池里第 `slot` 号员工的名字。
///
/// 指纹在就按指纹算，那是跨重装可携带的规范来源；不在才退回进程标识，并把
/// `stable` 置 false——这一位是"滚动一次会不会换名"的判据，不能糊。
pub fn worker_name(slot: usize) -> AgentName {
    let fingerprint = std::env::var(ENV_FINGERPRINT)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let group = group_name();
    match fingerprint {
        Some(fp) => name_for(&fp, &group, slot),
        None => {
            let material = format!("ephemeral:{}", agent_instance());
            let mut name = name_for(&material, &group, slot);
            name.stable = false;
            name
        }
    }
}

/// 本进程的装配标识：Pod 名优先，其次主机名。它描述"在哪一个进程里"，不再
/// 参与名字的稳定性——把 Pod 名写进名字正是滚动就换名的根源。
pub fn agent_instance() -> String {
    std::env::var("COGNEVA_POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| FALLBACK_GROUP.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 环境变量是进程级的，改它的用例串行跑。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        std::env::remove_var(ENV_FINGERPRINT);
        std::env::remove_var(ENV_GROUP);
        std::env::remove_var("COGNEVA_POD_NAME");
    }

    /// 滚动一次换一个 Pod 名，这是常态。名字必须还认得出是同一个人，否则按
    /// agent_id 记下的任务归属、变更作者、面主都会跟着换人。
    #[test]
    fn a_pod_roll_does_not_rename_the_worker() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        std::env::set_var(
            ENV_FINGERPRINT,
            "a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1a3f9d2c1",
        );
        std::env::set_var(ENV_GROUP, "main");

        std::env::set_var("COGNEVA_POD_NAME", "cogneva-worker-aaa-0");
        let before = worker_name(1);
        // 滚动：同一个槽位落到另一个 Pod 名下。
        std::env::set_var("COGNEVA_POD_NAME", "cogneva-worker-bbb-0");
        let after = worker_name(1);

        assert_eq!(before, after);
        assert!(before.stable, "有指纹的名字必须自称稳定");
        clear_env();
    }

    /// 主应用与进化两个进程同机同槽位：少了分组就会同名，一个注册覆盖另一个，
    /// 消息被谁读到取决于谁先轮询。
    #[test]
    fn two_pools_on_one_host_do_not_share_a_name() {
        let fingerprint = "b81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9fb81c0e9f";
        let main = name_for(fingerprint, "main", 0);
        let evolution = name_for(fingerprint, "evolution", 0);
        assert_ne!(main.id, evolution.id);
        assert_ne!(main.short, evolution.short);
    }

    #[test]
    fn every_slot_gets_its_own_name() {
        let fingerprint = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        let ids: Vec<String> = (0..8)
            .map(|slot| name_for(fingerprint, "main", slot).id)
            .collect();
        let mut unique = ids.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), ids.len(), "slots collided: {ids:?}");
    }

    /// 换一台机器（或重装后没带上指纹）就是换一批员工：指纹是唯一性的来源。
    #[test]
    fn a_different_machine_gets_different_names() {
        let a = name_for("0123456789abcdef0123456789abcdef", "main", 0);
        let b = name_for("fedcba9876543210fedcba9876543210", "main", 0);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn the_name_is_safe_as_a_stream_key_and_a_git_author() {
        for slot in 0..32 {
            let name = name_for("a3f9d2c1a3f9d2c1", "evolution", slot);
            assert!(NAME_POOL.contains(&name.persona.as_str()));
            assert_eq!(name.short.len(), 8);
            assert!(name.short.chars().all(|c| c.is_ascii_hexdigit()));
            assert_eq!(name.handle, format!("{}#{}", name.persona, name.short));
            assert!(
                name.id
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                "id {} is not stream-key safe",
                name.id
            );
            assert_eq!(name.email(), format!("{}@cogneva.ai", name.id));
        }
    }

    /// 没有指纹时名字仍然唯一（靠进程标识），但必须自称不稳定——把这一位说成
    /// 稳定，运维会以为滚动不换人。
    #[test]
    fn without_a_fingerprint_the_name_is_unique_but_not_stable() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        std::env::set_var("COGNEVA_POD_NAME", "cogneva-worker-test-0");

        let name = worker_name(0);
        assert!(!name.stable, "没有指纹就不能自称跨重启稳定");
        assert!(!name.id.is_empty());

        // 唯一性还留着：另一个 Pod 拿到的不是同一个名字。
        std::env::set_var("COGNEVA_POD_NAME", "cogneva-worker-test-1");
        assert_ne!(worker_name(0).id, name.id);
        clear_env();
    }
}
