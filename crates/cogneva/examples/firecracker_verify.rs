//! Firecracker 真机端到端验证工具（审计 L2 缺口收口）。
//!
//! 在带 /dev/kvm 的主机上跑完整「preflight → 冷启动（COW rootfs）→
//! 执行 → 阅后即焚」循环，逐项输出 PASS/FAIL 清单（人读 + JSON 一行）。
//! 通常由 deploy/scripts/verify-firecracker.sh 调用（它负责镜像准备与
//! PV 内容核验）；镜像齐备时也可独立运行：
//!
//! ```sh
//! cargo run -p cogneva --example firecracker_verify -- \
//!     --firecracker-bin /opt/cogneva/microvm/bin/firecracker \
//!     --kernel /opt/cogneva/microvm/vmlinux \
//!     --rootfs /opt/cogneva/microvm/rootfs.ext4 \
//!     --pv /opt/cogneva/microvm/evolution-pv.ext4
//! ```
//!
//! 退出码：0 = 全部通过；1 = 任一失败。

use cog_core::MicroVmConfig;
use cog_reflection::FirecrackerSandbox;

fn arg_value(args: &[String], flag: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn report(results: &[(&str, bool, String)]) {
    println!("\n== Firecracker 真机验证清单 ==");
    for (name, ok, detail) in results {
        let mark = if *ok { "PASS" } else { "FAIL" };
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!("  ({detail})")
        };
        println!("  [{mark}] {name}{suffix}");
    }
    let json: Vec<serde_json::Value> = results
        .iter()
        .map(|(name, ok, detail)| serde_json::json!({"check": name, "pass": ok, "detail": detail}))
        .collect();
    println!("{}", serde_json::json!({"checks": json}));
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cfg = MicroVmConfig {
        enabled: true,
        firecracker_bin: arg_value(&args, "--firecracker-bin", "firecracker"),
        kernel_image: arg_value(&args, "--kernel", "/opt/cogneva/microvm/vmlinux"),
        rootfs_image: arg_value(&args, "--rootfs", "/opt/cogneva/microvm/rootfs.ext4"),
        pv_image: arg_value(&args, "--pv", "/opt/cogneva/microvm/evolution-pv.ext4"),
        instance_root: arg_value(&args, "--instance-root", "/tmp/cogneva-microvm"),
        exec_timeout_secs: arg_value(&args, "--timeout", "300").parse().unwrap_or(300),
        ..Default::default()
    };

    let mut results: Vec<(&str, bool, String)> = Vec::new();
    let sandbox = FirecrackerSandbox::new(cfg.clone());

    // 1. 宿主机预检：/dev/kvm + 三镜像齐备。
    if let Err(e) = sandbox.preflight() {
        results.push(("preflight", false, e.to_string()));
        report(&results);
        std::process::exit(1);
    }
    results.push(("preflight", true, String::new()));

    // 2. 完整循环：冷启动 → guest init 执行 → 关机或超时 → 阅后即焚。
    //    guest init（rootfs 内 /evolution/init）应写 PV 标记并 poweroff；
    //    completed=false 表示超时被强杀（guest 卡住也算失败）。
    match sandbox.run_evolution().await {
        Ok(outcome) => {
            let detail = format!(
                "vm={} duration={}s completed={}",
                outcome.vm_id, outcome.duration_secs, outcome.completed
            );
            results.push(("cold_start_exec_destroy", outcome.completed, detail));
        }
        Err(e) => {
            results.push(("cold_start_exec_destroy", false, e.to_string()));
            report(&results);
            std::process::exit(1);
        }
    }

    // 3. 阅后即焚核验：instance_root 下不得有残留实例目录（rootfs 副本、
    //    API socket 都在实例目录内，随目录删除；PV 镜像在其外自然保留）。
    let leftover = match std::fs::read_dir(&cfg.instance_root) {
        Ok(mut it) => it.next().is_some(),
        Err(_) => false, // 目录本身不存在同样算干净
    };
    results.push((
        "instance_cleanup",
        !leftover,
        if leftover {
            format!("{} 下有残留实例目录", cfg.instance_root)
        } else {
            String::new()
        },
    ));

    let all_pass = results.iter().all(|r| r.1);
    report(&results);
    std::process::exit(if all_pass { 0 } else { 1 });
}
