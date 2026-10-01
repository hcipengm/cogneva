//! 启动失败的死因要留在一个活得比这个进程久的面上。
//!
//! 主应用从进程起来到插件表建好，失败只走 stdout：配置装配、密钥解析、插件初始化。
//! 而 stdout 随 Pod 一起消失——回滚时这个 Pod 被删，`/var/log/pods` 下那一份跟着没了；
//! 写 PVC 的 raw logger 要等存储插件初始化成功才在插件表里，恰好覆盖不到「插件初始化
//! 失败」这一类，而 redis 连不上、schema 建不出来正落在这一类。于是事后回看那一轮滚动，
//! 只剩「进程非零退出」，死因不可知。
//!
//! 这里把死因写进**容器终止消息文件**：kubelet 在容器退出时读它，放进 Pod 状态里的
//! `state.terminated.message` / `lastState.terminated.message`；而滚动端在**两条判死
//! 出口**上都读这段——等待态判死（崩溃的容器停在这里）与"已收敛"后的 Pod 采样——
//! 于是这条读数能一路走进滚动记录。载体因此活得比这个 Pod 久，不需要任何新读者。
//! （两条出口分别在读，是因为只读 reason 的那条覆盖不了未就绪的容器，而容器崩着的
//! 时候正是如此。）
//!
//! **覆盖边界（别把「消息为空」读成「失败没有原因」）**：只有进程带着 `Err` 退出时才
//! 走这条路径。panic、OOMKilled 之类根本不执行到这里，那几种死法的消息必然是空的
//! ——它们由退出码与重启计数分辨，不归这里。
//!
//! 写不进去不影响失败本身的交付：退出码与那份 stdout 都还在，少的只是一份证据。

use std::io::Write;
use std::path::Path;

/// 容器终止消息文件。必须与 kubelet 的 `terminationMessagePath` 默认值一致——
/// 换一个路径而清单里不同步声明，写进去的内容没人读。
pub const TERMINATION_LOG: &str = "/dev/termination-log";

/// 终止消息有大小上限，超了由 kubelet 截断且不说明截在哪里。这里先自己截，
/// 并且把「截过」写出来：看起来完整的读数比明说被截过更坏，人会照着半句话下结论。
const MESSAGE_CHARS: usize = 1500;

/// 记下这一轮启动为什么失败。任何内部错误都被吞掉——这条路径本身不能成为
/// 新的失败点，失败时再失败一次会把真原因顶掉。
pub fn record_failure(err: &dyn std::error::Error) {
    if let Err(e) = record_failure_to(Path::new(TERMINATION_LOG), err) {
        tracing::warn!(
            path = TERMINATION_LOG,
            error = %e,
            "could not record why startup failed; the rollout will see this exit without its cause"
        );
    }
}

fn record_failure_to(path: &Path, err: &dyn std::error::Error) -> std::io::Result<()> {
    let message = format!("cogneva startup failed: {}", chain_text(err));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bounded(&one_line(&message)).as_bytes())?;
    file.flush()
}

/// 错误的因果链，从最外一层到根因。
///
/// 只写最外层是不够的：装配层包出来的那句往往只说「哪个插件初始化失败」，
/// 而「连不上 redis」「端口被占」这类可处置的原因在 `source()` 里。
fn chain_text(err: &dyn std::error::Error) -> String {
    let mut parts = vec![err.to_string()];
    let mut cursor = err.source();
    while let Some(next) = cursor {
        let text = next.to_string();
        // 有些实现的 Display 就是把 source 原样转发，重复一遍只是噪声。
        if parts.last().map(|p| p != &text).unwrap_or(true) {
            parts.push(text);
        }
        cursor = next.source();
    }
    parts.join(" -> ")
}

/// 终止消息是一行一个字段的读数面，换行会把一条记录劈成两条。
fn one_line(text: &str) -> String {
    text.replace(['\n', '\r'], " ")
}

fn bounded(text: &str) -> String {
    if text.chars().count() <= MESSAGE_CHARS {
        return text.to_string();
    }
    let head: String = text.chars().take(MESSAGE_CHARS).collect();
    format!("{head}... (truncated)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Leaf;
    impl std::fmt::Display for Leaf {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "connection refused")
        }
    }
    impl std::error::Error for Leaf {}

    #[derive(Debug)]
    struct Wrapper(Leaf);
    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "storage plugin init failed")
        }
    }
    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    #[test]
    fn the_default_path_is_the_one_the_kubelet_reads() {
        // 换掉这个常量而不同步改清单里的 `terminationMessagePath`，写进去的内容
        // 就没人读了。
        assert_eq!(TERMINATION_LOG, "/dev/termination-log");
    }

    #[test]
    fn a_failure_carries_its_root_cause_not_just_the_outermost_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-log");
        record_failure_to(&path, &Wrapper(Leaf)).unwrap();
        let text = read(&path);
        assert!(text.contains("storage plugin init failed"), "{text}");
        assert!(text.contains("connection refused"), "{text}");
    }

    #[test]
    fn a_repeated_frame_is_not_written_twice() {
        // 有些实现的 Display 就是把 source 原样转发；同一句话写两遍读起来像两个原因。
        #[derive(Debug)]
        struct Echo(Leaf);
        impl std::fmt::Display for Echo {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl std::error::Error for Echo {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        assert_eq!(chain_text(&Echo(Leaf)), "connection refused");
    }

    #[test]
    fn a_multiline_failure_stays_one_line() {
        // 终止消息按行读；一个带换行的错误会把一条记录劈成两条，读的人以为
        // 后面那半是另一条记录。
        assert_eq!(one_line("a\nb\r\nc"), "a b  c");
    }

    #[test]
    fn an_overlong_failure_says_it_was_cut_instead_of_looking_complete() {
        #[derive(Debug)]
        struct Verbose(String);
        impl std::fmt::Display for Verbose {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl std::error::Error for Verbose {}

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-log");
        let long = Verbose("x".repeat(MESSAGE_CHARS + 40));
        record_failure_to(&path, &long).unwrap();
        let text = read(&path);
        assert!(text.ends_with("... (truncated)"), "{}", text.len());
        assert!(
            text.chars().count() < MESSAGE_CHARS + 40,
            "{}",
            text.chars().count()
        );
    }

    #[test]
    fn a_second_failure_replaces_the_first() {
        // 终止消息说的是「这一次为什么死」。留着上一条会让读者把旧病因安到新
        // 一次重启上，而那两次死法可能完全不同。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-log");
        record_failure_to(&path, &Wrapper(Leaf)).unwrap();
        record_failure_to(&path, &VerboseError).unwrap();
        let text = read(&path);
        assert!(!text.contains("storage plugin init failed"), "{text}");
        assert!(text.contains("port already in use"), "{text}");
    }

    #[derive(Debug)]
    struct VerboseError;
    impl std::fmt::Display for VerboseError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "port already in use")
        }
    }
    impl std::error::Error for VerboseError {}

    #[test]
    fn a_backend_that_cannot_be_written_reports_the_write_failure() {
        // 载体不可用时唯一允许的行为是什么都不做，而「什么都不做」的前提是这一层
        // 真的把错误交了出来——`record_failure` 把它降级成一条 warn（同一个文件里
        // 读得到）。并入失败路径的话，调用方就会用「写不进终止消息」盖掉真正的死因。
        // 这条断言故意不碰真的 `/dev/termination-log`：那是运行期进程的载体。
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-such-dir").join("termination-log");
        assert!(record_failure_to(&path, &Wrapper(Leaf)).is_err());
    }
}
