//! 集群**现役**的是哪个 rev：四个主负载是否全部落在同一个不可变 `main-<rev>` tag 上。
//! principle: feedback_an_answer_that_does_not_name_the_object_asked_for_is_a_read_failure
//!
//! 这一格读数有两个读者：金丝雀 overlay 的基底（直接调本模块的读法），与「落地≠生效」
//! 那一格（经 [`cog_core::LiveRevisionSource`] 服务面，由 [`KubectlLiveRevision`] 接上）。
//! 两处问的是同一个事实，所以**名字、jsonpath、解析**各只此一份：多一个读者不该多一份
//! 名单——两份名单会各自漂，而漂掉的那份不会报错，它只会让某个读者少认一个名字。
//!
//! 与 `mainline_deployer` 里那处按名读 deployment 的 image **不是**同一个问题：那里问
//! 「某一个具名容器的 image 是什么」（判一条变更要不要重打），这里问「四个主负载的**全部**
//! 容器是否落在同一个 `main-<rev>` 上」。两处共用 kubectl 的读法、不共用判据，别把它们的
//! 实现并到一起：判据一并，就会有人拿其中一个的结论去回答另一个问题。
//!
//! 逐名读、不列举：集合级 list 在 `resourceNames` 授权下必被拒（RBAC 要求 list 带对应的
//! field selector），而按名 get 只要那个名字上的授权。任何一个名字读不出来都算「读不出来」，
//! 且**要点名是哪个名字、为什么**——「权限被拒」与「集群里确实还是浮动 tag」是两件事，
//! 落成同一行就再也没人分得出来。

use cog_core::{SFError, SFResult};

/// 主负载：只有它们**全部**跑在同一个 `main-<rev>` tag 上，那个 tag 才是可信的现役版本。
///
/// 名单写在模块作用域、只此一份——按名把集群读回来的那条命令，与按名字过滤出这几行的
/// 解析函数，共用它。
pub const BASE_DEPLOYMENTS: [&str; 4] = [
    "cogneva",
    "cogneva-security-gateway",
    "cogneva-sandbox-executor",
    "cogneva-evolution",
];

/// 按名读一个 deployment 的输出形状：`<名字>\t<容器>=<image>,…\n`。
///
/// 单个对象不是 list，所以取 `.metadata.name` 而不是 `{range .items[*]}`；解析侧认的
/// 是这份 tab 分隔文本，换了它要两边一起换。
pub const DEPLOYMENTS_JSONPATH: &str = "{.metadata.name}{\"\\t\"}{range .spec.template.spec.containers[*]}{.name}={.image}{\",\"}{end}{\"\\n\"}";

/// 跑一条命令并交回 stdout。
///
/// 这一层单独分出来，是为了让「逐名读四个主负载」这条逻辑有一条**不碰集群**的测试路径：
/// 「第一个读不出来的名字就中止、且报出是哪一个」这件事，只能在没有真集群时量。
#[async_trait::async_trait]
pub trait CommandRunner: Send + Sync {
    async fn run(&self, program: &str, args: &[&str], timeout_secs: u64) -> SFResult<String>;
}

/// 四个主负载里哪一个读不出来、为什么。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableDeployment {
    pub name: &'static str,
    pub why: String,
}

/// 逐名读四个主负载，把输出合并成一份 tab 分隔文本（形状见 [`DEPLOYMENTS_JSONPATH`]）。
///
/// 第一个读不出来的名字即中止。后面几个即使读得出来也拼不成一份完整文本，而**半份文本**
/// 会被解析成「不是全员都在 `main-<rev>` 上」——那是另一个结论（滚动中间、或还挂着浮动
/// tag），不是「这一次没读到」。所以这两态在这里就分开交给调用方，别在解析那一层合回去。
pub async fn read_base_deployments(
    runner: &dyn CommandRunner,
    kubectl_bin: &str,
    namespace: &str,
) -> Result<String, UnreadableDeployment> {
    let mut out = String::new();
    // `-o` 的值必须是 `jsonpath=<模板>` 这**一个**参数：kubectl 按 `=` 左边挑打印机，
    // 只给裸模板会「unable to match a printer suitable for the output format」并非零退出
    // ——而那条路读出来的是 `ClusterUnreadable`，长得像「集群读不了」，与真正的原因
    // （参数拼错了）隔着一层。2026-10-16 现网实测：裸模板恒失败，加前缀立刻对。
    let format = format!("jsonpath={DEPLOYMENTS_JSONPATH}");
    for name in BASE_DEPLOYMENTS {
        // 名字只在这一处拼成目标。
        let target = format!("deployment/{name}");
        match runner
            .run(
                kubectl_bin,
                &["-n", namespace, "get", &target, "-o", &format],
                30,
            )
            .await
        {
            Ok(o) => {
                // 交回来的必须**就是问的那个对象**。一次成功但空（或张冠李戴）的回答，
                // 在解析层只表现为「这个名字不在 `main-<rev>` 上」——那是另一个结论。
                // 一个没读出内容的回答与一个读出来不是 main 的负载，处置相反：前者要
                // 再读一次，后者要去看滚动。所以这一条在这一层判，不进合并文本。
                let answered_as = o.lines().next().and_then(|l| l.split('\t').next());
                if answered_as != Some(name) {
                    return Err(UnreadableDeployment {
                        name,
                        why: format!(
                            "the answer does not name {name}: {}",
                            if o.trim().is_empty() {
                                "empty output".to_string()
                            } else {
                                format!("first line reads {:?}", o.lines().next().unwrap_or(""))
                            }
                        ),
                    });
                }
                out.push_str(&o);
            }
            Err(e) => {
                return Err(UnreadableDeployment {
                    name,
                    why: e.to_string(),
                })
            }
        }
    }
    Ok(out)
}

/// 解析 [`read_base_deployments`] 的合并输出：仅当四个主负载的**全部**容器都跑在同一个
/// `main-<rev>` tag 上时返回该 rev；浮动 tag（`:local` 时代）、混合态、空输出都返回
/// `None`。
///
/// 只认 `main-<rev>`：`:local` 是浮动签，构建机可能缓存着旧的 `:local` 层，拿它当现役
/// 版本会让基于旧基线的东西自我放大。
///
/// **它只判交进来的那几行是否一致，判不了自己是否收全了**：四个名字里只喂进来一个，
/// 它照样返回那个 rev——一份读了一半的输出，与一份四个都读到、正好一致的输出，在这个
/// 函数看来同形。「四个都读到了」是 [`read_base_deployments`] 的保证，不是这里的。
/// 谁要自己拼输入喂它，先把这一条接住。
pub fn deployed_main_rev(jsonpath_out: &str) -> Option<String> {
    let mut revs = std::collections::BTreeSet::new();
    let mut total = 0usize;
    let mut on_main = 0usize;
    for line in jsonpath_out.lines() {
        let Some((name, rest)) = line.split_once('\t') else {
            continue;
        };
        if !BASE_DEPLOYMENTS.contains(&name) {
            continue;
        }
        for pair in rest.split(',') {
            let Some((_, image)) = pair.split_once('=') else {
                continue;
            };
            if image.is_empty() {
                continue;
            }
            total += 1;
            if let Some(rev) = image
                .rsplit(':')
                .next()
                .and_then(|tag| tag.strip_prefix("main-"))
            {
                revs.insert(rev.to_string());
                on_main += 1;
            }
        }
    }
    if total > 0 && on_main == total && revs.len() == 1 {
        revs.into_iter().next()
    } else {
        None
    }
}

/// `CommandRunner` 的生产实现：起一个进程，交回它的 stdout。
///
/// 这道缝存在只为让「第一个读不出来的名字就中止」那一条能在没有集群时被量到。生产路径
/// 上它就是一次普通的进程执行：不认得 kubectl，也不碰命名空间——命名空间是
/// [`read_base_deployments`] 拼进 argv 的，形状只有一处。
pub struct Subprocess;

#[async_trait::async_trait]
impl CommandRunner for Subprocess {
    async fn run(&self, program: &str, args: &[&str], timeout_secs: u64) -> SFResult<String> {
        let cmdline = format!("{program} {}", args.join(" "));
        let fut = tokio::process::Command::new(program)
            .args(args)
            .kill_on_drop(true)
            .output();
        let out = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), fut)
            .await
            .map_err(|_| SFError::IO(format!("{cmdline} timed out after {timeout_secs}s")))?
            .map_err(|e| SFError::IO(format!("failed to run {program}: {e}")))?;
        if !out.status.success() {
            return Err(SFError::IO(format!(
                "{cmdline} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

/// 现役 rev 作为一项读数交给别的组件（[`cog_core::LiveRevisionSource`]）。
///
/// 读法就是本模块的逐名读 + 解析——这个类型不做任何判据，只把**两种失败分到各自的
/// 格子里**，因为它们的处置相反：一个还滚着（该去看滚动），一个根本没读到（该去查授权
/// 或 API server）。合成一个 `None` 会让读者去追错的那个。
pub struct KubectlLiveRevision {
    kubectl_bin: String,
    namespace: String,
}

impl KubectlLiveRevision {
    pub fn new(kubectl_bin: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            kubectl_bin: kubectl_bin.into(),
            namespace: namespace.into(),
        }
    }
}

#[async_trait::async_trait]
impl cog_core::LiveRevisionSource for KubectlLiveRevision {
    async fn live_revision(&self) -> cog_core::LiveRevision {
        as_reading(read_base_deployments(&Subprocess, &self.kubectl_bin, &self.namespace).await)
    }
}

/// 一次读取的结局 → 一格读数：三种结局各进各的格子，一个都不许并进另一个。
///
/// 单独一个函数，是因为**要测的就是这张对照表**，而不是进程那一层；把它埋进
/// `live_revision()` 里，就没有办法在没有集群时把三种结局都喂进来。
fn as_reading(read: Result<String, UnreadableDeployment>) -> cog_core::LiveRevision {
    use cog_core::{LiveRevision, LiveRevisionGap};

    match read {
        // 「读不出来」的所有形状都落 ClusterUnreadable：进程起不来、非零退出（授权被拒）、
        // 超时、以及**读回来是空的**。最后一种由 `read_base_deployments` 判掉——空回答在
        // 解析层会变成「不在 `main-<rev>` 上」，那是滚动，不是没读到。
        Err(_) => LiveRevision::Unmeasurable(LiveRevisionGap::ClusterUnreadable),
        Ok(out) => match deployed_main_rev(&out) {
            Some(rev) => LiveRevision::Known(rev),
            None => LiveRevision::Unmeasurable(LiveRevisionGap::NoUnanimousMainTag),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 四个名字各一份输出，形状与 `kubectl get deployment/<名> -o <jsonpath>` 一致：
    /// 首字段是**对象自己的名字**（解析器靠它过滤），后面是 `容器=image` 的逗号串。
    /// 主负载多半不止一个容器，所以 cogneva 那一份多给一个——「全部容器」是判据的一部分。
    const ON_MAIN_AAA: [(&str, &str); 4] = [
        (
            "cogneva",
            "cogneva\tcogneva=reg/cogneva:main-aaa,cogneva-worker=reg/cogneva:main-aaa\n",
        ),
        (
            "cogneva-security-gateway",
            "cogneva-security-gateway\tcogneva-security-gateway=reg/cogneva:main-aaa\n",
        ),
        (
            "cogneva-sandbox-executor",
            "cogneva-sandbox-executor\tcogneva-sandbox-executor=reg/cogneva:main-aaa\n",
        ),
        (
            "cogneva-evolution",
            "cogneva-evolution\tcogneva-evolution=reg/cogneva:main-aaa\n",
        ),
    ];

    /// 按名字交回一份预先备好的输出，或一条失败；记下都问了哪些名字。
    struct FakeRunner {
        answers: Vec<(&'static str, Result<&'static str, &'static str>)>,
        asked: Mutex<Vec<String>>,
    }

    impl FakeRunner {
        fn on_main() -> Self {
            Self {
                answers: ON_MAIN_AAA.iter().map(|(n, o)| (*n, Ok(*o))).collect(),
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl CommandRunner for FakeRunner {
        async fn run(&self, _program: &str, args: &[&str], _timeout_secs: u64) -> SFResult<String> {
            let target = args
                .iter()
                .find(|a| a.starts_with("deployment/"))
                .expect("读取一定是按名 get 一个 deployment");
            self.asked.lock().unwrap().push((*target).to_string());
            let name = target.trim_start_matches("deployment/");
            for (expected, answer) in &self.answers {
                if *expected == name {
                    return match answer {
                        Ok(out) => Ok((*out).to_string()),
                        Err(why) => Err(cog_core::SFError::Validation((*why).to_string())),
                    };
                }
            }
            panic!("没人给 {name} 备答案");
        }
    }

    /// 四个名字都读得到、且容器全在同一个 `main-<rev>` 上：这就是现役 rev。
    #[tokio::test]
    async fn unanimous_main_tag_is_the_live_rev() {
        let runner = FakeRunner::on_main();
        let out = read_base_deployments(&runner, "kubectl", "cogneva")
            .await
            .expect("四个名字都备了答案");

        assert_eq!(deployed_main_rev(&out).as_deref(), Some("aaa"));
        assert_eq!(
            runner.asked.lock().unwrap().len(),
            BASE_DEPLOYMENTS.len(),
            "四个名字都要按名读到，不是只读第一个"
        );
    }

    /// 读不出来的名字要**点名**，且**立刻中止**——后面几个读得出来也拼不成完整文本，
    /// 而半份文本会被解析成「不是全员都在 main-<rev>」，那是另一个结论。
    #[tokio::test]
    async fn the_first_unreadable_name_is_named_and_stops_the_read() {
        let mut runner = FakeRunner::on_main();
        runner.answers[1] = (BASE_DEPLOYMENTS[1], Err("cannot get resource"));

        let err = read_base_deployments(&runner, "kubectl", "cogneva")
            .await
            .expect_err("第二个名字读不出来，这一趟就不算读到了");

        assert_eq!(err.name, BASE_DEPLOYMENTS[1]);
        assert!(
            err.why.contains("cannot get resource"),
            "失败的原因要带出来：{}",
            err.why
        );
        assert_eq!(
            runner.asked.lock().unwrap().len(),
            2,
            "第一个读不出来即中止，不该继续往后读"
        );
    }

    /// rev 不一致（滚动未收敛）、混合态（一半主线上线一半还挂着旧签）、浮动签、空输出：
    /// 四种都**不是**现役 rev，也都不是「读不出来」。
    #[test]
    fn a_rolling_or_floating_cluster_has_no_live_rev() {
        let line =
            |deploy: &str, container: &str, image: &str| format!("{deploy}\t{container}={image},");
        let diverged = [
            line("cogneva", "cogneva", "r:5000/cogneva:main-aaa"),
            line(
                "cogneva-security-gateway",
                "security-gateway",
                "r:5000/cogneva:main-bbb",
            ),
        ]
        .join("\n");
        assert_eq!(deployed_main_rev(&diverged), None);

        let mixed = [
            line("cogneva", "cogneva", "localhost/cogneva:local"),
            line(
                "cogneva-security-gateway",
                "security-gateway",
                "r:5000/cogneva:main-aaa",
            ),
        ]
        .join("\n");
        assert_eq!(deployed_main_rev(&mixed), None);

        assert_eq!(
            deployed_main_rev("cogneva\tcogneva=reg/cogneva:local\n"),
            None
        );
        assert_eq!(deployed_main_rev(""), None);
    }

    /// 半份输入在解析器眼里与完整输入同形——这是它的已知边界，写在文档里，也钉在这里：
    /// 谁绕过 `read_base_deployments` 自己拼输入，就会拿它当现役 rev。
    #[test]
    fn the_parser_cannot_tell_it_was_handed_a_truncated_read() {
        let only_one = "cogneva\tcogneva=reg/cogneva:main-aaa\n";
        assert_eq!(deployed_main_rev(only_one).as_deref(), Some("aaa"));
    }

    /// 一次成功但**空**的回答是「没读到」，不是「这个负载不在 main 上」。
    ///
    /// 后者（滚动中间、或还挂着浮动 tag）要人去看滚动，前者要人再读一次或查授权；
    /// 空回答若被拼进合并文本，它在那层就变成后者——同一个错的脸，看着像另一个结论。
    #[tokio::test]
    async fn an_answer_that_does_not_name_the_object_asked_for_is_unreadable() {
        let mut runner = FakeRunner::on_main();
        runner.answers[2] = (BASE_DEPLOYMENTS[2], Ok(""));
        let empty = read_base_deployments(&runner, "kubectl", "cogneva")
            .await
            .expect_err("空回答不是「不在 main 上」");
        assert_eq!(empty.name, BASE_DEPLOYMENTS[2]);
        assert!(empty.why.contains("empty output"), "{}", empty.why);

        // 张冠李戴同样算没读到：交回来的是别的对象，我们对这个名字一无所知。
        let mut runner = FakeRunner::on_main();
        runner.answers[3] = (
            BASE_DEPLOYMENTS[3],
            Ok("some-other-deployment\tx=reg/cogneva:main-aaa\n"),
        );
        let wrong = read_base_deployments(&runner, "kubectl", "cogneva")
            .await
            .expect_err("答的不是问的那个对象");
        assert_eq!(wrong.name, BASE_DEPLOYMENTS[3]);
        assert!(wrong.why.contains("does not name"), "{}", wrong.why);
    }

    /// 生产那条进程路径本身：失败要报成失败，不能报成空输出。
    ///
    /// 这两者在这条读法里处置相反——空输出是「没读到」（该再读一次或查授权），而一次
    /// 非零退出若被读成空字符串，就会以「读到了、内容是空」的形状流下去，在解析层变成
    /// 「这个负载不在 `main-<rev>` 上」。
    #[tokio::test]
    async fn the_subprocess_runner_reports_failure_rather_than_empty_output() {
        assert_eq!(
            Subprocess
                .run("sh", &["-c", "printf 'a\\tb\\n'"], 5)
                .await
                .expect("成功的命令要交回 stdout"),
            "a\tb"
        );

        let err = Subprocess
            .run("sh", &["-c", "echo denied >&2; exit 1"], 5)
            .await
            .expect_err("非零退出是失败，不是空输出");
        assert!(err.to_string().contains("denied"), "{err}");
    }

    /// **装配层**要有自己的判据：`-o` 交出去的那**一个**参数必须是 kubectl 认的形状
    /// （`jsonpath=<模板>`），给裸模板它会「unable to match a printer」并非零退出，而那条
    /// 路读出来的是 `ClusterUnreadable`——长得像「集群读不了」，与真正的原因隔着一层。
    ///
    /// 这一格此前没有任何东西钉着：`FakeRunner` 把进程那一层整个换掉（它按
    /// `starts_with("deployment/")` 认目标，**不看**格式参数），`Subprocess` 那两条测试跑的是
    /// `sh -c`。测试覆盖了逻辑、没覆盖装配。2026-10-16 现网实测到：A5 那条读数每拍都落
    /// `cluster_unreadable`，恒等着一个永远不会出现的 `Known`。
    ///
    /// 替身是**真跑进程**的：一个假 `kubectl`，它按 kubectl 自己的规矩挑打印机——认
    /// `jsonpath=` 前缀、拒裸模板。所以这一格量的是「交出去的 argv 对不对」，不是「源码里
    /// 有没有那几个字面量」。
    #[tokio::test]
    async fn the_format_handed_to_kubectl_is_one_it_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("kubectl");
        std::fs::write(
            &fake,
            "#!/bin/sh\n\
             target=; fmt=\n\
             while [ $# -gt 0 ]; do\n\
             \x20 case \"$1\" in\n\
             \x20   -o) fmt=\"$2\"; shift 2 ;;\n\
             \x20   deployment/*) target=\"$1\"; shift ;;\n\
             \x20   *) shift ;;\n\
             \x20 esac\n\
             done\n\
             case \"$fmt\" in\n\
             \x20 jsonpath=*) ;;\n\
             \x20 *) echo \"error: unable to match a printer suitable for the output format \\\"$fmt\\\"\" >&2; exit 1 ;;\n\
             esac\n\
             name=\"${target#deployment/}\"\n\
             printf '%s\\t%s=reg/cogneva:main-abc,\\n' \"$name\" \"$name\"\n",
        )
        .unwrap();
        let mut perm = std::fs::metadata(&fake).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
        std::fs::set_permissions(&fake, perm).unwrap();
        let bin = fake.to_str().unwrap();

        // 反向对照：先证明这个替身**真的**会拒裸模板，否则上面那一格的绿是瞎的
        // （一个什么都不看的替身，也能让「读成功」这一格绿）。
        let bare = Subprocess
            .run(
                bin,
                &[
                    "-n",
                    "cogneva",
                    "get",
                    "deployment/cogneva",
                    "-o",
                    DEPLOYMENTS_JSONPATH,
                ],
                5,
            )
            .await;
        assert!(
            bare.is_err(),
            "替身必须拒裸模板，否则这一格量不到东西：{bare:?}"
        );

        let out = read_base_deployments(&Subprocess, bin, "cogneva")
            .await
            .expect("交出去的格式要是 kubectl 认的那一种");
        assert_eq!(deployed_main_rev(&out).as_deref(), Some("abc"));
    }

    /// 三种结局各进各的格子。这张对照表是这一格的判据，不是实现细节：把「没读到」
    /// 并进「不在现役 rev」会让读者去追滚动，把「读不齐」并进「没读到」会让它去查
    /// 授权——两边的处置相反。
    #[test]
    fn the_three_outcomes_stay_three() {
        use cog_core::{LiveRevision, LiveRevisionGap};

        let on_main: String = ON_MAIN_AAA.iter().map(|(_, o)| *o).collect();
        assert_eq!(
            as_reading(Ok(on_main)),
            LiveRevision::Known("aaa".to_string())
        );

        // 四个都读到了、但没全落在同一个 main tag 上：集群读得动，只是没有现役 rev。
        let rolling = "cogneva\tcogneva=reg/cogneva:main-aaa\n\
                       cogneva-security-gateway\tsecurity-gateway=reg/cogneva:local\n";
        assert_eq!(
            as_reading(Ok(rolling.to_string())),
            LiveRevision::Unmeasurable(LiveRevisionGap::NoUnanimousMainTag)
        );

        // 读不动：授权被拒、超时、进程起不来，以及空回答（由读的那一层判成 Err）。
        assert_eq!(
            as_reading(Err(UnreadableDeployment {
                name: BASE_DEPLOYMENTS[0],
                why: "Forbidden".into(),
            })),
            LiveRevision::Unmeasurable(LiveRevisionGap::ClusterUnreadable)
        );
    }
}
