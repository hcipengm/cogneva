//! 集群**现役**的是哪个 rev：四个主负载是否全部落在同一个不可变 `main-<rev>` tag 上。
//!
//! 这一格读数有两个读者（金丝雀 overlay 的基底、以及「落地≠生效」那一格），第三个马上
//! 就到。三处问的是同一个事实，所以**名字、jsonpath、解析**各只此一份：多一个读者不该
//! 多一份名单——两份名单会各自漂，而漂掉的那份不会报错，它只会让某个读者少认一个名字。
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

use cog_core::SFResult;

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
    for name in BASE_DEPLOYMENTS {
        // 名字只在这一处拼成目标。
        let target = format!("deployment/{name}");
        match runner
            .run(
                kubectl_bin,
                &["-n", namespace, "get", &target, "-o", DEPLOYMENTS_JSONPATH],
                30,
            )
            .await
        {
            Ok(o) => out.push_str(&o),
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
}
