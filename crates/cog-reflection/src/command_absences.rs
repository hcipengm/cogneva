//! Which of the two absences one invocation is serving.
//!
//! principle: feedback_one_command_serving_two_absences_names_neither_when_it_fails
//!
//! 一条命令同时承担「取回缺的东西」与「用已有的东西干活」时，它的失败只说这条
//! 命令没跑完，点的是命令不是缺的那一样——读的人被送去查执行器，而真正缺的那
//! 一样（那个 ref、那棵树、那个依赖、那个产物）从头到尾没被点名。
//!
//! 所以这道理不在失败之后读，而在动之前读：这个门只看 argv 的形状，不碰任何
//! I/O、时钟或存储，同一个 argv 永远给出同一个答案。两件事兼在一处的被拒，拒词
//! 必须说出缺的是哪一样；只担一件事的是正常命令，不许被这条门拦下——否则这条
//! 门退化成「什么都拒」，与没有门一样看不见。

/// 这条命令请自己去取回的那些动词。
///
/// 闭合的一小撮：每一个都表示「手上没有，得先拿到」。增补要连同下面的拒词一起
/// 想清楚，因为每加一个动词，就有更多本来只管一件事的命令被问成兼着两件。
pub const RETRIEVING_VERBS: &[&str] = &["clone", "fetch", "pull", "install"];

/// 这条命令想拿已有的东西干活的那些动词。
pub const WORKING_VERBS: &[&str] = &["build", "test", "lint", "run"];

/// 一条命令兼着担当的两件缺失。
///
/// 存在的唯一理由是「两件」这件事本身：一件的时候没有这个值，所以「顺带取回」
/// 不会被当成兼着两件事，而兼着两件事的命令也不能悄悄退回成一件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoAbsences {
    /// 取回那半边的动词，argv 里出现的那一个。
    pub fetch: String,
    /// 干活那半边的动词，argv 里出现的那一个。
    pub work: String,
    /// 缺的那一样：取回动词之后点名的那个对象。
    ///
    /// 这是拒词里唯一有用的部分。只有命令名的失败把人送回执行器，而这一格说的是
    /// 该去取的是哪一样东西。
    pub missing: String,
}

impl TwoAbsences {
    /// 拒词：点缺的那一样，不点那条没跑完的命令。
    pub fn sentence(&self) -> String {
        format!(
            "`{}` 与 `{}` 兼在一条命令里：它既要去取回 `{}`，又要用已有的东西干活；它没跑完时只会说命令没跑完，缺的是 `{}`，不是执行器。",
            self.fetch, self.work, self.missing, self.missing
        )
    }
}

/// 这条命令是不是兼着两件缺失；只说着一件、或一件都没说时是 `None`。
///
/// 纯函数：只看 argv 的形状，所以能在动之前问，答案也不随时钟走。
pub fn serves_two_absences(argv: &[&str]) -> Option<TwoAbsences> {
    let fetch_at = first_verb(argv, RETRIEVING_VERBS)?;
    let work_at = first_verb(argv, WORKING_VERBS)?;
    Some(TwoAbsences {
        fetch: argv[fetch_at].to_string(),
        work: argv[work_at].to_string(),
        missing: named_object(argv, fetch_at),
    })
}

/// 第一个说的是 `verbs` 里那些词的 argv 位置。
fn first_verb(argv: &[&str], verbs: &[&str]) -> Option<usize> {
    argv.iter()
        .position(|word| verbs.iter().any(|verb| word.eq_ignore_ascii_case(verb)))
}

/// 取回动词之后点名的那个对象，跳开选项。
fn named_object(argv: &[&str], verb_at: usize) -> String {
    match argv
        .iter()
        .skip(verb_at + 1)
        .find(|word| !word.starts_with('-'))
    {
        Some(word) => word.to_string(),
        None => format!("the thing `{}` was asked to fetch", argv[verb_at]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// principle: feedback_one_command_serving_two_absences_names_neither_when_it_fails
    ///
    /// 兼着两件事的命令必须被拒，而且拒词点的是缺的那一样；只担一件事的命令不许
    /// 被这条门拦下——后半是前半的反面，缺了它，把谓词写成「凡有动词就拒」也照样绿。
    #[test]
    fn a_command_that_fetches_and_works_names_the_thing_it_lacks() {
        let refusal = serves_two_absences(&[
            "git",
            "clone",
            "https://example.invalid/r.git",
            "&&",
            "cargo",
            "build",
        ])
        .expect("clone 与 build 兼在一条命令里，是两件缺失");
        assert_eq!(refusal.missing, "https://example.invalid/r.git");
        assert!(
            refusal.sentence().contains("https://example.invalid/r.git"),
            "拒词点的是命令而不是缺的那一样: {}",
            refusal.sentence()
        );
        assert!(
            serves_two_absences(&["git", "clone", "https://example.invalid/r.git"]).is_none(),
            "只取回一件的命令不是两件缺失"
        );
        assert!(
            serves_two_absences(&["cargo", "build", "--release"]).is_none(),
            "只拿已有的东西干活的命令不是两件缺失"
        );
    }
}
