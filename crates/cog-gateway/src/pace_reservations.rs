//! principle: feedback_a_queued_commit_rereads_its_bytes_each_attempt_but_keeps_the_pool_it_was_cut_in
//! 已经放行、还没在账上落地的任务占着的额度。
//!
//! 配速的判据读的是账上已经花掉多少，而账只在一发调用**结束**之后才多一行。
//! 一窗只装得下一件活时，一连放行十件的是同一条路径：每一件都在同一个快照上
//! 判过，都读到"还没花"，于是都进来了——2026-11-01 二十分钟里连开十四件就是
//! 这条路径的产物，而那天整池六小时没有一次成功的调用。
//!
//! 这张表补的就是放行到落账之间的空档：放行一件，就在这里记下它要占的额度；
//! 此后每一次判定先把这里已经许出去的额度从允许量里抠掉，判据才与"已经许了
//! 多少"对得上，而不是与"还没花多少"对得上。
//!
//! 两件事必须有界，否则这张表会自己变成新的停摆：一件记下的活要么在账上出现
//! （它真的开跑了，此前那段没落账的花销已经被账本收进去；它若还在过闸门，下
//! 一次调用会重新记上一条），要么过了 [`MAX_HOLD_SECS`] 还没出现（它根本没跑
//! 起来，它占的位置不再代表任何将发生的花销）。清账按拍做，不在请求路上做——
//! 那是每次调用都要查一次库的形状。

/// 一件活从被放行到在账上落第一行的最长等待。过了这个岁数的占位一律作废：
/// 一件十分钟都没发出过一次调用的任务，它占着的额度不代表任何将要发生的花销。
// cap: bound — 判词已经在上一句里说全了：十分钟没有一次调用就不再代表将要发生的花销；900 是那句话的余量，不是从某条分布里取的数，所以没有哪次实测会把它顶走
pub(crate) const MAX_HOLD_SECS: i64 = 900;

/// 两次问账的最小间隔。清账要查库，按拍做就够了；请求路只做算术。
// cap: bound — 它只被下面那句承诺用着（一件停下的活最多再占一个清账间隔），而那是个上界不是观测量：再小只是更频繁地查库，再大也只是把那个尾巴拉长
pub(crate) const SWEEP_INTERVAL_SECS: i64 = 30;

/// 一件占着额度的活。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Held {
    pub task: String,
    /// 放行它的是哪一家的尺子（`base_url|model`）。额度逐家算，记到别人头上
    /// 就是拿一家的额度关另一家。
    pub upstream: String,
    /// 它要占的额度：判据里那一份"一件整任务"的预留量。
    pub tokens: u64,
    pub admitted_unix: i64,
}

/// 放行过、还没在账上出现的那些活。
#[derive(Debug, Clone, Default)]
pub(crate) struct PaceReservations {
    held: Vec<Held>,
    /// 上一次问账的时刻，0 = 本进程还没问过。
    last_sweep_unix: i64,
}

impl PaceReservations {
    /// 这一家已经被许出去、账上还没有的额度。
    pub(crate) fn held_tokens(&self, upstream: &str) -> u64 {
        self.held
            .iter()
            .filter(|h| h.upstream == upstream)
            .map(|h| h.tokens)
            .sum()
    }

    /// 记下一件刚放行的活。
    ///
    /// 按任务去重：一件任务在被放行之后、落第一行之前会连续来好几次调用，每一次
    /// 都从这里走一遍。同名的占位只留一个，取两者中较大的那份预留守住它最早的
    /// 放行时刻——重复调用若把岁数推到当下，一件永远不落账的活就永远不会过期。
    pub(crate) fn admit(&mut self, task: &str, upstream: &str, tokens: u64, now_unix: i64) {
        if tokens == 0 {
            return;
        }
        if let Some(existing) = self.held.iter_mut().find(|h| h.task == task) {
            existing.tokens = existing.tokens.max(tokens);
            return;
        }
        self.held.push(Held {
            task: task.to_string(),
            upstream: upstream.to_string(),
            tokens,
            admitted_unix: now_unix,
        });
    }

    /// 还没清掉的占位里有哪些任务：清账要拿着这份名单去问账。
    pub(crate) fn tasks(&self) -> Vec<String> {
        self.held.iter().map(|h| h.task.clone()).collect()
    }

    /// 该不该在这一拍问账。空表不问，刚问过也不问。
    pub(crate) fn due_for_sweep(&self, now_unix: i64) -> bool {
        !self.held.is_empty()
            && (self.last_sweep_unix == 0 || now_unix - self.last_sweep_unix >= SWEEP_INTERVAL_SECS)
    }

    /// 清账：账上已经有这件任务的行，或者它已经老过 [`MAX_HOLD_SECS`]，都撤掉。
    ///
    /// 撤掉的判据只看"账上有没有它"，不看它成没成功：一发调用落了 ok 或 error
    /// 的行，它此前那段没落账的花销就已经被账本收进去了。
    ///
    /// 撤掉不等于这件活不再占额度——它下一次过闸门时会重新记上一条。所以占位的
    /// 净效果是"还在过闸门的活一直占着一份，停下来之后最多再占一个清账间隔"，
    /// 而那正是要的：一件已经停下的活，它没落账的尾巴也已经不存在了。
    pub(crate) fn retire(&mut self, started: &std::collections::HashSet<String>, now_unix: i64) {
        self.held.retain(|h| {
            !started.contains(&h.task) && now_unix.saturating_sub(h.admitted_unix) < MAX_HOLD_SECS
        });
        self.last_sweep_unix = now_unix;
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.held.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 这条判据答的那条规则，逐字写在模块头上。
    const PRINCIPLE: &str =
        "feedback_a_queued_commit_rereads_its_bytes_each_attempt_but_keeps_the_pool_it_was_cut_in";

    fn started(names: &[&str]) -> std::collections::HashSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    #[test]
    fn a_hold_is_counted_against_the_upstream_that_admitted_it() {
        let mut held = PaceReservations::default();
        held.admit("t1", "a|m", 400, 1_000);
        assert_eq!(held.held_tokens("a|m"), 400);
        assert_eq!(
            held.held_tokens("b|m"),
            0,
            "a job admitted by one upstream must not close another one's window"
        );
    }

    /// 同一件任务在落第一行之前会再来好几次，每次都从这里走。占位只留一个。
    #[test]
    fn repeated_calls_from_one_task_hold_one_place() {
        let mut held = PaceReservations::default();
        held.admit("t1", "a|m", 400, 1_000);
        held.admit("t1", "a|m", 400, 1_030);
        held.admit("t1", "a|m", 400, 1_060);
        assert_eq!(held.len(), 1);
        assert_eq!(held.held_tokens("a|m"), 400);
        // 岁数按第一次放行算：一件永远不落账的活必须到点作废，不能被自己的
        // 重试一直续命。
        held.retire(&started(&[]), 1_000 + MAX_HOLD_SECS - 1);
        assert_eq!(held.len(), 1);
        held.retire(&started(&[]), 1_000 + MAX_HOLD_SECS);
        assert_eq!(held.len(), 0);
    }

    #[test]
    fn a_task_whose_call_landed_stops_holding() {
        let mut held = PaceReservations::default();
        held.admit("t1", "a|m", 400, 1_000);
        held.admit("t2", "a|m", 400, 1_000);
        held.retire(&started(&["t1"]), 1_010);
        assert_eq!(held.tasks(), vec!["t2".to_string()]);
        assert_eq!(
            held.held_tokens("a|m"),
            400,
            "only the job that has not landed yet is still holding room"
        );
    }

    /// 排队中的提交每一轮重拍字节，但池的类别在放行那一刻就冻住了。
    ///
    /// 一件任务在账上落第一行之前会再来好几次，每一次都带着当下读到的那份预留
    /// 量从这里走一遍：占位只留一个，取两者中较大的那份，所以「每一轮重拍字节」
    /// 这一半是成立的——重来的那一轮改小了，占位不缩。
    ///
    /// 但池的类别（`upstream`，即 `base_url|model` 那把尺子）属于第一次放行：
    /// 找到同名占位就只抬额度，不改 `upstream`、不改放行时刻。所以进程里那个
    /// source 分类器事后改好，救不回已经入队的票；它冻住的是**票**，改好之后
    /// 才排上队的票才按新的类别算。
    ///
    /// 丢了这个冻结，第二次放行到 `b|m` 会让 `held_tokens("a|m")` 掉到 0 而
    /// `held_tokens("b|m")` 涨到 900——额度当场地从一家的窗口搬到另一家。
    #[test]
    fn a_queued_commit_rereads_its_bytes_each_attempt_but_keeps_the_pool_it_was_cut_in() {
        let mut held = PaceReservations::default();
        held.admit("t1", "a|m", 400, 1_000);
        // 同一张票再来一轮，报的是当下读到的那份预留量。
        held.admit("t1", "a|m", 900, 1_030);
        // 再来一轮，这一次分类器已经改好，报的是 b|m。
        held.admit("t1", "b|m", 900, 1_060);
        assert_eq!(
            held.held_tokens("a|m"),
            900,
            "字节重拍了，票的类别没重拍：这一轮仍然记在放行那一次冻住的 a|m 上"
        );
        assert_eq!(
            held.held_tokens("b|m"),
            0,
            "一张已经入队的票不许被后一次分类搬到 b|m：那等于拿一家的额度关另一家的窗"
        );
        assert_eq!(
            held.held
                .iter()
                .map(|h| h.upstream.as_str())
                .collect::<Vec<_>>(),
            vec!["a|m"],
            "占位只有一个，它的 upstream 出自放行那一次的实参，不是从任务当下的分类里重算的"
        );

        // 冻结属于票，不属于表：改好分类器之后才排上队的票，拿到的就是新类别。
        held.admit("t2", "b|m", 500, 1_060);
        assert_eq!(
            held.held_tokens("b|m"),
            500,
            "一张冻住的票不该把后来入队的票也按旧类别算：冻结是每张票各自那一刻的分类"
        );

        // 冻住的额度也只按这张表本来就有的两条路离开：过了岁数，或者落了账。
        held.retire(&started(&[]), 1_000 + MAX_HOLD_SECS - 1);
        assert_eq!(
            held.held_tokens("a|m"),
            900,
            "还没到岁数时，冻住的那家仍然替它将发生的花销占着位置"
        );
        held.retire(&started(&["t1"]), 1_100);
        assert_eq!(held.tasks(), vec!["t2".to_string()]);
        assert_eq!(
            held.held_tokens("a|m"),
            0,
            "落了账的票不再占额度，冻住的类别不会让它多占一拍"
        );
        held.retire(&started(&[]), 1_060 + MAX_HOLD_SECS);
        assert_eq!(held.held_tokens("b|m"), 0, "过了岁数的票不再占额度");

        // 判据读的是自己这一份源码：它答的是这个 revision 的树，不是别处的抄本。
        // 载体换了文件、头上那行声明挪了、Held 不再留着 upstream、或者 admit
        // 去重那一支开始改 upstream，这里当场红。
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pace_reservations.rs");
        let source = std::fs::read_to_string(&path).expect("读得到承载这条判据的源码");
        // 切掉测试模块：替身里的形状证明不了生产路径怎么记。
        let production = source.split("\n#[cfg(test)]\nmod ").next().unwrap_or("");
        let declaration = format!("//! principle: {PRINCIPLE}");
        assert!(
            production
                .lines()
                .any(|line| line.trim_end() == declaration),
            "模块头上没有 `{declaration}`：判据是照着这一行认领它答的规则的"
        );
        let squeezed: String = production.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            squeezed.contains("pubupstream:String,"),
            "Held 得留着 upstream：池的类别是这条规则的一半，字段没了就无处可冻"
        );
        assert!(
            squeezed.contains("upstream:upstream.to_string(),"),
            "占位的 upstream 出自放行那一次的实参——别处算来的类别就是另一次的冻结"
        );
        let admit = production
            .split("pub(crate) fn admit(")
            .nth(1)
            .expect("admit 还在这一份源码里");
        let body = admit.split_once('{').map_or("", |(_, body)| body);
        let dedupe = body.split("self.held.push(").next().unwrap_or("");
        assert!(
            !dedupe.contains("upstream"),
            "去重那一支只抬额度，不许碰 upstream：碰了它，后一次分类就会把已经入队的票搬到另一家"
        );
    }

    #[test]
    fn an_empty_table_asks_no_questions() {
        let mut held = PaceReservations::default();
        assert!(!held.due_for_sweep(10_000));
        held.admit("t1", "a|m", 400, 9_990);
        assert!(held.due_for_sweep(10_000));
        held.retire(&started(&[]), 10_000);
        assert_eq!(held.len(), 1);
        // 刚问过就不再问：请求路每一发都要过这里，不能每一发都查一次库。
        assert!(!held.due_for_sweep(10_010));
        assert!(held.due_for_sweep(10_000 + SWEEP_INTERVAL_SECS));
    }
}
