//! 动作前的可逆性判断与「不可逆一步一批」的批量纪律。
//!
//! 这是台账 N27 的前两句（改名后不再冒充具身）：动作之前先判一次这样做会不会
//! 永久失去什么；不可逆的那一步永远是一步一批。第三句「动作之后由现实的读数
//! 回答到没到」在这里只定义**判据的形状**（哪一类动作该由哪一条现实读数确认），
//! 取数在执行侧。
//!
//! 可逆性不是动作类型一个人的事：同一类「删」在 git 里有那一份时可恢复，在没有
//! 时就永久失去。所以判断收两样东西——动作类型，以及「世界还有没有另一份」
//! （`recoverable`）。把场景拍成「删总是可逆」或「删总是不可逆」，都会在另一半
//! 场景里答错。

/// 一次动作的类型。闭集：新加一种动作必须在这里给它一个可逆性答案（穷尽匹配）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    Read,
    Write,
    Create,
    Rename,
    Deploy,
    Delete,
    ForcePush,
    DropTable,
    RotateKey,
    RevokeKey,
    SendExternal,
}

/// 一次动作与它所在的世界：`recoverable` 表示世界还留着另一份（例如 git 里有
/// 这个对象、有备份、有上游副本）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Action {
    pub kind: ActionKind,
    pub recoverable: bool,
}

/// 这一步会不会永久失去什么。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loss {
    Nothing,
    /// 永久失去的那一件事，用一句人能读的话说清。
    Permanent(&'static str),
}

impl Loss {
    pub fn is_permanent(self) -> bool {
        matches!(self, Loss::Permanent(_))
    }
}

/// 判一次：只按动作类型与「世界还有没有另一份」作答，不看执行者的自信。
pub fn loss(action: Action) -> Loss {
    match action.kind {
        ActionKind::Read
        | ActionKind::Write
        | ActionKind::Create
        | ActionKind::Rename
        | ActionKind::Deploy => Loss::Nothing,
        ActionKind::Delete if action.recoverable => Loss::Nothing,
        ActionKind::Delete => Loss::Permanent("这份内容只在这一处"),
        ActionKind::ForcePush => Loss::Permanent("远端在这之前的历史"),
        ActionKind::DropTable => Loss::Permanent("表里的行"),
        ActionKind::RotateKey => Loss::Permanent("旧钥签发的访问在重叠期结束后不再成立"),
        ActionKind::RevokeKey => Loss::Permanent("被吊销的钥签过的包将不再受信"),
        ActionKind::SendExternal => Loss::Permanent("已经发出去的内容（发出去就收不回）"),
    }
}

/// 一批动作按纪律切成的批。可逆动作可以同批；**每一个不可逆动作各成一批**。
///
/// 顺序保持原样：纪律改变的是分批，不是执行次序。
pub fn batches(actions: &[Action]) -> Vec<Vec<usize>> {
    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    for (index, action) in actions.iter().enumerate() {
        if loss(*action).is_permanent() {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            out.push(vec![index]);
        } else {
            current.push(index);
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// 动作之后用来回答「到没到」的现实读数：不是执行者自报，而是从世界里读回来。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealityCheck {
    /// 提交在、且是那一个（读仓库，不听执行者）。
    CommitPresent,
    /// 路径在（读文件系统）。
    PathPresent,
    /// 路径不在（读文件系统）。
    PathAbsent,
    /// 服务应答（读端点）。
    ServiceAnswers,
    /// 表在/不在（读库的目录）。
    TablePresent,
    /// 表不在（读库的目录）。
    TableAbsent,
}

impl ActionKind {
    /// 这一类动作之后该由哪一条现实读数确认。`None` 表示它没有「到没到」可读：
    /// 发出去的内容只能被对方读回来，本侧没有一条读数能替它作证。
    pub fn confirmed_by(self) -> Option<RealityCheck> {
        match self {
            Self::Read => None,
            Self::Write | Self::Create | Self::Rename => Some(RealityCheck::PathPresent),
            Self::Delete => Some(RealityCheck::PathAbsent),
            Self::Deploy => Some(RealityCheck::ServiceAnswers),
            Self::DropTable => Some(RealityCheck::TableAbsent),
            Self::ForcePush => Some(RealityCheck::CommitPresent),
            Self::RotateKey | Self::RevokeKey => Some(RealityCheck::ServiceAnswers),
            Self::SendExternal => None,
        }
    }
}

impl RealityCheck {
    /// 只读文件系统的那两类读数可以就地取；其余三类（提交、表、服务应答）要各自的
    /// 客户端，本层不替它们取数——`None` 是「这一侧判不了」，不是「到了」。
    pub fn read_path(self, path: &std::path::Path) -> Option<bool> {
        match self {
            RealityCheck::PathPresent => Some(path.exists()),
            RealityCheck::PathAbsent => Some(!path.exists()),
            RealityCheck::CommitPresent
            | RealityCheck::ServiceAnswers
            | RealityCheck::TablePresent
            | RealityCheck::TableAbsent => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(kind: ActionKind, recoverable: bool) -> Action {
        Action { kind, recoverable }
    }

    /// 同一个「删」，世界还有另一份时可逆，没有时永久失去。
    #[test]
    fn the_same_delete_reads_differently_with_and_without_another_copy() {
        assert_eq!(loss(action(ActionKind::Delete, true)), Loss::Nothing);
        assert!(loss(action(ActionKind::Delete, false)).is_permanent());
    }

    /// 不可逆的那一步永远单独成批；可逆的可以同批；顺序不变。
    #[test]
    fn every_irreversible_step_is_its_own_batch() {
        let actions = vec![
            action(ActionKind::Write, true),
            action(ActionKind::Delete, false),
            action(ActionKind::Create, true),
            action(ActionKind::Create, true),
            action(ActionKind::DropTable, true),
        ];
        let batches = batches(&actions);
        assert_eq!(batches, vec![vec![0], vec![1], vec![2, 3], vec![4]]);
        for batch in &batches {
            if batch.len() > 1 {
                assert!(batch.iter().all(|i| !loss(actions[*i]).is_permanent()));
            }
        }
    }

    /// 全部可逆 ⇒ 一批；空 ⇒ 没有批。
    #[test]
    fn reversible_actions_may_share_one_batch() {
        let actions = vec![
            action(ActionKind::Write, true),
            action(ActionKind::Rename, true),
            action(ActionKind::Deploy, true),
        ];
        assert_eq!(batches(&actions), vec![vec![0, 1, 2]]);
        assert!(batches(&[]).is_empty());
    }

    /// 路径类的「到没到」就地读文件系统；其余三类本层答「判不了」。
    #[test]
    fn the_path_checks_read_the_filesystem_and_the_others_say_they_cannot() {
        let dir = std::env::temp_dir().join(format!("cog-irrev-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("present");
        std::fs::write(&file, "x").expect("write");
        assert_eq!(RealityCheck::PathPresent.read_path(&file), Some(true));
        assert_eq!(RealityCheck::PathAbsent.read_path(&file), Some(false));
        assert_eq!(
            RealityCheck::PathPresent.read_path(&dir.join("nope")),
            Some(false)
        );
        assert_eq!(RealityCheck::CommitPresent.read_path(&file), None);
        assert_eq!(RealityCheck::ServiceAnswers.read_path(&file), None);
        assert_eq!(RealityCheck::TableAbsent.read_path(&file), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 「到没到」只认现实读数；发出去的内容本侧没有读数能作证。
    #[test]
    fn every_action_names_the_reading_that_confirms_it_or_says_it_has_none() {
        assert_eq!(
            ActionKind::Delete.confirmed_by(),
            Some(RealityCheck::PathAbsent)
        );
        assert_eq!(
            ActionKind::ForcePush.confirmed_by(),
            Some(RealityCheck::CommitPresent)
        );
        assert_eq!(ActionKind::SendExternal.confirmed_by(), None);
        assert_eq!(ActionKind::Read.confirmed_by(), None);
    }
}
