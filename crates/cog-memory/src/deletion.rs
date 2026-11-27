//! 删除之前的可逆性判断：删一条记忆之前，必须已经有一份读得回来的归档。
//!
//! 判据来自 `cog_core::contract::irreversible`（N49）：同一个「删」，世界还留着
//! 另一份时可逆，没有时就永久失去。记忆的删除路径一直是「先归档、读回逐字相等、
//! 再删」——这个函数把那句话变成一次显式判断，而不是散在两条路径里各自写一遍
//! 「不相等就拒」。有人以后悄悄拿掉归档那一步，这里会拒，而且拒绝的理由是纪律
//! 自己的那句话。

use cog_core::contract::irreversible::{loss, Action, ActionKind, Loss};
use cog_core::{SFError, SFResult};

/// 归档读回来了吗？读回来才允许删。
pub(crate) fn deletion_is_allowed(archived_and_read_back: bool) -> SFResult<()> {
    match loss(Action {
        kind: ActionKind::Delete,
        recoverable: archived_and_read_back,
    }) {
        Loss::Nothing => Ok(()),
        Loss::Permanent(what) => Err(SFError::Agent(format!(
            "refusing an irreversible delete: {what}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_delete_without_a_readable_archive_is_refused() {
        let refused = deletion_is_allowed(false).expect_err("没有读得回来的归档就不能删");
        assert!(
            refused.to_string().contains("irreversible delete"),
            "{refused}"
        );
        assert!(deletion_is_allowed(true).is_ok());
    }
}
