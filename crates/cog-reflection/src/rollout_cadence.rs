//! 看一次滚动发布，隔多久再看一次。
//!
//! 两处都问这一件事（等发布完成的那条循环、读部署状态的那个面），曾经各写一个
//! 10s：同一件事两个答案，改一处那天就不一样了。

use std::time::Duration;

/// 滚动发布的读取周期。Pod 重建本身以十秒计：更密的轮询只多打几次 kubectl，
/// 不会让发布更早完成；它不参与判停——判停是实测窗口那条。
// cap: fixed — Pod 重建以十秒计，更密的轮询只多打几次 kubectl，不会让发布更早完成
// keep: 删掉后发布轮询没有节拍，更密只多打几次 kubectl、不会更早完成；由「Pod 重建以十秒计」兜住
pub const ROLLOUT_POLL: Duration = Duration::from_secs(10);
