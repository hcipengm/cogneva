//! 一条晋级变更在两个地方留名字：源码仓库里的 git tag，和 registry 里的镜像
//! tag。两边都是「一端写、另一端照着同一份拼法去找」。
//!
//! 名字写两遍时，改一处不报错：推送端把镜像推到 `cogneva:promote-…`，拉取端按
//! 另一个拼法去找，找回来的是一句「这个 change 还没有镜像」。拉取端不会把这句
//! 读成「名字对不上」，它读成「还没轮到这条」——于是循环每一拍都空转，而面上看
//! 起来只是慢。
//!
//! 所以拼法在这里写一次，两端都从这里取。白名单也是同一条理由：change id 里带
//! 一个标点时，两端各自清洗的结果一旦不同，写下去的名字与找回来的名字就对不上，
//! 而 typo 在两个不同的实现里长得一模一样。

/// 源码仓库里一条晋级的 tag 前缀，后面接清洗过的 change id。
pub const PROMOTE_GIT_TAG_PREFIX: &str = "promote/";

/// registry 里一条晋级的镜像 tag 前缀，后面接清洗过的 change id。
///
/// 前面还要接 registry 端点与仓库名，这一段只到仓库名之后。
pub const PROMOTE_IMAGE_TAG_PREFIX: &str = "cogneva:promote-";

/// 只保留 `[A-Za-z0-9-_]`，其余替换成 `-`。
///
/// 这是 git ref 与镜像 tag 共同接受的字符集：不清洗就可能写进一个两边都不收的
/// 名字，而清洗方式不一致时两边会写进两个都合法的名字——后者更难查。
pub fn tag_safe(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// 一条晋级变更在源码仓库里的 tag。
pub fn promote_git_tag(change_id: &str) -> String {
    format!("{PROMOTE_GIT_TAG_PREFIX}{}", tag_safe(change_id))
}

/// 一条晋级变更在 registry 里的镜像 tag。
pub fn promote_image_tag(change_id: &str) -> String {
    format!("{PROMOTE_IMAGE_TAG_PREFIX}{}", tag_safe(change_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 线上的拼法已经存在：盘上的 tag 与 registry 里推过的镜像都按它命名，所以
    /// 这里钉的是拼法本身，不是「函数等于自己」。
    #[test]
    fn the_promotion_names_do_not_drift() {
        assert_eq!(promote_git_tag("chg-1"), "promote/chg-1");
        assert_eq!(promote_image_tag("chg-1"), "cogneva:promote-chg-1");
    }

    /// 两端清洗方式一致才谈得上「同名」：这里钉住清洗的边界，改它等于改线上
    /// 已有的名字。
    #[test]
    fn punctuation_becomes_a_dash_and_the_rest_survives() {
        assert_eq!(tag_safe("p/1 x:y.z"), "p-1-x-y-z");
        assert_eq!(tag_safe("A_b-9"), "A_b-9");
        assert_eq!(
            promote_git_tag("p/1").trim_start_matches(PROMOTE_GIT_TAG_PREFIX),
            promote_image_tag("p/1").trim_start_matches(PROMOTE_IMAGE_TAG_PREFIX),
            "两个名字的清洗结果必须逐字相同，否则写下去的和找回来的不是同一条"
        );
    }
}
