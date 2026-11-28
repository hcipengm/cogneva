//! 落地一笔只改了文档的提交时不重建：输入没变，重建付的是同一份验证的第二遍钱，
//! 而产出的字节与原来的那个二进制一模一样。
//!
//! 判错的方向只能是多编一次：判成「文档」的那一笔一旦其实改了代码，部署出去的二进制
//! 里就没有这一笔，而它在读数上与「这一笔没有改代码」一模一样。所以只有能证明与二进
//! 制无关的路径才算文档。
//!
//! 没有这次改动这两条会红：判断不存在、构建那一步不看它。

#[test]
fn the_deployer_asks_whether_the_change_can_reach_the_binary() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    let deployer =
        std::fs::read_to_string(root.join("crates/cog-reflection/src/evolution_deployer.rs"))
            .expect("evolution_deployer.rs");
    assert!(
        deployer.contains("fn documentation_only("),
        "没有「这一笔能不能影响到二进制」的判断"
    );
    assert!(
        deployer.contains("documentation_only(&paths)"),
        "构建那一步没有问这个判断：文档单仍然会重建"
    );
    assert!(
        deployer.contains("Only documentation changed"),
        "跳过的原因没有留一条读数：事后看不出这个二进制为什么没重建"
    );
    // 跳过的这一笔记成「没有构建发生」，不是「构建成功、耗时零」。
    assert!(
        deployer.contains("BuildEnding::Unstarted, None);")
            && deployer.contains("Only documentation changed"),
        "跳过被记成了一次成功的构建：读数里会多出一份没发生过的构建"
    );
}
