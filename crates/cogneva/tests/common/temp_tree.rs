//! 把一棵夹具树写进临时目录，再把判词函数跑在它上面：消融用例的同一个开头。
//!
//! 两份部署面判据的消融都是这么搭的：把（相对路径，正文）写进一个新目录，然后拿
//! 同一份读取器去读它。这段开头在两边各写了一遍——建目录、写文件、跑判词——而它
//! 一旦分叉（一边忘了 `create_dir_all`、一边换了临时目录的生命周期），两份消融证明
//! 的就不是同一件事了：都还是绿的，只是绿的来源不同。

// 这一份按「每个测试二进制各判一次」编译：只有搭临时树的那几个夹具用它，其余的把
// 它编进来却一次都不调。去掉这个 allow 等于要求每份夹具都写一遍自己的临时树。
#![allow(dead_code)]

use std::path::Path;

/// 把 `files` 写进一个新的临时目录，然后交给 `judge` 判，返回它的抱怨。
///
/// 目录在返回前就被删掉：抱怨里的路径只是给人看的，用例不该依赖它还在盘上。
/// 写不进去就 panic——一棵没搭起来的夹具树会让判词在空树上跑，而空树对大多数
/// 判据都是「没有问题」。
pub fn judge(files: &[(&str, &str)], judge: impl FnOnce(&Path) -> Vec<String>) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (path, body) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().expect("parent")).expect("dir");
        std::fs::write(full, body).expect("write");
    }
    judge(dir.path())
}
