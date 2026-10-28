//! 一份多文档 YAML 的读法：每一篇都要解析出来，解析不了就停。
//!
//! 这段规则短到每份读清单的夹具都顺手写一遍，而抄件对失败的处理各不相同：
//! 有的 `if let Ok(..)` 跳过这一篇，有的 `filter_map` 把整份文件丢掉。跳过的那一篇
//! 从此不在人口里，剩下的那批照样断言「都对得上」——「它有冲突但没读进来」与
//! 「它干净」在判词上同形。所以这里只留一份，且读不出来就 panic。
//!
//! 判据在 `the_fixture_reading_steps_are_written_once_test`。

#![allow(dead_code)]

use serde::Deserialize;

/// `text` 里的每一篇 YAML 文档，按出现顺序；空文档（`---` 之间什么都没有）不算一篇。
///
/// 解析不出来的文档是**失败**，不是跳过：丢掉它就把它带的规则一起丢掉了，闸门会在一片
/// 它从没读过的输入上报绿。（`serde_yaml` 在文档反序列化失败时不推进游标，所以把错误
/// 丢掉不只是有损——它可能让同一篇反复被读，也可能让读取停在这里。）
///
/// `file` 只用来写进 panic：解析失败时要点名是哪份文件，否则判据只报「有一篇文档
/// 解析不出来」，而读的是几百份清单，读者不知道该翻哪一份。
pub fn docs(text: &str, file: &str) -> Vec<serde_yaml::Value> {
    let mut out: Vec<serde_yaml::Value> = Vec::new();
    for document in serde_yaml::Deserializer::from_str(text) {
        match serde_yaml::Value::deserialize(document) {
            Ok(value) if value.is_null() => {}
            Ok(value) => out.push(value),
            Err(error) => panic!(
                "{file}: 有一篇文档解析不出来（{error}）：解析不了是判据的形态问题，\
                 不能当它不存在"
            ),
        }
    }
    out
}
