//! 一条流该不该收尾，量的是**间隔**，不是它已经跑了多久。
//!
//! 曾经两个 provider 各有一个「整段流的硬上限」：到点就把还在吐字的流掐断，
//! 已经收到的部分连同它马上要发的那一段一起扔掉。长推理的上游正是被这条钟
//! 掐死的对象——它没有卡住，它只是说得慢。
//!
//! 这条测试读源码，因为它要守的是一个**不能被写回来的东西**：只要有人在任一个
//! provider 里重新引入整段上限，这里就红。行为层面测不到它——旧上限是十分钟，
//! 夹具不可能真等十分钟才证明它不该开枪。

use std::path::Path;

fn provider_source(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("provider")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// 整段上限的两种写法：一个总时长的常数，或者把已有的界夹进「剩余总时长」。
#[test]
fn no_provider_caps_the_total_length_of_a_stream() {
    let banned = [
        ("STREAM_TOTAL_TIMEOUT", "整段流的硬上限"),
        ("CHAT_CONSUME_TIMEOUT", "chat() 消费整条流的上限"),
    ];
    for provider in ["openai.rs", "responses.rs"] {
        let src = provider_source(provider);
        for (symbol, what) in banned {
            assert!(
                !src.contains(symbol),
                "{provider} 里又出现了 {symbol}（{what}）：\
                 一条仍在吐字的流会在钟点被掐断，已经产出的部分随之作废"
            );
        }
        // 「把总时长减去已用时间」是整段上限的另一种形状：它不需要那个常数，
        // 只要有一个开始时刻就够了。
        assert!(
            !src.contains("total_timeout"),
            "{provider} 里仍在按总时长截断流；间隔才是进展的度量"
        );
    }
}

/// 收尾的两个判据都还在，而且首事件之前比之后宽：长推理窗口是上游在想，
/// 不是上游卡住。少一条，就会重新退化成「一律等满一个数」。
#[test]
fn both_gaps_are_declared_and_the_first_one_is_the_wider() {
    let src = provider_source("mod.rs");
    assert!(src.contains("STREAM_FIRST_EVENT_GAP"));
    assert!(src.contains("STREAM_EVENT_TIMEOUT"));
    assert!(
        cog_llm::provider::STREAM_FIRST_EVENT_GAP > cog_llm::provider::STREAM_EVENT_TIMEOUT,
        "首事件之前的静默窗口必须比之后宽，否则长推理窗口会被当成卡死"
    );
}
