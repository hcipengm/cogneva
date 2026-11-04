//! 控制台日志的时间戳必须是容器本机时区，而且必须把偏移量印出来。
//!
//! 默认定时器按 UTC 打印、结尾写 `Z`。容器里没有本地时区时这看不出来任何问题——
//! 直到有人拿 `kubectl logs` 的一行去对账本（存 UTC）或对墙上的钟：09:41 与 17:41
//! 是同一件事，差额只存在于读它的人脑子里，而且没有任何一处读数说出这件事。
//!
//! 两条判据：
//!
//! * 一个时刻的写法是「本机钟面 ＋ 显式偏移」，读回来还是同一个瞬间；
//! * 生产用的那三处 `fmt::layer()` 都挂上了这个定时器。只测格式化函数是不够的：
//!   函数可以是对的，而某处 layer 忘了 `.with_timer`，那一处的日志照样按 UTC 印。

use std::io::Write;
use std::sync::{Arc, Mutex};

use chrono::{FixedOffset, TimeZone, Utc};
use cog_observability::logs::{local_stamp, LocalTime};
use tracing_subscriber::layer::SubscriberExt;

#[test]
fn a_stamp_carries_the_wall_clock_and_the_offset() {
    let east8 = FixedOffset::east_opt(8 * 3600).expect("+08:00 is a real offset");
    let instant = Utc
        .with_ymd_and_hms(2026, 11, 4, 9, 41, 29)
        .single()
        .expect("a fixed instant");

    let printed = local_stamp(instant.with_timezone(&east8));
    assert_eq!(
        printed, "2026-11-04T17:41:29.000000+08:00",
        "日志时间戳必须是本机钟面加显式偏移；印成 {printed} 的话，读它的人要么得自己\
         和 UTC 相减，要么根本不知道要减"
    );

    // 印出来的这一行必须自己说清是哪个瞬间：解析回来与原来的 UTC 时刻相等，
    // 才算这行没有歧义。
    let parsed = chrono::DateTime::parse_from_rfc3339(&printed).expect("rfc3339");
    assert_eq!(parsed.to_utc(), instant);

    // 同一瞬间换个偏移，钟面必须跟着变、瞬间必须不变——这判的是「偏移真的参与了
    // 换算」，不是把偏移原样抄在 UTC 钟面后面。
    let utc_form = local_stamp(instant.with_timezone(&FixedOffset::east_opt(0).unwrap()));
    assert_eq!(utc_form, "2026-11-04T09:41:29.000000+00:00");
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&utc_form)
            .expect("rfc3339")
            .to_utc(),
        instant
    );
}

/// `MakeWriter` 收的一格：把字节收进内存，好在断言里读。
#[derive(Clone)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_layer_built_with_this_timer_prints_an_offset_not_a_z() {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = sink.clone();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(LocalTime)
        .with_writer(move || Captured(writer.clone()));
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("一行日志，只为量它的时间戳");
    });

    let text = String::from_utf8(sink.lock().unwrap().clone()).expect("utf-8");
    let first = text
        .split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("这一行没有时间戳：{text:?}"));

    // `2026-11-04T17:41:29.123456+08:00`：26 个字符的钟面，后面 6 个字符是偏移。
    // 去掉定时器时这里是 `...Z`（27 个字符），落到下面第一条断言上。
    assert_eq!(
        first.len(),
        32,
        "时间戳 {first:?} 不是「钟面＋偏移」的形状：结尾的 `Z` 说明这一层仍在按 UTC 印"
    );
    let tail = &first[26..];
    let sign = tail.as_bytes()[0];
    assert!(
        sign == b'+' || sign == b'-',
        "时间戳 {first:?} 的偏移位置是 {tail:?}，不是 ±HH:MM"
    );
    assert_eq!(&tail[3..4], ":", "偏移 {tail:?} 少了冒号");
    assert!(
        chrono::DateTime::parse_from_rfc3339(first).is_ok(),
        "时间戳 {first:?} 解析不回一个瞬间"
    );
}

#[test]
fn every_console_layer_in_the_production_stack_carries_this_timer() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/logs.rs"))
        .expect("read logs.rs");

    // 注释里不出现这两个字面量（否则数出来的不是构造点）。
    let layers = source.matches("fmt::layer()").count();
    let timers = source.matches(".with_timer(").count();
    assert!(
        layers >= 3,
        "只找到 {layers} 处 fmt::layer()：这份门读的已经不是那三处控制台输出层"
    );
    assert_eq!(
        layers, timers,
        "{layers} 处 fmt::layer() 里只有 {timers} 处挂上了 with_timer：\
         没挂的那一处仍按 UTC 印时刻，而它与挂了的那几处并排出现在同一个日志流里"
    );
}

#[test]
fn the_default_timer_really_was_utc_so_this_timer_is_doing_work() {
    // 这条不是判据，是上面那条的对照：默认定时器在同一格上印 `Z`。
    // 少了它，一个被误删的 with_timer 只会让两条断言「都通过」。
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = sink.clone();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(move || Captured(writer.clone()));
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("同一行日志，默认定时器");
    });

    let text = String::from_utf8(sink.lock().unwrap().clone()).expect("utf-8");
    let first = text.split_whitespace().next().expect("timestamp");
    assert!(
        first.ends_with('Z'),
        "默认定时器此刻印的是 {first:?}，不再是 UTC 的 `Z`：\
         上一条判据的对照失去了意义，得先弄清这个依赖换成了什么口径"
    );
}
