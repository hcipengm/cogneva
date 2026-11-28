-- Migration: 096_progress_bounds
-- 一个作业类「多久没有一处推进算停」，从这里读，不从部署里写死的数读。
--
-- 在被它替换掉的那套里，判停是一个挑出来的钟（备份 Job 的 activeDeadlineSeconds、
-- 演练的单步静默 3600 s）。判据换成「还有没有在搬」之后，界本身也只能是一个读数：
-- 这个作业类成功跑过的那些趟里，最长的一次间隔有多长。它随作业长大而变化，所以每趟
-- 收尾写回自己量到的值；它也会随作业变快而变小，所以写的是这一跑的读数，不是历史
-- 最大值——一个只会变宽的界和常量是同一种毛病。
--
-- 没有行 ＝ 还没有一条实测。那时不判停（见 crates/cogneva/src/backup.rs 与
-- crates/cog-core/src/progress.rs），而不是拿一个默认数顶上：默认数会让「量不到」
-- 与「量到就是这个值」在读数面上同形，那正是这一整套要拆掉的东西。
--
-- 一行一个作业类，不按实例分：界是这一类工作的形状，不是某一次运行的形状；每一次
-- 运行自己的读数由备份账本里那一行记着。

CREATE TABLE IF NOT EXISTS progress_bounds (
    job_class            TEXT PRIMARY KEY,
    longest_gap_seconds  DOUBLE PRECISION NOT NULL CHECK (longest_gap_seconds > 0),
    samples              BIGINT NOT NULL CHECK (samples >= 0),
    observed_at          TIMESTAMPTZ NOT NULL
);
