-- Migration: 098_backup_drill_attempts
-- 一趟演练**开过工**这件事，从它开工那一刻起就落一行。
--
-- backup_drills 的行是一趟的结论，写在收尾时。于是一个被容器外探针杀掉的进程什么都
-- 不留：那一夜在账上看起来与「根本没跑」一模一样，而它其实跑到了半路、把一条临时库
-- 建了一半、也可能已经把某一块数据搬完了。这不是一个可以靠「再等一晚」读出来的差别：
-- 「没跑」要修的是调度，「跑到半路被杀」要修的是判据或资源。
--
-- 开工先写一行，收尾再把它合上。于是三种形状分开：
--   ended_by = 'itself'      —— 这一趟自己走到了收尾（ok 或 failed 在 backup_drills 里）
--   ended_by = 'never_seen'  —— 开过工，结局没被任何人看见；下一次开工时才由账本合上
--   ended_by 为空            —— 还在飞（或进程刚没，还没人扫到）
--   没有这一行                —— 这一趟从来没开工（老行也没有这一行）
--
-- 「被外部杀掉」与「自己判停」因此不再同形：后者留一行 failed，前者留一行
-- `never_seen` 并带上它开工的时刻与当时那个界。
--
-- 谁来合上 `never_seen`：下一趟开工。和 `backup_runs` 的 `abandon_overdue` 同一个
-- 道理——被杀掉的进程回不来合自己那一行，而一行永远开着就等于说「它还在跑」。账本写
-- 这个结论时必须标明是它写的（`ended_by`），不能借用这一趟自己的名义。
--
-- 这一行只记两样读数，不记结论：它证明不了任何一份包可恢复，那是 backup_drills 的事。
-- 它回答的是另一个问题——这一趟当时是**按哪个静默界在跑**（progress_bounds 只有当前值，
-- 下一趟就覆盖；backup_drills 只在有结论时才写得出这一格）。
CREATE TABLE IF NOT EXISTS backup_drill_attempts (
    drill_id              TEXT PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    opening_silence_bound TEXT NOT NULL,
    finished_at           TIMESTAMPTZ,
    ended_by              TEXT,
    CONSTRAINT backup_drill_attempts_bound_is_a_reading
        CHECK (
            opening_silence_bound = 'unbounded'
            OR opening_silence_bound ~ '^[1-9][0-9]*$'
        ),
    -- 谁写下了这个结尾。`itself` 是这一趟自己；`never_seen` 是账本在下一趟开工时替它
    -- 合上的——那一趟的结尾没有任何人看见。两者分开，才不会把一次被杀读成一次判停。
    CONSTRAINT backup_drill_attempts_ended_by_is_known
        CHECK (ended_by IS NULL OR ended_by IN ('itself', 'never_seen')),
    -- 有结尾就必须说清是谁写的；没有结尾就不能装作有。
    CONSTRAINT backup_drill_attempts_end_is_one_fact
        CHECK ((finished_at IS NULL) = (ended_by IS NULL)),
    CONSTRAINT backup_drill_attempts_finish_is_not_before_its_start
        CHECK (finished_at IS NULL OR finished_at >= started_at)
);

-- 读的人问的是「有没有一趟开工了却没合上」。只扫没合上的那些行。
CREATE INDEX IF NOT EXISTS backup_drill_attempts_open_idx
    ON backup_drill_attempts (started_at)
    WHERE ended_by IS NULL;
