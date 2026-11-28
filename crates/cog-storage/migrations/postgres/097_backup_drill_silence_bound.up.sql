-- Migration: 097_backup_drill_silence_bound
-- 这一趟演练是**按哪个静默界在跑**，写在这一行上。
--
-- 判停的界不在部署里，在 progress_bounds 里（096）。于是「这一趟有没有界、界是多少」
-- 只剩那张表能不能查到那一行——而那张表只有当前值，没有历史：下一趟收尾会把它覆盖。
-- 一次被自己的判据停下的演练，事后读账本只能从 error 文本里抠出那个数，其它任何一趟
-- （跑成功的、被外部杀掉的、从没跑过的）都读不出它当时用的是哪个界。
--
-- 所以这一行记一份判据在写行时的样子，词与进度文件里那个词是同一份
-- （cog_core::declared_wall::Bound::words）：
--   '<秒数>'    —— 这个作业类有一条实测界，这一趟按它判
--   'unbounded' —— 还没有人量过，这一趟没有界可判（不判停，等它自己结束）
--
-- NULL 是「这一列存在之前写的行」，不是「unbounded」。把两者并成一个值，会让
-- 「从没量过」与「老行没记」在读数面上同形，而这正是这一整套要拆掉的那种同形。
ALTER TABLE backup_drills
    ADD COLUMN IF NOT EXISTS silence_bound TEXT;

ALTER TABLE backup_drills
    DROP CONSTRAINT IF EXISTS backup_drills_silence_bound_is_a_reading;

ALTER TABLE backup_drills
    ADD CONSTRAINT backup_drills_silence_bound_is_a_reading
    CHECK (
        silence_bound IS NULL
        OR silence_bound = 'unbounded'
        OR silence_bound ~ '^[1-9][0-9]*$'
    );

COMMENT ON COLUMN backup_drills.silence_bound IS
    '判停用的静默界（秒数字符串或 ``unbounded``），写行时那一道；NULL ＝ 这一列存在之前的行';
