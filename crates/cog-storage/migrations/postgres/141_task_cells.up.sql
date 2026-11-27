-- 待办的三格升成任务定义的列：判据、界面与定批都按名读它们，埋在 input 的
-- 文本载荷里不可查询。最后一版定义带的是此刻的三格；改终态仍走
-- task/terminal_revised（带理由），不在这里改历史。
ALTER TABLE task_defined ADD COLUMN IF NOT EXISTS terminal TEXT NOT NULL DEFAULT '';
ALTER TABLE task_defined ADD COLUMN IF NOT EXISTS now      TEXT NOT NULL DEFAULT '';
ALTER TABLE task_defined ADD COLUMN IF NOT EXISTS gap      TEXT NOT NULL DEFAULT '';
