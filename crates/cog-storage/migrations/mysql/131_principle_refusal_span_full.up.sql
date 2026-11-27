-- 同 postgres：展示列按列宽裁，事实列存完整原文。
ALTER TABLE principle_refusals ADD COLUMN span_full TEXT NOT NULL;
