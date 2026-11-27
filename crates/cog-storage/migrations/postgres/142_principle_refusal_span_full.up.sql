-- 拒绝的原文不再被截断：`span` 保留原来的 512 字符展示列，完整原文进 `span_full`。
--
-- 原先写入时按列宽 `VARCHAR(512)` 截断，超出的部分是「这条拒绝当时的证据」，丢了
-- 就没有第二份。展示列仍按列宽裁，事实列不裁：一份是给人看的读数，一份是事实。
ALTER TABLE principle_refusals ADD COLUMN IF NOT EXISTS span_full TEXT NOT NULL DEFAULT '';
