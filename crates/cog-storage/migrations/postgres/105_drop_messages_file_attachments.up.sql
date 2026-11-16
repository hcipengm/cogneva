-- 这一列没有任何读者，也没有任何写者：它从建表起就只有一个默认值。
-- 库只存列、且每列都要有读者；没人读的列不是「暂时空着」，是一条不该存在的形状。
ALTER TABLE IF EXISTS messages DROP COLUMN IF EXISTS file_attachments;
