-- 这一列本构建从不写也不读：插入语句里没有它，查询列清单里也没有它。
-- 库只存列、且每列都要有读者；没人读的列不是「预留」，是一条不该存在的形状。
ALTER TABLE notifications DROP COLUMN IF EXISTS data;
