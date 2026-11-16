-- Migration: 112_payload_columns_to_text
--
-- 库只存列：开集载荷存 text；要按某个键查，就给它加列。
--
-- 这一批是「整块读」的那几列：写进去一个结构、读回来是同一个结构，没有任何一条
-- SQL 按里面的键筛过行。整块读的东西存 text 就够——它不再假装可查询，也就没有
-- 人会照着 `data->>'x'` 写一条打错键只返回零行的查询。
--
-- 每一条都先 `DROP DEFAULT` 再 `TYPE` 再 `SET DEFAULT`：jsonb 的默认值
-- （`'{}'::jsonb`）不会自动转成 text，直接 TYPE 会以「default cannot be cast
-- automatically」失败——而它失败在迁移里，等于整个部署升不上去。

-- 解释链：`{reasoning_chain, metadata}` 只有一个读者，它把两者都解出来当字段用。
ALTER TABLE explainability ALTER COLUMN data DROP DEFAULT;
ALTER TABLE explainability ALTER COLUMN data TYPE TEXT USING data::text;
ALTER TABLE explainability ALTER COLUMN data SET DEFAULT '{}';

-- 023 建的那张按月分区表：父表上的 ALTER 会下发到已挂上的每一个分区。
ALTER TABLE explainability_part ALTER COLUMN data DROP DEFAULT;
ALTER TABLE explainability_part ALTER COLUMN data TYPE TEXT USING data::text;
ALTER TABLE explainability_part ALTER COLUMN data SET DEFAULT '{}';

-- 资源事件的自由字段：读者读整块，筛选靠它旁边的 subject / reading / outcome。
ALTER TABLE resource_events ALTER COLUMN detail DROP DEFAULT;
ALTER TABLE resource_events ALTER COLUMN detail TYPE TEXT USING detail::text;
ALTER TABLE resource_events ALTER COLUMN detail SET DEFAULT '{}';

-- 每拍的度量：056 的注释当时就写着「measures 在 JSON 里，所以索引不进去」——
-- 一句「不可查询」的自述。换成 text 只是让这句话在类型上也成立。
ALTER TABLE token_face_beat ALTER COLUMN measures TYPE TEXT USING measures::text;

-- audit_logs：`details` 里有两个字段是被**按值筛**过的——项目时间线按
-- `details->>'project_id'` 取行、把 `details->>'actor'` 当一列读。它们升成真列，
-- 剩下的 before/after 没有查询、只整块读，跟着转 text。
-- 先拆那条表达式索引（`(details->>'project_id')`），再改类型。
--
-- 它在 store 的 `DDL` 里建，迁移链里没有它——空库重放时它不存在，所以那个环境看不出
-- 问题；而真库上它在，`ALTER COLUMN TYPE` 会重建依赖它的索引，重建时 `->>` 的左边
-- 已经变成 text，语句以 `operator does not exist: text ->> text` 失败。实测于 2026-11-16。
DROP INDEX IF EXISTS idx_audit_logs_project;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS actor TEXT;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS project_id TEXT;
UPDATE audit_logs
   SET actor = details ->> 'actor',
       project_id = details ->> 'project_id'
 WHERE details IS NOT NULL
   AND (actor IS NULL OR project_id IS NULL);
-- 升成列之后，文档里再留着这两格就是同一个事实两个写者：改一处不改另一处，
-- 读的人按哪一处都得不出「这一行说的是什么」。
UPDATE audit_logs
   SET details = jsonb_build_object('before', details -> 'before',
                                    'after', details -> 'after')
 WHERE jsonb_typeof(details) = 'object'
   AND (details ? 'actor' OR details ? 'project_id');
ALTER TABLE audit_logs ALTER COLUMN details DROP DEFAULT;
ALTER TABLE audit_logs ALTER COLUMN details TYPE TEXT USING details::text;
ALTER TABLE audit_logs ALTER COLUMN details SET DEFAULT '{}';

-- 那条表达式索引换成一个真列上的索引。父表上的 CREATE 会把索引下发给分区。
CREATE INDEX IF NOT EXISTS idx_audit_logs_project ON audit_logs(project_id);
