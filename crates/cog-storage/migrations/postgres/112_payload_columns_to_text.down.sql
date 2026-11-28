-- 撤回 112：载荷列回到 jsonb。
--
-- 文本都是 JSON 文档，`::jsonb` 解析得回去；解析不了的（迁移之后写进来的
-- 非 JSON 文本）会让这次回滚失败，而不是让它静默丢掉那些行。
ALTER TABLE explainability ALTER COLUMN data DROP DEFAULT;
ALTER TABLE explainability ALTER COLUMN data TYPE JSONB USING data::jsonb;
ALTER TABLE explainability ALTER COLUMN data SET DEFAULT '{}'::jsonb;

ALTER TABLE explainability_part ALTER COLUMN data DROP DEFAULT;
ALTER TABLE explainability_part ALTER COLUMN data TYPE JSONB USING data::jsonb;
ALTER TABLE explainability_part ALTER COLUMN data SET DEFAULT '{}'::jsonb;

ALTER TABLE resource_events ALTER COLUMN detail DROP DEFAULT;
ALTER TABLE resource_events ALTER COLUMN detail TYPE JSONB USING detail::jsonb;
ALTER TABLE resource_events ALTER COLUMN detail SET DEFAULT '{}'::jsonb;

ALTER TABLE token_face_beat ALTER COLUMN measures TYPE JSONB USING measures::jsonb;

ALTER TABLE audit_logs ALTER COLUMN details DROP DEFAULT;
ALTER TABLE audit_logs ALTER COLUMN details TYPE JSONB USING details::jsonb;
ALTER TABLE audit_logs ALTER COLUMN details SET DEFAULT '{}'::jsonb;

-- `actor` / `project_id` 不拆：它们是从 details 里升出来的，放着不影响谁读
-- details，而删掉会连迁移之后写下的行一起丢掉。要真正回到旧形状，先把它们
-- 并回 details 再删。
DROP INDEX IF EXISTS idx_audit_logs_project;
CREATE INDEX IF NOT EXISTS idx_audit_logs_project ON audit_logs ((details ->> 'project_id'));
