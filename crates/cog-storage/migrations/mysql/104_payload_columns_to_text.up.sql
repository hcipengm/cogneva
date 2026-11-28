-- Migration: 104_payload_columns_to_text
--
-- PostgreSQL 侧 112 的同一条决定。这一侧不能就地 `MODIFY` 一个 JSON 列成别的
-- 类型，所以每一列都是「加新列 → 拷值 → 删旧列 → 改名」：中间没有一步让库处在
-- 「旧的还在、新的没到」的状态，而 `CAST(... AS CHAR)` 写出的是 MySQL 自己那份
-- JSON 文本，读回来还是同一份文档。
ALTER TABLE explainability ADD COLUMN data_text TEXT;
UPDATE explainability SET data_text = CAST(data AS CHAR);
ALTER TABLE explainability DROP COLUMN data;
ALTER TABLE explainability CHANGE COLUMN data_text data TEXT;

ALTER TABLE resource_events ADD COLUMN detail_text TEXT;
UPDATE resource_events SET detail_text = CAST(detail AS CHAR);
ALTER TABLE resource_events DROP COLUMN detail;
ALTER TABLE resource_events CHANGE COLUMN detail_text detail TEXT NOT NULL;

ALTER TABLE token_face_beat ADD COLUMN measures_text TEXT;
UPDATE token_face_beat SET measures_text = CAST(measures AS CHAR);
ALTER TABLE token_face_beat DROP COLUMN measures;
ALTER TABLE token_face_beat CHANGE COLUMN measures_text measures TEXT NOT NULL;

-- audit_logs：两个被按值筛过的字段升成真列，before/after 跟着转 text。
ALTER TABLE audit_logs ADD COLUMN actor TEXT;
ALTER TABLE audit_logs ADD COLUMN project_id TEXT;
UPDATE audit_logs
   SET actor = JSON_UNQUOTE(JSON_EXTRACT(details, '$.actor')),
       project_id = JSON_UNQUOTE(JSON_EXTRACT(details, '$.project_id'))
 WHERE details IS NOT NULL
   AND (actor IS NULL OR project_id IS NULL);
UPDATE audit_logs
   SET details = JSON_OBJECT('before', JSON_EXTRACT(details, '$.before'),
                             'after', JSON_EXTRACT(details, '$.after'))
 WHERE JSON_TYPE(details) = 'OBJECT'
   AND (JSON_CONTAINS_PATH(details, 'one', '$.actor')
        OR JSON_CONTAINS_PATH(details, 'one', '$.project_id'));
ALTER TABLE audit_logs ADD COLUMN details_text TEXT;
UPDATE audit_logs SET details_text = CAST(details AS CHAR);
ALTER TABLE audit_logs DROP COLUMN details;
ALTER TABLE audit_logs CHANGE COLUMN details_text details TEXT NOT NULL DEFAULT ('{}');

CREATE INDEX idx_audit_logs_project ON audit_logs(project_id);
