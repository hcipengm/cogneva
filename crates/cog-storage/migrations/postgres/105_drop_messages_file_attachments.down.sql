ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_attachments JSONB NOT NULL DEFAULT '[]'::jsonb;
