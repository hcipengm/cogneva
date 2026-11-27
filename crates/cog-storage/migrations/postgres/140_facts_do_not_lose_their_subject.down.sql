-- Migration: 140_facts_do_not_lose_their_subject (down)
ALTER TABLE IF EXISTS files
    DROP CONSTRAINT IF EXISTS files_workspace_id_fkey;
ALTER TABLE IF EXISTS files
    ADD CONSTRAINT files_workspace_id_fkey
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL;
ALTER TABLE IF EXISTS audit_logs
    ADD CONSTRAINT audit_logs_user_id_fkey
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE IF EXISTS billing_records
    ADD CONSTRAINT billing_records_workspace_id_fkey
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL;
