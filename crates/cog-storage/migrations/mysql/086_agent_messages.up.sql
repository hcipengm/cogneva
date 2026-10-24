-- Migration: 086_agent_messages
-- See the Postgres migration of the same name for why a sent command is kept
-- beside the stream that carried it.
CREATE TABLE IF NOT EXISTS agent_messages (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    from_unit VARCHAR(191) NOT NULL,
    to_unit VARCHAR(191) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    body LONGTEXT NOT NULL,
    outcome VARCHAR(16) NOT NULL,
    reply LONGTEXT NULL,
    outcome_detail LONGTEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_agent_messages_peer (to_unit, id),
    KEY idx_agent_messages_sender (from_unit, id)
);
