-- Migration: 016_notifications
-- In-app notifications

CREATE TABLE IF NOT EXISTS notifications (
    id BINARY(16) PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    ntype ENUM('task_completed', 'quota_warning', 'mention', 'system') NOT NULL,
    title VARCHAR(256) NOT NULL,
    body TEXT,
    data JSON NOT NULL DEFAULT (JSON_OBJECT()),
    is_read BOOLEAN NOT NULL DEFAULT false,
    read_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id, created_at DESC),
    INDEX idx_unread (user_id, is_read),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
