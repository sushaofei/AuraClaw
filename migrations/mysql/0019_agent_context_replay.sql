CREATE TABLE IF NOT EXISTS `security_agent_context_replay` (
    `jti_hash` varchar(64) NOT NULL,
    `command_id` varchar(255) NOT NULL,
    `expires_at` datetime(6) NOT NULL,
    PRIMARY KEY (`jti_hash`),
    KEY `idx_agent_context_replay_expiry` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
