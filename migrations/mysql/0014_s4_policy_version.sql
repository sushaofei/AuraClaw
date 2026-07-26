BEGIN;

CREATE TABLE IF NOT EXISTS `policy_active_bundle` (
    singleton TINYINT(1) PRIMARY KEY DEFAULT 1,
    policy_version VARCHAR(64) NOT NULL,
    activated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CHECK (singleton = 1)
);

COMMIT;
