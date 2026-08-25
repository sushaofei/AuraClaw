BEGIN;

-- MySQL auto-names CHECK constraints. Drop any CHECK on endpoint before widening.
SET @drop_endpoint_check := (
    SELECT CONCAT('ALTER TABLE `hands_downstream_mcp_server` DROP CHECK `', CONSTRAINT_NAME, '`')
    FROM information_schema.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA = DATABASE()
      AND TABLE_NAME = 'hands_downstream_mcp_server'
      AND CONSTRAINT_TYPE = 'CHECK'
      AND CONSTRAINT_NAME <> 'downstream_mcp_server_endpoint_check'
    LIMIT 1
);
SET @drop_endpoint_check := IFNULL(@drop_endpoint_check, 'SELECT 1');
PREPARE stmt FROM @drop_endpoint_check;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE `hands_downstream_mcp_server`
    MODIFY COLUMN endpoint VARCHAR(512) NOT NULL;

SET @add_endpoint_check := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'hands_downstream_mcp_server'
              AND CONSTRAINT_NAME = 'downstream_mcp_server_endpoint_check'
        ),
        'SELECT 1',
        'ALTER TABLE `hands_downstream_mcp_server` ADD CONSTRAINT downstream_mcp_server_endpoint_check CHECK (endpoint REGEXP ''^https?://'')'
    )
);
PREPARE stmt FROM @add_endpoint_check;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS `hands_mcp_server` (
    server_id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64),
    desired_state VARCHAR(32) NOT NULL,
    latest_revision INT NOT NULL,
    active_revision INT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CHECK (desired_state IN ('disabled', 'enabled', 'retired')),
    CHECK (latest_revision >= 0)
);

CREATE TABLE IF NOT EXISTS `hands_mcp_server_revision` (
    server_id VARCHAR(128) NOT NULL,
    revision INT NOT NULL,
    config_json json NOT NULL,
    config_digest VARCHAR(64) NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (server_id, revision),
    CONSTRAINT mcp_server_revision_server_fk
        FOREIGN KEY (server_id) REFERENCES `hands_mcp_server` (server_id)
);

CREATE TABLE IF NOT EXISTS `hands_mcp_server_runtime` (
    server_id VARCHAR(128) PRIMARY KEY,
    loaded_revision INT NULL,
    observed_state VARCHAR(32) NOT NULL,
    last_test_at datetime(6) NULL,
    last_sync_at datetime(6) NULL,
    consecutive_failures INT NOT NULL DEFAULT 0,
    safe_error_code VARCHAR(64),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT mcp_server_runtime_server_fk
        FOREIGN KEY (server_id) REFERENCES `hands_mcp_server` (server_id)
);

CREATE TABLE IF NOT EXISTS `hands_mcp_server_operation` (
    operation_id VARCHAR(64) PRIMARY KEY,
    server_id VARCHAR(128) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    target_revision INT NULL,
    command_id VARCHAR(256) NOT NULL,
    actor_id VARCHAR(128) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    causation_id VARCHAR(128) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    safe_error_code VARCHAR(64),
    result_json json NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at datetime(6) NULL,
    UNIQUE KEY mcp_server_operation_command_uidx (tenant_id, command_id)
);

SET @create_tenant_idx := (
    SELECT IF(
        EXISTS(
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'hands_mcp_server'
              AND index_name = 'mcp_server_tenant_idx'
        ),
        'SELECT 1',
        'CREATE INDEX mcp_server_tenant_idx ON `hands_mcp_server` (tenant_id, desired_state)'
    )
);
PREPARE stmt FROM @create_tenant_idx;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @create_operation_idx := (
    SELECT IF(
        EXISTS(
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'hands_mcp_server_operation'
              AND index_name = 'mcp_server_operation_server_idx'
        ),
        'SELECT 1',
        'CREATE INDEX mcp_server_operation_server_idx ON `hands_mcp_server_operation` (server_id, created_at)'
    )
);
PREPARE stmt FROM @create_operation_idx;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

COMMIT;
