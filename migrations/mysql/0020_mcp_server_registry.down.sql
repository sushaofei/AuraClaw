BEGIN;

DROP TABLE IF EXISTS `hands_mcp_server_operation`;
DROP TABLE IF EXISTS `hands_mcp_server_runtime`;
DROP TABLE IF EXISTS `hands_mcp_server_revision`;
DROP TABLE IF EXISTS `hands_mcp_server`;

SET @drop_new_check := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'hands_downstream_mcp_server'
              AND CONSTRAINT_NAME = 'downstream_mcp_server_endpoint_check'
        ),
        'ALTER TABLE `hands_downstream_mcp_server` DROP CHECK `downstream_mcp_server_endpoint_check`',
        'SELECT 1'
    )
);
PREPARE stmt FROM @drop_new_check;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE `hands_downstream_mcp_server`
    MODIFY COLUMN endpoint VARCHAR(64) NOT NULL;

SET @restore_https_check := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'hands_downstream_mcp_server'
              AND CONSTRAINT_TYPE = 'CHECK'
              AND CONSTRAINT_NAME = 'hands_downstream_mcp_server_chk_1'
        ),
        'SELECT 1',
        'ALTER TABLE `hands_downstream_mcp_server` ADD CONSTRAINT `hands_downstream_mcp_server_chk_1` CHECK (endpoint REGEXP ''^https://'')'
    )
);
PREPARE stmt FROM @restore_https_check;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

COMMIT;
