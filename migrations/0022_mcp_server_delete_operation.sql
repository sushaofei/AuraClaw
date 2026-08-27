BEGIN;

ALTER TABLE hands.mcp_server_operation
    DROP CONSTRAINT IF EXISTS mcp_server_operation_operation_check;

ALTER TABLE hands.mcp_server_operation
    ADD CONSTRAINT mcp_server_operation_operation_check
    CHECK (
        operation IN (
            'create',
            'update',
            'test',
            'enable',
            'disable',
            'reconcile',
            'retire',
            'delete'
        )
    );

COMMIT;
