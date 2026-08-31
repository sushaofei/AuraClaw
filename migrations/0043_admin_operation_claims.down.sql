BEGIN;

DO $$
DECLARE schema_name text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY['projection', 'delivery', 'artifact'] LOOP
        EXECUTE format(
            'DROP INDEX IF EXISTS %I.%I',
            schema_name,
            schema_name || '_admin_operation_claim_idx'
        );
        EXECUTE format(
            'ALTER TABLE %I.admin_operation
             DROP COLUMN IF EXISTS tenant_id,
             DROP COLUMN IF EXISTS owner_service,
             DROP COLUMN IF EXISTS request_digest,
             DROP COLUMN IF EXISTS actor_identity,
             DROP COLUMN IF EXISTS correlation_id,
             DROP COLUMN IF EXISTS causation_id,
             DROP COLUMN IF EXISTS claimed_by,
             DROP COLUMN IF EXISTS claim_token,
             DROP COLUMN IF EXISTS claim_expires_at,
             DROP COLUMN IF EXISTS started_at,
             DROP COLUMN IF EXISTS completed_at,
             DROP COLUMN IF EXISTS last_error_code',
            schema_name
        );
    END LOOP;
END $$;

COMMIT;
