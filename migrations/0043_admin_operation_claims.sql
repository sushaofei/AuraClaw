BEGIN;

DO $$
DECLARE schema_name text;
BEGIN
    FOREACH schema_name IN ARRAY ARRAY['projection', 'delivery', 'artifact'] LOOP
        EXECUTE format(
            'ALTER TABLE %I.admin_operation
             ADD COLUMN IF NOT EXISTS tenant_id text,
             ADD COLUMN IF NOT EXISTS owner_service text,
             ADD COLUMN IF NOT EXISTS request_digest text,
             ADD COLUMN IF NOT EXISTS actor_identity text,
             ADD COLUMN IF NOT EXISTS correlation_id text,
             ADD COLUMN IF NOT EXISTS causation_id text,
             ADD COLUMN IF NOT EXISTS claimed_by text,
             ADD COLUMN IF NOT EXISTS claim_token text,
             ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
             ADD COLUMN IF NOT EXISTS started_at timestamptz,
             ADD COLUMN IF NOT EXISTS completed_at timestamptz,
             ADD COLUMN IF NOT EXISTS last_error_code text',
            schema_name
        );
        EXECUTE format(
            'UPDATE %I.admin_operation SET
             tenant_id=COALESCE(tenant_id, ''legacy''),
             owner_service=COALESCE(owner_service, %L),
             request_digest=COALESCE(request_digest, ''legacy:'' || operation_id),
             actor_identity=COALESCE(actor_identity, ''legacy''),
             correlation_id=COALESCE(correlation_id, operation_id),
             causation_id=COALESCE(causation_id, operation_id)',
            schema_name,
            CASE schema_name
                WHEN 'projection' THEN 'projection-worker'
                WHEN 'delivery' THEN 'delivery-worker'
                ELSE 'artifact-service'
            END
        );
        EXECUTE format(
            'ALTER TABLE %I.admin_operation
             ALTER COLUMN tenant_id SET NOT NULL,
             ALTER COLUMN owner_service SET NOT NULL,
             ALTER COLUMN request_digest SET NOT NULL,
             ALTER COLUMN actor_identity SET NOT NULL,
             ALTER COLUMN correlation_id SET NOT NULL,
             ALTER COLUMN causation_id SET NOT NULL',
            schema_name
        );
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I.admin_operation
             (status, claim_expires_at)',
            schema_name || '_admin_operation_claim_idx',
            schema_name
        );
    END LOOP;
END $$;

COMMIT;
