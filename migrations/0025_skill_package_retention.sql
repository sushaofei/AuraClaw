ALTER TABLE hands.skill_package
    ADD COLUMN IF NOT EXISTS retention_until timestamptz,
    ADD COLUMN IF NOT EXISTS legal_hold boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS retention_revision integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS retention_updated_by text,
    ADD COLUMN IF NOT EXISTS retention_updated_at timestamptz;

UPDATE hands.skill_package
SET retention_until = COALESCE(retention_until, created_at + interval '90 days'),
    retention_updated_by = COALESCE(retention_updated_by, 'migration'),
    retention_updated_at = COALESCE(retention_updated_at, created_at);

ALTER TABLE hands.skill_package
    ALTER COLUMN retention_until SET NOT NULL,
    ALTER COLUMN retention_updated_by SET NOT NULL,
    ALTER COLUMN retention_updated_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'skill_package_retention_revision_positive'
    ) THEN
        ALTER TABLE hands.skill_package
            ADD CONSTRAINT skill_package_retention_revision_positive
            CHECK (retention_revision >= 1);
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('session_core.canonical_event') IS NOT NULL THEN
        EXECUTE $migration$
            CREATE INDEX IF NOT EXISTS canonical_event_skill_reference_idx
            ON session_core.canonical_event (tenant_id, event_type)
            WHERE event_type = 'skill.activated'
        $migration$;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('artifact.metadata') IS NOT NULL THEN
        EXECUTE $migration$
            UPDATE artifact.metadata metadata
            SET retention_until = package.retention_until
            FROM hands.skill_package package
            WHERE metadata.tenant_id = package.tenant_id
              AND metadata.artifact_id = package.artifact_ref->>'artifact_id'
              AND metadata.retention_until IS NULL
        $migration$;
    END IF;
END $$;
