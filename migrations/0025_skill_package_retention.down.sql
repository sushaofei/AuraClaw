DO $$
BEGIN
    IF to_regclass('session_core.canonical_event') IS NOT NULL THEN
        EXECUTE 'DROP INDEX IF EXISTS session_core.canonical_event_skill_reference_idx';
    END IF;
END $$;

ALTER TABLE hands.skill_package
    DROP CONSTRAINT IF EXISTS skill_package_retention_revision_positive,
    DROP COLUMN IF EXISTS retention_updated_at,
    DROP COLUMN IF EXISTS retention_updated_by,
    DROP COLUMN IF EXISTS retention_revision,
    DROP COLUMN IF EXISTS legal_hold,
    DROP COLUMN IF EXISTS retention_until;
