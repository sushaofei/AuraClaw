BEGIN;
-- Finish already-completed upgrades that left unsigned rejection records behind.
-- Preserve current versions, registered packages, other tenants and later attempts.
DELETE FROM hands.skill_admission_audit audit
USING hands.skill_upgrade_current upgrade
WHERE upgrade.phase = 'completed'
  AND audit.tenant_id = upgrade.tenant_id
  AND audit.publisher = upgrade.publisher
  AND audit.name = upgrade.name
  AND audit.version <> upgrade.current_version
  AND audit.package_digest IS NULL
  AND audit.outcome = 'rejected'
  AND audit.occurred_at <= upgrade.updated_at
  AND NOT EXISTS (
    SELECT 1 FROM hands.skill_package package
    WHERE package.tenant_id = audit.tenant_id AND package.publisher = audit.publisher
      AND package.name = audit.name AND package.version = audit.version
  );
COMMIT;
