BEGIN;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM hands.skill_upgrade_current WHERE phase <> 'completed') THEN
  RAISE EXCEPTION 'Skill upgrade cleanup is still pending';
 END IF;
 IF EXISTS (SELECT 1 FROM hands.skill_command WHERE version IS NULL OR package_digest IS NULL) THEN
  RAISE EXCEPTION 'Removed Skill command material cannot be restored';
 END IF;
END $$;
DROP TABLE IF EXISTS hands.skill_upgrade_claim;
ALTER TABLE hands.skill_command ALTER COLUMN version SET NOT NULL;
ALTER TABLE hands.skill_command ALTER COLUMN package_digest SET NOT NULL;
COMMIT;
