BEGIN;
-- Downgrade only before upgrades are admitted; it cannot restore deleted package content.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM hands.skill_upgrade_current WHERE phase <> 'completed') THEN
        RAISE EXCEPTION 'Pending Skill upgrades must finish before schema downgrade';
    END IF;
END $$;
DROP TABLE IF EXISTS hands.skill_upgrade_current;
COMMIT;
