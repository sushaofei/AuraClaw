BEGIN;

DROP TABLE IF EXISTS hands.skill_publisher_command;
DROP INDEX IF EXISTS hands.skill_publisher_one_active_key_idx;
DROP TABLE IF EXISTS hands.skill_publisher_key;
DROP TABLE IF EXISTS hands.skill_publisher;

COMMIT;
