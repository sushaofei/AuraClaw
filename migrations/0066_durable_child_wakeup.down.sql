BEGIN;

ALTER TABLE control.assignment
    DROP COLUMN wake_pending;

COMMIT;
