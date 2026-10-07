BEGIN;

ALTER TABLE control.assignment
    ADD COLUMN wake_pending boolean NOT NULL DEFAULT false;

COMMIT;
