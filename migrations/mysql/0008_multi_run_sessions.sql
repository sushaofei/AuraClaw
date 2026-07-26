BEGIN;

-- Idempotent on fresh DBs: column may already exist when replaying repairs.
-- Fresh installs from 0001..0007 will add the column here.
ALTER TABLE `projection_task_view`
    ADD COLUMN run_status VARCHAR(64) NULL;

UPDATE `projection_task_view`
SET run_status = CASE
    WHEN status IN ('pending', 'runnable', 'running', 'waiting_for_human', 'paused',
                    'retry_wait', 'completed', 'failed', 'cancelled')
        THEN status
    ELSE run_status
END;

UPDATE `projection_task_view`
SET status = 'ready'
WHERE role = 'root' AND status IN ('completed', 'failed', 'cancelled');

COMMIT;
