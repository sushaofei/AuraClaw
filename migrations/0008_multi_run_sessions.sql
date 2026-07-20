BEGIN;

ALTER TABLE projection.task_view
    ADD COLUMN run_status text;

UPDATE projection.task_view
SET run_status = CASE
    WHEN status IN ('pending', 'runnable', 'running', 'waiting_for_human', 'paused',
                    'retry_wait', 'completed', 'failed', 'cancelled')
        THEN status
    ELSE NULL
END;

-- Before this migration a Root Session inherited the latest Run terminal state.
-- No explicit close event existed, so all historical Root terminal rows are resumable.
UPDATE projection.task_view
SET status = 'ready'
WHERE role = 'root' AND status IN ('completed', 'failed', 'cancelled');

COMMIT;
