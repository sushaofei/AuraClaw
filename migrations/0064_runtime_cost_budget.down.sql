BEGIN;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM model_gateway.run_cost_budget WHERE cost_reserved > 0) THEN
  RAISE EXCEPTION 'Cannot remove unresolved cost reservations';
 END IF;
END $$;
DROP TABLE model_gateway.run_cost_budget;
ALTER TABLE model_gateway.model_call DROP COLUMN cost_reservation;
ALTER TABLE projection.task_view DROP COLUMN runtime_budget;
COMMIT;
