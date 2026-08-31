BEGIN;

DROP INDEX IF EXISTS delivery.sink_circuit_open_idx;
DROP TABLE IF EXISTS delivery.sink_circuit_state;

COMMIT;
