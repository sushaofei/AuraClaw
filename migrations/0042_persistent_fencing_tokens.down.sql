BEGIN;

DROP TABLE IF EXISTS hands.fencing_token_high_watermark;
DROP TABLE IF EXISTS control.fencing_token_high_watermark;
DROP TABLE IF EXISTS session_core.fencing_token_high_watermark;

COMMIT;
