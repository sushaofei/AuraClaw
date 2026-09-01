-- Data-only consistency cleanup is intentionally irreversible. Restoring a
-- provider requires registering it again and publishing a validated snapshot.
SELECT 1;
