BEGIN;

CREATE TABLE IF NOT EXISTS policy.active_bundle (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    policy_version text NOT NULL,
    activated_at timestamptz NOT NULL DEFAULT now()
);

COMMIT;
