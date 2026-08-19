BEGIN;

CREATE TABLE IF NOT EXISTS security.agent_context_replay (
    jti_hash text PRIMARY KEY,
    command_id text NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_context_replay_expiry
    ON security.agent_context_replay (expires_at);

COMMIT;
