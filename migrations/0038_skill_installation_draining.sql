BEGIN;

ALTER TABLE hands.skill_installation
    DROP CONSTRAINT IF EXISTS skill_installation_status_check;
ALTER TABLE hands.skill_installation
    ADD CONSTRAINT skill_installation_status_check
    CHECK (status IN ('active','disabled','draining','uninstalled'));

ALTER TABLE hands.skill_installation
    ADD COLUMN IF NOT EXISTS uninstall_action text,
    ADD COLUMN IF NOT EXISTS uninstall_policy_version text,
    ADD COLUMN IF NOT EXISTS uninstall_policy_decision_id text;

ALTER TABLE hands.skill_installation
    ADD CONSTRAINT skill_installation_uninstall_action_check
    CHECK (uninstall_action IS NULL OR uninstall_action IN ('continue','cancel')),
    ADD CONSTRAINT skill_installation_uninstall_evidence_check
    CHECK (
        (status IN ('draining','uninstalled'))
        OR (
            uninstall_action IS NULL
            AND uninstall_policy_version IS NULL
            AND uninstall_policy_decision_id IS NULL
        )
    ),
    ADD CONSTRAINT skill_installation_uninstall_policy_check
    CHECK (
        (uninstall_action IS NULL
         AND uninstall_policy_version IS NULL
         AND uninstall_policy_decision_id IS NULL)
        OR (uninstall_action IS NOT NULL
            AND NULLIF(uninstall_policy_version, '') IS NOT NULL)
    ),
    ADD CONSTRAINT skill_installation_draining_policy_check
    CHECK (
        status <> 'draining'
        OR (uninstall_action IS NOT NULL AND NULLIF(uninstall_policy_version, '') IS NOT NULL)
    );

CREATE INDEX IF NOT EXISTS skill_installation_draining_idx
    ON hands.skill_installation (tenant_id,status,publisher,name)
    WHERE status='draining';

CREATE TABLE IF NOT EXISTS hands.skill_installation_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    request_digest text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    operation text NOT NULL,
    force_uninstall boolean NOT NULL DEFAULT false,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    reason_code text,
    previous_revision integer NOT NULL,
    resulting_revision integer NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,command_id),
    FOREIGN KEY (tenant_id,publisher,name)
        REFERENCES hands.skill_installation (tenant_id,publisher,name),
    CHECK (operation IN ('install','enable','disable','uninstall')),
    CHECK (NOT force_uninstall OR operation='uninstall'),
    CHECK (previous_revision >= 1),
    CHECK (resulting_revision >= previous_revision)
);

COMMIT;
