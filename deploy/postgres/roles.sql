-- Run as a PostgreSQL administrator. Passwords are supplied separately by the platform.
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'auraclaw_session', 'auraclaw_projection', 'auraclaw_control',
        'auraclaw_delivery', 'auraclaw_hands', 'auraclaw_policy',
        'auraclaw_credential', 'auraclaw_artifact', 'auraclaw_task_query_ro'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT', role_name);
        END IF;
    END LOOP;
END $$;

REVOKE ALL ON SCHEMA session_core, projection, control, delivery,
    hands, policy, credential, artifact FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA session_core TO auraclaw_session;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA session_core TO auraclaw_session;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA session_core TO auraclaw_session;

GRANT USAGE ON SCHEMA projection TO auraclaw_projection, auraclaw_task_query_ro;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA projection TO auraclaw_projection;
GRANT SELECT ON ALL TABLES IN SCHEMA projection TO auraclaw_task_query_ro;

GRANT USAGE ON SCHEMA control TO auraclaw_control;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO auraclaw_control;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO auraclaw_control;

GRANT USAGE ON SCHEMA delivery TO auraclaw_delivery;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA delivery TO auraclaw_delivery;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA delivery TO auraclaw_delivery;

GRANT USAGE ON SCHEMA hands TO auraclaw_hands;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA hands TO auraclaw_hands;
GRANT USAGE ON SCHEMA policy TO auraclaw_policy;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA policy TO auraclaw_policy;
GRANT USAGE ON SCHEMA credential TO auraclaw_credential;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA credential TO auraclaw_credential;
GRANT USAGE ON SCHEMA artifact TO auraclaw_artifact;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA artifact TO auraclaw_artifact;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA artifact TO auraclaw_artifact;

-- Keep grants correct for tables and sequences created by later expand migrations.
-- Run migrations and this bootstrap as the same deployment owner; PostgreSQL default
-- privileges are scoped to the object-creating role.
ALTER DEFAULT PRIVILEGES IN SCHEMA session_core
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_session;
ALTER DEFAULT PRIVILEGES IN SCHEMA session_core
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_session;

ALTER DEFAULT PRIVILEGES IN SCHEMA projection
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_projection;
ALTER DEFAULT PRIVILEGES IN SCHEMA projection
    GRANT SELECT ON TABLES TO auraclaw_task_query_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA control
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_control;
ALTER DEFAULT PRIVILEGES IN SCHEMA control
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_control;

ALTER DEFAULT PRIVILEGES IN SCHEMA delivery
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_delivery;
ALTER DEFAULT PRIVILEGES IN SCHEMA delivery
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_delivery;

ALTER DEFAULT PRIVILEGES IN SCHEMA hands
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_hands;
ALTER DEFAULT PRIVILEGES IN SCHEMA hands
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_hands;

ALTER DEFAULT PRIVILEGES IN SCHEMA policy
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_policy;
ALTER DEFAULT PRIVILEGES IN SCHEMA policy
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_policy;

ALTER DEFAULT PRIVILEGES IN SCHEMA credential
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_credential;
ALTER DEFAULT PRIVILEGES IN SCHEMA credential
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_credential;

ALTER DEFAULT PRIVILEGES IN SCHEMA artifact
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO auraclaw_artifact;
ALTER DEFAULT PRIVILEGES IN SCHEMA artifact
    GRANT USAGE, SELECT ON SEQUENCES TO auraclaw_artifact;
