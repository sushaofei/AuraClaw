-- Development evaluation: one application login shared by every SQL service.
-- First apply deploy/postgres/roles.sql as the migration owner. Reuse its
-- existing service privileges through membership; do not grant database-wide
-- privileges, DDL, migration ownership, or access to unrelated databases.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auraclaw_app') THEN
        CREATE ROLE auraclaw_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
    END IF;
END $$;
GRANT auraclaw_task_api, auraclaw_session, auraclaw_projection, auraclaw_control,
    auraclaw_delivery, auraclaw_hands, auraclaw_policy, auraclaw_credential,
    auraclaw_artifact, auraclaw_streaming, auraclaw_model TO auraclaw_app;
-- Set the password separately from the private deployment configuration.
