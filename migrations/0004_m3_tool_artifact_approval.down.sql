BEGIN;

DROP TABLE IF EXISTS artifact.access_audit;
DROP TABLE IF EXISTS artifact.metadata;
DROP TABLE IF EXISTS security.credential_usage_audit;
DROP TABLE IF EXISTS security.credential_reference;
DROP TABLE IF EXISTS security.tool_invocation_dedup;
DROP INDEX IF EXISTS projection.approval_session_digest_idx;
DROP TABLE IF EXISTS projection.approval_view;
DROP TABLE IF EXISTS security.tool_capability;

DROP SCHEMA IF EXISTS artifact;
DROP SCHEMA IF EXISTS security;

COMMIT;
