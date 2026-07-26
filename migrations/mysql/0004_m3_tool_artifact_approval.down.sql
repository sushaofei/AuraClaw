BEGIN;

DROP TABLE IF EXISTS `artifact_access_audit`;
DROP TABLE IF EXISTS `artifact_metadata`;
DROP TABLE IF EXISTS `security_credential_usage_audit`;
DROP TABLE IF EXISTS `security_credential_reference`;
DROP TABLE IF EXISTS `security_tool_invocation_dedup`;
DROP INDEX IF EXISTS `projection_approval_session_digest_idx`;
DROP TABLE IF EXISTS `projection_approval_view`;
DROP TABLE IF EXISTS `security_tool_capability`;

DROP SCHEMA IF EXISTS artifact;
DROP SCHEMA IF EXISTS security;

COMMIT;
