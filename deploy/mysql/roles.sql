-- MySQL privilege bootstrap for AuraClaw role-scoped service accounts.
-- Run as a MySQL administrator. Passwords are supplied separately by the platform.
-- Table naming uses schema prefixes (session_core_*, projection_*, ...).
--
-- Replace `auraclaw` below with the deployment DB_NAME (e.g. auraclaw_dev).
-- Role DSN examples (after creating users + grants):
--   SESSION_DATABASE_URL=mysql+aiomysql://auraclaw_session:...@host:3306/auraclaw
--   CONTROL_DATABASE_URL=mysql+aiomysql://auraclaw_control:...@host:3306/auraclaw
-- PostgreSQL equivalent: deploy/postgres/roles.sql
-- Compose migrate defaults to /app/migrations/mysql; override with
-- AURACLAW_MIGRATIONS_DIRECTORY=/app/migrations for PostgreSQL.
--
-- NOTE: Some managed MySQL builds reject GRANT wildcards (`schema_%`) with
-- errno 1146. Prefer:
--   uv run python scripts/apply_mysql_roles.py --host ... --database ... --role-password ...
-- which expands prefixes to concrete tables (same privilege matrix as below).

CREATE USER IF NOT EXISTS 'auraclaw_session'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_projection'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_control'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_delivery'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_hands'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_policy'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_credential'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_artifact'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_streaming'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_model'@'%' IDENTIFIED BY 'change-me';
CREATE USER IF NOT EXISTS 'auraclaw_task_query_ro'@'%' IDENTIFIED BY 'change-me';

-- Adjust database name to match DB_NAME / deployment.
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`session_core_%` TO 'auraclaw_session'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`projection_%` TO 'auraclaw_projection'@'%';
GRANT SELECT ON `auraclaw`.`projection_%` TO 'auraclaw_task_query_ro'@'%';
GRANT SELECT ON `auraclaw`.`projection_task_view` TO 'auraclaw_streaming'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`control_%` TO 'auraclaw_control'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`delivery_%` TO 'auraclaw_delivery'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`hands_%` TO 'auraclaw_hands'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`policy_%` TO 'auraclaw_policy'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`credential_%` TO 'auraclaw_credential'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`artifact_%` TO 'auraclaw_artifact'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`security_%` TO 'auraclaw_hands'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`security_%` TO 'auraclaw_policy'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`security_%` TO 'auraclaw_credential'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`streaming_%` TO 'auraclaw_streaming'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`model_gateway_%` TO 'auraclaw_model'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `auraclaw`.`observability_%` TO 'auraclaw_projection'@'%';
GRANT SELECT, INSERT, UPDATE ON `auraclaw`.`auraclaw_meta_%` TO 'auraclaw_session'@'%';

FLUSH PRIVILEGES;
