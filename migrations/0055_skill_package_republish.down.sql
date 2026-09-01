BEGIN;

ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_package_digest_fk;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_tenant_id_publisher_name_version_package_fkey;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_package_digest_fk
    FOREIGN KEY (tenant_id, publisher, name, version, package_digest)
    REFERENCES hands.skill_package
        (tenant_id, publisher, name, version, package_digest);

DROP TABLE IF EXISTS hands.skill_package_tombstone;

COMMIT;
