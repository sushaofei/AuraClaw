from __future__ import annotations

from auraclaw.action.skill_packages import version_satisfies
from auraclaw.contracts.skills import PublishedSkill, SkillInstallationRecord


def installation_availability(
    installation: SkillInstallationRecord | None, version: str, package_digest: str
) -> str:
    """Pure selection rule shared by management and executable catalog rebuilding."""
    if installation is None:
        return "not_installed"
    if installation.status.value != "active":
        return f"installation_{installation.status.value}"
    if not version_satisfies(version, installation.version_constraint):
        return "installation_version_mismatch"
    if (
        installation.pinned_package_digest is not None
        and installation.pinned_package_digest != package_digest
    ):
        return "installation_digest_mismatch"
    return "available"


def skill_availability(
    publication: PublishedSkill, installation: SkillInstallationRecord | None
) -> str:
    if publication.status.value != "active":
        return "publication_unavailable"
    if installation is not None and (
        installation.tenant_id != publication.tenant_id
        or installation.publisher != publication.manifest.publisher
        or installation.name != publication.manifest.name
    ):
        return "installation_identity_mismatch"
    return installation_availability(
        installation, publication.manifest.version, publication.package_digest
    )
