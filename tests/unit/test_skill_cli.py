from __future__ import annotations

import asyncio
import hashlib
import json

import httpx
import pytest

from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    skill_package_archive,
    skill_package_digest,
    validate_skill_test_vectors,
)
from auraclaw.composition.cli import (
    _load_skill_directory,
    _publish_skill_archive,
    _validate_local_skill,
    build_parser,
)
from auraclaw.config import Settings
from auraclaw.contracts.errors import SchemaValidationError
from auraclaw.contracts.skills import SkillManifest

_KEY = b"auraclaw-development-platform-skill-key"


def _package(*, tests: dict[str, bytes] | None = None) -> SkillPackage:
    verifier = HmacSkillSignatureVerifier({"platform": _KEY})
    unsigned = SkillManifest(
        name="release.prepare",
        version="3.0.0",
        description="Prepare release",
        publisher="platform",
        signature=f"hmac-sha256:{'0' * 64}",
    )
    files = {"SKILL.md": b"# Release\n", **(tests or {})}
    manifest = unsigned.model_copy(
        update={"signature": verifier.sign(unsigned, files)}
    )
    return SkillPackage(
        manifest=manifest,
        files={"manifest.json": manifest.model_dump_json().encode(), **files},
    )


def test_skills_cli_parser_and_local_validation(tmp_path) -> None:
    package = _package(
        tests={
            "tests/basic.json": json.dumps(
                {"name": "basic", "input": {}, "expected_output": {}}
            ).encode()
        }
    )
    for path, content in package.files.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    loaded = _load_skill_directory(str(tmp_path))
    validated = _validate_local_skill(loaded, Settings(_env_file=None))
    assert skill_package_digest(validated) == skill_package_digest(package)
    assert validate_skill_test_vectors(validated) == 1
    parsed = build_parser().parse_args(
        [
            "skills",
            "publish",
            str(tmp_path),
            "--tenant",
            "tenant-a",
            "--publisher",
            "platform",
        ]
    )
    assert parsed.action == "publish"


def test_declarative_tests_reject_package_code() -> None:
    package = _package(tests={"tests/run.py": b"print('unsafe')"})
    with pytest.raises(SchemaValidationError, match="only contain JSON"):
        validate_skill_test_vectors(package)


def test_cli_publish_uses_staged_upload_then_artifact_publication() -> None:
    async def scenario() -> None:
        package = _package()
        archive = skill_package_archive(package)
        checksum = hashlib.sha256(archive).hexdigest()
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(f"{request.method} {request.url.path}")
            if request.method == "POST" and request.url.path.endswith(
                "/skill-package-uploads"
            ):
                return httpx.Response(
                    201,
                    json={
                        "api_version": "2026-07-28",
                        "artifact_id": "art_cli",
                        "version": 1,
                        "upload_id": "upl_cli",
                        "upload_url": "https://objects.test/staged",
                        "expires_at": "2026-08-29T00:00:00Z",
                        "upload_mode": "single",
                        "part_urls": [],
                    },
                )
            if request.method == "PUT":
                assert request.read() == archive
                return httpx.Response(200)
            if request.method == "POST" and request.url.path.endswith(
                "/art_cli:finalize"
            ):
                return httpx.Response(
                    200,
                    json={
                        "api_version": "2026-07-28",
                        "artifact_ref": {
                            "artifact_id": "art_cli",
                            "version": 1,
                            "content_hash": checksum,
                            "media_type": "application/vnd.auraclaw.skill-package+json",
                            "size": len(archive),
                        },
                        "status": "ready",
                    },
                )
            if request.method == "POST" and request.url.path.endswith(
                "/skill-publications"
            ):
                payload = json.loads(request.read())
                assert "files" not in payload
                assert payload["expected_digest"] == skill_package_digest(package)
                assert payload["artifact_ref"]["artifact_id"] == "art_cli"
                return httpx.Response(
                    201,
                    json={
                        "publisher": "platform",
                        "name": "release.prepare",
                        "version": "3.0.0",
                        "status": "active",
                        "package_digest": skill_package_digest(package),
                    },
                )
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        async with httpx.AsyncClient(
            base_url="http://task-api",
            transport=httpx.MockTransport(handler),
        ) as client:
            result = await _publish_skill_archive(
                client=client,
                package=package,
                tenant_id="tenant-a",
                actor_id="admin-a",
                publisher="platform",
                source_id="sks_admin_upload",
                activate=True,
                expected_revision=0,
                command_id="publish-cli-1",
                token="secret-token",
            )
        assert result["status"] == "active"
        assert seen == [
            "POST /v1/admin/skill-package-uploads",
            "PUT /staged",
            "POST /v1/admin/skill-package-uploads/art_cli:finalize",
            "POST /v1/admin/skill-publications",
        ]

    asyncio.run(scenario())
