from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from auraclaw.action.mcp_primitives import RegisteredResource
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
)
from auraclaw.contracts.mcp import McpResourceContent, McpResourceDescriptor
from auraclaw.contracts.skills import SkillManifest

PRICE_INSIGHT_SERVER_ID = "auraclaw-price-insight"
PRICE_INSIGHT_SKILL_DIR = Path(__file__).parents[1] / "skills" / "procurement-price-insight"
_RESOURCE_FILES = {
    "repo://business-skills/price-insight/metric-definitions/1.0.0": (
        "references/metric-definitions.md",
        "价格洞察指标定义",
        "受治理的采购价格洞察八项指标、计算公式与分析粒度。",
        "text/markdown",
    ),
    "repo://business-skills/price-insight/comparability-rules/1.0.0": (
        "references/comparability-rules.md",
        "价格可比规则",
        "历史、区域和市场价格对标的可比键、准入规则与限制。",
        "text/markdown",
    ),
    "repo://business-skills/price-insight/output-contract/1.0.0": (
        "references/output-contract.json",
        "价格洞察输出契约",
        "价格洞察 Tool 返回给 Agent 的版本化 JSON 输出契约。",
        "application/schema+json",
    ),
}


def signed_price_insight_package(
    signer: HmacSkillSignatureVerifier,
) -> SkillPackage:
    files = {
        path.relative_to(PRICE_INSIGHT_SKILL_DIR).as_posix(): path.read_bytes()
        for path in PRICE_INSIGHT_SKILL_DIR.rglob("*")
        if path.is_file()
    }
    manifest = SkillManifest.model_validate_json(files["manifest.json"])
    signature = signer.sign(manifest, files)
    signed_manifest = manifest.model_copy(update={"signature": signature})
    files["manifest.json"] = json.dumps(
        signed_manifest.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return SkillPackage.from_files(files)


def price_insight_resources(
    tenant_id: str,
) -> tuple[RegisteredResource, ...]:
    resources = []
    for uri, (relative_path, title, description, mime_type) in _RESOURCE_FILES.items():
        content = (PRICE_INSIGHT_SKILL_DIR / relative_path).read_text()
        digest = f"sha256:{hashlib.sha256(content.encode()).hexdigest()}"
        resources.append(
            RegisteredResource(
                descriptor=McpResourceDescriptor(
                    uri=uri,
                    name=title,
                    title=title,
                    description=description,
                    mime_type=mime_type,
                    size=len(content.encode()),
                    meta={
                        "auraclaw": {
                            "classification": "internal",
                            "contentDigest": digest,
                            "sourceRevision": "1.0.0",
                        }
                    },
                ),
                contents=(
                    McpResourceContent(
                        uri=uri,
                        mime_type=mime_type,
                        text=content,
                        meta={
                            "auraclaw": {
                                "classification": "internal",
                                "contentDigest": digest,
                                "sourceRevision": "1.0.0",
                            }
                        },
                    ),
                ),
                tenant_ids=(tenant_id,),
            )
        )
    return tuple(resources)


def price_insight_resource_descriptors(
    tenant_id: str,
) -> tuple[CapabilityDescriptor, ...]:
    descriptors = []
    for resource in price_insight_resources(tenant_id):
        descriptor = resource.descriptor
        auraclaw_meta = descriptor.meta["auraclaw"]
        uri = descriptor.uri
        digest = str(auraclaw_meta["contentDigest"])
        descriptors.append(
            CapabilityDescriptor(
                capability_id=f"cap_{hashlib.sha256(uri.encode()).hexdigest()[:32]}",
                kind=CapabilityKind.RESOURCE,
                server_id=PRICE_INSIGHT_SERVER_ID,
                canonical_name=uri,
                version="1.0.0",
                content_digest=digest,
                title=descriptor.title or descriptor.name,
                description=descriptor.description or "",
                tags=("采购", "价格洞察", "governance"),
                tenant_id=tenant_id,
                trust_level=CapabilityTrustLevel.PLATFORM,
                classification="internal",
                permission="read-only",
                risk_level="low",
                status=CapabilityStatus.ACTIVE,
                source_revision="1.0.0",
                updated_at=datetime.now(UTC),
                metadata={"source": {"uri": uri, "mimeType": descriptor.mime_type}},
            )
        )
    return tuple(descriptors)
