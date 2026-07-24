from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pymysql  # type: ignore[import-untyped]

from auraclaw.contracts.model_skills import ModelSkillSnapshot

_SECTION_TABLES = {
    "dependencies": "ct_model_dependency",
    "feature_mappings": "ct_model_feature_mapping",
    "input_features": "ct_model_input_feature",
    "input_sources": "ct_model_input_source",
    "output_schemas": "ct_model_output_schema",
    "output_sinks": "ct_model_output_sink",
    "switches": "ct_model_switch_config",
    "tags": "ct_model_tag",
    "thresholds": "ct_model_threshold_config",
    "weights": "ct_model_weight_config",
}


class MySqlModelSkillSource:
    """Loads ct_model_* edit data through fixed, tenant-scoped SELECT statements."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        tenant_id: int,
        include_drafts: bool = True,
        connect_timeout_seconds: int = 8,
    ) -> None:
        self._connection = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
            "connect_timeout": connect_timeout_seconds,
            "read_timeout": 20,
            "write_timeout": connect_timeout_seconds,
            "cursorclass": pymysql.cursors.DictCursor,
        }
        self._tenant_id = tenant_id
        self._include_drafts = include_drafts

    async def load_snapshots(self) -> tuple[ModelSkillSnapshot, ...]:
        return await asyncio.to_thread(self._load_snapshots)

    def _load_snapshots(self) -> tuple[ModelSkillSnapshot, ...]:
        connection = pymysql.connect(**self._connection)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"
                )
                cursor.execute(
                    """
                    SELECT
                        d.id AS model_id,
                        d.model_code,
                        d.model_name,
                        d.model_type,
                        d.target_type,
                        d.business_domain,
                        d.current_version_id,
                        d.current_version_no,
                        d.status AS model_status,
                        d.description,
                        d.remark AS model_remark,
                        d.update_time AS model_update_time,
                        v.id AS version_id,
                        v.version_no,
                        v.version_name,
                        v.status AS version_status,
                        v.config_snapshot_json,
                        v.publish_time,
                        v.change_log,
                        v.remark AS version_remark,
                        v.update_time AS version_update_time
                    FROM ct_model_definition d
                    JOIN ct_model_version v
                      ON v.tenant_id = d.tenant_id
                     AND v.model_id = d.id
                     AND v.deleted = b'0'
                    WHERE d.tenant_id = %s
                      AND d.deleted = b'0'
                      AND (
                        %s = 1 OR (
                          d.status = 'ENABLED'
                          AND v.status = 'PUBLISHED'
                          AND d.current_version_id = v.id
                        )
                      )
                    ORDER BY d.model_code, v.version_no, v.id
                    """,
                    (self._tenant_id, int(self._include_drafts)),
                )
                roots = list(cursor.fetchall())
                snapshots = [
                    self._snapshot(cursor, root)
                    for root in roots
                ]
                return tuple(snapshots)
        finally:
            connection.rollback()
            connection.close()

    def _snapshot(
        self,
        cursor: Any,
        root: dict[str, Any],
    ) -> ModelSkillSnapshot:
        model_id = int(root["model_id"])
        version_id = int(root["version_id"])
        sections: dict[str, list[dict[str, Any]]] = {}
        for section, table in _SECTION_TABLES.items():
            cursor.execute(
                f"""
                SELECT *
                FROM `{table}`
                WHERE tenant_id = %s
                  AND model_id = %s
                  AND model_version_id = %s
                  AND deleted = b'0'
                ORDER BY id
                """,
                (self._tenant_id, model_id, version_id),
            )
            sections[section] = [
                _normalize_mapping(row)
                for row in cursor.fetchall()
            ]
        model = _normalize_mapping(
            {
                "id": root["model_id"],
                "model_code": root["model_code"],
                "model_name": root["model_name"],
                "model_type": root["model_type"],
                "target_type": root["target_type"],
                "business_domain": root["business_domain"],
                "current_version_id": root["current_version_id"],
                "current_version_no": root["current_version_no"],
                "status": root["model_status"],
                "description": root["description"],
                "remark": root["model_remark"],
                "update_time": root["model_update_time"],
            }
        )
        version = _normalize_mapping(
            {
                "id": root["version_id"],
                "version_no": root["version_no"],
                "version_name": root["version_name"],
                "status": root["version_status"],
                "config_snapshot_json": root["config_snapshot_json"],
                "publish_time": root["publish_time"],
                "change_log": root["change_log"],
                "remark": root["version_remark"],
                "update_time": root["version_update_time"],
            }
        )
        semantic = {
            "tenant_id": str(self._tenant_id),
            "model": model,
            "version": version,
            "sections": sections,
        }
        encoded = _canonical_json(semantic).encode()
        digest = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
        return ModelSkillSnapshot(
            **semantic,
            source_revision=f"mysql:{model_id}:{version_id}:{digest[7:23]}",
            source_digest=digest,
        )


def _normalize_mapping(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _normalize_value(item, parse_json=key.endswith("_json"))
        for key, item in value.items()
        if key not in {"creator", "updater", "create_time", "deleted", "tenant_id"}
    }


def _normalize_value(value: Any, *, parse_json: bool = False) -> Any:
    if value is None:
        return None
    if parse_json and isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bool(int.from_bytes(value))
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
