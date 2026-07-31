#!/usr/bin/env python3
"""Validate, inspect, or publish the Price Insight ct_model configuration."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql  # type: ignore[import-untyped]

from auraclaw.config import Settings
from auraclaw.contracts.model_skills import ExecutableModelSkillConfig

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = (
    ROOT / "config" / "model-skills" / "procurement-price-insight.json"
)
ACTOR = "auraclaw-model-skill-bootstrap"
PRICE_INSIGHT_METRIC_WEIGHTS = {
    "history_dev_pct": Decimal("0.150000"),
    "region_gap_max": Decimal("0.150000"),
    "market_dev_pct": Decimal("0.250000"),
    "impact_amount": Decimal("0.150000"),
    "impact_neg_amount": Decimal("0.100000"),
    "impact_share_pct": Decimal("0.100000"),
    "impact_neg_share_pct": Decimal("0.050000"),
    "deviation_cnt": Decimal("0.050000"),
}
PRICE_INSIGHT_TAG_CODES = {
    "price_management_control_tower",
    "industry_price_benchmark",
    "price_impact",
    "price_anomaly",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Configure the ct_model-backed Price Insight Skill."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    spec = json.loads(args.config.read_text())
    _validate_spec(spec)
    skill = ExecutableModelSkillConfig.model_validate(spec["auraclaw_skill"])
    if not args.plan and not args.apply:
        print(
            json.dumps(
                {
                    "status": "valid",
                    "model_code": spec["model"]["model_code"],
                    "version": spec["version"]["version_no"],
                    "skill": skill.name,
                    "tools": [item.name for item in skill.required_tools],
                    "skills": [item.name for item in skill.required_skills],
                    "tables": list(skill.data_tables),
                    "metrics": list(skill.metric_keys),
                },
                ensure_ascii=False,
            )
        )
        return 0

    settings = Settings()
    options = _connection_options(settings)
    connection = pymysql.connect(
        **options,
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with connection.cursor() as cursor:
            if args.plan:
                print(
                    json.dumps(
                        _current_state(
                            cursor,
                            tenant_id=settings.model_skill_source_tenant_id,
                            model_code=str(spec["model"]["model_code"]),
                            version_no=str(spec["version"]["version_no"]),
                        ),
                        ensure_ascii=False,
                        default=str,
                    )
                )
                return 0
            result = _apply(
                cursor,
                tenant_id=settings.model_skill_source_tenant_id,
                spec=spec,
            )
            connection.commit()
            print(json.dumps(result, ensure_ascii=False, default=str))
            return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _connection_options(settings: Settings) -> dict[str, Any]:
    if not (
        settings.model_skill_mysql_host
        and settings.model_skill_mysql_user
        and settings.model_skill_mysql_password is not None
        and settings.model_skill_mysql_password.get_secret_value()
        and settings.model_skill_mysql_database
    ):
        raise ValueError("MYSQL_DB_* Model Skill source configuration is incomplete")
    password = settings.model_skill_mysql_password
    assert settings.model_skill_mysql_host is not None
    assert settings.model_skill_mysql_user is not None
    assert settings.model_skill_mysql_database is not None
    assert password is not None
    return {
        "host": settings.model_skill_mysql_host,
        "port": settings.model_skill_mysql_port,
        "user": settings.model_skill_mysql_user,
        "password": password.get_secret_value(),
        "database": settings.model_skill_mysql_database,
        "charset": "utf8mb4",
        "connect_timeout": 8,
        "read_timeout": 20,
        "write_timeout": 20,
    }


def _current_state(
    cursor: Any,
    *,
    tenant_id: int,
    model_code: str,
    version_no: str,
) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT id, model_code, model_name, status, current_version_id,
               current_version_no
        FROM ct_model_definition
        WHERE tenant_id = %s AND model_code = %s AND deleted = b'0'
        ORDER BY id
        """,
        (tenant_id, model_code),
    )
    models = list(cursor.fetchall())
    versions: list[dict[str, Any]] = []
    if len(models) == 1:
        cursor.execute(
            """
            SELECT id, model_id, version_no, version_name, status, publish_time,
                   config_snapshot_json IS NOT NULL AS has_snapshot
            FROM ct_model_version
            WHERE tenant_id = %s AND model_id = %s AND version_no = %s
              AND deleted = b'0'
            ORDER BY id
            """,
            (tenant_id, models[0]["id"], version_no),
        )
        versions = list(cursor.fetchall())
    return {
        "status": "plan",
        "tenant_id": tenant_id,
        "model_code": model_code,
        "target_version": version_no,
        "models": models,
        "versions": versions,
        "action": (
            "create-version-and-publish"
            if len(models) == 1 and not versions
            else "create-model-and-version"
            if not models
            else "validate-or-reconcile"
        ),
    }


def _apply(
    cursor: Any,
    *,
    tenant_id: int,
    spec: dict[str, Any],
) -> dict[str, Any]:
    existing = _published_target(
        cursor,
        tenant_id=tenant_id,
        model_code=str(spec["model"]["model_code"]),
        version_no=str(spec["version"]["version_no"]),
        snapshot=spec,
    )
    if existing is not None:
        return {
            "status": "unchanged",
            "tenant_id": tenant_id,
            "model_id": existing["model_id"],
            "version_id": existing["version_id"],
            "model_code": spec["model"]["model_code"],
            "version": spec["version"]["version_no"],
            "skill": spec["auraclaw_skill"]["name"],
        }
    model_id = _upsert_model(cursor, tenant_id=tenant_id, model=spec["model"])
    version_id = _upsert_version(
        cursor,
        tenant_id=tenant_id,
        model_id=model_id,
        version=spec["version"],
        snapshot=spec,
    )
    _upsert_rows(
        cursor,
        table="ct_model_input_source",
        key_column="source_code",
        rows=spec["input_sources"],
        common={
            "tenant_id": tenant_id,
            "model_id": model_id,
            "model_version_id": version_id,
            "status": "ENABLED",
        },
    )
    _upsert_rows(
        cursor,
        table="ct_model_input_feature",
        key_column="feature_code",
        rows=spec["input_features"],
        common={
            "tenant_id": tenant_id,
            "model_id": model_id,
            "model_version_id": version_id,
        },
    )
    outputs = [
        {**row, "rule_field_flag": False}
        for row in spec["output_schemas"]
    ]
    _upsert_rows(
        cursor,
        table="ct_model_output_schema",
        key_column="output_code",
        rows=outputs,
        common={
            "tenant_id": tenant_id,
            "model_id": model_id,
            "model_version_id": version_id,
        },
    )
    _upsert_rows(
        cursor,
        table="ct_model_weight_config",
        key_column="feature_code",
        rows=spec["weights"],
        common={
            "tenant_id": tenant_id,
            "model_id": model_id,
            "model_version_id": version_id,
        },
    )
    for tag in spec["tags"]:
        _upsert_tag(
            cursor,
            tag=tag,
            common={
                "tenant_id": tenant_id,
                "model_id": model_id,
                "model_version_id": version_id,
            },
        )
    _upsert_switch(
        cursor,
        switch=spec["switch"],
        common={
            "tenant_id": tenant_id,
            "model_id": model_id,
            "model_code": spec["model"]["model_code"],
            "model_version_id": version_id,
        },
    )
    cursor.execute(
        """
        UPDATE ct_model_version
        SET status = 'PUBLISHED',
            config_snapshot_json = %s,
            publish_time = COALESCE(publish_time, CURRENT_TIMESTAMP),
            change_log = %s,
            updater = %s,
            update_time = CURRENT_TIMESTAMP
        WHERE tenant_id = %s AND id = %s AND model_id = %s AND deleted = b'0'
        """,
        (
            _json(spec),
            spec["version"].get("change_log"),
            ACTOR,
            tenant_id,
            version_id,
            model_id,
        ),
    )
    cursor.execute(
        """
        UPDATE ct_model_definition
        SET current_version_id = %s,
            current_version_no = %s,
            status = 'ENABLED',
            updater = %s,
            update_time = CURRENT_TIMESTAMP
        WHERE tenant_id = %s AND id = %s AND deleted = b'0'
        """,
        (
            version_id,
            spec["version"]["version_no"],
            ACTOR,
            tenant_id,
            model_id,
        ),
    )
    return {
        "status": "published",
        "tenant_id": tenant_id,
        "model_id": model_id,
        "version_id": version_id,
        "model_code": spec["model"]["model_code"],
        "version": spec["version"]["version_no"],
        "skill": spec["auraclaw_skill"]["name"],
    }


def _validate_spec(spec: dict[str, Any]) -> None:
    if spec.get("model", {}).get("model_code") != "PRICE_IMPACT":
        raise ValueError("This bootstrap only manages PRICE_IMPACT")
    weights = spec.get("weights")
    if not isinstance(weights, list):
        raise ValueError("PRICE_IMPACT weights must be configured")
    by_metric = {
        str(row.get("feature_code")): row
        for row in weights
        if isinstance(row, dict)
    }
    if (
        len(weights) != len(PRICE_INSIGHT_METRIC_WEIGHTS)
        or set(by_metric) != set(PRICE_INSIGHT_METRIC_WEIGHTS)
    ):
        raise ValueError("PRICE_IMPACT interpretation weights are incomplete")
    total = Decimal("0")
    for metric_key, expected in PRICE_INSIGHT_METRIC_WEIGHTS.items():
        row = by_metric[metric_key]
        value = Decimal(str(row.get("weight_value")))
        total += value
        quantification = row.get("quantification_json")
        if (
            value != expected
            or row.get("weight_group_code") != "INTERPRETATION_PRIORITY"
            or row.get("status") != "ENABLED"
            or not isinstance(quantification, dict)
            or quantification.get("usage") != "INTERPRETATION_PRIORITY"
            or quantification.get("metric_key") != metric_key
            or quantification.get("authoritative_computation") != "TOOL_ONLY"
            or quantification.get("normalization") != "NONE"
        ):
            raise ValueError(
                f"Invalid PRICE_IMPACT interpretation weight: {metric_key}"
            )
    if total != Decimal("1"):
        raise ValueError("PRICE_IMPACT interpretation weights must sum to 1")

    tags = spec.get("tags")
    if not isinstance(tags, list):
        raise ValueError("PRICE_IMPACT discovery tags must be configured")
    by_code = {
        str(row.get("tag_code")): row
        for row in tags
        if isinstance(row, dict)
    }
    if len(tags) != len(PRICE_INSIGHT_TAG_CODES) or set(by_code) != PRICE_INSIGHT_TAG_CODES:
        raise ValueError("PRICE_IMPACT discovery tags are incomplete")
    for tag_code, row in by_code.items():
        rule = row.get("tag_rule_json")
        if (
            row.get("status") != "ENABLED"
            or row.get("target_type") != "PRICE"
            or not isinstance(rule, dict)
            or rule.get("kind") != "CAPABILITY_DISCOVERY"
            or not isinstance(rule.get("keywords"), list)
            or not rule["keywords"]
            or any(not isinstance(keyword, str) or not keyword for keyword in rule["keywords"])
        ):
            raise ValueError(f"Invalid PRICE_IMPACT discovery tag: {tag_code}")

    switch = spec.get("switch")
    if not isinstance(switch, dict) or (
        switch.get("switch_code"),
        switch.get("scene_code"),
        switch.get("enabled"),
        switch.get("priority"),
    ) != (
        "price_insight_agent",
        "PRICE_MANAGEMENT_CONTROL_TOWER",
        True,
        100,
    ):
        raise ValueError("PRICE_IMPACT scene switch is invalid")


def _published_target(
    cursor: Any,
    *,
    tenant_id: int,
    model_code: str,
    version_no: str,
    snapshot: dict[str, Any],
) -> dict[str, int] | None:
    cursor.execute(
        """
        SELECT d.id AS model_id, v.id AS version_id, v.config_snapshot_json
        FROM ct_model_definition d
        JOIN ct_model_version v
          ON v.tenant_id = d.tenant_id
         AND v.model_id = d.id
         AND v.deleted = b'0'
        WHERE d.tenant_id = %s
          AND d.model_code = %s
          AND d.deleted = b'0'
          AND d.status = 'ENABLED'
          AND d.current_version_id = v.id
          AND d.current_version_no = v.version_no
          AND v.version_no = %s
          AND v.status = 'PUBLISHED'
        FOR UPDATE
        """,
        (tenant_id, model_code, version_no),
    )
    rows = list(cursor.fetchall())
    if len(rows) > 1:
        raise ValueError("Published Model Skill target is ambiguous")
    if not rows:
        return None
    row = rows[0]
    if _json(row["config_snapshot_json"]) != _json(snapshot):
        raise ValueError(
            "Published model version is immutable; create a new version"
        )
    return {
        "model_id": int(row["model_id"]),
        "version_id": int(row["version_id"]),
    }


def _upsert_model(cursor: Any, *, tenant_id: int, model: dict[str, Any]) -> int:
    cursor.execute(
        """
        SELECT id
        FROM ct_model_definition
        WHERE tenant_id = %s AND model_code = %s AND deleted = b'0'
        ORDER BY id
        FOR UPDATE
        """,
        (tenant_id, model["model_code"]),
    )
    rows = list(cursor.fetchall())
    if len(rows) > 1:
        raise ValueError("Model code is ambiguous within the source tenant")
    values = (
        model["model_name"],
        model["model_type"],
        model["target_type"],
        model.get("business_domain"),
        model.get("description"),
        ACTOR,
    )
    if rows:
        model_id = int(rows[0]["id"])
        cursor.execute(
            """
            UPDATE ct_model_definition
            SET model_name = %s, model_type = %s, target_type = %s,
                business_domain = %s, description = %s, updater = %s,
                update_time = CURRENT_TIMESTAMP
            WHERE tenant_id = %s AND id = %s AND deleted = b'0'
            """,
            (*values, tenant_id, model_id),
        )
        return model_id
    cursor.execute(
        """
        INSERT INTO ct_model_definition (
            model_code, model_name, model_type, target_type, business_domain,
            status, description, creator, updater, tenant_id
        ) VALUES (%s, %s, %s, %s, %s, 'DRAFT', %s, %s, %s, %s)
        """,
        (
            model["model_code"],
            *values[:5],
            ACTOR,
            ACTOR,
            tenant_id,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_version(
    cursor: Any,
    *,
    tenant_id: int,
    model_id: int,
    version: dict[str, Any],
    snapshot: dict[str, Any],
) -> int:
    cursor.execute(
        """
        SELECT id, status, config_snapshot_json
        FROM ct_model_version
        WHERE tenant_id = %s AND model_id = %s AND version_no = %s
          AND deleted = b'0'
        ORDER BY id
        FOR UPDATE
        """,
        (tenant_id, model_id, version["version_no"]),
    )
    rows = list(cursor.fetchall())
    if len(rows) > 1:
        raise ValueError("Model version is ambiguous within the source tenant")
    if rows:
        existing = rows[0]
        existing_snapshot = existing.get("config_snapshot_json")
        if (
            str(existing["status"]).upper() == "PUBLISHED"
            and existing_snapshot is not None
            and _json(existing_snapshot) != _json(snapshot)
        ):
            raise ValueError(
                "Published model version is immutable; create a new version"
            )
        return int(existing["id"])
    cursor.execute(
        """
        INSERT INTO ct_model_version (
            model_id, version_no, version_name, status, config_snapshot_json,
            change_log, creator, updater, tenant_id
        ) VALUES (%s, %s, %s, 'DRAFT', %s, %s, %s, %s, %s)
        """,
        (
            model_id,
            version["version_no"],
            version["version_name"],
            _json(snapshot),
            version.get("change_log"),
            ACTOR,
            ACTOR,
            tenant_id,
        ),
    )
    return int(cursor.lastrowid)


def _upsert_rows(
    cursor: Any,
    *,
    table: str,
    key_column: str,
    rows: list[dict[str, Any]],
    common: dict[str, Any],
) -> None:
    for configured in rows:
        row = {**common, **configured}
        row = {
            key: (_json(value) if key.endswith("_json") and value is not None else value)
            for key, value in row.items()
        }
        cursor.execute(
            f"""
            SELECT id FROM `{table}`
            WHERE tenant_id = %s AND model_id = %s AND model_version_id = %s
              AND `{key_column}` = %s AND deleted = b'0'
            ORDER BY id
            FOR UPDATE
            """,
            (
                common["tenant_id"],
                common["model_id"],
                common["model_version_id"],
                row[key_column],
            ),
        )
        matches = list(cursor.fetchall())
        if len(matches) > 1:
            raise ValueError(f"Duplicate {table}.{key_column}: {row[key_column]}")
        writable = {**row, "updater": ACTOR}
        if matches:
            assignments = ", ".join(f"`{column}` = %s" for column in writable)
            cursor.execute(
                f"UPDATE `{table}` SET {assignments}, "
                "update_time = CURRENT_TIMESTAMP WHERE id = %s",
                (*writable.values(), matches[0]["id"]),
            )
            continue
        insertable = {**row, "creator": ACTOR, "updater": ACTOR}
        columns = ", ".join(f"`{column}`" for column in insertable)
        placeholders = ", ".join("%s" for _ in insertable)
        cursor.execute(
            f"INSERT INTO `{table}` ({columns}) VALUES ({placeholders})",
            tuple(insertable.values()),
        )


def _upsert_tag(
    cursor: Any,
    *,
    tag: dict[str, Any],
    common: dict[str, Any],
) -> None:
    """Move a tenant-global capability tag to the current model version."""
    cursor.execute(
        """
        SELECT id, model_id
        FROM ct_model_tag
        WHERE tenant_id = %s AND tag_code = %s AND deleted = b'0'
        ORDER BY id
        FOR UPDATE
        """,
        (common["tenant_id"], tag["tag_code"]),
    )
    matches = list(cursor.fetchall())
    if len(matches) > 1:
        raise ValueError(f"Duplicate model tag: {tag['tag_code']}")
    if matches and int(matches[0]["model_id"]) != int(common["model_id"]):
        raise ValueError(
            f"Model tag is already owned by another model: {tag['tag_code']}"
        )
    row = {
        **common,
        **{
            key: (_json(value) if key.endswith("_json") and value is not None else value)
            for key, value in tag.items()
        },
        "updater": ACTOR,
    }
    if matches:
        assignments = ", ".join(f"`{column}` = %s" for column in row)
        cursor.execute(
            f"UPDATE ct_model_tag SET {assignments}, "
            "update_time = CURRENT_TIMESTAMP WHERE id = %s",
            (*row.values(), matches[0]["id"]),
        )
        return
    insertable = {**row, "creator": ACTOR}
    columns = ", ".join(f"`{column}`" for column in insertable)
    placeholders = ", ".join("%s" for _ in insertable)
    cursor.execute(
        f"INSERT INTO ct_model_tag ({columns}) VALUES ({placeholders})",
        tuple(insertable.values()),
    )


def _upsert_switch(
    cursor: Any,
    *,
    switch: dict[str, Any],
    common: dict[str, Any],
) -> None:
    """Move the scene-unique switch to the newly published model version."""
    row = {**common, **switch}
    cursor.execute(
        """
        SELECT id
        FROM ct_model_switch_config
        WHERE tenant_id = %s AND model_code = %s AND scene_code = %s
          AND switch_code = %s AND deleted = b'0'
        ORDER BY id
        FOR UPDATE
        """,
        (
            common["tenant_id"],
            common["model_code"],
            switch["scene_code"],
            switch["switch_code"],
        ),
    )
    matches = list(cursor.fetchall())
    if len(matches) > 1:
        raise ValueError("Duplicate scene switch configuration")
    writable = {**row, "updater": ACTOR}
    if matches:
        assignments = ", ".join(
            f"`{column}` = %s" for column in writable
        )
        cursor.execute(
            f"UPDATE ct_model_switch_config SET {assignments}, "
            "update_time = CURRENT_TIMESTAMP WHERE id = %s",
            (*writable.values(), matches[0]["id"]),
        )
        return
    insertable = {**row, "creator": ACTOR, "updater": ACTOR}
    columns = ", ".join(f"`{column}`" for column in insertable)
    placeholders = ", ".join("%s" for _ in insertable)
    cursor.execute(
        f"INSERT INTO ct_model_switch_config ({columns}) "
        f"VALUES ({placeholders})",
        tuple(insertable.values()),
    )


def _json(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return str(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


if __name__ == "__main__":
    raise SystemExit(main())
