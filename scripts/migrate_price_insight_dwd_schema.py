#!/usr/bin/env python3
"""Plan or apply the additive legacy Price Insight DWD compatibility migration."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pymysql  # type: ignore[import-untyped]

from auraclaw.config import Settings

EVENT_TABLE = "dwd_pr_price_event_detail_di"
BENCHMARK_TABLE = "dwd_pr_industry_price_benchmark_di"
COMPARE_TABLE = "dwd_pr_price_compare_pair_di"
RULE_TABLE = "dwd_pr_price_insight_rule_di"
GOVERNED_TABLES = {
    EVENT_TABLE,
    BENCHMARK_TABLE,
    COMPARE_TABLE,
    RULE_TABLE,
}
REQUIRED_COLUMNS = {
    EVENT_TABLE: {"tenant_id", "price_line_id"},
    BENCHMARK_TABLE: {"tenant_id", "benchmark_statistic_type"},
    COMPARE_TABLE: {
        "tenant_id",
        "compare_pair_id",
        "price_line_id",
        "benchmark_statistic_type",
        "material_match_score",
        "material_mapping_version",
    },
    RULE_TABLE: {
        "tenant_id",
        "rule_version",
        "rule_code",
        "anchor_type",
        "deviation_threshold_pct",
        "min_benchmark_sample_count",
        "min_material_match_score",
        "enabled",
        "dt",
    },
}
SIMULATED_QUALITY_FLAGS = {
    "DEMO_ONLY",
    "DERIVED_FROM_INTERNAL_CURRENT_PRICE",
    "INDUSTRY_BENCHMARK_SIMULATED",
}


@dataclass(frozen=True)
class MigrationOptions:
    tenant_id: str
    benchmark_statistic_type: str
    material_mapping_version: str
    deviation_threshold_pct: Decimal
    min_benchmark_sample_count: int
    min_material_match_score: Decimal


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit or additively migrate legacy Price Insight DWD tables. "
            "The default mode is read-only plan."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--tenant-id")
    parser.add_argument(
        "--benchmark-statistic-type",
        choices=("MEAN", "MEDIAN", "P50", "INDEX"),
    )
    parser.add_argument("--material-mapping-version")
    parser.add_argument("--deviation-threshold-pct", type=Decimal)
    parser.add_argument("--min-benchmark-sample-count", type=int)
    parser.add_argument("--min-material-match-score", type=Decimal)
    parser.add_argument(
        "--allow-demo-benchmark",
        action="store_true",
        help=(
            "Allow schema migration when upstream benchmarks are explicitly "
            "marked demo/simulated. The Tool quality gate will still block "
            "authoritative market insight."
        ),
    )
    parser.add_argument(
        "--confirm-database",
        help="Required in apply mode and must exactly match the configured database.",
    )
    args = parser.parse_args()

    settings = Settings()
    connection_options = _connection_options(settings)
    connection = pymysql.connect(
        **connection_options,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with connection.cursor() as cursor:
            audit = _audit(cursor)
            if not args.apply:
                print(
                    json.dumps(
                        {
                            "status": "plan",
                            "database": connection_options["database"],
                            "audit": audit,
                            "required_apply_arguments": [
                                "--tenant-id",
                                "--benchmark-statistic-type",
                                "--material-mapping-version",
                                "--deviation-threshold-pct",
                                "--min-benchmark-sample-count",
                                "--min-material-match-score",
                                "--confirm-database",
                            ],
                            "demo_benchmark_requires_explicit_override": bool(
                                audit["simulated_quality_flags"]
                            ),
                            "operations": _planned_operations(audit),
                        },
                        ensure_ascii=False,
                        default=str,
                    )
                )
                return 0

            options = _apply_options(args)
            if args.confirm_database != connection_options["database"]:
                raise ValueError(
                    "--confirm-database must exactly match the configured database"
                )
            if (
                audit["simulated_quality_flags"]
                and not args.allow_demo_benchmark
            ):
                raise ValueError(
                    "Demo/simulated benchmark data requires "
                    "--allow-demo-benchmark; migration does not make it "
                    "authoritative"
                )
            result = _apply(cursor, options=options)
            print(
                json.dumps(
                    {
                        "status": "migrated",
                        "database": connection_options["database"],
                        "result": result,
                        "audit": _audit(cursor),
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
            return 0
    finally:
        connection.close()


def _connection_options(settings: Settings) -> dict[str, Any]:
    if not settings.price_insight_mysql_configured:
        raise ValueError("AURACLAW_PRICE_INSIGHT_MYSQL_* is incomplete")
    password = settings.price_insight_mysql_password
    assert settings.price_insight_mysql_host is not None
    assert settings.price_insight_mysql_user is not None
    assert settings.price_insight_mysql_database is not None
    assert password is not None
    return {
        "host": settings.price_insight_mysql_host,
        "port": settings.price_insight_mysql_port,
        "user": settings.price_insight_mysql_user,
        "password": password.get_secret_value(),
        "database": settings.price_insight_mysql_database,
        "charset": "utf8mb4",
        "connect_timeout": 8,
        "read_timeout": 60,
        "write_timeout": 60,
    }


def _apply_options(args: argparse.Namespace) -> MigrationOptions:
    required = {
        "--tenant-id": args.tenant_id,
        "--benchmark-statistic-type": args.benchmark_statistic_type,
        "--material-mapping-version": args.material_mapping_version,
        "--deviation-threshold-pct": args.deviation_threshold_pct,
        "--min-benchmark-sample-count": args.min_benchmark_sample_count,
        "--min-material-match-score": args.min_material_match_score,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ValueError(
            f"Apply mode requires explicit arguments: {', '.join(missing)}"
        )
    if not str(args.tenant_id).strip():
        raise ValueError("--tenant-id cannot be blank")
    if not str(args.material_mapping_version).strip():
        raise ValueError("--material-mapping-version cannot be blank")
    threshold = Decimal(args.deviation_threshold_pct)
    match_score = Decimal(args.min_material_match_score)
    if threshold < 0:
        raise ValueError("--deviation-threshold-pct must be non-negative")
    if int(args.min_benchmark_sample_count) < 0:
        raise ValueError("--min-benchmark-sample-count must be non-negative")
    if match_score < 0 or match_score > 1:
        raise ValueError("--min-material-match-score must be between 0 and 1")
    return MigrationOptions(
        tenant_id=str(args.tenant_id),
        benchmark_statistic_type=str(args.benchmark_statistic_type),
        material_mapping_version=str(args.material_mapping_version),
        deviation_threshold_pct=threshold,
        min_benchmark_sample_count=int(args.min_benchmark_sample_count),
        min_material_match_score=match_score,
    )


def _audit(cursor: Any) -> dict[str, Any]:
    schema = _schema(cursor)
    missing_columns = {
        table: sorted(REQUIRED_COLUMNS[table].difference(schema.get(table, set())))
        for table in sorted(GOVERNED_TABLES)
    }
    cursor.execute(
        f"""
        SELECT COUNT(*) AS rows_total,
               COUNT(DISTINCT dt) AS partitions,
               MIN(dt) AS min_dt,
               MAX(dt) AS max_dt,
               SUM(
                 source_file_name IS NULL
                 OR source_sheet_name IS NULL
                 OR source_excel_row IS NULL
               ) AS missing_source_key,
               COUNT(*) - COUNT(DISTINCT CONCAT_WS(
                 CHAR(31), source_file_name, source_sheet_name,
                 source_excel_row, dt
               )) AS duplicate_source_key_rows
        FROM `{EVENT_TABLE}`
        """
    )
    events = cursor.fetchone()
    cursor.execute(
        f"""
        SELECT COUNT(*) AS rows_total,
               COUNT(DISTINCT dt) AS partitions,
               MIN(dt) AS min_dt,
               MAX(dt) AS max_dt,
               SUM(benchmark_id IS NULL) AS missing_benchmark_id
        FROM `{COMPARE_TABLE}`
        """
    )
    comparisons = cursor.fetchone()
    cursor.execute(
        f"""
        SELECT COUNT(*) AS comparisons,
               SUM(b.benchmark_id IS NULL) AS orphan_benchmarks,
               SUM(NOT (c.material_code <=> b.material_code)) AS material_mismatch,
               SUM(
                 NOT (c.standard_uom_code <=> b.standard_uom_code)
               ) AS uom_mismatch,
               SUM(NOT (c.currency_code <=> b.currency_code)) AS currency_mismatch,
               SUM(NOT (c.tax_basis_code <=> b.tax_basis_code)) AS tax_mismatch,
               SUM(
                 NOT (c.benchmark_period <=> b.benchmark_period)
               ) AS period_mismatch
        FROM `{COMPARE_TABLE}` c
        LEFT JOIN `{BENCHMARK_TABLE}` b ON b.benchmark_id = c.benchmark_id
        """
    )
    join_quality = cursor.fetchone()
    quality_flags = _quality_flags(cursor)
    blockers: list[str] = []
    if int(events["missing_source_key"] or 0):
        blockers.append("event source key is incomplete")
    if int(events["duplicate_source_key_rows"] or 0):
        blockers.append("event source key is not unique")
    if int(comparisons["missing_benchmark_id"] or 0):
        blockers.append("comparison benchmark_id is incomplete")
    for field in (
        "orphan_benchmarks",
        "material_mismatch",
        "uom_mismatch",
        "currency_mismatch",
        "tax_mismatch",
        "period_mismatch",
    ):
        if int(join_quality[field] or 0):
            blockers.append(f"comparison-to-benchmark {field}")
    return {
        "missing_columns": missing_columns,
        "events": events,
        "comparisons": comparisons,
        "comparison_benchmark_join": join_quality,
        "quality_flags": quality_flags,
        "simulated_quality_flags": sorted(
            SIMULATED_QUALITY_FLAGS.intersection(quality_flags)
        ),
        "migration_blockers": blockers,
        "schema_compatible": not any(missing_columns.values()),
        "authoritative_market_ready": (
            not blockers
            and not SIMULATED_QUALITY_FLAGS.intersection(quality_flags)
        ),
    }


def _schema(cursor: Any) -> dict[str, set[str]]:
    placeholders = ", ".join("%s" for _ in GOVERNED_TABLES)
    cursor.execute(
        f"""
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME IN ({placeholders})
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """,
        tuple(sorted(GOVERNED_TABLES)),
    )
    result: dict[str, set[str]] = {}
    for row in cursor.fetchall():
        result.setdefault(str(row["TABLE_NAME"]), set()).add(
            str(row["COLUMN_NAME"])
        )
    return result


def _quality_flags(cursor: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in (EVENT_TABLE, BENCHMARK_TABLE, COMPARE_TABLE):
        cursor.execute(
            f"""
            SELECT data_quality_status, COUNT(*) AS rows_total
            FROM `{table}`
            GROUP BY data_quality_status
            """
        )
        for row in cursor.fetchall():
            for flag in str(row["data_quality_status"]).split(";"):
                normalized = flag.strip().upper()
                if normalized and normalized != "PASS":
                    counts[normalized] = counts.get(normalized, 0) + int(
                        row["rows_total"]
                    )
    return dict(sorted(counts.items()))


def _planned_operations(audit: dict[str, Any]) -> list[str]:
    operations = [
        "add missing governance columns without dropping existing data",
        "backfill tenant_id and deterministic stable IDs from existing keys",
        "backfill declared benchmark statistic and exact-match evidence",
        "validate completeness, uniqueness, and benchmark join integrity",
        "make governance columns NOT NULL and add tenant-scoped unique indexes",
        "create the versioned Price Insight rule table",
        "seed one explicit market rule for each existing rule_version",
    ]
    if audit["simulated_quality_flags"]:
        operations.append(
            "preserve demo/simulated quality flags so market insight remains blocked"
        )
    return operations


def _apply(cursor: Any, *, options: MigrationOptions) -> dict[str, Any]:
    before = _audit(cursor)
    if before["migration_blockers"]:
        raise ValueError(
            "Migration preflight failed: "
            + "; ".join(before["migration_blockers"])
        )
    _add_governance_columns(cursor)
    _backfill_governance_columns(cursor, options=options)
    _validate_backfill(cursor)
    _enforce_governance_columns(cursor)
    _create_rule_table(cursor)
    rules = _seed_rules(cursor, options=options)
    return {
        "tenant_id": options.tenant_id,
        "rules_seeded_or_reconciled": rules,
        "simulated_quality_flags_preserved": before[
            "simulated_quality_flags"
        ],
    }


def _add_governance_columns(cursor: Any) -> None:
    additions = {
        EVENT_TABLE: {
            "tenant_id": "VARCHAR(64) NULL COMMENT '租户ID'",
            "price_line_id": "VARCHAR(128) NULL COMMENT '稳定价格行ID'",
        },
        BENCHMARK_TABLE: {
            "tenant_id": "VARCHAR(64) NULL COMMENT '租户ID'",
            "benchmark_statistic_type": (
                "VARCHAR(32) NULL COMMENT 'MEAN/MEDIAN/P50/INDEX'"
            ),
        },
        COMPARE_TABLE: {
            "tenant_id": "VARCHAR(64) NULL COMMENT '租户ID'",
            "compare_pair_id": "VARCHAR(128) NULL COMMENT '稳定比对证据ID'",
            "price_line_id": "VARCHAR(128) NULL COMMENT '稳定价格行ID'",
            "benchmark_statistic_type": (
                "VARCHAR(32) NULL COMMENT 'MEAN/MEDIAN/P50/INDEX'"
            ),
            "material_match_score": (
                "DECIMAL(8,6) NULL COMMENT '物料匹配置信分'"
            ),
            "material_mapping_version": (
                "VARCHAR(64) NULL COMMENT '物料映射版本'"
            ),
        },
    }
    schema = _schema(cursor)
    for table, columns in additions.items():
        for column, ddl in columns.items():
            if column not in schema.get(table, set()):
                cursor.execute(
                    f"ALTER TABLE `{table}` ADD COLUMN `{column}` {ddl}"
                )


def _backfill_governance_columns(
    cursor: Any,
    *,
    options: MigrationOptions,
) -> None:
    for table in (EVENT_TABLE, BENCHMARK_TABLE, COMPARE_TABLE):
        cursor.execute(
            f"""
            UPDATE `{table}`
            SET tenant_id = %s
            WHERE tenant_id IS NULL OR tenant_id = ''
            """,
            (options.tenant_id,),
        )
    event_line_expression = _price_line_id_expression()
    cursor.execute(
        f"""
        UPDATE `{EVENT_TABLE}`
        SET price_line_id = {event_line_expression}
        WHERE price_line_id IS NULL OR price_line_id = ''
        """
    )
    cursor.execute(
        f"""
        UPDATE `{COMPARE_TABLE}`
        SET price_line_id = {_price_line_id_expression()},
            compare_pair_id = {_compare_pair_id_expression()}
        WHERE price_line_id IS NULL OR price_line_id = ''
           OR compare_pair_id IS NULL OR compare_pair_id = ''
        """
    )
    cursor.execute(
        f"""
        UPDATE `{BENCHMARK_TABLE}`
        SET benchmark_statistic_type = %s
        WHERE benchmark_statistic_type IS NULL
           OR benchmark_statistic_type = ''
        """,
        (options.benchmark_statistic_type,),
    )
    cursor.execute(
        f"""
        UPDATE `{COMPARE_TABLE}` c
        JOIN `{BENCHMARK_TABLE}` b
          ON b.tenant_id = c.tenant_id
         AND b.benchmark_id = c.benchmark_id
        SET c.benchmark_statistic_type = b.benchmark_statistic_type,
            c.material_match_score = 1.0,
            c.material_mapping_version = %s
        WHERE c.is_selected = 1
          AND c.material_code <=> b.material_code
          AND c.standard_uom_code <=> b.standard_uom_code
          AND c.currency_code <=> b.currency_code
          AND c.tax_basis_code <=> b.tax_basis_code
          AND c.benchmark_period <=> b.benchmark_period
        """,
        (options.material_mapping_version,),
    )


def _price_line_id_expression() -> str:
    return """
    SHA2(CONCAT_WS(
      CHAR(31), bid_section_id, material_source_guid, supplier_code,
      transaction_period, CAST(current_purchase_unit_price AS CHAR), dt
    ), 256)
    """.strip()


def _compare_pair_id_expression() -> str:
    return """
    SHA2(CONCAT_WS(
      CHAR(31), bid_section_id, material_source_guid, supplier_code,
      transaction_period, CAST(current_purchase_unit_price AS CHAR),
      rule_version, dt
    ), 256)
    """.strip()


def _validate_backfill(cursor: Any) -> None:
    checks = {
        EVENT_TABLE: ("tenant_id", "price_line_id"),
        BENCHMARK_TABLE: ("tenant_id", "benchmark_statistic_type"),
        COMPARE_TABLE: (
            "tenant_id",
            "compare_pair_id",
            "price_line_id",
        ),
    }
    for table, columns in checks.items():
        predicate = " OR ".join(
            f"`{column}` IS NULL OR `{column}` = ''" for column in columns
        )
        cursor.execute(
            f"SELECT COUNT(*) AS invalid_rows FROM `{table}` WHERE {predicate}"
        )
        invalid = int(cursor.fetchone()["invalid_rows"])
        if invalid:
            raise ValueError(
                f"{table} has {invalid} rows with incomplete governance fields"
            )
    uniqueness = {
        EVENT_TABLE: ("tenant_id", "price_line_id", "dt"),
        BENCHMARK_TABLE: ("tenant_id", "benchmark_id", "dt"),
        COMPARE_TABLE: ("tenant_id", "compare_pair_id", "dt"),
    }
    for table, columns in uniqueness.items():
        names = ", ".join(f"`{column}`" for column in columns)
        cursor.execute(
            f"""
            SELECT COUNT(*) AS duplicate_groups
            FROM (
              SELECT {names}
              FROM `{table}`
              GROUP BY {names}
              HAVING COUNT(*) > 1
            ) duplicates
            """
        )
        duplicates = int(cursor.fetchone()["duplicate_groups"])
        if duplicates:
            raise ValueError(
                f"{table} has {duplicates} duplicate governed keys"
            )


def _enforce_governance_columns(cursor: Any) -> None:
    modifications = {
        EVENT_TABLE: {
            "tenant_id": "VARCHAR(64) NOT NULL COMMENT '租户ID'",
            "price_line_id": "VARCHAR(128) NOT NULL COMMENT '稳定价格行ID'",
        },
        BENCHMARK_TABLE: {
            "tenant_id": "VARCHAR(64) NOT NULL COMMENT '租户ID'",
            "benchmark_statistic_type": (
                "VARCHAR(32) NOT NULL COMMENT 'MEAN/MEDIAN/P50/INDEX'"
            ),
        },
        COMPARE_TABLE: {
            "tenant_id": "VARCHAR(64) NOT NULL COMMENT '租户ID'",
            "compare_pair_id": (
                "VARCHAR(128) NOT NULL COMMENT '稳定比对证据ID'"
            ),
            "price_line_id": "VARCHAR(128) NOT NULL COMMENT '稳定价格行ID'",
            "benchmark_statistic_type": (
                "VARCHAR(32) NULL COMMENT 'MEAN/MEDIAN/P50/INDEX'"
            ),
            "material_match_score": (
                "DECIMAL(8,6) NULL COMMENT '物料匹配置信分'"
            ),
            "material_mapping_version": (
                "VARCHAR(64) NULL COMMENT '物料映射版本'"
            ),
        },
    }
    for table, columns in modifications.items():
        for column, ddl in columns.items():
            cursor.execute(
                f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {ddl}"
            )
    indexes = {
        EVENT_TABLE: (
            "uq_dwd_price_tenant_line_dt",
            ("tenant_id", "price_line_id", "dt"),
        ),
        BENCHMARK_TABLE: (
            "uq_dwd_benchmark_tenant_id_dt",
            ("tenant_id", "benchmark_id", "dt"),
        ),
        COMPARE_TABLE: (
            "uq_dwd_pair_tenant_pair_dt",
            ("tenant_id", "compare_pair_id", "dt"),
        ),
    }
    for table, (index_name, columns) in indexes.items():
        if not _index_exists(cursor, table=table, index_name=index_name):
            names = ", ".join(f"`{column}`" for column in columns)
            cursor.execute(
                f"ALTER TABLE `{table}` ADD UNIQUE KEY "
                f"`{index_name}` ({names})"
            )


def _index_exists(cursor: Any, *, table: str, index_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS index_count
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND INDEX_NAME = %s
        """,
        (table, index_name),
    )
    return int(cursor.fetchone()["index_count"]) > 0


def _create_rule_table(cursor: Any) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{RULE_TABLE}` (
          tenant_id VARCHAR(64) NOT NULL COMMENT '租户ID',
          rule_version VARCHAR(64) NOT NULL COMMENT '规则版本',
          rule_code VARCHAR(128) NOT NULL COMMENT '规则编码',
          anchor_type VARCHAR(32) NOT NULL COMMENT 'HISTORY/REGION/MARKET',
          deviation_threshold_pct DECIMAL(10,4) NOT NULL,
          min_benchmark_sample_count INT UNSIGNED NULL,
          min_material_match_score DECIMAL(8,6) NULL,
          effective_start_time DATETIME NOT NULL,
          effective_end_time DATETIME NULL,
          enabled TINYINT(1) NOT NULL DEFAULT 1,
          etl_batch_no VARCHAR(64) NOT NULL,
          etl_load_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          dt DATE NOT NULL,
          PRIMARY KEY (tenant_id, rule_version, rule_code, dt),
          CONSTRAINT chk_dwd_rule_threshold
            CHECK (deviation_threshold_pct >= 0),
          CONSTRAINT chk_dwd_rule_match_score
            CHECK (
              min_material_match_score IS NULL
              OR (
                min_material_match_score >= 0
                AND min_material_match_score <= 1
              )
            ),
          CONSTRAINT chk_dwd_rule_enabled CHECK (enabled IN (0, 1))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_0900_ai_ci
          COMMENT='价格洞察版本化阈值与可比规则DWD明细'
        """
    )


def _seed_rules(cursor: Any, *, options: MigrationOptions) -> int:
    cursor.execute(
        f"""
        SELECT rule_version,
               MIN(CONCAT(transaction_period, '-01')) AS effective_start_time,
               MAX(dt) AS dt
        FROM `{COMPARE_TABLE}`
        WHERE tenant_id = %s
        GROUP BY rule_version
        ORDER BY rule_version
        """,
        (options.tenant_id,),
    )
    versions = list(cursor.fetchall())
    for row in versions:
        cursor.execute(
            f"""
            INSERT INTO `{RULE_TABLE}` (
              tenant_id, rule_version, rule_code, anchor_type,
              deviation_threshold_pct, min_benchmark_sample_count,
              min_material_match_score, effective_start_time,
              effective_end_time, enabled, etl_batch_no, dt
            ) VALUES (
              %s, %s, 'MARKET_DEVIATION_DEFAULT', 'MARKET',
              %s, %s, %s, %s, NULL, 1, 'auraclaw-legacy-schema-v1', %s
            )
            ON DUPLICATE KEY UPDATE
              deviation_threshold_pct = VALUES(deviation_threshold_pct),
              min_benchmark_sample_count =
                VALUES(min_benchmark_sample_count),
              min_material_match_score =
                VALUES(min_material_match_score),
              enabled = VALUES(enabled),
              etl_batch_no = VALUES(etl_batch_no),
              etl_load_time = CURRENT_TIMESTAMP
            """,
            (
                options.tenant_id,
                row["rule_version"],
                options.deviation_threshold_pct,
                options.min_benchmark_sample_count,
                options.min_material_match_score,
                row["effective_start_time"],
                row["dt"],
            ),
        )
    return len(versions)


if __name__ == "__main__":
    raise SystemExit(main())
