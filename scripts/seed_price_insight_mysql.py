#!/usr/bin/env python3
"""Create the Price Insight DWD tables and load deterministic local test rows."""

from __future__ import annotations

import argparse
import calendar
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pymysql  # type: ignore[import-untyped]
from pymysql.constants import CLIENT  # type: ignore[import-untyped]

from auraclaw.config import Settings
from auraclaw.contracts.price_insight import PriceInsightDataset

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DDL = ROOT / "docs" / "ddl" / "行业均价智能横向比对-DWD-MySQL-DDL.sql"
DEFAULT_FIXTURE = (
    ROOT
    / "src"
    / "auraclaw"
    / "skills"
    / "procurement-price-insight"
    / "tests"
    / "golden-data.json"
)
EVENT_TABLE = "dwd_pr_price_event_detail_di"
BENCHMARK_TABLE = "dwd_pr_industry_price_benchmark_di"
COMPARE_TABLE = "dwd_pr_price_compare_pair_di"
RULE_TABLE = "dwd_pr_price_insight_rule_di"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create Price Insight DWD tables and seed local acceptance data."
    )
    parser.add_argument("--ddl", type=Path, default=DEFAULT_DDL)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--ddl-only", action="store_true")
    args = parser.parse_args()

    settings = Settings()
    connection_options = _connection_options(settings)
    tenant_id = args.tenant_id or settings.price_insight_target_tenant_id
    connection = pymysql.connect(
        **connection_options,
        autocommit=False,
        client_flag=CLIENT.MULTI_STATEMENTS,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with connection.cursor() as cursor:
            _execute_ddl(cursor, args.ddl.read_text())
            _validate_schema(cursor)
            if args.ddl_only:
                connection.commit()
                print(
                    json.dumps(
                        {
                            "status": "ready",
                            "database": connection_options["database"],
                            "tenant_id": tenant_id,
                            "seeded": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 0
            dataset = PriceInsightDataset.model_validate_json(
                args.fixture.read_bytes()
            )
            counts = _replace_seed(cursor, tenant_id, dataset)
            connection.commit()
            print(
                json.dumps(
                    {
                        "status": "ready",
                        "database": connection_options["database"],
                        "tenant_id": tenant_id,
                        "source_revision": dataset.source_revision,
                        **counts,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _connection_options(settings: Settings) -> dict[str, Any]:
    if settings.price_insight_mysql_configured:
        password = settings.price_insight_mysql_password
        assert password is not None
        assert settings.price_insight_mysql_host is not None
        assert settings.price_insight_mysql_user is not None
        assert settings.price_insight_mysql_database is not None
        return {
            "host": settings.price_insight_mysql_host,
            "port": settings.price_insight_mysql_port,
            "user": settings.price_insight_mysql_user,
            "password": password.get_secret_value(),
            "database": settings.price_insight_mysql_database,
            "charset": "utf8mb4",
        }
    if (
        settings.db_host
        and settings.db_user
        and settings.db_password is not None
        and settings.db_name
    ):
        return {
            "host": settings.db_host,
            "port": settings.db_port,
            "user": settings.db_user,
            "password": settings.db_password,
            "database": settings.db_name,
            "charset": "utf8mb4",
        }
    raise ValueError(
        "Configure AURACLAW_PRICE_INSIGHT_MYSQL_* or DB_HOST/DB_USER/DB_PWD/DB_NAME"
    )


def _execute_ddl(cursor: Any, ddl: str) -> None:
    cursor.execute(ddl)
    while cursor.nextset():
        pass


def _validate_schema(cursor: Any) -> None:
    required = {
        EVENT_TABLE: {"tenant_id", "price_line_id", "dt"},
        BENCHMARK_TABLE: {
            "tenant_id",
            "benchmark_id",
            "benchmark_statistic_type",
            "dt",
        },
        COMPARE_TABLE: {
            "tenant_id",
            "compare_pair_id",
            "price_line_id",
            "benchmark_statistic_type",
            "material_match_score",
            "dt",
        },
        RULE_TABLE: {"tenant_id", "rule_version", "rule_code", "dt"},
    }
    for table, expected in required.items():
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            """,
            (table,),
        )
        actual = {str(row["COLUMN_NAME"]) for row in cursor.fetchall()}
        missing = expected.difference(actual)
        if missing:
            raise RuntimeError(
                f"{table} is missing required columns: {sorted(missing)}"
            )


def _replace_seed(
    cursor: Any,
    tenant_id: str,
    dataset: PriceInsightDataset,
) -> dict[str, int]:
    event_ids = tuple(row.price_line_id for row in dataset.events)
    comparison_ids = tuple(row.compare_pair_id for row in dataset.comparisons)
    benchmark_ids = tuple(
        sorted(
            {
                row.benchmark_id
                for row in dataset.comparisons
                if row.benchmark_id is not None
            }
        )
    )
    rule_versions = tuple(
        sorted({row.rule_version for row in dataset.comparisons})
    )
    _delete_ids(cursor, COMPARE_TABLE, "compare_pair_id", tenant_id, comparison_ids)
    _delete_ids(cursor, EVENT_TABLE, "price_line_id", tenant_id, event_ids)
    _delete_ids(cursor, BENCHMARK_TABLE, "benchmark_id", tenant_id, benchmark_ids)
    _delete_ids(cursor, RULE_TABLE, "rule_version", tenant_id, rule_versions)

    event_by_id = {row.price_line_id: row for row in dataset.events}
    event_rows = [
        {
            "tenant_id": tenant_id,
            **row.model_dump(mode="python"),
            "purchase_project_name": f"本地验收项目 {row.purchase_project_code}",
            "bid_section_name": f"本地验收标段 {row.bid_section_id}",
            "transaction_time": f"{row.transaction_period}-15 12:00:00",
            "org_name": row.org_code,
            "subcategory_code": None,
            "subcategory_name": None,
            "supplier_name": f"验收供应商 {row.supplier_code}",
            "price_stage_code": "FINAL_TRANSACTION",
            "current_data_flag": "GOLDEN_SEED",
            "final_transaction_status": "CONFIRMED",
            "supplier_identity_flag": "MASKED_TEST",
            "source_file_name": "golden-data.json",
            "source_sheet_name": "events",
            "source_excel_row": index,
            "etl_batch_no": "price-insight-local-v1",
            "dt": _period_end(row.transaction_period),
        }
        for index, row in enumerate(dataset.events, start=2)
    ]
    _insert_rows(cursor, EVENT_TABLE, event_rows)

    selected_by_benchmark: dict[str, Any] = {}
    for row in dataset.comparisons:
        if row.benchmark_id and row.industry_avg_unit_price is not None:
            selected_by_benchmark.setdefault(row.benchmark_id, row)
    benchmark_rows = [
        {
            "tenant_id": tenant_id,
            "benchmark_id": benchmark_id,
            "benchmark_version": row.benchmark_version,
            "benchmark_period": row.benchmark_period,
            "industry_source_code": "LOCAL_GOLDEN",
            "source_price_record_id": f"local:{benchmark_id}",
            "category_code": row.category_code,
            "category_name": row.category_name,
            "subcategory_code": None,
            "subcategory_name": None,
            "material_code": row.material_code,
            "material_name": row.material_name,
            "spec_model": row.spec_model,
            "region_code": None,
            "standard_uom_code": row.standard_uom_code,
            "currency_code": row.currency_code,
            "tax_basis_code": row.tax_basis_code,
            "industry_min_unit_price": row.industry_avg_unit_price,
            "industry_avg_unit_price": row.industry_avg_unit_price,
            "benchmark_statistic_type": row.benchmark_statistic_type,
            "industry_max_unit_price": row.industry_avg_unit_price,
            "industry_sample_count": row.industry_sample_count,
            "confidence_level": row.confidence_level or "UNKNOWN",
            "effective_start_date": f"{row.benchmark_period}-01",
            "effective_end_date": _period_end(str(row.benchmark_period)),
            "industry_data_flag": "GOLDEN_SEED",
            "data_quality_status": row.data_quality_status,
            "etl_batch_no": "price-insight-local-v1",
            "dt": _period_end(str(row.benchmark_period)),
        }
        for benchmark_id, row in selected_by_benchmark.items()
    ]
    _insert_rows(cursor, BENCHMARK_TABLE, benchmark_rows)

    comparison_rows = []
    for row in dataset.comparisons:
        event = event_by_id[row.price_line_id]
        comparison_rows.append(
            {
                "tenant_id": tenant_id,
                "compare_pair_id": row.compare_pair_id,
                "price_line_id": row.price_line_id,
                "purchase_project_code": row.purchase_project_code,
                "bid_section_id": event.bid_section_id,
                "transaction_period": row.transaction_period,
                "org_code": row.org_code,
                "region_code": row.region_code,
                "category_code": row.category_code,
                "category_name": row.category_name,
                "subcategory_code": None,
                "subcategory_name": None,
                "material_code": row.material_code,
                "material_source_guid": event.material_source_guid,
                "material_name": row.material_name,
                "spec_model": row.spec_model,
                "supplier_code": row.supplier_code,
                "supplier_name": f"验收供应商 {row.supplier_code}",
                "standard_quantity": row.standard_quantity,
                "standard_uom_code": row.standard_uom_code,
                "currency_code": row.currency_code,
                "tax_basis_code": row.tax_basis_code,
                "current_purchase_unit_price": row.current_purchase_unit_price,
                "current_purchase_amount": row.current_purchase_amount,
                "current_data_flag": "GOLDEN_SEED",
                "benchmark_id": row.benchmark_id,
                "benchmark_version": row.benchmark_version,
                "benchmark_period": row.benchmark_period,
                "industry_source_code": (
                    "LOCAL_GOLDEN" if row.benchmark_id else None
                ),
                "industry_min_unit_price": row.industry_avg_unit_price,
                "industry_avg_unit_price": row.industry_avg_unit_price,
                "benchmark_statistic_type": row.benchmark_statistic_type,
                "industry_max_unit_price": row.industry_avg_unit_price,
                "industry_sample_count": row.industry_sample_count,
                "confidence_level": row.confidence_level,
                "industry_data_flag": (
                    "GOLDEN_SEED" if row.benchmark_id else "NO_BENCHMARK"
                ),
                "benchmark_match_rule_code": row.benchmark_match_rule_code,
                "benchmark_match_status": row.benchmark_match_status,
                "is_selected": row.is_selected,
                "uncomparable_reason_code": row.uncomparable_reason_code,
                "material_match_score": row.material_match_score,
                "material_mapping_version": row.material_mapping_version,
                "rule_version": row.rule_version,
                "data_quality_status": row.data_quality_status,
                "etl_batch_no": "price-insight-local-v1",
                "dt": _period_end(row.transaction_period),
            }
        )
    _insert_rows(cursor, COMPARE_TABLE, comparison_rows)

    rule_rows = [
        {
            "tenant_id": tenant_id,
            "rule_version": version,
            "rule_code": "MARKET_DEVIATION_DEFAULT",
            "anchor_type": "MARKET",
            "deviation_threshold_pct": "8",
            "min_benchmark_sample_count": 10,
            "min_material_match_score": "0.8",
            "effective_start_time": "2026-01-01 00:00:00",
            "effective_end_time": None,
            "enabled": 1,
            "etl_batch_no": "price-insight-local-v1",
            "dt": date(2026, 2, 28),
        }
        for version in rule_versions
    ]
    _insert_rows(cursor, RULE_TABLE, rule_rows)
    return {
        "events": len(event_rows),
        "benchmarks": len(benchmark_rows),
        "comparisons": len(comparison_rows),
        "rules": len(rule_rows),
    }


def _delete_ids(
    cursor: Any,
    table: str,
    column: str,
    tenant_id: str,
    values: Sequence[str],
) -> None:
    if not values:
        return
    placeholders = ", ".join("%s" for _ in values)
    cursor.execute(
        f"DELETE FROM `{table}` WHERE tenant_id = %s "
        f"AND `{column}` IN ({placeholders})",
        (tenant_id, *values),
    )


def _insert_rows(
    cursor: Any,
    table: str,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    materialized = tuple(rows)
    if not materialized:
        return
    columns = tuple(materialized[0])
    placeholders = ", ".join("%s" for _ in columns)
    names = ", ".join(f"`{name}`" for name in columns)
    cursor.executemany(
        f"INSERT INTO `{table}` ({names}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in materialized],
    )


def _period_end(period: str) -> date:
    year, month = (int(value) for value in period.split("-", 1))
    return date(year, month, calendar.monthrange(year, month)[1])


if __name__ == "__main__":
    raise SystemExit(main())
