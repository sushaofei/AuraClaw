from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pymysql  # type: ignore[import-untyped]

from auraclaw.contracts.price_insight import (
    PriceCompareRecord,
    PriceEventRecord,
    PriceInsightDataset,
    PriceInsightFilter,
)

_EVENT_COLUMNS = (
    "price_line_id",
    "purchase_project_code",
    "bid_section_id",
    "transaction_period",
    "org_code",
    "region_code",
    "category_code",
    "category_name",
    "material_code",
    "material_source_guid",
    "material_name",
    "spec_model",
    "supplier_code",
    "standard_quantity",
    "standard_uom_code",
    "currency_code",
    "tax_basis_code",
    "current_purchase_unit_price",
    "current_purchase_amount",
    "data_quality_status",
)
_COMPARE_COLUMNS = (
    "compare_pair_id",
    "price_line_id",
    "purchase_project_code",
    "transaction_period",
    "org_code",
    "region_code",
    "category_code",
    "category_name",
    "material_code",
    "material_name",
    "spec_model",
    "supplier_code",
    "standard_quantity",
    "standard_uom_code",
    "currency_code",
    "tax_basis_code",
    "current_purchase_unit_price",
    "current_purchase_amount",
    "benchmark_id",
    "benchmark_version",
    "benchmark_period",
    "benchmark_statistic_type",
    "industry_avg_unit_price",
    "industry_sample_count",
    "confidence_level",
    "benchmark_match_rule_code",
    "benchmark_match_status",
    "is_selected",
    "uncomparable_reason_code",
    "material_match_score",
    "material_mapping_version",
    "rule_version",
    "data_quality_status",
)


class JsonPriceInsightSource:
    """Loads a deterministic fixture while applying the production filter contract."""

    def __init__(self, path: Path) -> None:
        self._path = path

    async def load_dataset(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> PriceInsightDataset:
        payload = json.loads(self._path.read_text())
        fixture = PriceInsightDataset.model_validate(payload)
        if fixture.tenant_id != tenant_id:
            return PriceInsightDataset(
                tenant_id=tenant_id,
                source_revision=f"{fixture.source_revision}:tenant-empty",
            )
        events = tuple(
            event for event in fixture.events if _matches_common_filter(event.model_dump(), filters)
        )
        comparisons = tuple(
            row
            for row in fixture.comparisons
            if _matches_common_filter(row.model_dump(), filters)
            and (
                filters.benchmark_version is None
                or row.benchmark_version == filters.benchmark_version
            )
            and (filters.rule_version is None or row.rule_version == filters.rule_version)
        )
        return PriceInsightDataset(
            tenant_id=tenant_id,
            source_revision=fixture.source_revision,
            events=events,
            comparisons=comparisons,
        )


class MySqlPriceInsightSource:
    """Reads governed DWD tables using fixed, tenant-scoped SELECT statements."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
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

    async def load_dataset(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> PriceInsightDataset:
        return await asyncio.to_thread(self._load_dataset, tenant_id, filters)

    def _load_dataset(
        self,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> PriceInsightDataset:
        connection = pymysql.connect(**self._connection)
        try:
            with connection.cursor() as cursor:
                cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                event_sql, event_args = _select_statement(
                    table="dwd_pr_price_event_detail_di",
                    columns=_EVENT_COLUMNS,
                    tenant_id=tenant_id,
                    filters=filters,
                    include_benchmark=False,
                )
                cursor.execute(event_sql, event_args)
                events = tuple(PriceEventRecord.model_validate(row) for row in cursor.fetchall())
                compare_sql, compare_args = _select_statement(
                    table="dwd_pr_price_compare_pair_di",
                    columns=_COMPARE_COLUMNS,
                    tenant_id=tenant_id,
                    filters=filters,
                    include_benchmark=True,
                )
                cursor.execute(compare_sql, compare_args)
                comparisons = tuple(
                    PriceCompareRecord.model_validate(row) for row in cursor.fetchall()
                )
                revision = _source_revision(filters, events, comparisons)
                return PriceInsightDataset(
                    tenant_id=tenant_id,
                    source_revision=revision,
                    events=events,
                    comparisons=comparisons,
                )
        finally:
            connection.rollback()
            connection.close()


def _matches_common_filter(
    row: Mapping[str, Any],
    filters: PriceInsightFilter,
) -> bool:
    return (
        filters.period_from <= str(row["transaction_period"]) <= filters.period_to
        and _matches_set(row.get("org_code"), filters.org_codes)
        and _matches_set(row.get("region_code"), filters.region_codes)
        and _matches_set(row.get("category_code"), filters.category_codes)
        and _matches_set(row.get("material_code"), filters.material_codes)
    )


def _matches_set(value: Any, accepted: tuple[str, ...]) -> bool:
    return not accepted or value in accepted


def _select_statement(
    *,
    table: str,
    columns: Sequence[str],
    tenant_id: str,
    filters: PriceInsightFilter,
    include_benchmark: bool,
) -> tuple[str, tuple[Any, ...]]:
    clauses = [
        "tenant_id = %s",
        "transaction_period BETWEEN %s AND %s",
        "price_stage_code = 'FINAL_TRANSACTION'" if not include_benchmark else "1 = 1",
    ]
    arguments: list[Any] = [tenant_id, filters.period_from, filters.period_to]
    for column, values in (
        ("org_code", filters.org_codes),
        ("region_code", filters.region_codes),
        ("category_code", filters.category_codes),
        ("material_code", filters.material_codes),
    ):
        if values:
            placeholders = ", ".join("%s" for _ in values)
            clauses.append(f"{column} IN ({placeholders})")
            arguments.extend(values)
    if include_benchmark and filters.benchmark_version:
        clauses.append("benchmark_version = %s")
        arguments.append(filters.benchmark_version)
    if include_benchmark and filters.rule_version:
        clauses.append("rule_version = %s")
        arguments.append(filters.rule_version)
    selected_columns = ", ".join(columns)
    where = " AND ".join(clauses)
    stable_id = "compare_pair_id" if include_benchmark else "price_line_id"
    return (
        f"SELECT {selected_columns} FROM ("
        f"SELECT {selected_columns}, "
        f"ROW_NUMBER() OVER (PARTITION BY tenant_id, {stable_id} "
        "ORDER BY dt DESC, etl_load_time DESC) AS _latest_rank "
        f"FROM `{table}` WHERE {where}"
        ") AS latest WHERE _latest_rank = 1 "
        "ORDER BY transaction_period, price_line_id",
        tuple(arguments),
    )


def _source_revision(
    filters: PriceInsightFilter,
    events: tuple[PriceEventRecord, ...],
    comparisons: tuple[PriceCompareRecord, ...],
) -> str:
    payload = {
        "filter": filters.model_dump(mode="json"),
        "events": [row.model_dump(mode="json") for row in events],
        "comparisons": [row.model_dump(mode="json") for row in comparisons],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
    return f"mysql-price-insight:{hashlib.sha256(encoded).hexdigest()[:16]}"
