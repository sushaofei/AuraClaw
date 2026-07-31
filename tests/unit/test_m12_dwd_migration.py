from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT = (
    Path(__file__).parents[2]
    / "scripts"
    / "migrate_price_insight_dwd_schema.py"
)
SPEC = importlib.util.spec_from_file_location("m12_dwd_migration", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MIGRATION = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MIGRATION
SPEC.loader.exec_module(MIGRATION)


def _migration(name: str) -> Any:
    assert isinstance(MIGRATION, ModuleType)
    return getattr(MIGRATION, name)


MigrationOptions = _migration("MigrationOptions")
_apply_options = _migration("_apply_options")
_compare_pair_id_expression = _migration("_compare_pair_id_expression")
_planned_operations = _migration("_planned_operations")
_price_line_id_expression = _migration("_price_line_id_expression")


def _args(**overrides: object) -> Namespace:
    values: dict[str, object] = {
        "tenant_id": "development",
        "benchmark_statistic_type": "MEDIAN",
        "material_mapping_version": "legacy-exact-v1",
        "deviation_threshold_pct": Decimal("8"),
        "min_benchmark_sample_count": 10,
        "min_material_match_score": Decimal("1"),
    }
    values.update(overrides)
    return Namespace(**values)


def test_dwd_migration_apply_requires_explicit_business_semantics() -> None:
    with pytest.raises(ValueError, match="--benchmark-statistic-type"):
        _apply_options(_args(benchmark_statistic_type=None))


def test_dwd_migration_validates_rule_ranges() -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        _apply_options(_args(min_material_match_score=Decimal("1.1")))
    with pytest.raises(ValueError, match="non-negative"):
        _apply_options(_args(deviation_threshold_pct=Decimal("-1")))


def test_dwd_migration_builds_explicit_options() -> None:
    assert _apply_options(_args()) == MigrationOptions(
        tenant_id="development",
        benchmark_statistic_type="MEDIAN",
        material_mapping_version="legacy-exact-v1",
        deviation_threshold_pct=Decimal("8"),
        min_benchmark_sample_count=10,
        min_material_match_score=Decimal("1"),
    )


def test_dwd_migration_stable_ids_cover_legacy_primary_key_grain() -> None:
    price_line = _price_line_id_expression()
    compare_pair = _compare_pair_id_expression()

    for field in (
        "bid_section_id",
        "material_source_guid",
        "supplier_code",
        "transaction_period",
        "current_purchase_unit_price",
        "dt",
    ):
        assert field in price_line
        assert field in compare_pair
    assert "rule_version" in compare_pair
    assert "rule_version" not in price_line


def test_dwd_migration_plan_preserves_demo_quality_flags() -> None:
    operations = _planned_operations(
        {"simulated_quality_flags": ["INDUSTRY_BENCHMARK_SIMULATED"]}
    )

    assert any("remains blocked" in operation for operation in operations)
