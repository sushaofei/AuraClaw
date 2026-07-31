from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import Field, field_validator, model_validator

from auraclaw.contracts.internal import ContractModel


class PriceInsightAnchor(StrEnum):
    HISTORY = "history"
    REGION = "region"
    MARKET = "market"


class PriceInsightQualityStatus(StrEnum):
    PASS = "pass"
    WARNING = "warning"
    BLOCKED = "blocked"


class PriceInsightFilter(ContractModel):
    period_from: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    period_to: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    org_codes: tuple[str, ...] = ()
    region_codes: tuple[str, ...] = ()
    category_codes: tuple[str, ...] = ()
    material_codes: tuple[str, ...] = ()
    benchmark_version: str | None = None
    rule_version: str | None = None
    anchor: PriceInsightAnchor = PriceInsightAnchor.HISTORY
    deviation_threshold_pct: Decimal = Field(default=Decimal("8"), ge=0, le=1000)

    @model_validator(mode="after")
    def validate_period_range(self) -> PriceInsightFilter:
        if self.period_from > self.period_to:
            raise ValueError("period_from must not be after period_to")
        return self


class PriceEventRecord(ContractModel):
    price_line_id: str
    purchase_project_code: str
    bid_section_id: str
    transaction_period: str
    org_code: str | None = None
    region_code: str | None = None
    category_code: str | None = None
    category_name: str | None = None
    material_code: str | None = None
    material_source_guid: str
    material_name: str
    spec_model: str | None = None
    supplier_code: str
    standard_quantity: Decimal = Field(gt=0)
    standard_uom_code: str | None = None
    currency_code: str
    tax_basis_code: str
    current_purchase_unit_price: Decimal = Field(gt=0)
    current_purchase_amount: Decimal = Field(ge=0)
    data_quality_status: str = "PASS"


class PriceCompareRecord(ContractModel):
    compare_pair_id: str
    price_line_id: str
    purchase_project_code: str
    transaction_period: str
    org_code: str | None = None
    region_code: str | None = None
    category_code: str | None = None
    category_name: str | None = None
    material_code: str | None = None
    material_name: str
    spec_model: str | None = None
    supplier_code: str
    standard_quantity: Decimal = Field(gt=0)
    standard_uom_code: str | None = None
    currency_code: str
    tax_basis_code: str
    current_purchase_unit_price: Decimal = Field(gt=0)
    current_purchase_amount: Decimal = Field(ge=0)
    benchmark_id: str | None = None
    benchmark_version: str | None = None
    benchmark_period: str | None = None
    benchmark_statistic_type: str | None = None
    industry_avg_unit_price: Decimal | None = Field(default=None, gt=0)
    industry_sample_count: int | None = Field(default=None, ge=0)
    confidence_level: str | None = None
    benchmark_match_rule_code: str
    benchmark_match_status: str
    is_selected: bool = False
    uncomparable_reason_code: str | None = None
    material_match_score: Decimal | None = Field(default=None, ge=0, le=1)
    material_mapping_version: str | None = None
    rule_version: str
    data_quality_status: str = "PASS"


class PriceBenchmarkRecord(ContractModel):
    benchmark_id: str
    benchmark_version: str
    benchmark_period: str
    category_code: str | None = None
    material_code: str | None = None
    material_name: str
    spec_model: str | None = None
    region_code: str | None = None
    standard_uom_code: str
    currency_code: str
    tax_basis_code: str
    industry_avg_unit_price: Decimal = Field(gt=0)
    benchmark_statistic_type: str
    industry_sample_count: int | None = Field(default=None, ge=0)
    confidence_level: str
    data_quality_status: str = "PASS"


class PriceInsightRuleRecord(ContractModel):
    rule_version: str
    rule_code: str
    anchor_type: PriceInsightAnchor
    deviation_threshold_pct: Decimal = Field(ge=0)
    min_benchmark_sample_count: int | None = Field(default=None, ge=0)
    min_material_match_score: Decimal | None = Field(default=None, ge=0, le=1)
    enabled: bool = True

    @field_validator("anchor_type", mode="before")
    @classmethod
    def normalize_anchor_type(cls, value: object) -> object:
        return value.lower() if isinstance(value, str) else value


class PriceInsightDataset(ContractModel):
    tenant_id: str
    source_revision: str
    events: tuple[PriceEventRecord, ...] = ()
    comparisons: tuple[PriceCompareRecord, ...] = ()
    benchmarks: tuple[PriceBenchmarkRecord, ...] = ()
    rules: tuple[PriceInsightRuleRecord, ...] = ()


class PriceInsightQualityFinding(ContractModel):
    code: str
    severity: str
    message: str
    affected_count: int = Field(default=0, ge=0)
    evidence: dict[str, Any] = Field(default_factory=dict)


class PriceInsightSnapshot(ContractModel):
    filter: dict[str, Any]
    kpis: tuple[dict[str, Any], ...]
    analytics: dict[str, Any]
    data_quality: dict[str, Any]
    evidence: tuple[dict[str, Any], ...]
    recommendations: tuple[dict[str, Any], ...] = ()
