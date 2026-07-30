from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from auraclaw.action.ports import PriceInsightSource
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
)
from auraclaw.contracts.price_insight import (
    PriceCompareRecord,
    PriceEventRecord,
    PriceInsightAnchor,
    PriceInsightDataset,
    PriceInsightFilter,
    PriceInsightQualityFinding,
    PriceInsightQualityStatus,
    PriceInsightSnapshot,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

PRICE_INSIGHT_SNAPSHOT_TOOL = "procurement.price_insight.snapshot"
PRICE_INSIGHT_DRILLDOWN_TOOL = "procurement.price_insight.drilldown"
PRICE_INSIGHT_DATA_QUALITY_TOOL = "procurement.price_insight.data_quality"
PRICE_INSIGHT_TOOL_VERSION = "1.0.0"
PRICE_INSIGHT_OWNER = "business-skill:price-insight"
_HUNDRED = Decimal("100")
_ZERO = Decimal("0")


@dataclass(frozen=True)
class _AnchorItem:
    event: PriceEventRecord
    benchmark: Decimal

    @property
    def deviation_pct(self) -> Decimal:
        return (self.event.current_purchase_unit_price - self.benchmark) / self.benchmark * _HUNDRED

    @property
    def signed_amount(self) -> Decimal:
        return (
            self.event.current_purchase_unit_price - self.benchmark
        ) * self.event.standard_quantity


class PriceInsightService:
    """Computes governed price metrics from tenant-scoped source records."""

    def __init__(self, source: PriceInsightSource) -> None:
        self._source = source

    async def snapshot(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> PriceInsightSnapshot:
        dataset = await self._source.load_dataset(
            tenant_id=tenant_id,
            filters=filters,
        )
        findings = _quality_findings(dataset)
        quality_status = _quality_status(findings)
        events = tuple(_eligible_events(dataset.events))
        history_items = _history_anchor_items(events)
        region_items = _region_anchor_items(events)
        market_items = _market_anchor_items(dataset.comparisons)
        anchors = {
            PriceInsightAnchor.HISTORY: history_items,
            PriceInsightAnchor.REGION: region_items,
            PriceInsightAnchor.MARKET: market_items,
        }
        selected_items = anchors[filters.anchor]
        history = _history_analytics(events, history_items)
        region = _region_analytics(events, region_items)
        market = _market_analytics(
            dataset.comparisons,
            market_items,
            threshold=filters.deviation_threshold_pct,
        )
        impact = {
            anchor.value: _impact_analytics(items, anchor) for anchor, items in anchors.items()
        }
        default_impact = impact[filters.anchor.value]
        evidence = (
            {
                "kind": "dataset",
                "tenant_id": tenant_id,
                "source_revision": dataset.source_revision,
                "event_count": len(dataset.events),
                "comparison_count": len(dataset.comparisons),
            },
            {
                "kind": "metric-definition",
                "version": "price-insight-metrics/1.0.0",
                "anchor": filters.anchor.value,
                "rule_version": filters.rule_version,
                "benchmark_version": filters.benchmark_version,
            },
        )
        kpis = (
            _kpi("history_dev_pct", "历史维偏离", history["kpi_deviation_pct"], "%"),
            _kpi("region_gap_max", "区域维价差", region["kpi_gap_max_pct"], "%"),
            _kpi("market_dev_pct", "市场维偏离", market["kpi_deviation_pct"], "%"),
            _kpi("impact_amount", "正偏移金额", default_impact["total_pos_amount"], "元"),
            _kpi(
                "impact_neg_amount",
                "负偏移金额",
                default_impact["total_neg_amount"],
                "元",
            ),
            _kpi(
                "impact_share_pct",
                "正偏移占采购额",
                default_impact["share_pct"],
                "%",
            ),
            _kpi(
                "impact_neg_share_pct",
                "负偏移占采购额",
                default_impact["neg_share_pct"],
                "%",
            ),
            _kpi("deviation_cnt", "市场偏离行", market["deviation_cnt"], "行"),
        )
        recommendations = _recommendations(
            quality_status,
            history=history,
            region=region,
            market=market,
            selected_items=selected_items,
        )
        return PriceInsightSnapshot(
            filter={
                **filters.model_dump(mode="json"),
                "records": len(dataset.events),
                "comparisons": len(dataset.comparisons),
                "source_revision": dataset.source_revision,
            },
            kpis=kpis,
            analytics={
                "price_compare_3d": {
                    "history": history,
                    "region": region,
                    "market": market,
                    "dominant_dimension": _dominant_dimension(history, region, market),
                    "default_anchor": filters.anchor.value,
                },
                "price_impact": {
                    "default_anchor": filters.anchor.value,
                    "anchors": impact,
                    "formula": (
                        "正偏移=max(0,成交价-对标价)×数量；"
                        "负偏移=max(0,对标价-成交价)×数量；两侧互不抵消。"
                    ),
                },
            },
            data_quality={
                "status": quality_status.value,
                "finding_count": len(findings),
                "findings": [finding.model_dump(mode="json") for finding in findings],
            },
            evidence=evidence,
            recommendations=recommendations,
        )

    async def data_quality(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
    ) -> dict[str, Any]:
        dataset = await self._source.load_dataset(
            tenant_id=tenant_id,
            filters=filters,
        )
        findings = _quality_findings(dataset)
        return {
            "status": _quality_status(findings).value,
            "source_revision": dataset.source_revision,
            "event_count": len(dataset.events),
            "comparison_count": len(dataset.comparisons),
            "findings": [finding.model_dump(mode="json") for finding in findings],
        }

    async def drilldown(
        self,
        *,
        tenant_id: str,
        filters: PriceInsightFilter,
        metric_key: str,
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        snapshot = await self.snapshot(tenant_id=tenant_id, filters=filters)
        rows = _drilldown_rows(snapshot, metric_key)
        return {
            "metric_key": metric_key,
            "offset": offset,
            "limit": limit,
            "total": len(rows),
            "rows": rows[offset : offset + limit],
            "source_revision": snapshot.filter["source_revision"],
            "data_quality": snapshot.data_quality,
        }


@dataclass(frozen=True)
class PriceInsightToolExecutor:
    service: PriceInsightService

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, Any]:
        filters = PriceInsightFilter.model_validate(invocation.arguments["filter"])
        if capability.name == PRICE_INSIGHT_SNAPSHOT_TOOL:
            snapshot = await self.service.snapshot(
                tenant_id=invocation.tenant_id,
                filters=filters,
            )
            return snapshot.model_dump(mode="json")
        if capability.name == PRICE_INSIGHT_DATA_QUALITY_TOOL:
            return await self.service.data_quality(
                tenant_id=invocation.tenant_id,
                filters=filters,
            )
        if capability.name == PRICE_INSIGHT_DRILLDOWN_TOOL:
            return await self.service.drilldown(
                tenant_id=invocation.tenant_id,
                filters=filters,
                metric_key=str(invocation.arguments["metric_key"]),
                offset=int(invocation.arguments.get("offset", 0)),
                limit=int(invocation.arguments.get("limit", 50)),
            )
        raise ValueError(f"Unsupported price insight Tool: {capability.name}")


def price_insight_tools() -> tuple[ToolCapability, ...]:
    filter_schema = _filter_schema()
    common = {
        "type": "object",
        "properties": {"filter": filter_schema},
        "required": ["filter"],
        "additionalProperties": False,
    }
    return (
        ToolCapability(
            name=PRICE_INSIGHT_SNAPSHOT_TOOL,
            version=PRICE_INSIGHT_TOOL_VERSION,
            description=(
                "Compute deterministic historical, regional and market procurement "
                "price KPIs with impact amounts, quality findings and evidence."
            ),
            input_schema=common,
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
            owner=PRICE_INSIGHT_OWNER,
        ),
        ToolCapability(
            name=PRICE_INSIGHT_DRILLDOWN_TOOL,
            version=PRICE_INSIGHT_TOOL_VERSION,
            description=(
                "Return a bounded drilldown for a previously defined procurement "
                "price insight metric."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "filter": filter_schema,
                    "metric_key": {
                        "type": "string",
                        "enum": [
                            "history_dev_pct",
                            "region_gap_max",
                            "market_dev_pct",
                            "impact_amount",
                            "impact_neg_amount",
                            "deviation_cnt",
                        ],
                    },
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
                "required": ["filter", "metric_key"],
                "additionalProperties": False,
            },
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
            owner=PRICE_INSIGHT_OWNER,
        ),
        ToolCapability(
            name=PRICE_INSIGHT_DATA_QUALITY_TOOL,
            version=PRICE_INSIGHT_TOOL_VERSION,
            description=(
                "Validate price insight grain, comparability, benchmark coverage "
                "and cross-field consistency before metrics are interpreted."
            ),
            input_schema=common,
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
            owner=PRICE_INSIGHT_OWNER,
        ),
    )


def price_insight_tool_descriptors(
    *,
    server_id: str,
    tenant_id: str | None = None,
) -> tuple[CapabilityDescriptor, ...]:
    descriptors = []
    for tool in price_insight_tools():
        source = {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": tool.input_schema,
            "outputSchema": tool.output_schema,
            "annotations": {"readOnlyHint": True},
        }
        digest = _digest(source)
        descriptors.append(
            CapabilityDescriptor(
                capability_id=f"cap_{digest[7:39]}",
                kind=CapabilityKind.TOOL,
                server_id=server_id,
                canonical_name=tool.name,
                version=tool.version,
                content_digest=digest,
                title=tool.name,
                description=tool.description,
                tags=(
                    "采购",
                    "成本",
                    "价格洞察",
                    "price",
                    "procurement",
                ),
                tenant_id=tenant_id,
                trust_level=CapabilityTrustLevel.PLATFORM,
                classification="internal",
                permission=tool.permission.value,
                risk_level=tool.risk_level.value,
                status=CapabilityStatus.ACTIVE,
                source_revision=tool.version,
                updated_at=datetime.now(UTC),
                metadata={"source": source},
            )
        )
    return tuple(descriptors)


def _filter_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "period_from": {"type": "string", "minLength": 7, "maxLength": 7},
            "period_to": {"type": "string", "minLength": 7, "maxLength": 7},
            "org_codes": {"type": "array", "items": {"type": "string"}},
            "region_codes": {"type": "array", "items": {"type": "string"}},
            "category_codes": {"type": "array", "items": {"type": "string"}},
            "material_codes": {"type": "array", "items": {"type": "string"}},
            "benchmark_version": {"type": "string"},
            "rule_version": {"type": "string"},
            "anchor": {
                "type": "string",
                "enum": [anchor.value for anchor in PriceInsightAnchor],
            },
            "deviation_threshold_pct": {
                "type": "number",
                "minimum": 0,
                "maximum": 1000,
            },
        },
        "required": ["period_from", "period_to"],
        "additionalProperties": False,
    }


def _quality_findings(
    dataset: PriceInsightDataset,
) -> tuple[PriceInsightQualityFinding, ...]:
    findings: list[PriceInsightQualityFinding] = []
    if not dataset.events:
        findings.append(
            PriceInsightQualityFinding(
                code="NO_PRICE_EVENTS",
                severity="critical",
                message="No final transaction rows matched the requested scope.",
            )
        )
        return tuple(findings)
    identifiers = [event.price_line_id for event in dataset.events]
    duplicate_count = len(identifiers) - len(set(identifiers))
    if duplicate_count:
        findings.append(
            PriceInsightQualityFinding(
                code="DUPLICATE_PRICE_LINE",
                severity="critical",
                message="Price line identifiers are not unique at the intended grain.",
                affected_count=duplicate_count,
            )
        )
    missing_material = sum(not event.material_code for event in dataset.events)
    if missing_material:
        findings.append(
            PriceInsightQualityFinding(
                code="MISSING_MATERIAL_CODE",
                severity="high",
                message="Rows without a standard material code cannot use market benchmarks.",
                affected_count=missing_material,
            )
        )
    missing_uom = sum(not event.standard_uom_code for event in dataset.events)
    if missing_uom:
        findings.append(
            PriceInsightQualityFinding(
                code="MISSING_STANDARD_UOM",
                severity="high",
                message="Rows without a standard unit are excluded from comparable metrics.",
                affected_count=missing_uom,
            )
        )
    amount_mismatch = sum(
        abs(
            event.current_purchase_unit_price * event.standard_quantity
            - event.current_purchase_amount
        )
        > Decimal("0.02")
        for event in dataset.events
    )
    if amount_mismatch:
        findings.append(
            PriceInsightQualityFinding(
                code="AMOUNT_PRICE_QUANTITY_MISMATCH",
                severity="high",
                message="Stored amount differs from unit price multiplied by quantity.",
                affected_count=amount_mismatch,
            )
        )
    line_ids = set(identifiers)
    orphan_comparisons = sum(
        comparison.price_line_id not in line_ids for comparison in dataset.comparisons
    )
    if orphan_comparisons:
        findings.append(
            PriceInsightQualityFinding(
                code="ORPHAN_BENCHMARK_COMPARISON",
                severity="high",
                message="Benchmark comparison rows do not map to a selected price line.",
                affected_count=orphan_comparisons,
            )
        )
    selected_missing_benchmark = sum(
        comparison.is_selected
        and comparison.industry_avg_unit_price is None
        and comparison.uncomparable_reason_code is None
        for comparison in dataset.comparisons
    )
    if selected_missing_benchmark:
        findings.append(
            PriceInsightQualityFinding(
                code="SELECTED_BENCHMARK_MISSING_PRICE",
                severity="high",
                message="Selected benchmark rows have neither a price nor an explanation.",
                affected_count=selected_missing_benchmark,
            )
        )
    matched_statistics = {
        comparison.benchmark_statistic_type
        for comparison in dataset.comparisons
        if comparison.is_selected and comparison.industry_avg_unit_price is not None
    }
    if None in matched_statistics:
        findings.append(
            PriceInsightQualityFinding(
                code="BENCHMARK_STATISTIC_UNDECLARED",
                severity="medium",
                message="Market benchmark cannot be described as P50 or mean without a type.",
                affected_count=sum(
                    comparison.is_selected
                    and comparison.industry_avg_unit_price is not None
                    and comparison.benchmark_statistic_type is None
                    for comparison in dataset.comparisons
                ),
            )
        )
    return tuple(findings)


def _quality_status(
    findings: tuple[PriceInsightQualityFinding, ...],
) -> PriceInsightQualityStatus:
    severities = {finding.severity for finding in findings}
    if "critical" in severities:
        return PriceInsightQualityStatus.BLOCKED
    if severities:
        return PriceInsightQualityStatus.WARNING
    return PriceInsightQualityStatus.PASS


def _eligible_events(
    events: Iterable[PriceEventRecord],
) -> Iterable[PriceEventRecord]:
    return (
        event
        for event in events
        if event.standard_uom_code and (event.material_code or event.material_source_guid)
    )


def _comparable_key(event: PriceEventRecord) -> tuple[str, str, str, str, str]:
    return (
        event.material_code or event.material_source_guid,
        event.spec_model or "",
        event.standard_uom_code or "",
        event.currency_code,
        event.tax_basis_code,
    )


def _weighted_price(events: Iterable[PriceEventRecord]) -> Decimal:
    rows = tuple(events)
    quantity = sum((row.standard_quantity for row in rows), _ZERO)
    if not rows or quantity <= 0:
        return _ZERO
    amount = sum(
        (row.current_purchase_unit_price * row.standard_quantity for row in rows),
        _ZERO,
    )
    return amount / quantity


def _history_anchor_items(
    events: tuple[PriceEventRecord, ...],
) -> tuple[_AnchorItem, ...]:
    groups: dict[
        tuple[str, str, str, str, str],
        list[PriceEventRecord],
    ] = defaultdict(list)
    for event in events:
        groups[_comparable_key(event)].append(event)
    items: list[_AnchorItem] = []
    for rows in groups.values():
        periods = sorted({row.transaction_period for row in rows})
        for period_index, period in enumerate(periods):
            if period_index == 0:
                continue
            previous_period = periods[period_index - 1]
            benchmark = _weighted_price(
                row for row in rows if row.transaction_period == previous_period
            )
            if benchmark <= 0:
                continue
            items.extend(
                _AnchorItem(row, benchmark) for row in rows if row.transaction_period == period
            )
    return tuple(items)


def _region_anchor_items(
    events: tuple[PriceEventRecord, ...],
) -> tuple[_AnchorItem, ...]:
    groups: dict[
        tuple[str, str, str, str, str],
        list[PriceEventRecord],
    ] = defaultdict(list)
    for event in events:
        if event.region_code:
            groups[_comparable_key(event)].append(event)
    items: list[_AnchorItem] = []
    for rows in groups.values():
        if len({row.region_code for row in rows}) < 2:
            continue
        benchmark = _weighted_price(rows)
        if benchmark <= 0:
            continue
        items.extend(_AnchorItem(row, benchmark) for row in rows)
    return tuple(items)


def _market_anchor_items(
    comparisons: tuple[PriceCompareRecord, ...],
) -> tuple[_AnchorItem, ...]:
    items = []
    for comparison in comparisons:
        if (
            not comparison.is_selected
            or comparison.industry_avg_unit_price is None
            or comparison.benchmark_match_status not in {"MATCHED", "SELECTED", "COMPARABLE"}
        ):
            continue
        event = PriceEventRecord(
            price_line_id=comparison.price_line_id,
            purchase_project_code=comparison.purchase_project_code,
            bid_section_id=comparison.compare_pair_id,
            transaction_period=comparison.transaction_period,
            org_code=comparison.org_code,
            region_code=comparison.region_code,
            category_code=comparison.category_code,
            category_name=comparison.category_name,
            material_code=comparison.material_code,
            material_source_guid=comparison.price_line_id,
            material_name=comparison.material_name,
            spec_model=comparison.spec_model,
            supplier_code=comparison.supplier_code,
            standard_quantity=comparison.standard_quantity,
            standard_uom_code=comparison.standard_uom_code,
            currency_code=comparison.currency_code,
            tax_basis_code=comparison.tax_basis_code,
            current_purchase_unit_price=comparison.current_purchase_unit_price,
            current_purchase_amount=comparison.current_purchase_amount,
            data_quality_status=comparison.data_quality_status,
        )
        items.append(_AnchorItem(event, comparison.industry_avg_unit_price))
    return tuple(items)


def _history_analytics(
    events: tuple[PriceEventRecord, ...],
    items: tuple[_AnchorItem, ...],
) -> dict[str, Any]:
    series = []
    by_period: dict[str, list[PriceEventRecord]] = defaultdict(list)
    for event in events:
        by_period[event.transaction_period].append(event)
    prior_prices: list[Decimal] = []
    for period in sorted(by_period):
        avg_price = _weighted_price(by_period[period])
        history_average = (
            sum(prior_prices, _ZERO) / len(prior_prices) if prior_prices else avg_price
        )
        series.append(
            {
                "month": period,
                "avg_price": _number(avg_price),
                "hist_ma": _number(history_average),
            }
        )
        prior_prices.append(avg_price)
    return {
        "kpi_deviation_pct": _number(_weighted_deviation(items)),
        "series": series,
        "top_materials": _top_materials(items),
        "matched_line_count": len(items),
        "formula": "当前月成交价相对同可比物料上一可用月加权均价。",
    }


def _region_analytics(
    events: tuple[PriceEventRecord, ...],
    items: tuple[_AnchorItem, ...],
) -> dict[str, Any]:
    comparable_groups: dict[
        tuple[str, str, str, str, str],
        list[PriceEventRecord],
    ] = defaultdict(list)
    for event in events:
        if event.region_code:
            comparable_groups[_comparable_key(event)].append(event)
    gaps: list[Decimal] = []
    for rows in comparable_groups.values():
        region_prices = [
            _weighted_price(row for row in rows if row.region_code == region)
            for region in sorted(
                {row.region_code for row in rows if row.region_code is not None}
            )
        ]
        region_prices = [price for price in region_prices if price > 0]
        if len(region_prices) >= 2:
            gaps.append((max(region_prices) - min(region_prices)) / min(region_prices) * _HUNDRED)
    by_region: dict[str, list[PriceEventRecord]] = defaultdict(list)
    for event in events:
        if event.region_code:
            by_region[event.region_code].append(event)
    overall = _weighted_price(event for event in events if event.region_code)
    regions = [
        {
            "region": region,
            "avg_price": _number(_weighted_price(rows)),
            "vs_mean_pct": _number(
                (_weighted_price(rows) - overall) / overall * _HUNDRED if overall > 0 else _ZERO
            ),
            "count": len(rows),
        }
        for region, rows in sorted(by_region.items())
    ]
    return {
        "kpi_gap_max_pct": _number(max(gaps, default=_ZERO)),
        "regions": regions,
        "matched_line_count": len(items),
        "formula": (
            "先按物料、规格、单位、币种和税价口径分组，再取组内跨区域"
            "(最高加权均价-最低加权均价)/最低加权均价的最大值。"
        ),
    }


def _market_analytics(
    comparisons: tuple[PriceCompareRecord, ...],
    items: tuple[_AnchorItem, ...],
    *,
    threshold: Decimal,
) -> dict[str, Any]:
    selected = [
        comparison
        for comparison in comparisons
        if comparison.is_selected
        and comparison.industry_avg_unit_price is not None
        and comparison.benchmark_match_status in {"MATCHED", "SELECTED", "COMPARABLE"}
    ]
    return {
        "kpi_deviation_pct": _number(_weighted_deviation(items)),
        "hit_cnt": len(selected),
        "deviation_cnt": sum(abs(item.deviation_pct) > threshold for item in items),
        "threshold_pct": _number(threshold),
        "top_materials": _top_materials(items),
        "series": _market_series(items, threshold),
        "formula": "当前成交价相对已选行业基准价的采购额加权偏离。",
    }


def _market_series(
    items: tuple[_AnchorItem, ...],
    threshold: Decimal,
) -> list[dict[str, Any]]:
    by_period: dict[str, list[_AnchorItem]] = defaultdict(list)
    for item in items:
        by_period[item.event.transaction_period].append(item)
    return [
        {
            "month": period,
            "avg_dev_pct": _number(_weighted_deviation(rows)),
            "hit_cnt": len(rows),
            "deviation_cnt": sum(abs(row.deviation_pct) > threshold for row in rows),
        }
        for period, rows in sorted(by_period.items())
    ]


def _impact_analytics(
    items: tuple[_AnchorItem, ...],
    anchor: PriceInsightAnchor,
) -> dict[str, Any]:
    total_po = sum((item.event.current_purchase_amount for item in items), _ZERO)
    positive = sum((max(item.signed_amount, _ZERO) for item in items), _ZERO)
    negative = sum((max(-item.signed_amount, _ZERO) for item in items), _ZERO)
    rows = [_impact_row(item) for item in items]
    return {
        "anchor": anchor.value,
        "total_pos_amount": _number(positive),
        "total_neg_amount": _number(negative),
        "total_impact_amount": _number(positive + negative),
        "total_po_amount": _number(total_po),
        "share_pct": _number(positive / total_po * _HUNDRED if total_po else _ZERO),
        "neg_share_pct": _number(negative / total_po * _HUNDRED if total_po else _ZERO),
        "pos_line_cnt": sum(item.signed_amount > 0 for item in items),
        "neg_line_cnt": sum(item.signed_amount < 0 for item in items),
        "line_cnt": len(items),
        "by_category": _impact_groups(rows, "category"),
        "by_material": _impact_groups(rows, "material_code"),
        "by_amount_bucket": _impact_groups(rows, "bucket"),
        "items": rows,
    }


def _impact_row(item: _AnchorItem) -> dict[str, Any]:
    signed = item.signed_amount
    return {
        "po_no": item.event.purchase_project_code,
        "price_line_id": item.event.price_line_id,
        "material_code": (item.event.material_code or item.event.material_source_guid),
        "material_name": item.event.material_name,
        "category": item.event.category_name or "未分类",
        "amount_impact": _number(abs(signed)),
        "po_amount": _number(item.event.current_purchase_amount),
        "bucket": _amount_bucket(item.event.current_purchase_amount),
        "side": "pos" if signed > 0 else "neg" if signed < 0 else "zero",
        "deviation_pct": _number(item.deviation_pct),
        "unit_price": _number(item.event.current_purchase_unit_price),
        "benchmark": _number(item.benchmark),
    }


def _impact_groups(
    rows: list[dict[str, Any]],
    field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[field])].append(row)
    totals = []
    for key, group in grouped.items():
        totals.append(
            {
                field: key,
                "amount_impact": _number(
                    sum((Decimal(str(row["amount_impact"])) for row in group), _ZERO)
                ),
                "po_amount": _number(sum((Decimal(str(row["po_amount"])) for row in group), _ZERO)),
                "line_cnt": len(group),
            }
        )
    totals.sort(key=lambda item: (-float(item["amount_impact"]), str(item[field])))
    return totals


def _top_materials(
    items: Iterable[_AnchorItem],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[_AnchorItem]] = defaultdict(list)
    for item in items:
        grouped[item.event.material_code or item.event.material_source_guid].append(item)
    rows = []
    for material, group in grouped.items():
        current = _weighted_price(item.event for item in group)
        benchmark_quantity = sum((item.event.standard_quantity for item in group), _ZERO)
        benchmark = (
            sum(
                (item.benchmark * item.event.standard_quantity for item in group),
                _ZERO,
            )
            / benchmark_quantity
            if benchmark_quantity
            else _ZERO
        )
        deviation = (current - benchmark) / benchmark * _HUNDRED if benchmark else _ZERO
        rows.append(
            {
                "material_code": material,
                "material_name": group[0].event.material_name,
                "avg_price": _number(current),
                "benchmark": _number(benchmark),
                "deviation_pct": _number(deviation),
                "line_cnt": len(group),
            }
        )
    rows.sort(
        key=lambda item: (
            -abs(float(item["deviation_pct"])),
            str(item["material_code"]),
        )
    )
    return rows[:20]


def _weighted_deviation(items: Iterable[_AnchorItem]) -> Decimal:
    rows = tuple(items)
    denominator = sum(
        (row.benchmark * row.event.standard_quantity for row in rows),
        _ZERO,
    )
    if denominator <= 0:
        return _ZERO
    numerator = sum(
        (
            (row.event.current_purchase_unit_price - row.benchmark) * row.event.standard_quantity
            for row in rows
        ),
        _ZERO,
    )
    return numerator / denominator * _HUNDRED


def _amount_bucket(amount: Decimal) -> str:
    if amount < Decimal("10000"):
        return "<1万"
    if amount < Decimal("100000"):
        return "1–10万"
    if amount < Decimal("500000"):
        return "10–50万"
    return ">50万"


def _kpi(key: str, label: str, value: Any, suffix: str) -> dict[str, Any]:
    return {"key": key, "label": label, "value": value, "suffix": suffix}


def _number(value: Decimal | int | float) -> int | float:
    decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    rounded = decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if rounded == rounded.to_integral():
        return int(rounded)
    return float(rounded)


def _dominant_dimension(
    history: dict[str, Any],
    region: dict[str, Any],
    market: dict[str, Any],
) -> str:
    values = {
        "history": abs(float(history["kpi_deviation_pct"])),
        "region": abs(float(region["kpi_gap_max_pct"])),
        "market": abs(float(market["kpi_deviation_pct"])),
    }
    return max(values, key=values.__getitem__)


def _recommendations(
    quality_status: PriceInsightQualityStatus,
    *,
    history: dict[str, Any],
    region: dict[str, Any],
    market: dict[str, Any],
    selected_items: tuple[_AnchorItem, ...],
) -> tuple[dict[str, Any], ...]:
    if quality_status is PriceInsightQualityStatus.BLOCKED:
        return (
            {
                "code": "FIX_DATA_QUALITY",
                "priority": "P0",
                "message": "修复阻断性数据质量问题后再生成价格判断。",
            },
        )
    recommendations: list[dict[str, Any]] = []
    if abs(float(market["kpi_deviation_pct"])) >= float(market["threshold_pct"]):
        recommendations.append(
            {
                "code": "REVIEW_MARKET_DEVIATION",
                "priority": "P0",
                "message": "市场维偏离超过阈值，建议复核行业基准和议价空间。",
            }
        )
    if float(region["kpi_gap_max_pct"]) >= 10:
        recommendations.append(
            {
                "code": "REVIEW_REGION_GAP",
                "priority": "P1",
                "message": "跨区域可比价格差异较大，建议复核运距、组织和供应结构。",
            }
        )
    if abs(float(history["kpi_deviation_pct"])) >= 8:
        recommendations.append(
            {
                "code": "REVIEW_HISTORY_DEVIATION",
                "priority": "P1",
                "message": "历史维偏离较大，建议下钻最近成交和前期锚点。",
            }
        )
    if not selected_items:
        recommendations.append(
            {
                "code": "NO_COMPARABLE_LINES",
                "priority": "P0",
                "message": "当前默认锚点没有可比行，请补齐映射或切换锚点。",
            }
        )
    return tuple(recommendations)


def _drilldown_rows(
    snapshot: PriceInsightSnapshot,
    metric_key: str,
) -> list[dict[str, Any]]:
    compare = snapshot.analytics["price_compare_3d"]
    impact = snapshot.analytics["price_impact"]["anchors"][
        snapshot.analytics["price_impact"]["default_anchor"]
    ]
    if metric_key == "history_dev_pct":
        return list(compare["history"]["top_materials"])
    if metric_key == "region_gap_max":
        return list(compare["region"]["regions"])
    if metric_key in {"market_dev_pct", "deviation_cnt"}:
        return list(compare["market"]["top_materials"])
    if metric_key == "impact_amount":
        return [row for row in impact["items"] if row["side"] == "pos"]
    if metric_key == "impact_neg_amount":
        return [row for row in impact["items"] if row["side"] == "neg"]
    raise ValueError(f"Unsupported drilldown metric: {metric_key}")


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
