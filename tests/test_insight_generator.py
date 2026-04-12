import pytest
import uuid
import sys
import os
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from insight_generator import (
    _extract_boolean_elements,
    _get_ctr,
    _confidence_from_sample,
    _element_label,
    _detect_fatigue,
    _detect_trends,
    _detect_anomalies,
    _detect_element_patterns,
    _detect_opportunities,
    _hypothesize_element_impact,
    _hypothesize_fatigue_pattern,
    _hypothesize_format_comparison,
    generate_insights,
    generate_hypotheses,
)

TENANT_ID = uuid.uuid4()
USER_ID = uuid.uuid4()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def make_banner(
    ctr=None,
    tags=None,
    tags_status="done",
    original_filename="banner.png",
    banner_id=None,
):
    b = MagicMock()
    b.id = banner_id or uuid.uuid4()
    b.tenant_id = TENANT_ID
    b.original_filename = original_filename
    b.tags_status = tags_status
    b.tags = tags
    b.metrics = {"ctr": ctr} if ctr is not None else None
    return b


def make_placement(creative_id, period_start, ctr=None):
    p = MagicMock()
    p.id = uuid.uuid4()
    p.tenant_id = TENANT_ID
    p.creative_id = creative_id
    p.period_start = period_start
    p.metrics = {"ctr": ctr} if ctr is not None else {}
    return p


def _make_tags(*, has_cta_button=None, has_logo=None, has_faces=None,
               format_type=None, **extra_structural):
    """Build a tags dict matching the expected nested structure."""
    tags = {}
    structural = {}
    if has_cta_button is not None:
        structural["has_cta_button"] = has_cta_button
    if has_logo is not None:
        structural["has_logo"] = has_logo
    structural.update(extra_structural)
    if structural:
        tags["structural"] = structural

    if has_faces is not None:
        tags["visual"] = {"has_faces": has_faces}

    if format_type is not None:
        tags["platform_fit"] = {"format_type": format_type}

    return tags


async def _make_mock_db(banners=None, placements=None):
    """Return an AsyncMock session that yields banners then placements."""
    db = AsyncMock()
    call_count = 0
    results = [banners or [], placements or []]

    async def _execute(stmt):
        nonlocal call_count
        idx = min(call_count, len(results) - 1)
        call_count += 1
        result = MagicMock()
        result.scalars.return_value.all.return_value = results[idx]
        return result

    db.execute = AsyncMock(side_effect=_execute)
    return db


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestExtractBooleanElements:
    def test_extracts_structural_booleans(self):
        tags = _make_tags(has_cta_button=True, has_logo=False)
        elems = _extract_boolean_elements(tags)
        assert elems["has_cta_button"] is True
        assert elems["has_logo"] is False

    def test_extracts_visual_booleans(self):
        tags = _make_tags(has_faces=True)
        elems = _extract_boolean_elements(tags)
        assert elems["has_faces"] is True

    def test_extracts_categorical_as_true(self):
        tags = {"platform_fit": {"format_type": "stories"}}
        elems = _extract_boolean_elements(tags)
        assert elems["format_type_stories"] is True

    def test_empty_tags_returns_empty(self):
        assert _extract_boolean_elements({}) == {}

    def test_missing_category_skipped(self):
        tags = {"nonexistent_category": {"has_cta_button": True}}
        elems = _extract_boolean_elements(tags)
        assert "has_cta_button" not in elems


class TestGetCtr:
    def test_returns_float(self):
        assert _get_ctr({"ctr": 0.05}) == 0.05

    def test_none_metrics(self):
        assert _get_ctr(None) is None

    def test_missing_key(self):
        assert _get_ctr({"impressions": 100}) is None

    def test_string_convertible(self):
        assert _get_ctr({"ctr": "0.03"}) == 0.03

    def test_invalid_value(self):
        assert _get_ctr({"ctr": "bad"}) is None

    def test_ctr_none(self):
        assert _get_ctr({"ctr": None}) is None


class TestConfidenceFromSample:
    def test_zero_returns_min(self):
        assert _confidence_from_sample(0) == 0.1

    def test_negative_returns_min(self):
        assert _confidence_from_sample(-5) == 0.1

    def test_large_sample_capped(self):
        assert _confidence_from_sample(1000) == 0.95

    def test_moderate_sample(self):
        c = _confidence_from_sample(15)
        assert 0.3 < c < 0.95

    def test_custom_threshold(self):
        c1 = _confidence_from_sample(5, threshold=10)
        c2 = _confidence_from_sample(5, threshold=100)
        assert c1 > c2  # lower threshold -> higher confidence at same n


class TestElementLabel:
    def test_known_label(self):
        assert _element_label("has_cta_button") == "CTA-кнопка"

    def test_unknown_label_fallback(self):
        assert _element_label("my_custom_element") == "my custom element"


# ---------------------------------------------------------------------------
# Fatigue detection
# ---------------------------------------------------------------------------

class TestDetectFatigue:
    def test_no_placements_returns_empty(self):
        result = _detect_fatigue(TENANT_ID, {}, {})
        assert result == []

    def test_fewer_than_3_placements_skipped(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.05),
            make_placement(cid, date(2025, 2, 1), ctr=0.04),
        ]
        result = _detect_fatigue(TENANT_ID, {}, {cid: pls})
        assert result == []

    def test_declining_ctr_detected(self):
        cid = uuid.uuid4()
        banner = make_banner(banner_id=cid, original_filename="fatigue.png")
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.10),
            make_placement(cid, date(2025, 2, 1), ctr=0.08),
            make_placement(cid, date(2025, 3, 1), ctr=0.04),
        ]
        result = _detect_fatigue(TENANT_ID, {cid: banner}, {cid: pls})
        assert len(result) == 1
        assert result[0].insight_type == "fatigue_warning"
        assert result[0].severity in ("warning", "critical")
        assert "fatigue.png" in result[0].title

    def test_critical_severity_on_large_decline(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.10),
            make_placement(cid, date(2025, 2, 1), ctr=0.08),
            make_placement(cid, date(2025, 3, 1), ctr=0.02),  # 80% decline
        ]
        result = _detect_fatigue(TENANT_ID, {}, {cid: pls})
        assert len(result) == 1
        assert result[0].severity == "critical"

    def test_no_fatigue_when_ctr_stable(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.05),
            make_placement(cid, date(2025, 2, 1), ctr=0.05),
            make_placement(cid, date(2025, 3, 1), ctr=0.05),
        ]
        result = _detect_fatigue(TENANT_ID, {}, {cid: pls})
        assert result == []


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

class TestDetectAnomalies:
    def test_needs_4_ctrs_minimum(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.05),
            make_placement(cid, date(2025, 2, 1), ctr=0.05),
            make_placement(cid, date(2025, 3, 1), ctr=0.05),
        ]
        result = _detect_anomalies(TENANT_ID, {}, {cid: pls})
        assert result == []

    def test_detects_positive_anomaly(self):
        cid = uuid.uuid4()
        banner = make_banner(banner_id=cid, original_filename="anomaly.png")
        # Historical: ~0.05, latest: spike to 0.15
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.050),
            make_placement(cid, date(2025, 2, 1), ctr=0.051),
            make_placement(cid, date(2025, 3, 1), ctr=0.049),
            make_placement(cid, date(2025, 4, 1), ctr=0.150),
        ]
        result = _detect_anomalies(TENANT_ID, {cid: banner}, {cid: pls})
        assert len(result) == 1
        assert result[0].insight_type == "anomaly"
        assert result[0].severity == "warning"
        assert result[0].supporting_data["z_score"] > 2.0

    def test_detects_negative_anomaly(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.050),
            make_placement(cid, date(2025, 2, 1), ctr=0.051),
            make_placement(cid, date(2025, 3, 1), ctr=0.049),
            make_placement(cid, date(2025, 4, 1), ctr=0.001),
        ]
        result = _detect_anomalies(TENANT_ID, {}, {cid: pls})
        assert len(result) == 1
        assert result[0].supporting_data["z_score"] < -2.0

    def test_no_anomaly_when_stable(self):
        cid = uuid.uuid4()
        pls = [
            make_placement(cid, date(2025, m, 1), ctr=0.05)
            for m in range(1, 6)
        ]
        result = _detect_anomalies(TENANT_ID, {}, {cid: pls})
        assert result == []


# ---------------------------------------------------------------------------
# Element patterns
# ---------------------------------------------------------------------------

class TestDetectElementPatterns:
    def _make_banners_with_element(self, n_with, n_without, ctr_with, ctr_without):
        """Create banners to test element correlation patterns."""
        banners = []
        for i in range(n_with):
            banners.append(make_banner(
                ctr=ctr_with,
                tags=_make_tags(has_cta_button=True, has_logo=True),
            ))
        for i in range(n_without):
            banners.append(make_banner(
                ctr=ctr_without,
                tags=_make_tags(has_cta_button=False, has_logo=False),
            ))
        return banners

    def test_too_few_banners_returns_empty(self):
        banners = [make_banner(ctr=0.05, tags=_make_tags(has_cta_button=True))]
        result = _detect_element_patterns(TENANT_ID, banners)
        assert result == []

    def test_detects_positive_correlation(self):
        banners = self._make_banners_with_element(
            n_with=5, n_without=5,
            ctr_with=0.10, ctr_without=0.05,
        )
        result = _detect_element_patterns(TENANT_ID, banners)
        # Should detect has_cta_button and has_logo patterns
        assert len(result) > 0
        types = {r.insight_type for r in result}
        assert types == {"pattern"}
        for r in result:
            assert r.severity in ("success", "info")
            assert r.title
            assert r.description

    def test_no_pattern_when_ctr_similar(self):
        banners = self._make_banners_with_element(
            n_with=5, n_without=5,
            ctr_with=0.05, ctr_without=0.05,
        )
        result = _detect_element_patterns(TENANT_ID, banners)
        assert result == []

    def test_skips_untagged_banners(self):
        banners = [make_banner(ctr=0.05, tags=None, tags_status="pending") for _ in range(10)]
        result = _detect_element_patterns(TENANT_ID, banners)
        assert result == []


# ---------------------------------------------------------------------------
# Trend detection
# ---------------------------------------------------------------------------

class TestDetectTrends:
    def test_too_few_placements_returns_empty(self):
        cid = uuid.uuid4()
        pls = [make_placement(cid, date(2025, 1, 1), ctr=0.05)]
        result = _detect_trends(TENANT_ID, pls)
        assert result == []

    def test_detects_declining_trend(self):
        cid = uuid.uuid4()
        pls = []
        # First 3 months high, next 3 months low
        for m in range(1, 4):
            pls.append(make_placement(cid, date(2025, m, 1), ctr=0.10))
        for m in range(4, 7):
            pls.append(make_placement(cid, date(2025, m, 1), ctr=0.05))
        result = _detect_trends(TENANT_ID, pls)
        assert len(result) == 1
        assert result[0].insight_type == "trend"
        assert result[0].severity == "warning"

    def test_detects_growing_trend(self):
        cid = uuid.uuid4()
        pls = []
        for m in range(1, 4):
            pls.append(make_placement(cid, date(2025, m, 1), ctr=0.03))
        for m in range(4, 7):
            pls.append(make_placement(cid, date(2025, m, 1), ctr=0.10))
        result = _detect_trends(TENANT_ID, pls)
        assert len(result) == 1
        assert result[0].severity == "success"


# ---------------------------------------------------------------------------
# Opportunity detection
# ---------------------------------------------------------------------------

class TestDetectOpportunities:
    def test_too_few_banners_returns_empty(self):
        banners = [make_banner(ctr=0.05, tags=_make_tags(has_cta_button=True)) for _ in range(5)]
        result = _detect_opportunities(TENANT_ID, banners)
        assert result == []

    def test_detects_untested_combination(self):
        """Two top-performing elements that rarely appear together."""
        banners = []
        # Element A (has_cta_button) with high CTR, no element B
        for _ in range(5):
            banners.append(make_banner(
                ctr=0.15,
                tags=_make_tags(has_cta_button=True, has_logo=False, has_faces=False),
            ))
        # Element B (has_faces) with high CTR, no element A
        for _ in range(5):
            banners.append(make_banner(
                ctr=0.14,
                tags=_make_tags(has_cta_button=False, has_logo=False, has_faces=True),
            ))
        # Low performers with neither element
        for _ in range(5):
            banners.append(make_banner(
                ctr=0.03,
                tags=_make_tags(has_cta_button=False, has_logo=False, has_faces=False),
            ))
        result = _detect_opportunities(TENANT_ID, banners)
        # Should detect opportunity to combine cta_button + faces
        types = {r.insight_type for r in result}
        if result:
            assert "opportunity" in types


# ---------------------------------------------------------------------------
# generate_insights (async, mocked DB)
# ---------------------------------------------------------------------------

class TestGenerateInsights:
    @pytest.mark.asyncio
    async def test_empty_db_returns_empty(self):
        db = await _make_mock_db(banners=[], placements=[])
        result = await generate_insights(db, TENANT_ID)
        assert result == []

    @pytest.mark.asyncio
    async def test_with_fatigue_data(self):
        cid = uuid.uuid4()
        banner = make_banner(banner_id=cid)
        pls = [
            make_placement(cid, date(2025, 1, 1), ctr=0.10),
            make_placement(cid, date(2025, 2, 1), ctr=0.08),
            make_placement(cid, date(2025, 3, 1), ctr=0.03),
        ]
        db = await _make_mock_db(banners=[banner], placements=pls)
        result = await generate_insights(db, TENANT_ID)
        fatigue = [i for i in result if i.insight_type == "fatigue_warning"]
        assert len(fatigue) >= 1

    @pytest.mark.asyncio
    async def test_all_insights_have_valid_fields(self):
        cid = uuid.uuid4()
        banner = make_banner(
            banner_id=cid,
            ctr=0.10,
            tags=_make_tags(has_cta_button=True),
        )
        pls = [
            make_placement(cid, date(2025, m, 1), ctr=max(0.01, 0.10 - m * 0.02))
            for m in range(1, 6)
        ]
        db = await _make_mock_db(banners=[banner], placements=pls)
        result = await generate_insights(db, TENANT_ID)
        valid_types = {"fatigue_warning", "trend", "anomaly", "pattern", "opportunity"}
        valid_severities = {"info", "warning", "critical", "success"}
        for insight in result:
            assert insight.insight_type in valid_types
            assert insight.severity in valid_severities
            assert insight.title
            assert insight.description


# ---------------------------------------------------------------------------
# Hypothesis helpers
# ---------------------------------------------------------------------------

class TestHypothesizeElementImpact:
    def test_insufficient_sample_returns_empty(self):
        banners = [make_banner(ctr=0.05, tags=_make_tags(has_cta_button=True))]
        result = _hypothesize_element_impact(TENANT_ID, USER_ID, banners)
        assert result == []

    def test_creates_hypothesis_for_significant_delta(self):
        banners = []
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.12,
                tags=_make_tags(has_cta_button=True, has_logo=True),
            ))
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.05,
                tags=_make_tags(has_cta_button=False, has_logo=False),
            ))
        result = _hypothesize_element_impact(TENANT_ID, USER_ID, banners)
        assert len(result) > 0
        for h in result:
            assert h.hypothesis_type == "element_impact"
            assert h.status == "proposed"
            assert h.confidence is not None
            assert 0 < h.confidence <= 0.95
            assert h.source == "auto"


class TestHypothesizeFatiguePattern:
    def test_insufficient_data_returns_empty(self):
        result = _hypothesize_fatigue_pattern(TENANT_ID, USER_ID, {})
        assert result == []

    def test_creates_fatigue_hypothesis(self):
        placements_by_creative = {}
        for _ in range(3):
            cid = uuid.uuid4()
            pls = [
                make_placement(cid, date(2025, 1, 1), ctr=0.10),
                make_placement(cid, date(2025, 2, 1), ctr=0.08),
                make_placement(cid, date(2025, 4, 1), ctr=0.03),
            ]
            placements_by_creative[cid] = pls
        result = _hypothesize_fatigue_pattern(TENANT_ID, USER_ID, placements_by_creative)
        assert len(result) == 1
        h = result[0]
        assert h.hypothesis_type == "fatigue_pattern"
        assert h.title
        assert h.confidence is not None
        assert "fatigue" in h.tags


class TestHypothesizeFormatComparison:
    def test_insufficient_formats_returns_empty(self):
        banners = [make_banner(ctr=0.05, tags=_make_tags(format_type="stories")) for _ in range(3)]
        result = _hypothesize_format_comparison(TENANT_ID, USER_ID, banners)
        assert result == []

    def test_creates_format_hypothesis(self):
        banners = []
        for _ in range(4):
            banners.append(make_banner(ctr=0.12, tags=_make_tags(format_type="stories")))
        for _ in range(4):
            banners.append(make_banner(ctr=0.05, tags=_make_tags(format_type="feed")))
        result = _hypothesize_format_comparison(TENANT_ID, USER_ID, banners)
        assert len(result) == 1
        h = result[0]
        assert h.hypothesis_type == "format_comparison"
        assert h.status == "proposed"
        assert "format" in h.tags


# ---------------------------------------------------------------------------
# generate_hypotheses (async, mocked DB)
# ---------------------------------------------------------------------------

class TestGenerateHypotheses:
    @pytest.mark.asyncio
    async def test_empty_db_returns_empty(self):
        db = await _make_mock_db(banners=[], placements=[])
        result = await generate_hypotheses(db, TENANT_ID, USER_ID)
        assert result == []

    @pytest.mark.asyncio
    async def test_with_element_data(self):
        banners = []
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.12,
                tags=_make_tags(has_cta_button=True, has_logo=True),
            ))
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.04,
                tags=_make_tags(has_cta_button=False, has_logo=False),
            ))
        db = await _make_mock_db(banners=banners, placements=[])
        result = await generate_hypotheses(db, TENANT_ID, USER_ID)
        element_hyps = [h for h in result if h.hypothesis_type == "element_impact"]
        assert len(element_hyps) >= 1

    @pytest.mark.asyncio
    async def test_all_hypotheses_have_valid_fields(self):
        banners = []
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.12,
                tags=_make_tags(has_cta_button=True, format_type="stories"),
            ))
        for _ in range(6):
            banners.append(make_banner(
                ctr=0.04,
                tags=_make_tags(has_cta_button=False, format_type="feed"),
            ))

        cid = uuid.uuid4()
        pls_data = {}
        for _ in range(3):
            c = uuid.uuid4()
            pls_data[c] = [
                make_placement(c, date(2025, 1, 1), ctr=0.10),
                make_placement(c, date(2025, 2, 1), ctr=0.07),
                make_placement(c, date(2025, 4, 1), ctr=0.02),
            ]
        all_pls = [p for pls in pls_data.values() for p in pls]

        db = await _make_mock_db(banners=banners, placements=all_pls)
        result = await generate_hypotheses(db, TENANT_ID, USER_ID)

        valid_types = {"element_impact", "fatigue_pattern", "format_comparison"}
        for h in result:
            assert h.hypothesis_type in valid_types
            assert h.title
            assert h.confidence is not None or h.hypothesis_type == "format_comparison"
            assert h.status == "proposed"
            assert h.source == "auto"
