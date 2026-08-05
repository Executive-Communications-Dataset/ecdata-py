"""Integration tests against published release assets.

Skipped unless pytest is run with --run-network. The countries used here are
the smallest in the release, so the whole module downloads well under 15 MB.
"""

import polars as pl
import pytest

import ecdata as ec

pytestmark = pytest.mark.network

SMALL = ["Costa Rica", "Nigeria", "Bolivia"]


def test_requires_a_selector():
    with pytest.raises(ValueError):
        ec.lazy_load_ecd()


def test_single_country_is_canonical():
    df = ec.load_ecd(country="Costa Rica")
    assert df.columns == ec.CANONICAL_COLUMNS
    assert not df.is_empty()
    assert df["country"].unique().to_list() == ["Costa Rica"]


def test_multiple_countries_concat():
    df = ec.load_ecd(country=SMALL)
    assert sorted(df["country"].unique().to_list()) == sorted(SMALL)


def test_set_input_matches_list_input():
    assert ec.load_ecd(country=set(SMALL)).height == ec.load_ecd(country=SMALL).height


def test_schema_drift_does_not_raise():
    """portugal.parquet is missing six canonical columns and stores `date` as
    Date rather than Datetime. Concatenating it used to raise."""
    df = ec.load_ecd(country=["Portugal", "Costa Rica"])
    assert df.columns == ec.CANONICAL_COLUMNS
    assert sorted(df["country"].unique().to_list()) == ["Costa Rica", "Portugal"]


def test_aliased_columns_are_recovered():
    """Portugal's document URL lives in `urls`, not `url`."""
    df = ec.load_ecd(country="Portugal")
    assert df["url"].null_count() == 0


def test_normalize_schema_can_be_disabled():
    raw = ec.load_ecd(country="Portugal", normalize_schema=False)
    assert "urls" in raw.columns


def test_deduplicate_reduces_rows():
    plain = ec.load_ecd(country="Jamaica")
    deduped = ec.load_ecd(country="Jamaica", deduplicate=True)
    assert deduped.height < plain.height
    assert deduped.height == deduped.unique(
        subset=["country", "url", "text", "date"]).height


def test_known_issue_warns():
    with pytest.warns(UserWarning, match="jamaica"):
        ec.load_ecd(country="Jamaica", ecd_version="1.0.0")


def test_lazy_returns_lazyframe():
    lf = ec.lazy_load_ecd(country="Costa Rica")
    assert isinstance(lf, pl.LazyFrame)
    assert not lf.collect().is_empty()


def test_known_issue_warns_again_on_cache_hit():
    """A memoized result must still warn; otherwise the caveat is shown once
    per session and silently dropped from then on."""
    ec.load_ecd(country="Jamaica", ecd_version="1.0.0")
    with pytest.warns(UserWarning, match="jamaica"):
        ec.load_ecd(country="Jamaica", ecd_version="1.0.0")


def test_cache_false_still_returns_data():
    assert not ec.load_ecd(country="Costa Rica", cache=False).is_empty()
