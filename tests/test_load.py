"""Integration tests against published release assets.

Skipped unless pytest is run with --run-network. The countries used here are
the smallest in the release, so the whole module downloads well under 15 MB.
"""

import warnings

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
    """In 1.0.0 portugal.parquet is missing six canonical columns and stores
    `date` as Date rather than Datetime. Concatenating it used to raise."""
    df = ec.load_ecd(country=["Portugal", "Costa Rica"], ecd_version="1.0.0")
    assert df.columns == ec.CANONICAL_COLUMNS
    assert sorted(df["country"].unique().to_list()) == ["Costa Rica", "Portugal"]


def test_aliased_columns_are_recovered():
    """In 1.0.0 Portugal's document URL lives in `urls`, not `url`."""
    df = ec.load_ecd(country="Portugal", ecd_version="1.0.0")
    assert df["url"].null_count() == 0


def test_normalize_schema_can_be_disabled():
    """The alias is only present in 1.0.0; 1.0.1 publishes the canonical name."""
    raw = ec.load_ecd(country="Portugal", normalize_schema=False,
                      ecd_version="1.0.0")
    assert "urls" in raw.columns


def test_deduplicate_reduces_rows():
    plain = ec.load_ecd(country="Jamaica", ecd_version="1.0.0")
    deduped = ec.load_ecd(country="Jamaica", deduplicate=True,
                          ecd_version="1.0.0")
    assert deduped.height < plain.height
    assert deduped.height == deduped.unique(
        subset=["country", "url", "text", "date"]).height


# --- 1.0.1, the repair release ------------------------------------------------

def test_default_version_is_the_repaired_release():
    assert ec.DEFAULT_ECD_VERSION == "1.0.5"


def test_language_is_populated_everywhere():
    """1.0.2 fills `language`, which was null for all of Portugal and the US."""
    for country in ("Portugal", "United States of America"):
        df = ec.load_ecd(country=country)
        assert df["language"].null_count() == 0, country


def test_repaired_release_has_no_duplicates():
    """deduplicate=True is a no-op on 1.0.1: the duplicates are gone at source."""
    plain = ec.load_ecd(country="Jamaica")
    deduped = ec.load_ecd(country="Jamaica", deduplicate=True)
    assert plain.height == deduped.height


def test_repaired_release_publishes_the_canonical_column_names():
    """Portugal's `urls` is published as `url`, so the alias is no longer needed."""
    raw = ec.load_ecd(country="Portugal", normalize_schema=False)
    assert "urls" not in raw.columns
    assert raw["url"].null_count() == 0


def test_repaired_release_unpooled_ecuador():
    """Ecuador and the Dominican Republic no longer share a corpus."""
    ecu = ec.load_ecd(country="Ecuador")
    assert ecu["country"].unique().to_list() == ["Ecuador"]
    assert ecu["url"].str.contains(r"\.do/").sum() == 0


def test_no_composite_executives_remain():
    """1.0.3 splits Italy's `Romano Prodi/Massimo D'Alema` and friends."""
    df = ec.load_ecd(country="Italy")
    assert df["executive"].str.contains("/").sum() == 0


def test_countries_whose_overlaps_were_a_validator_bug_do_not_warn():
    """Denmark, Israel, Austria and Greece hold no double-attributed rows."""
    for country in ("Denmark", "Israel", "Austria", "Greece"):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            ec.load_ecd(country=country, ecd_version="1.0.3")


def test_us_type_has_no_president_names():
    """1.0.5 rebuilt `type` from the source url."""
    df = ec.load_ecd(country="United States of America")
    presidents = {"Barack Obama", "Ronald Reagan", "Richard Nixon",
                  "Donald J. Trump (1st Term)", "Joseph R. Biden, Jr."}
    assert df.filter(pl.col("type").is_in(list(presidents))).height == 0


def test_repaired_release_still_warns_about_what_is_unfixed():
    with pytest.warns(UserWarning, match="kremlin"):
        ec.load_ecd(country="Russia")


def test_repaired_release_does_not_warn_about_what_is_fixed():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        ec.load_ecd(country="Jamaica")


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


# --- the sentence view --------------------------------------------------------

def test_sentence_unit_returns_more_rows_than_documents():
    docs = ec.load_ecd(country="Chile")
    sents = ec.load_ecd(country="Chile", unit="sentence")
    assert sents.height > docs.height
    assert sents.height == 16787


def test_sentence_view_keeps_its_identifiers_through_normalize():
    """normalize_schema used to project onto CANONICAL_COLUMNS and would have
    dropped these, leaving a sentence with no way back to its document."""
    df = ec.load_ecd(country="Chile", unit="sentence", normalize_schema=True)
    for column in ec.SENTENCE_COLUMNS:
        assert column in df.columns, column
    assert df.columns[:len(ec.CANONICAL_COLUMNS)] == ec.CANONICAL_COLUMNS


def test_sentence_rows_join_back_to_their_document():
    df = ec.load_ecd(country="Chile", unit="sentence")
    assert df["document_id"].null_count() == 0
    assert df["document_id"].n_unique() < df.height


def test_unit_is_part_of_the_cache_key():
    """Both views are memoized; without unit in the key the second call would
    return the first one's frame."""
    a = ec.load_ecd(country="Costa Rica")
    b = ec.load_ecd(country="Costa Rica", unit="sentence")
    assert a.height != b.height


def test_sentence_view_has_no_pooled_file():
    with pytest.raises(ValueError, match="pooled"):
        ec.load_ecd(full_ecd=True, unit="sentence")


def test_lazy_sentence_unit():
    lf = ec.lazy_load_ecd(country="Costa Rica", unit="sentence")
    assert isinstance(lf, pl.LazyFrame)
    assert "sentence_index" in lf.collect_schema().names()


def test_unsegmented_country_warns():
    with pytest.warns(UserWarning, match="colombia is not segmented"):
        ec.load_ecd(country="Colombia", unit="sentence")


def test_rejects_an_unknown_unit():
    with pytest.raises(ValueError, match="unit must be"):
        ec.load_ecd(country="Costa Rica", unit="paragraph")
