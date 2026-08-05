"""Tests for the country lookup table. No network required."""

import importlib

import pytest

from ecdata import country_dictionary
from ecdata.countries import COUNTRIES, COUNTRY_VARIANTS, _build_countries


def test_country_dictionary_columns():
    df = country_dictionary()
    assert not df.is_empty()
    # The frame exposes name_in_dataset, not `country`.
    assert set(df.columns) == {"file_name", "language", "abbr", "name_in_dataset"}


def test_no_duplicate_rows():
    assert len(COUNTRIES) == len(set(COUNTRIES)), "duplicate rows in COUNTRIES"


def test_one_file_per_country_language_pair():
    pairs = [(c.file_name, c.language) for c in COUNTRIES]
    # India is the only country carried in two languages.
    multi = {f for f, _ in pairs if sum(1 for g, _ in pairs if g == f) > 1}
    for name in multi - set(COUNTRY_VARIANTS):
        langs = {c.language for c in COUNTRIES if c.file_name == name}
        assert len(langs) > 1, f"{name} is duplicated without a language split"


def test_language_spelling():
    assert "Portugese" not in {c.language for c in COUNTRIES}
    assert "Portuguese" in {c.language for c in COUNTRIES}


def test_build_is_pure():
    """Building twice must not accumulate alias rows."""
    assert _build_countries() == _build_countries()


def test_reload_is_idempotent():
    import ecdata.countries as m

    before = len(m.COUNTRIES)
    importlib.reload(m)
    importlib.reload(m)
    assert len(m.COUNTRIES) == before


def test_every_variant_resolves():
    names = {c.name_in_dataset.lower() for c in COUNTRIES}
    abbrs = {c.abbr.lower() for c in COUNTRIES}
    for variants in COUNTRY_VARIANTS.values():
        for v in variants:
            assert v.lower() in names | abbrs, f"{v} does not resolve"
