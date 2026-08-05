"""Tests for input handling and URL construction. No network required."""

import pytest

from ecdata.country_manager import CountryManager


@pytest.fixture(scope="module")
def manager():
    return CountryManager()


@pytest.mark.parametrize("value", [
    "France",
    ["France"],
    ("France",),
    {"France"},
    {"France": 1},
])
def test_accepts_any_iterable(manager, value):
    """A set or tuple used to raise; the README example is a set literal."""
    manager.validate_input(country=value)
    assert manager.build_urls(country=value)


def test_multiple_countries_give_distinct_urls(manager):
    urls = manager.build_urls(country={"France", "Turkey", "Japan"})
    assert len(urls) == 3
    assert len(set(urls)) == 3


def test_country_in_two_languages_yields_one_url(manager):
    """India ships one file despite two dictionary rows."""
    assert len(manager.build_urls(country="India")) == 1


def test_abbreviations_and_aliases(manager):
    for alias in ("USA", "US", "United States", "GB", "South Korea"):
        assert manager.build_urls(country=alias), f"{alias} did not resolve"


def test_rejects_non_iterable(manager):
    with pytest.raises(ValueError):
        manager.validate_input(country=42)


def test_rejects_non_string_elements(manager):
    with pytest.raises(ValueError):
        manager.validate_input(country=["France", 7])


def test_rejects_unknown_country(manager):
    with pytest.raises(ValueError):
        manager.validate_input(country="Atlantis")


def test_rejects_unknown_language(manager):
    with pytest.raises(ValueError):
        manager.validate_input(language="Klingon")


def test_url_points_at_requested_version(manager):
    url = manager.build_urls(country="France", version="9.9.9")[0]
    assert "/releases/download/9.9.9/" in url
    assert url.endswith("france.parquet")
