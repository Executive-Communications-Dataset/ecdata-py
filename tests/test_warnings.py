"""Tests for the known-issue warnings. No network required.

Warnings are raised from the URL the request resolves to, before anything is
downloaded, so the whole affected-country table can be exercised offline.
"""

import warnings

import pytest

import ecdata as ec


def warnings_for(country=None, language=None, full_ecd=False):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ec._warn_for_request(country, language, full_ecd, "1.0.0")
    return [str(w.message) for w in caught]


# The countries the README's known-issues table promises a warning for. Keeping
# the list here means the table and the code cannot drift apart silently.
DUPLICATED = ["India", "Denmark", "France", "Turkey", "Mexico", "Austria",
              "Republic of Korea"]
OTHER_AFFECTED = ["Ecuador", "Dominican Republic", "Venezuela", "Jamaica",
                  "Colombia", "Russia", "United States of America"]


@pytest.mark.parametrize("country", DUPLICATED)
def test_duplicated_country_warns_and_points_at_deduplicate(country):
    messages = warnings_for(country=country)
    assert messages, f"{country} is documented as duplicated but does not warn"
    assert any("deduplicate=True" in m for m in messages), messages


@pytest.mark.parametrize("country", OTHER_AFFECTED)
def test_affected_country_warns(country):
    assert warnings_for(country=country), f"{country} does not warn"


def test_unaffected_country_stays_quiet():
    assert warnings_for(country="Costa Rica") == []


def test_language_request_warns_for_the_files_it_touches():
    messages = warnings_for(language="Danish")
    assert any("denmark.parquet" in m for m in messages), messages


def test_no_selector_does_not_warn():
    """A call with no selector warned about six unrelated files, then raised.

    _warn_for_request ran before the selector check and build_urls(None, None)
    matches the whole release.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(ValueError):
            ec.load_ecd()
    assert [str(w.message) for w in caught] == []


def test_full_ecd_warns_about_ecuador_and_portugal():
    messages = warnings_for(full_ecd=True)
    assert len(messages) == 1
    assert "Ecuador" in messages[0] and "Portugal" in messages[0]


def test_unknown_country_does_not_warn_before_raising():
    assert warnings_for(country="Atlantis") == []


def test_a_later_release_carries_no_warnings():
    """The table is keyed on version, so 1.0.0's caveats must not leak."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ec._warn_for_request("Jamaica", None, False, "2.0.0")
    assert [str(w.message) for w in caught] == []
