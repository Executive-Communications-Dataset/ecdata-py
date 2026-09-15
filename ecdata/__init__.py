"""ecdata -- download and load the Executive Communications Dataset.

Packaging notes for maintainers:
    build a source dist   python setup.py sdist
    upload to PyPI        twine upload dist/*
    (clear old versions out of dist/ first; PyPI rejects re-uploads)
"""

import warnings
from typing import Dict, List, Optional, Union

import polars as pl
from collections.abc import Iterable, Mapping
from memoization import cached

from .country_manager import CountryManager, CountryInput

__all__ = [
    "country_dictionary",
    "load_ecd",
    "lazy_load_ecd",
    "get_ecd_release",
    "CANONICAL_COLUMNS",
    "SENTENCE_COLUMNS",
]

_manager = CountryManager()

DEFAULT_ECD_VERSION = "1.0.5"

# The schema the dataset documents. Country assets do not all conform to it, so
# `normalize_schema=True` projects whatever arrives onto this shape.
CANONICAL_COLUMNS: List[str] = [
    "country", "url", "text", "date", "title", "executive", "type",
    "language", "file", "isonumber", "gwc", "cowcodes", "polity_v",
    "polity_iv", "vdem", "year_of_statement", "office",
]

# The sentence view adds these to the documented schema. They are kept through
# normalisation rather than dropped with the scraper leftovers: without them a
# sentence cannot be put back in its document, and `block_index` is the only
# record of the release's own unit of observation.
SENTENCE_COLUMNS: List[str] = [
    "document_id", "block_index", "sentence_index", "segmenter",
]


def _release_tag(ecd_version: str, unit: str) -> str:
    """The release holding a version at a given unit of observation.

    The sentence view lives under its own tag, so publishing it never modifies
    the release it was derived from.
    """
    return f"{ecd_version}-sentences" if unit == "sentence" else ecd_version


# Columns that carry canonical data under a different name in some assets.
# Verified against release 1.0.0: portugal.parquet has no `url` but its `urls`
# column is fully populated; dominican_republic.parquet has no `title` but
# `subject` holds the headline.
_COLUMN_ALIASES: Dict[str, str] = {
    "urls": "url",
    "subject": "title",
}

# Assets with known upstream defects, so users are told before they analyse
# rather than after. Every entry is a finding in docs/validation-report-1.0.0.txt
# at ERROR or CRITICAL; the README table lists the same set. Remove entries here
# as the data is corrected upstream.
_KNOWN_DATA_ISSUES: Dict[str, Dict[str, str]] = {
    "1.0.0": {
        "ecuador": (
            "ecuador.parquet and dominican_republic.parquet contain the same "
            "pooled corpus (identical 14,994 URLs). About 90% of the rows "
            "labelled Ecuador are sourced from gobiernodanilomedina.do and are "
            "Dominican Republic documents."
        ),
        "dominican_republic": (
            "dominican_republic.parquet shares its corpus with ecuador.parquet; "
            "roughly 21,000 rows attributed to Luis Abinader are sourced from "
            "Ecuadorian government sites."
        ),
        "venezuela": (
            "venezuela.parquet text is mis-decoded (UTF-8 read as latin-1) in "
            "almost every row, e.g. 'RepÃºblica' for 'República'."
        ),
        "jamaica": (
            "Every jamaica.parquet url has the host concatenated onto an "
            "already absolute link, so none of them resolve."
        ),
        "india": (
            "india.parquet holds 7.97M rows from only 3,029 distinct URLs; "
            "about 99% are exact duplicates. Pass deduplicate=True."
        ),
        "denmark": (
            "denmark.parquet holds 4.8M rows from only 2,658 distinct URLs; "
            "about 99% are exact duplicates. Pass deduplicate=True."
        ),
        "france": (
            "123,286 of france.parquet's 139,666 rows (88.3%) are exact "
            "duplicates; 16,380 distinct rows remain. Pass deduplicate=True."
        ),
        "turkey": (
            "234,327 of turkey.parquet's 299,628 rows (78.2%) are exact "
            "duplicates; 65,301 distinct rows remain. Pass deduplicate=True."
        ),
        "mexico": (
            "594,967 of mexico.parquet's 933,412 rows (63.7%) are exact "
            "duplicates; 338,445 distinct rows remain. Pass deduplicate=True."
        ),
        "austria": (
            "20,685 of austria.parquet's 34,579 rows (59.8%) are exact "
            "duplicates; 13,894 distinct rows remain. Pass deduplicate=True."
        ),
        "republic_of_korea": (
            "7,218 of republic_of_korea.parquet's 16,420 rows (44.0%) are exact "
            "duplicates; 9,202 distinct rows remain. Pass deduplicate=True."
        ),
        "colombia": (
            "All 2,603 colombia.parquet rows cite a YouTube watch URL rather "
            "than an official source, so no document is traceable to a citable "
            "government record. 22.5% are also exact duplicates."
        ),
        "russia": (
            "url is 100% null in russia.parquet, so no document is traceable to "
            "a source. The text is the English-language kremlin.ru edition, not "
            "the Russian original, and language is labelled 'English'."
        ),
        "united_states_of_america": (
            "executive is unreliable in united_states_of_america.parquet: Biden "
            "is split across 'Joseph R. Biden, Jr.' (475 rows) and 'Jooseph R. "
            "Biden, Jr.' (115,871), the Obama/Trump handover is dated 2016-01-20 "
            "rather than 2017-01-20, and Gerald R. Ford's rows run to 1996. The "
            "type column also holds president names, and language is 100% null."
        ),
    },
    # 1.0.1 is 1.0.0 with the repairable defects fixed (see the release notes).
    # What is left needs the source data back or a decision about what a column
    # means, so it still warns.
    "1.0.1": {
        "colombia": (
            "Every url in colombia.parquet is a YouTube link rather than an "
            "official record, and whether the text is an official transcript or "
            "an auto-generated caption track is not stated."
        ),
        "russia": (
            "url is 100% null in russia.parquet, and the text is the English "
            "kremlin.ru edition rather than the Russian original."
        ),
        "united_states_of_america": (
            "executive is unreliable in united_states_of_america.parquet: the "
            "Obama/Trump handover is dated 2016-01-20 rather than 2017-01-20 and "
            "Gerald R. Ford's rows run to 1996. The type column also holds "
            "president names, and language is 100% null."
        ),
        "venezuela": (
            "100 rows of venezuela.parquet could not be repaired: a byte was lost "
            "at ingest, so the mis-decoded text does not round-trip."
        ),
        "ecuador": (
            "ecuador.parquet covers 2023-11-23 to 2024-03-19 only (2,236 "
            "documents, Daniel Noboa). The wider span in 1.0.0 was an artefact of "
            "a corpus pooled with the Dominican Republic."
        ),
        "dominican_republic": (
            "dominican_republic.parquet ends 2020-08-16 and is almost entirely "
            "Danilo Medina. There is no Luis Abinader corpus."
        ),
        **{
            country: (f"Executive terms overlap in {country}.parquet: some "
                      f"documents are credited to the wrong leader.")
            for country in ("austria", "brazil", "chile", "denmark", "greece",
                            "israel")
        },
        "italy": (
            "Executive terms overlap in italy.parquet, and some values are "
            "composite ('Romano Prodi/Massimo D'Alema'), so they will not group "
            "or join."
        ),
    },
}

# 1.0.2 is 1.0.1 with `language` filled for Portugal and the United States. Every
# other caveat carries over, so the table is inherited rather than restated.
_KNOWN_DATA_ISSUES["1.0.2"] = {
    **_KNOWN_DATA_ISSUES["1.0.1"],
    "united_states_of_america": (
        "executive is unreliable in united_states_of_america.parquet: the "
        "Obama/Trump handover is dated 2016-01-20 rather than 2017-01-20 and "
        "Gerald R. Ford's rows run to 1996. The type column also holds "
        "president names."
    ),
}

# 1.0.3 corrects `executive`: the Obama/Trump handover, and Italy's composite
# values. It is not built on 1.0.2's table, because several of that table's
# entries were warning about term overlaps that turned out to be an artefact of
# the validator comparing date spans rather than counting documents. Austria,
# Denmark, Greece, Israel and Italy hold no double-attributed documents at all.
_KNOWN_DATA_ISSUES["1.0.3"] = {
    "colombia": _KNOWN_DATA_ISSUES["1.0.1"]["colombia"],
    "venezuela": _KNOWN_DATA_ISSUES["1.0.1"]["venezuela"],
    "ecuador": _KNOWN_DATA_ISSUES["1.0.1"]["ecuador"],
    "dominican_republic": _KNOWN_DATA_ISSUES["1.0.1"]["dominican_republic"],
    "russia": (
        "url is 100% null in russia.parquet, and the text is the English "
        "kremlin.ru edition rather than the Russian original."
    ),
    "united_states_of_america": (
        "The type column in united_states_of_america.parquet holds president "
        "names alongside genuine document categories, so it cannot be used as a "
        "filter."
    ),
    "brazil": (
        "A few brazil.parquet rows are credited to Michel Temer but dated before "
        "he took office in May 2016."
    ),
    "chile": (
        "A few chile.parquet rows are credited to Gabriel Boric but dated before "
        "he took office in March 2022."
    ),
}

# 1.0.4 corrected three mis-dated documents; 1.0.5 rebuilt the US `type` column
# and settled Colombia's and Russia's provenance as documented rather than
# defective. Both inherit 1.0.3's table with the entries that no longer apply
# removed, and Colombia and Russia reworded: they describe how those corpora were
# collected, not something wrong with them.
_KNOWN_DATA_ISSUES["1.0.4"] = {
    k: v for k, v in _KNOWN_DATA_ISSUES["1.0.3"].items()
    if k not in ("brazil", "chile")
}

_KNOWN_DATA_ISSUES["1.0.5"] = {
    "colombia": (
        "colombia.parquet is transcripts of YouTube videos published by the "
        "presidency, so url points at the video a transcript came from rather "
        "than at a government page. That is the source, not a defect."
    ),
    "russia": (
        "russia.parquet comes from a pre-existing dataset rather than a scrape "
        "of kremlin.ru, so the fields a scrape would have filled are absent: url "
        "and type are empty, and the text is the English-language edition."
    ),
    "venezuela": _KNOWN_DATA_ISSUES["1.0.1"]["venezuela"],
    "ecuador": _KNOWN_DATA_ISSUES["1.0.1"]["ecuador"],
    "dominican_republic": _KNOWN_DATA_ISSUES["1.0.1"]["dominican_republic"],
    "united_states_of_america": (
        "type in united_states_of_america.parquet was rebuilt from the source "
        "url in 1.0.5. 17,243 rows had no reliable prefix and are null rather "
        "than guessed at."
    ),
}

# Releases whose full_ecd.parquet does not reconcile against the country assets.
# 1.0.1 rebuilt it from them, so it is absent here and no warning fires.
_FULL_ECD_ISSUES: Dict[str, str] = {
    "1.0.0": (
        "full_ecd.parquet duplicates Ecuador (429,954 rows against 214,977 in "
        "the country asset) and contains a single empty row for Portugal in "
        "place of its 64,522 documents. Load those two countries individually."
    ),
}


def country_dictionary() -> pl.DataFrame:
    """Return the countries in the dataset and their release file names.

    A copy, so that a caller reshaping the frame it gets back cannot leave the
    lookup table the rest of the package depends on in a broken state.
    """
    return _manager._df.clone()


def get_ecd_release(**kwargs) -> List[str]:
    """List the dataset releases published on GitHub."""
    return CountryManager.get_ecd_release(**kwargs)


def _warn_known_issues(urls: List[str], version: str) -> None:
    issues = _KNOWN_DATA_ISSUES.get(version, {})
    if not issues:
        return
    for url in urls:
        name = url.rsplit("/", 1)[-1].removesuffix(".parquet")
        if name in issues:
            warnings.warn(f"[ecdata {version}] {issues[name]}",
                          UserWarning, stacklevel=3)


def _warn_for_request(country, language, full_ecd, ecd_version,
                      unit="document") -> None:
    """Emit known-issue warnings for a request.

    Called from the public entry points rather than from _build, so that a
    memoized result still warns. Otherwise a user sees the caveat once per
    session and never again.
    """
    if full_ecd:
        message = _FULL_ECD_ISSUES.get(ecd_version)
        if message:
            warnings.warn(f"[ecdata {ecd_version}] {message}",
                          UserWarning, stacklevel=3)
        return
    if not country and not language:
        # No selector: build_urls would match the whole release and warn about
        # every affected file, for a call that is about to raise anyway.
        return
    try:
        urls = _manager.build_urls(country, language, ecd_version)
    except Exception:
        return          # invalid input is reported by validate_input
    _warn_known_issues(urls, ecd_version)

    # The release's own caveats still apply to its sentence view; this is the
    # one the view adds.
    if unit == "sentence" and any(
            u.rsplit("/", 1)[-1] == "colombia.parquet" for u in urls):
        warnings.warn(
            "[ecdata] colombia is not segmented in the sentence view: its rows "
            "are auto-generated YouTube captions with no punctuation to split "
            "on, so each row is returned whole. See the `segmenter` column.",
            UserWarning, stacklevel=3,
        )


def _hashable(value):
    """Make a selector usable as a memoization key.

    Lists and sets are unhashable, so passing one to the cached helper raised.
    Sets are also unordered, so they are sorted to keep the key stable.
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        return tuple(sorted(str(k) for k in value))
    if isinstance(value, Iterable):
        return tuple(sorted(str(v) for v in value))
    return value


def _normalize(frame, columns: List[str]):
    """Project a frame onto CANONICAL_COLUMNS, recovering aliased columns."""
    renames = {src: dst for src, dst in _COLUMN_ALIASES.items()
               if src in columns and dst not in columns}
    if renames:
        frame = frame.rename(renames)
        columns = [renames.get(c, c) for c in columns]

    missing = [c for c in CANONICAL_COLUMNS if c not in columns]
    if missing:
        frame = frame.with_columns(
            [pl.lit(None).alias(c) for c in missing]
        )
    extra = [c for c in SENTENCE_COLUMNS if c in columns]
    return frame.select(CANONICAL_COLUMNS + extra)


def _build(country, language, full_ecd, ecd_version, normalize_schema,
           deduplicate, lazy, unit="document"):
    if unit not in ("document", "sentence"):
        raise ValueError("unit must be 'document' or 'sentence'")
    if not any([country, language, full_ecd]):
        raise ValueError(
            "Please provide a country name, language or set full_ecd to True"
        )
    if unit == "sentence" and full_ecd:
        raise ValueError(
            "There is no pooled file for the sentence view. "
            "Ask for countries or languages instead."
        )

    release = _release_tag(ecd_version, unit)
    reader = pl.scan_parquet if lazy else pl.read_parquet

    if full_ecd:
        url = (f"https://github.com/Executive-Communications-Dataset/ecdata"
               f"/releases/download/{release}/full_ecd.parquet")
        frame = reader(url)
    else:
        _manager.validate_input(country, language)
        urls = _manager.build_urls(country, language, release)
        if not urls:
            raise ValueError(
                "No release files matched that country/language combination. "
                "Call country_dictionary() for the valid values."
            )
        frames = [reader(url) for url in urls]
        # Country assets do not share a schema: some are missing canonical
        # columns, some carry extra scraper columns, and `date` is Date in two
        # of them and Datetime(UTC) elsewhere. A plain vertical concat raises
        # on all of those, so align by name and let dtypes find a supertype.
        frame = (frames[0] if len(frames) == 1
                 else pl.concat(frames, how="diagonal_relaxed"))

    columns = (frame.collect_schema().names() if lazy else frame.columns)
    if normalize_schema:
        frame = _normalize(frame, columns)

    if deduplicate:
        subset = [c for c in ("country", "url", "text", "date")
                  if c in (CANONICAL_COLUMNS if normalize_schema else columns)]
        frame = frame.unique(subset=subset, keep="first", maintain_order=True)

    return frame


@cached(ttl=86400)
def _load_ecd_cached(country, language, full_ecd, ecd_version,
                     normalize_schema, deduplicate, unit):
    return _build(country, language, full_ecd, ecd_version,
                  normalize_schema, deduplicate, lazy=False, unit=unit)


@cached(ttl=86400)
def _lazy_load_ecd_cached(country, language, full_ecd, ecd_version,
                          normalize_schema, deduplicate, unit):
    return _build(country, language, full_ecd, ecd_version,
                  normalize_schema, deduplicate, lazy=True, unit=unit)


def load_ecd(country: Optional[CountryInput] = None,
             language: Optional[CountryInput] = None,
             full_ecd: bool = False,
             ecd_version: str = DEFAULT_ECD_VERSION,
             cache: bool = True,
             unit: str = "document",
             normalize_schema: bool = True,
             deduplicate: bool = False) -> pl.DataFrame:
    """Load the Executive Communications Dataset.

    Args:
        country: Country name(s) to filter by. A single name, any iterable of
            names, or a mapping whose keys are names. See country_dictionary().
        language: Language(s) to filter by.
        full_ecd: When True, download the pooled dataset instead.
        ecd_version: Release tag to download.
        cache: Memoize the result for 24 hours. Set False to force a fresh
            download; previously this argument was accepted and ignored.
        unit: "document", the default, is the release as published, where a
            row is whatever the scraper produced for that country -- a whole
            document, a paragraph, a sentence or an HTML block. "sentence"
            loads the sentence-level view of the same release: one row per
            sentence, with document_id, block_index and sentence_index added so
            a sentence can be put back in its document. Colombia is not
            segmented there; its rows carry no punctuation to split on.
        normalize_schema: Project the result onto CANONICAL_COLUMNS, filling
            absent columns with null and recovering columns that appear under a
            different name in some assets. Set False to see the raw columns.
        deduplicate: Drop exact duplicate rows on
            (country, url, text, date). Several assets in release 1.0.0 are
            heavily duplicated; this is off by default so that row counts match
            the published files unless you ask otherwise.

    Returns:
        pl.DataFrame
    """
    args = (_hashable(country), _hashable(language), full_ecd, ecd_version,
            normalize_schema, deduplicate, unit)
    _warn_for_request(country, language, full_ecd, ecd_version, unit)
    if cache:
        return _load_ecd_cached(*args)
    return _build(*args[:-1], lazy=False, unit=unit)


def lazy_load_ecd(country: Optional[CountryInput] = None,
                  language: Optional[CountryInput] = None,
                  full_ecd: bool = False,
                  ecd_version: str = DEFAULT_ECD_VERSION,
                  cache: bool = True,
                  unit: str = "document",
                  normalize_schema: bool = True,
                  deduplicate: bool = False) -> pl.LazyFrame:
    """Lazily load the Executive Communications Dataset.

    Takes the same arguments as load_ecd and returns a pl.LazyFrame.
    """
    args = (_hashable(country), _hashable(language), full_ecd, ecd_version,
            normalize_schema, deduplicate, unit)
    _warn_for_request(country, language, full_ecd, ecd_version, unit)
    if cache:
        return _lazy_load_ecd_cached(*args)
    return _build(*args[:-1], lazy=True, unit=unit)
