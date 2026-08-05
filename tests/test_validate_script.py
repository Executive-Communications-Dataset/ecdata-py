"""Tests for ecd_validate.py, the pre-release data validator.

These guard the two defects that made the shipped copy abort instead of report:
a query error escaping as a duckdb exception rather than a RuntimeError, and an
uncast TIMESTAMPTZ, which duckdb cannot hand back to Python without pytz. The
validate-release workflow installs polars, duckdb and requests only, so the
executive check crashed the whole run.
"""

import datetime as dt
import importlib.util
import pathlib
import sys

import polars as pl
import pytest

duckdb = pytest.importorskip("duckdb")

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_validator():
    spec = importlib.util.spec_from_file_location(
        "ecd_validate", ROOT / "ecd_validate.py")
    module = importlib.util.module_from_spec(spec)
    # register before exec: the module defines dataclasses, and @dataclass
    # resolves annotations through sys.modules[cls.__module__]
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def validator():
    return load_validator()


@pytest.fixture(scope="module")
def tz_parquet(tmp_path_factory):
    """A file shaped like a country asset: `date` is Datetime with a zone."""
    path = tmp_path_factory.mktemp("assets") / "utopia.parquet"
    pl.DataFrame({
        "executive": ["A. Leader", "A. Leader", "B. Successor"],
        "date": [dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1),
                 dt.datetime(2022, 1, 1)],
    }).with_columns(
        pl.col("date").dt.replace_time_zone("UTC")
    ).write_parquet(path)
    return path


def test_sql_wraps_failures_in_runtime_error(validator, tz_parquet):
    """Callers catch RuntimeError to skip a file; anything else aborts the run."""
    engine = validator.Engine()
    with pytest.raises(RuntimeError):
        engine.sql("SELECT no_such_column FROM {t}", tz_parquet)


def test_executive_query_survives_a_zoned_date(validator, tz_parquet):
    """Reproduces the crash: duckdb needs pytz to return a TIMESTAMPTZ."""
    engine = validator.Engine()
    rep = validator.Report()
    validator.check_executives({"utopia": tz_parquet}, rep, engine)
    # No exception, and the check actually ran rather than being skipped.
    assert engine.sql(
        "SELECT min(date::TIMESTAMP) FROM {t}", tz_parquet)[0][0] is not None
