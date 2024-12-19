import pytest
import polars as pl
from ecdata import lazy_load_ecd, country_dictionary

def test_country_dictionary():
    df = country_dictionary()
    assert isinstance(df, pl.DataFrame)
    assert not df.is_empty()
    assert 'country' in df.columns
    assert 'language' in df.columns

def test_lazy_load_ecd_single_country():
    df = lazy_load_ecd(country='United States')
    assert isinstance(df, pl.LazyFrame)
    collected = df.collect()
    assert not collected.is_empty()
    assert 'text' in collected.columns

def test_lazy_load_ecd_multiple_countries():
    df = lazy_load_ecd(country=['United States', 'United Kingdom'])
    assert isinstance(df, pl.LazyFrame)
    collected = df.collect()
    assert not collected.is_empty()

def test_lazy_load_ecd_single_language():
    df = lazy_load_ecd(language='English')
    assert isinstance(df, pl.LazyFrame)
    collected = df.collect()
    assert not collected.is_empty()

def test_lazy_load_ecd_full():
    df = lazy_load_ecd(full_ecd=True)
    assert isinstance(df, pl.LazyFrame)
    collected = df.collect()
    assert not collected.is_empty()

def test_invalid_input():
    with pytest.raises(ValueError):
        lazy_load_ecd()  # No parameters should raise ValueError

def test_invalid_country():
    with pytest.raises(ValueError):
        lazy_load_ecd(country='NonexistentCountry')

def test_invalid_language():
    with pytest.raises(ValueError):
        lazy_load_ecd(language='NonexistentLanguage')