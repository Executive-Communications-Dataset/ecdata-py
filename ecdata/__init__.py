# so you don't forget the equivalent of devtools::build is python setup.py sdist
# equivalent of submit_cran() is twine upload dist/*
# an important note you need to delete prior versions in dists just because pypi will yell at you
# because well it already exists on pypi

from typing import Optional, Union, Dict, List
import polars as pl
from memoization import cached
from .country_manager import CountryManager

_manager = CountryManager()

def country_dictionary() -> pl.DataFrame:
    """Returns a Polars dataframe of countries in the dataset"""
    return _manager._df

@cached(ttl=86400)
def load_ecd(country: Optional[Union[str, List[str], Dict]] = None,
             language: Optional[Union[str, List[str], Dict]] = None,
             full_ecd: bool = False,
             ecd_version: str = '1.0.0',
             cache: bool = True) -> pl.DataFrame:
    """Load Executive Communications Dataset
    
    Args:
        country: Country name(s) to filter by
        language: Language(s) to filter by
        full_ecd: When True downloads the full dataset
        ecd_version: Dataset version to use
        cache: Whether to cache results
    """
    if not any([country, language, full_ecd]):
        raise ValueError('Please provide a country name, language or set full_ecd to True')
        
    _manager.validate_input(country, language)
    
    if full_ecd:
        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'
        return pl.read_parquet(url)
        
    urls = _manager.build_urls(country, language, ecd_version)
    return pl.concat([pl.read_parquet(url) for url in urls], how='vertical')

@cached(ttl=86400)
def lazy_load_ecd(country: Optional[Union[str, List[str], Dict]] = None,
                  language: Optional[Union[str, List[str], Dict]] = None,
                  full_ecd: bool = False,
                  ecd_version: str = '1.0.0',
                  cache: bool = True) -> pl.LazyFrame:
    """Lazy load Executive Communications Dataset
    
    Args:
        country: Country name(s) to filter by. For a full list use country_dictionary()
        language: Language(s) to filter by. For a full list use country_dictionary()
        full_ecd: When True downloads the full dataset
        ecd_version: Dataset version to use
        cache: When true implements a caching method
    
    Returns:
        pl.LazyFrame: A lazy-loaded dataframe
    """
    if not any([country, language, full_ecd]):
        raise ValueError('Please provide a country name, language or set full_ecd to True')
        
    _manager.validate_input(country, language)
    
    if full_ecd:
        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'
        return pl.scan_parquet(url)
        
    urls = _manager.build_urls(country, language, ecd_version)
    return pl.concat([pl.scan_parquet(url) for url in urls], how='vertical')