name = 'ecdata'

# so you don't forget the equivalent of devtools::build is python setup.py sdist
# equivalent of submit_cran() is twine upload dist/*
# an important note you need to delete prior versions in dists just because pypi will yell at you
# because well it already exists on pypi


import polars as pl
import requests 
import os
import subprocess
from pathlib import Path
import warnings
from memoization import cached

__doc__ = """

ecdata - A Python package for working with The Executive Communications Dataset 

=========================================================
**ecdata** is a Python package that streamlines importing The Executive Communications Dataset

Functions
---------

country_dictionary - Returns a Polars dataframe of countries in the dataset
load_ecd - Main function for loading in dataset 
example_scrapper- Opens an example scrapper 


"""




import polars as pl

def country_dictionary():

    country_names =  [{"file_name":"argentina","language":"Spanish","abbr":"ARG","name_in_dataset":"Argentina"},{"file_name":"argentina","language":"Spanish","abbr":"AR","name_in_dataset":"Argentina"},{"file_name":"australia","language":"English","abbr":"AUS","name_in_dataset":"Australia"},{"file_name":"australia","language":"English","abbr":"AU","name_in_dataset":"Australia"},{"file_name":"austria","language":"German","abbr":"AUT","name_in_dataset":"Austria"},{"file_name":"austria","language":"German","abbr":"AT","name_in_dataset":"Austria"},{"file_name":"azerbaijan","language":"English","abbr":"AZE","name_in_dataset":"Azerbaijan"},{"file_name":"azerbaijan","language":"English","abbr":"AZ","name_in_dataset":"Azerbaijan"},{"file_name":"azerbaijan","language":"English","abbr":"AZE","name_in_dataset":"Azerbaijan"},{"file_name":"azerbaijan","language":"English","abbr":"AZ","name_in_dataset":"Azerbaijan"},{"file_name":"bolivia","language":"Spanish","abbr":"BOL","name_in_dataset":"Bolivia"},{"file_name":"bolivia","language":"Spanish","abbr":"BO","name_in_dataset":"Bolivia"},{"file_name":"brazil","language":"Portugese","abbr":"BRA","name_in_dataset":"Brazil"},{"file_name":"brazil","language":"Portugese","abbr":"BR","name_in_dataset":"Brazil"},{"file_name":"canada","language":"English","abbr":"CAN","name_in_dataset":"Canada"},{"file_name":"canada","language":"English","abbr":"CA","name_in_dataset":"Canada"},{"file_name":"chile","language":"Spanish","abbr":"CHL","name_in_dataset":"Chile"},{"file_name":"chile","language":"Spanish","abbr":"CL","name_in_dataset":"Chile"},{"file_name":"colombia","language":"Spanish","abbr":"COL","name_in_dataset":"Colombia"},{"file_name":"colombia","language":"Spanish","abbr":"CO","name_in_dataset":"Colombia"},{"file_name":"costa_rica","language":"Spanish","abbr":"CRI","name_in_dataset":"Costa Rica"},{"file_name":"costa_rica","language":"Spanish","abbr":"CR","name_in_dataset":"Costa Rica"},{"file_name":"czechia","language":"Czech","abbr":"CZE","name_in_dataset":"Czechia"},{"file_name":"czechia","language":"Czech","abbr":"CZ","name_in_dataset":"Czechia"},{"file_name":"denmark","language":"Danish","abbr":"DNK","name_in_dataset":"Denmark"},{"file_name":"denmark","language":"Danish","abbr":"DK","name_in_dataset":"Denmark"},{"file_name":"dominican_republic","language":"Spanish","abbr":"DOM","name_in_dataset":"Dominican Republic"},{"file_name":"dominican_republic","language":"Spanish","abbr":"DO","name_in_dataset":"Dominican Republic"},{"file_name":"ecuador","language":"Spanish","abbr":"ECU","name_in_dataset":"Ecuador"},{"file_name":"ecuador","language":"Spanish","abbr":"EC","name_in_dataset":"Ecuador"},{"file_name":"france","language":"French","abbr":"FRA","name_in_dataset":"France"},{"file_name":"france","language":"French","abbr":"FR","name_in_dataset":"France"},{"file_name":"georgia","language":"Georgian","abbr":"GEO","name_in_dataset":"Georgia"},{"file_name":"georgia","language":"Georgian","abbr":"GE","name_in_dataset":"Georgia"},{"file_name":"germany","language":"German","abbr":"DEU","name_in_dataset":"Germany"},{"file_name":"germany","language":"German","abbr":"DE","name_in_dataset":"Germany"},{"file_name":"greece","language":"Greek","abbr":"GRC","name_in_dataset":"Greece"},{"file_name":"greece","language":"Greek","abbr":"GR","name_in_dataset":"Greece"},{"file_name":"hong_kong","language":"Chinese","abbr":"HKG","name_in_dataset":"Hong Kong"},{"file_name":"hong_kong","language":"Chinese","abbr":"HK","name_in_dataset":"Hong Kong"},{"file_name":"hungary","language":"Hungarian","abbr":"HUN","name_in_dataset":"Hungary"},{"file_name":"hungary","language":"Hungarian","abbr":"HU","name_in_dataset":"Hungary"},{"file_name":"iceland","language":"Icelandic","abbr":"ISL","name_in_dataset":"Iceland"},{"file_name":"iceland","language":"Icelandic","abbr":"IS","name_in_dataset":"Iceland"},{"file_name":"india","language":"English","abbr":"IND","name_in_dataset":"India"},{"file_name":"india","language":"English","abbr":"IN","name_in_dataset":"India"},{"file_name":"india","language":"Hindi","abbr":"IND","name_in_dataset":"India"},{"file_name":"india","language":"Hindi","abbr":"IN","name_in_dataset":"India"},{"file_name":"indonesia","language":"Indonesian","abbr":"IDN","name_in_dataset":"Indonesia"},{"file_name":"indonesia","language":"Indonesian","abbr":"ID","name_in_dataset":"Indonesia"},{"file_name":"israel","language":"Hebrew","abbr":"ISR","name_in_dataset":"Israel"},{"file_name":"israel","language":"Hebrew","abbr":"IL","name_in_dataset":"Israel"},{"file_name":"italy","language":"Italian","abbr":"ITA","name_in_dataset":"Italy"},{"file_name":"italy","language":"Italian","abbr":"IT","name_in_dataset":"Italy"},{"file_name":"jamaica","language":"English","abbr":"JAM","name_in_dataset":"Jamaica"},{"file_name":"jamaica","language":"English","abbr":"JM","name_in_dataset":"Jamaica"},{"file_name":"japan","language":"Japanese","abbr":"JPN","name_in_dataset":"Japan"},{"file_name":"japan","language":"Japanese","abbr":"JP","name_in_dataset":"Japan"},{"file_name":"mexico","language":"Spanish","abbr":"MEX","name_in_dataset":"Mexico"},{"file_name":"mexico","language":"Spanish","abbr":"MX","name_in_dataset":"Mexico"},{"file_name":"new_zealand","language":"English","abbr":"NZL","name_in_dataset":"New Zealand"},{"file_name":"new_zealand","language":"English","abbr":"NZ","name_in_dataset":"New Zealand"},{"file_name":"nigeria","language":"English","abbr":"NGA","name_in_dataset":"Nigeria"},{"file_name":"nigeria","language":"English","abbr":"NG","name_in_dataset":"Nigeria"},{"file_name":"norway","language":"Norwegian","abbr":"NOR","name_in_dataset":"Norway"},{"file_name":"norway","language":"Norwegian","abbr":"NO","name_in_dataset":"Norway"},{"file_name":"philippines","language":"Filipino","abbr":"PHL","name_in_dataset":"Philippines"},{"file_name":"philippines","language":"Filipino","abbr":"PH","name_in_dataset":"Philippines"},{"file_name":"poland","language":"Polish","abbr":"POL","name_in_dataset":"Poland"},{"file_name":"poland","language":"Polish","abbr":"PL","name_in_dataset":"Poland"},{"file_name":"portugal","language":"Portugese","abbr":"PRT","name_in_dataset":"Portugal"},{"file_name":"portugal","language":"Portugese","abbr":"PT","name_in_dataset":"Portugal"},{"file_name":"russia","language":"English","abbr":"RUS","name_in_dataset":"Russia"},{"file_name":"russia","language":"English","abbr":"RU","name_in_dataset":"Russia"},{"file_name":"russia","language":"English","abbr":"RUS","name_in_dataset":"Russia"},{"file_name":"russia","language":"English","abbr":"RU","name_in_dataset":"Russia"},{"file_name":"spain","language":"Spanish","abbr":"ESP","name_in_dataset":"Spain"},{"file_name":"spain","language":"Spanish","abbr":"ES","name_in_dataset":"Spain"},{"file_name":"turkey","language":"Turkish","abbr":"TUR","name_in_dataset":"Turkey"},{"file_name":"turkey","language":"Turkish","abbr":"TR","name_in_dataset":"Turkey"},{"file_name":"united_kingdom","language":"English","abbr":"GBR","name_in_dataset":"United Kingdom"},{"file_name":"united_kingdom","language":"English","abbr":"GBR","name_in_dataset":"Great Britain"},{"file_name":"united_kingdom","language":"English","abbr":"GB","name_in_dataset":"United Kingdom"},{"file_name":"united_kingdom","language":"English","abbr":"GB","name_in_dataset":"Great Britain"},{"file_name":"united_kingdom","language":"English","abbr":"UK","name_in_dataset":"United Kingdom"},{"file_name":"united_kingdom","language":"English","abbr":"UK","name_in_dataset":"Great Britain"},{"file_name":"uruguay","language":"Spanish","abbr":"URY","name_in_dataset":"Uruguay"},{"file_name":"uruguay","language":"Spanish","abbr":"UY","name_in_dataset":"Uruguay"},{"file_name":"venezuela","language":"Spanish","abbr":"VEN","name_in_dataset":"Venezuela"},{"file_name":"venezuela","language":"Spanish","abbr":"VE","name_in_dataset":"Venezuela"},{"file_name":"united_states_of_america","language":"English","abbr":"USA","name_in_dataset":"United States of America"},{"file_name":"united_states_of_america","language":"English","abbr":"USA","name_in_dataset":"United States"},{"file_name":"united_states_of_america","language":"English","abbr":"US","name_in_dataset":"United States of America"},{"file_name":"united_states_of_america","language":"English","abbr":"US","name_in_dataset":"United States"},{"file_name":"republic_of_korea","language":"Korean","abbr":"KOR","name_in_dataset":"Republic of Korea"},{"file_name":"republic_of_korea","language":"Korean","abbr":"KOR","name_in_dataset":"South Korea"},{"file_name":"republic_of_korea","language":"Korean","abbr":"KR","name_in_dataset":"Republic of Korea"},{"file_name":"republic_of_korea","language":"Korean","abbr":"KR","name_in_dataset":"South Korea"}]
   
    return pl.DataFrame(country_names)



def link_builder(country=None, language=None, ecd_version='1.0.0'):
    if isinstance(country, str):
        country = [country]
    
    if isinstance(language, str):
        language = [language]

    country = [c.lower() for c in country] if country else None
    language = [l.lower() for l in language] if language else None
    
    
    country_names = country_dictionary().with_columns(
        (pl.col('name_in_dataset').str.to_lowercase().alias('name_in_dataset')),
        (pl.col('language').str.to_lowercase().alias('language')),
        (pl.col('abbr').str.to_lowercase().alias('abbr'))
    )
    
    if country:
        country_names = country_names.filter((pl.col('name_in_dataset').is_in(country)) | (pl.col('abbr').is_in(country)))
    elif language:
        country_names = country_names.filter(pl.col('language').is_in(language))
    
    
    country_names = country_names.with_columns(
        url='https://github.com/Executive-Communications-Dataset/ecdata/releases/download/' + 
            f'{ecd_version}' + '/' + pl.col('file_name') + '.parquet'
    )
    
    country_names = country_names.unique(subset= 'url')
    
    country_names = country_names['url']
    return country_names





def get_ecd_release(repo='Executive-Communications-Dataset/ecdata', token=None, verbose=True):
   
    owner, repo_name = repo.split('/')
    
    headers = {}
    if token:
        headers['Authorization'] = f'token {token}'
    
    try:
        releases_url = f"https://api.github.com/repos/{owner}/{repo_name}/releases"
        releases_response = requests.get(releases_url, headers=headers)
        releases_response.raise_for_status()
        releases = releases_response.json()
        
        if len(releases) == 0:
            if verbose:
                print(f"No GitHub releases found for {repo}!")
            return []
        
    except requests.exceptions.RequestException as e:
        print(f"Cannot access release data for repo {repo}. Error: {str(e)}")
        return []
    
    try:
        latest_url = f"https://api.github.com/repos/{owner}/{repo_name}/releases/latest"
        latest_response = requests.get(latest_url, headers=headers)
        latest_response.raise_for_status()
        latest_release = latest_response.json().get('tag_name', None)
    except requests.exceptions.RequestException as e:
        print(f"Cannot access latest release data for repo {repo}. Error: {str(e)}")
        latest_release = None


    out = []
    for release in releases:
        release_data = {
            "release_name": release.get("name", ""),
            "release_id": release.get("id", ""),
            "release_body": release.get("body", ""),
            "tag_name": release.get("tag_name", ""),
            "draft": release.get("draft", False),
            "latest": release.get("tag_name", "") == latest_release,
            "created_at": release.get("created_at", ""),
            "published_at": release.get("published_at", ""),
            "html_url": release.get("html_url", ""),
            "upload_url": release.get("upload_url", ""),
            "n_assets": len(release.get("assets", []))
        }
        out.append(release_data)
        out = pl.concat([pl.DataFrame(i) for i in out], how = 'vertical')
        out = out['release_name']
    
    return out


def validate_input(country=None,language=None , full_ecd=False, version='1.0.0'):
    
    release = get_ecd_release()
    
    countries_df = country_dictionary().with_columns(
        (pl.col('name_in_dataset').str.to_lowercase().alias('name_in_dataset')),
        (pl.col('language').str.to_lowercase().alias('language')),
        (pl.col('abbr').str.to_lowercase().alias('abbr'))
    )

    valid_countries = countries_df['name_in_dataset'].to_list()

    valid_languages = countries_df['language'].to_list()

    valid_abbr = countries_df['abbr'].to_list()

   
    if country is not None and not isinstance(country, (str, list, dict)):
        country_type = type(country)
        raise ValueError(f'Please provide a str, list, or dict to country. You provided {country_type}')
    
    if language is not None and not isinstance(language, (str, list, dict)):
        country_type = type(country)
        raise ValueError(f'Please provide a str, list, or dict to country. You provided {country_type}')

    
    if country is None and not full_ecd and language is None:
        raise ValueError('Please provide a country name, language or set full_ecd to True')


    if version not in release:
        raise ValueError(f'{version} is not a valid version. Set ecd_version to one of {release}')
    
   
    if language is not None:
        if isinstance(language, str):
            language_lower = language.lower()
            if language_lower not in valid_languages:
                raise ValueError(f'{language} is not a valid language name in our dataset. Call country_dictionary for a list of valid inputs')
        elif isinstance(language, list):
            invalid_languages = [c for c in language if c.lower() not in language]
            if invalid_languages:
                raise ValueError(f'These countries are not valid: {invalid_languages}. Call country_dictionary for a list of valid inputs')
        elif isinstance(language, dict):
            invalid_langauges = [c for c in language.keys() if c.lower() not in valid_languages]
            if invalid_languages:
                raise ValueError(f'These keys in your dictionary are not valid language names: {invalid_languages}. Call country_dictionary for a list of valid inputs')
    if country is not None:
        if isinstance(country, str):
            country_lower = country.lower()
            if country_lower not in valid_countries and country_lower not in valid_abbr :
                raise ValueError(f'{country} is not a valid country name in our dataset. Call country_dictionary for a list of valid inputs')
        elif isinstance(country, list):
            invalid_countries = [cty for cty in country if cty.lower() not in valid_countries and cty.lower() not in valid_abbr ]
            if invalid_countries:
                raise ValueError(f'These countries are not valid: {invalid_countries}. Call country_dictionary for a list of valid inputs')
        elif isinstance(country, dict):
            invalid_countries = [cty for cty in country.keys() if cty.lower() not in valid_countries and cty.lower() not in valid_abbr]
            if invalid_countries:
                raise ValueError(f'These keys in your dictionary are not valid country names: {invalid_countries}. Call country_dictionary for a list of valid inputs')

    return True 



def load_from_url(country = None,language = None, full_ecd = False, ecd_version = '1.0.0'):
 
    validate_input(country = country,language= language, full_ecd=full_ecd, version=ecd_version)


    if country is None and full_ecd is True:

        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'

        ecd_data = pl.read_parquet(url)
    
    elif country is not None and full_ecd is False:

        urls = link_builder(country = country, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')
    
    elif country is None and full_ecd is False and language is not None:

        urls = link_builder(language = language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')

    elif country is not None and full_ecd is False and language is not None:

        urls = link_builder(country = country, language= language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')

    return ecd_data

@cached(ttl=86400)
def load_from_url_cache(country=None, language=None, full_ecd=False, ecd_version='1.0.0'):
    validate_input(country=country, language=language, full_ecd=full_ecd, version=ecd_version)

    if country is None and full_ecd is True:
        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'
        ecd_data = pl.read_parquet(url)
    
    elif country is not None and full_ecd is False:
        urls = link_builder(country=country, ecd_version=ecd_version)
        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how='vertical')
    
    elif country is None and full_ecd is False and language is not None:
        urls = link_builder(language=language, ecd_version=ecd_version)
        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how='vertical')

    elif country is not None and full_ecd is False and language is not None:
        urls = link_builder(country=country, language=language, ecd_version=ecd_version)
        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how='vertical')

    return ecd_data

def load_ecd(country=None, language=None, full_ecd=False, ecd_version='1.0.0', cache=True):
    """
    Args:
    country: (List[str], dict{'country1', 'country2'}, str): name of a country in our dataset. For a full list of countries do country_dictionary()
    language: (List[str], dict{'language1', 'language2'}, str): name of a language in our dataset. For a full list of languages do country_dictionary()
    full_ecd: (Bool): when True downloads the full Executive Communications Dataset
    ecd_version: (str): a valid version of the Executive Communications Dataset. 
    cache: (Bool): When true implements a cacheing method
    """
    if cache is True:
        ecd_data = load_from_url_cache(country=country, language=language, full_ecd=full_ecd, ecd_version=ecd_version)
    else:
        ecd_data = load_from_url(country=country, language=language, full_ecd=full_ecd, ecd_version=ecd_version)

    return ecd_data

def example_scrapper(scrapper_type= 'static'):

    """
    Args:
    scrapper_type: Str: specify static or dynamic. Note right now the static scrapper is written in R.  
    """
    scrapper_type = scrapper_type.lower()
    
    
    scrappers_dir = Path('scrappers')
    
    if scrapper_type == 'static':
        file_path = scrappers_dir / 'static-scrapper.R'
        warnings.warn("Note this scrapper is written in R. If somebody wants to translate this into Python we welcome pull requests.")
    elif scrapper_type == 'dynamic':
        file_path = scrappers_dir / 'dynamic-scrapper.py'
    else:
        raise ValueError("Invalid scrapper_type. Must be 'static' or 'dynamic'.")
    
    
    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} does not exist.")
    
    
    if os.name == 'posix':  
        subprocess.run(['open', file_path])
    elif os.name == 'nt':  
        os.startfile(file_path)
    else:
        raise OSError("Unsupported OS")


@cached(ttl=86400)
def lazy_load_from_url_cache(country = None,language = None, full_ecd = False, ecd_version = '1.0.0'):
 
    validate_input(country = country,language= language, full_ecd=full_ecd, version=ecd_version)


    if country is None and full_ecd is True:

        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'

        ecd_data = pl.scan_parquet(url)
    
    elif country is not None and full_ecd is False:

        urls = link_builder(country = country, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')
    
    elif country is None and full_ecd is False and language is not None:

        urls = link_builder(language = language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')

    elif country is not None and full_ecd is False and language is not None:

        urls = link_builder(country = country, language= language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')

    return ecd_data

def lazy_load_from_url(country = None,language = None, full_ecd = False, ecd_version = '1.0.0'):
 
    validate_input(country = country,language= language, full_ecd=full_ecd, version=ecd_version)


    if country is None and full_ecd is True:

        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'

        ecd_data = pl.scan_parquet(url)
    
    elif country is not None and full_ecd is False:

        urls = link_builder(country = country, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')
    
    elif country is None and full_ecd is False and language is not None:

        urls = link_builder(language = language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')

    elif country is not None and full_ecd is False and language is not None:

        urls = link_builder(country = country, language= language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.scan_parquet(i) for i in urls], how = 'vertical')

    return ecd_data


def lazy_load_ecd(country=None, language=None, full_ecd=False, ecd_version='1.0.0', cache=True):
    """
    Args:
    country: (List[str], dict{'country1', 'country2'}, str): name of a country in our dataset. For a full list of countries do country_dictionary()
    language: (List[str], dict{'language1', 'language2'}, str): name of a language in our dataset. For a full list of languages do country_dictionary()
    full_ecd: (Bool): when True downloads the full Executive Communications Dataset
    ecd_version: (str): a valid version of the Executive Communications Dataset. 
    cache: (Bool): When true implements a cacheing method
    """
    if cache is True:
        ecd_data = lazy_load_from_url_cache(country=country, language=language, full_ecd=full_ecd, ecd_version=ecd_version)
    else:
        ecd_data = lazy_load_url(country=country, language=language, full_ecd=full_ecd, ecd_version=ecd_version)

    return ecd_data