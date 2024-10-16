name = 'ecdata'

# so you don't forget the equivalent of devtools::build is python setup.py sdist

import polars as pl
import requests 
import os
import subprocess
from pathlib import Path
import warnings

__doc__ = """

ecdata - A Python package for working with The Executive Communications Dataset 

=========================================================
**ecdata** is a Python package that streamlines importing The Executive Communications Dataset

Functions
---------

country_dictionary - Returns a Polars dataframe of countries in the dataset
load_ecd - Main function for loading in dataset 
example_scrapper- Opens 


"""




def country_dictionary():
    data = {
          'name_in_dataset': ["Argentina","Australia","Austria","Azerbaijan","Bolivia","Brazil","Canada","Chile","Colombia","Costa Rica","Czechia","Denmark","Ecuador","France","Georgia","Germany","Greece","Hong Kong","Hungary","Iceland","India", "India","Indonesia","Israel","Italy","Jamaica","Japan","Mexico","New Zealand","Nigeria","Norway","Philippines","Poland","Portugal","Republic of Korea","Russia","Spain","Turkey","United Kingdom","United States of America","Uruguay","Venezuela"],
          'file_name': ["argentina","australia","austria","azerbaijan","bolivia","brazil","canada","chile","colombia","costa_rica","czechia","denmark","ecuador","france","georgia","germany","greece","hong_kong","hungary","iceland","india",'india' ,"indonesia","israel","italy","jamaica","japan","mexico","new_zealand","nigeria","norway","philippines","poland","portugal","republic_of_korea","russia","spain","turkey","united_kingdom","united_states_of_america","uruguay","venezuela"],
          'language': ['Spanish', 'English', 'German', 'English', 'Spanish', 'Portugese', 'English', 'Spanish', 'Spanish', 'Spanish', 'Czech', 'Danish', 'Spanish', 'French', 'Georgian', 'German', 'Greek', 'Chineses', 'Hungarian', 'Icelandic', 'Hindi', 'English', 'Indonesian' ,'Hebrew', 'Italian', 'English', 'Japanese', 'Spanish', 'English', 'English', 'Norwegian', 'Filipino', 'Polish', 'Portugese', 'Korean', 'English', 'Spanish', 'Turkish', 'English', 'English', 'Spanish', 'Spanish']
    }
    return pl.DataFrame(data)


def link_builder(country=None, language=None, ecd_version='1.0.0'):
    if isinstance(country, str):
        country = [country]
    
    if isinstance(language, str):
        language = [language]

    country = [c.lower() for c in country] if country else None
    language = [l.lower() for l in language] if language else None
    
    
    country_names = country_dictionary().with_columns(
        (pl.col('name_in_dataset').str.to_lowercase().alias('name_in_dataset')),
        (pl.col('language').str.to_lowercase().alias('language'))
    )
    
    if country:
        country_names = country_names.filter(pl.col('name_in_dataset').is_in(country))
    elif language:
        country_names = country_names.filter(pl.col('language').is_in(language))
    
    
    country_names = country_names.with_columns(
        url='https://github.com/Executive-Communications-Dataset/ecdata/releases/download/' + 
            f'{ecd_version}' + '/' + pl.col('file_name') + '.parquet'
    )
    
    
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
        (pl.col('language').str.to_lowercase().alias('language'))
    )

   
    valid_countries = countries_df['name_in_dataset'].to_list()

    valid_languages = countries_df['language'].to_list()

   
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
            if country_lower not in valid_countries:
                raise ValueError(f'{country} is not a valid country name in our dataset. Call country_dictionary for a list of valid inputs')
        elif isinstance(country, list):
            invalid_countries = [c for c in country if c.lower() not in valid_countries]
            if invalid_countries:
                raise ValueError(f'These countries are not valid: {invalid_countries}. Call country_dictionary for a list of valid inputs')
        elif isinstance(country, dict):
            invalid_countries = [c for c in country.keys() if c.lower() not in valid_countries]
            if invalid_countries:
                raise ValueError(f'These keys in your dictionary are not valid country names: {invalid_countries}. Call country_dictionary for a list of valid inputs')

    return True 



def load_ecd(country = None,language = None, full_ecd = False, ecd_version = '1.0.0'):

    """
    Args:
    country: (List[str], dict{'country1', 'country2'}, str): name of a country in our dataset. For a full list of countries do country_dictionary()
    language: (List[str], dict{'language1', 'language2'}, str): name of a language in our dataset. For a full list of languages do country_dictionary()
    full_ecd: (Bool): when True downloads the full Executive Communications Dataset
    ecd_version: (str): a valid version of the Executive Communications Dataset. 
    """


    validate_input(country = country,language= language, full_ecd=full_ecd, version=ecd_version)

    if country is None and full_ecd is True:

        url = f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{ecd_version}/full_ecd.parquet'

        ecd_data = pl.read_parquet(url)

    elif country is not None and full_ecd is False and len(country) == 1:

        url = link_builder(country=country, ecd_version=ecd_version)

        ecd_data = pl.read_parquet(url)
    
    elif country is not None and full_ecd is False and len(country) > 1:

        urls = link_builder(country = country, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')
    
    elif country is None and full_ecd is False and language is not None:

        urls = link_builder(language = language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')

    elif country is not None and full_ecd is False and language is not None:

        urls = link_builder(country = country, language= language, ecd_version=ecd_version)

        ecd_data = pl.concat([pl.read_parquet(i) for i in urls], how = 'vertical')

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