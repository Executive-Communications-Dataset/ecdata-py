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




import polars as pl

def country_dictionary():
    
    country_names =  {
    "file_name": [
        "argentina", "argentina", "australia", "australia", "austria", "austria",
        "azerbaijan", "azerbaijan", "azerbaijan", "azerbaijan", "bolivia", "bolivia",
        "brazil", "brazil", "canada", "canada", "chile", "chile", "colombia", "colombia",
        "costa_rica", "costa_rica", "czechia", "czechia", "denmark", "denmark",
        "dominican_republic", "dominican_republic", "ecuador", "ecuador", "france",
        "france", "georgia", "georgia", "germany", "germany", "greece", "greece",
        "hong_kong", "hong_kong", "hungary", "hungary", "iceland", "iceland",
        "india", "india", "india", "india", "indonesia", "indonesia", "israel", "israel",
        "italy", "italy", "jamaica", "jamaica", "japan", "japan", "mexico", "mexico",
        "new_zealand", "new_zealand", "nigeria", "nigeria", "norway", "norway",
        "philippines", "philippines", "poland", "poland", "portugal", "portugal",
        "russia", "russia", "russia", "russia", "spain", "spain", "turkey", "turkey",
        "united_kingdom", "united_kingdom", "united_kingdom", "united_kingdom",
        "united_kingdom", "united_kingdom", "uruguay", "uruguay", "venezuela", "venezuela",
        "united_states_of_america", "united_states_of_america", "united_states_of_america",
        "united_states_of_america", "republic_of_korea", "republic_of_korea",
        "republic_of_korea", "republic_of_korea"
    ],
    "language": [
        "Spanish", "Spanish", "English", "English", "German", "German",
        "English", "English", "English", "English", "Spanish", "Spanish",
        "Portuguese", "Portuguese", "English", "English", "Spanish", "Spanish",
        "Spanish", "Spanish", "Spanish", "Spanish", "Czech", "Czech",
        "Danish", "Danish", "Spanish", "Spanish", "Spanish", "Spanish",
        "French", "French", "Georgian", "Georgian", "German", "German",
        "Greek", "Greek", "Chinese", "Chinese", "Hungarian", "Hungarian",
        "Icelandic", "Icelandic", "English", "English", "Hindi", "Hindi",
        "Indonesian", "Indonesian", "Hebrew", "Hebrew", "Italian", "Italian",
        "English", "English", "Japanese", "Japanese", "Spanish", "Spanish",
        "English", "English", "English", "English", "Norwegian", "Norwegian",
        "Filipino", "Filipino", "Polish", "Polish", "Portuguese", "Portuguese",
        "English", "English", "English", "English", "Spanish", "Spanish",
        "Turkish", "Turkish", "English", "English", "English", "English",
        "English", "English", "Spanish", "Spanish", "Spanish", "Spanish",
        "English", "English", "English", "English", "Korean", "Korean",
        "Korean", "Korean"
    ],
    "abbr": [
        "ARG", "AR", "AUS", "AU", "AUT", "AT", "AZE", "AZ", "AZE", "AZ", 
        "BOL", "BO", "BRA", "BR", "CAN", "CA", "CHL", "CL", "COL", "CO", 
        "CRI", "CR", "CZE", "CZ", "DNK", "DK", "DOM", "DO", "ECU", "EC", 
        "FRA", "FR", "GEO", "GE", "DEU", "DE", "GRC", "GR", "HKG", "HK", 
        "HUN", "HU", "ISL", "IS", "IND", "IN", "IND", "IN", "IDN", "ID", 
        "ISR", "IL", "ITA", "IT", "JAM", "JM", "JPN", "JP", "MEX", "MX", 
        "NZL", "NZ", "NGA", "NG", "NOR", "NO", "PHL", "PH", "POL", "PL", 
        "PRT", "PT", "RUS", "RU", "RUS", "RU", "ESP", "ES", "TUR", "TR", 
        "GBR", "GBR", "GB", "GB", "UK", "UK", "URY", "UY", "VEN", "VE", 
        "USA", "USA", "US", "US", "KOR", "KOR", "KR", "KR"
    ],
    "name_in_dataset": [
        "Argentina", "Argentina", "Australia", "Australia", "Austria", "Austria",
        "Azerbaijan", "Azerbaijan", "Azerbaijan", "Azerbaijan", "Bolivia", 
        "Bolivia", "Brazil", "Brazil", "Canada", "Canada", "Chile", "Chile", 
        "Colombia", "Colombia", "Costa Rica", "Costa Rica", "Czechia", 
        "Czechia", "Denmark", "Denmark", "Dominican Republic", "Dominican Republic", 
        "Ecuador", "Ecuador", "France", "France", "Georgia", "Georgia", 
        "Germany", "Germany", "Greece", "Greece", "Hong Kong", "Hong Kong", 
        "Hungary", "Hungary", "Iceland", "Iceland", "India", "India", 
        "India", "India", "Indonesia", "Indonesia", "Israel", "Israel", 
        "Italy", "Italy", "Jamaica", "Jamaica", "Japan", "Japan", "Mexico", 
        "Mexico", "New Zealand", "New Zealand", "Nigeria", "Nigeria", 
        "Norway", "Norway", "Philippines", "Philippines", "Poland", "Poland", 
        "Portugal", "Portugal", "Russia", "Russia", "Russia", "Russia", 
        "Spain", "Spain", "Turkey", "Turkey", "United Kingdom", "Great Britain", 
        "United Kingdom", "Great Britain", "United Kingdom", "Great Britain", 
        "Uruguay", "Uruguay", "Venezuela", "Venezuela", "United States of America", 
        "United States", "United States of America", "United States", 
        "Republic of Korea", "South Korea", "Republic of Korea", "South Korea"
    ]
}
   
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
        (pl.col('language').str.to_lowercase().alias('language'))
    )
    
    if country:
        country_names = country_names.filter((pl.col('name_in_dataset').is_in(country)) | (pl.col('abbr').is_in(country)))
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
        (pl.col('language').str.to_lowercase().alias('language')),
        (pl.col('abbr').str_to_lowercase().alias('abbr'))
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