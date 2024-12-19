import polars as pl
import requests
from typing import Optional, Union, List, Dict, Any
from .countries import COUNTRIES

class CountryManager:
    def __init__(self):
        self._df = pl.DataFrame([vars(c) for c in COUNTRIES])
        self._df = self._df.with_columns([
            pl.col('name_in_dataset').str.to_lowercase(),
            pl.col('language').str.to_lowercase(),
            pl.col('abbr').str.to_lowercase()
        ])
        
    @property
    def valid_countries(self) -> List[str]:
        return self._df['name_in_dataset'].unique().to_list()
    
    @property
    def valid_languages(self) -> List[str]:
        return self._df['language'].unique().to_list()
    
    @property
    def valid_abbr(self) -> List[str]:
        return self._df['abbr'].unique().to_list()
        
    def build_urls(self, 
                  country: Optional[Union[str, List[str], Dict]] = None,
                  language: Optional[Union[str, List[str], Dict]] = None,
                  version: str = '1.0.0') -> List[str]:
        """Build download URLs based on country/language filters"""
        df = self._df.clone()
        
        if country and language:
            country = self._normalize_input(country)
            language = self._normalize_input(language)
            df = df.filter(
                ((pl.col('name_in_dataset').is_in(country)) | 
                 (pl.col('abbr').is_in(country))) &
                (pl.col('language').is_in(language))
            )
        elif country:
            country = self._normalize_input(country)
            df = df.filter(
                (pl.col('name_in_dataset').is_in(country)) | 
                (pl.col('abbr').is_in(country))
            )
        elif language:
            language = self._normalize_input(language)
            df = df.filter(pl.col('language').is_in(language))
            
        urls = df.with_columns(
            url=f'https://github.com/Executive-Communications-Dataset/ecdata/releases/download/{version}/' + 
                pl.col('file_name') + '.parquet'
        )
        
        return urls.unique(subset='url')['url'].to_list()
    
    def validate_input(self,
                      country: Optional[Union[str, List[str], Dict]] = None,
                      language: Optional[Union[str, List[str], Dict]] = None) -> None:
        """Validate country and language inputs"""
        if country is not None:
            self._validate_type(country, "country")
            self._validate_values(country, self.valid_countries + self.valid_abbr, "country")
            
        if language is not None:
            self._validate_type(language, "language")
            self._validate_values(language, self.valid_languages, "language")
    
    @staticmethod
    def _normalize_input(value: Union[str, List[str], Dict]) -> List[str]:
        """Convert input to normalized list of lowercase strings"""
        if isinstance(value, str):
            return [value.lower()]
        elif isinstance(value, list):
            return [v.lower() for v in value]
        elif isinstance(value, dict):
            return [k.lower() for k in value.keys()]
        return []
        
    @staticmethod
    def _validate_type(value: Any, name: str) -> None:
        if not isinstance(value, (str, list, dict)):
            raise ValueError(f'Please provide a str, list, or dict to {name}. You provided {type(value)}')
            
    @staticmethod
    def _validate_values(value: Union[str, List[str], Dict], valid_values: List[str], name: str) -> None:
        normalized = CountryManager._normalize_input(value)
        invalid = [v for v in normalized if v not in valid_values]
        if invalid:
            raise ValueError(f'These {name}s are not valid: {invalid}. Call country_dictionary for valid inputs') 
    
    @staticmethod
    def get_ecd_release(repo: str = 'Executive-Communications-Dataset/ecdata',
                       token: Optional[str] = None,
                       verbose: bool = True) -> List[str]:
        """
        Get available releases from GitHub repository
        
        Args:
            repo: GitHub repository path (owner/repo_name)
            token: Optional GitHub authentication token
            verbose: Whether to print status messages
            
        Returns:
            List of release names
        """
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

        releases_data = []
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
            releases_data.append(release_data)
        
        if releases_data:
            df = pl.concat([pl.DataFrame(data) for data in releases_data], how='vertical')
            return df['release_name'].to_list()
        return []
   