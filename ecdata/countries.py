"""Country lookup table backing `country_dictionary()` and URL construction.

`COUNTRIES` is built once, at import, from `_BASE_COUNTRIES` plus the alias rows
derived from `COUNTRY_VARIANTS`. It is deliberately assembled by a function
rather than by mutating a module-level list in place, so that re-importing or
reloading the module cannot append the aliases a second time.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class Country:
    file_name: str
    language: str
    abbr: str
    name_in_dataset: str


# One row per (file_name, language). A country appears more than once only when
# the dataset genuinely carries more than one language for it -- India is the
# only such case.
_BASE_COUNTRIES: Tuple[Country, ...] = (
    Country("argentina", "Spanish", "ARG", "Argentina"),
    Country("australia", "English", "AUS", "Australia"),
    Country("austria", "German", "AUT", "Austria"),
    Country("azerbaijan", "English", "AZE", "Azerbaijan"),
    Country("bolivia", "Spanish", "BOL", "Bolivia"),
    Country("brazil", "Portuguese", "BRA", "Brazil"),
    Country("canada", "English", "CAN", "Canada"),
    Country("chile", "Spanish", "CHL", "Chile"),
    Country("colombia", "Spanish", "COL", "Colombia"),
    Country("costa_rica", "Spanish", "CRI", "Costa Rica"),
    Country("czechia", "Czech", "CZE", "Czechia"),
    Country("denmark", "Danish", "DNK", "Denmark"),
    Country("dominican_republic", "Spanish", "DOM", "Dominican Republic"),
    Country("ecuador", "Spanish", "ECU", "Ecuador"),
    Country("france", "French", "FRA", "France"),
    Country("georgia", "Georgian", "GEO", "Georgia"),
    Country("germany", "German", "DEU", "Germany"),
    Country("greece", "Greek", "GRC", "Greece"),
    Country("hong_kong", "Chinese", "HKG", "Hong Kong"),
    Country("hungary", "Hungarian", "HUN", "Hungary"),
    Country("iceland", "Icelandic", "ISL", "Iceland"),
    Country("india", "English", "IND", "India"),
    Country("india", "Hindi", "IND", "India"),
    Country("indonesia", "Indonesian", "IDN", "Indonesia"),
    Country("israel", "Hebrew", "ISR", "Israel"),
    Country("italy", "Italian", "ITA", "Italy"),
    Country("jamaica", "English", "JAM", "Jamaica"),
    Country("japan", "Japanese", "JPN", "Japan"),
    Country("mexico", "Spanish", "MEX", "Mexico"),
    Country("new_zealand", "English", "NZL", "New Zealand"),
    Country("nigeria", "English", "NGA", "Nigeria"),
    Country("norway", "Norwegian", "NOR", "Norway"),
    Country("philippines", "Filipino", "PHL", "Philippines"),
    Country("poland", "Polish", "POL", "Poland"),
    Country("portugal", "Portuguese", "PRT", "Portugal"),
    Country("republic_of_korea", "Korean", "KOR", "Republic of Korea"),
    Country("russia", "English", "RUS", "Russia"),
    Country("spain", "Spanish", "ESP", "Spain"),
    Country("turkey", "Turkish", "TUR", "Turkey"),
    Country("united_kingdom", "English", "GBR", "United Kingdom"),
    Country("united_states_of_america", "English", "USA", "United States of America"),
    Country("uruguay", "Spanish", "URY", "Uruguay"),
    Country("venezuela", "Spanish", "VEN", "Venezuela"),
)

# Two-letter codes and colloquial names users are likely to reach for. A
# two-character entry is treated as an additional `abbr`; anything longer is
# treated as an additional `name_in_dataset`.
COUNTRY_VARIANTS: Dict[str, List[str]] = {
    "united_kingdom": ["GB", "UK", "Great Britain"],
    "united_states_of_america": ["US", "United States", "USA"],
    "republic_of_korea": ["KR", "South Korea"],
}


def _build_countries() -> List[Country]:
    """Return the base table plus one alias row per variant.

    Pure: called once below, and safe to call again from tests.
    """
    countries = list(_BASE_COUNTRIES)
    aliases: List[Country] = []
    for country in _BASE_COUNTRIES:
        for variant in COUNTRY_VARIANTS.get(country.file_name, ()):
            if len(variant) == 2:
                aliases.append(Country(country.file_name, country.language,
                                       variant, country.name_in_dataset))
            else:
                aliases.append(Country(country.file_name, country.language,
                                       country.abbr, variant))
    countries.extend(aliases)
    return countries


COUNTRIES: List[Country] = _build_countries()
