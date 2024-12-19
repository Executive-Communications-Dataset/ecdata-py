from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Country:
    file_name: str
    language: str
    abbr: str
    name_in_dataset: str

COUNTRIES: List[Country] = [
    Country("argentina", "Spanish", "ARG", "Argentina"),
    Country("australia", "English", "AUS", "Australia"),
    Country("austria", "German", "AUT", "Austria"),
    Country("azerbaijan", "English", "AZE", "Azerbaijan"),
    Country("azerbaijan", "English", "AZE", "Azerbaijan"),
    Country("bolivia", "Spanish", "BOL", "Bolivia"),
    Country("brazil", "Portugese", "BRA", "Brazil"),
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
    Country("portugal", "Portugese", "PRT", "Portugal"),
    Country("russia", "English", "RUS", "Russia"),
    Country("russia", "English", "RUS", "Russia"),
    Country("spain", "Spanish", "ESP", "Spain"),
    Country("turkey", "Turkish", "TUR", "Turkey"),
    Country("united_kingdom", "English", "GBR", "United Kingdom"),
    Country("uruguay", "Spanish", "URY", "Uruguay"),
    Country("venezuela", "Spanish", "VEN", "Venezuela"),
    Country("united_states_of_america", "English", "USA", "United States of America"),
    Country("republic_of_korea", "Korean", "KOR", "Republic of Korea")
]

# Add two-letter codes and alternative names
COUNTRY_VARIANTS = {
    "united_kingdom": ["GB", "UK", "Great Britain"],
    "united_states_of_america": ["US", "United States", "USA"],
    "republic_of_korea": ["KR", "South Korea"]
}

# Add additional country entries for variants
additional_countries = []
for country in COUNTRIES:
    if country.file_name in COUNTRY_VARIANTS:
        for variant in COUNTRY_VARIANTS[country.file_name]:
            if len(variant) == 2:  # Two-letter code
                additional_countries.append(
                    Country(country.file_name, country.language, variant, country.name_in_dataset)
                )
            else:  # Alternative name
                additional_countries.append(
                    Country(country.file_name, country.language, country.abbr, variant)
                )

COUNTRIES.extend(additional_countries) 