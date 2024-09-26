name = 'ecdata'

import polars as pl

def country_dictionary():
    data = {
          'name_in_dataset': ["Argentina","Australia","Austria","Azerbaijan","Bolivia","Brazil","Canada","Chile","Colombia","Costa Rica","Czechia","Denmark","Ecuador","France","Georgia","Germany","Greece","Hong Kong","Hungary","Iceland","India","Indonesia","Israel","Italy","Jamaica","Japan","Mexico","New Zealand","Nigeria","Norway","Philippines","Poland","Portugal","Republic of South Korea","Russia","Spain","Turkey","United Kingdom","United States of America","Uruguay","Venzuela"],
          'file_name': ["argentina","australia","austria","azerbaijan","bolivia","brazil","canada","chile","colombia","costa_rica","czechia","denmark","ecuador","france","georgia","germany","greece","hong_kong","hungary","iceland","india","indonesia","israel","italy","jamaica","japan","mexico","new_zealand","nigeria","norway","philippines","poland","portugal","republic_of_south_korea","russia","spain","turkey","united_kingdom","united_states_of_america","uruguay","venzuela"]
    }
    return pl.DataFrame(data)

def link_builder(country, ecd_version):
    if isinstance(country, str):
        country = [country]

    country_names = country_dictionary()
    
    country_names = country_names.filter(pl.col('name_in_dataset').is_in(country))

    country_names = country_names.with_columns(url = 'https://github.com/joshuafayallen/executivestatements/releases/download/' + 'f{ecd_version}' + '/' + pl.col('file_name') + '.parquet')

    country_names = country_names['url']
    return country_names



def load_ecd(
    country = None,
    full_ecd = False,
    version = '1.0.0'
):
""""Imports the Executive Communications Dataset

Args:
    country (List[str]): 

"""
