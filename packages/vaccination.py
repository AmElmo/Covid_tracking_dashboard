import pandas as pd
import requests

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

## Dictionaries loading

# Dictionary of iso codes / population (> 300.000)
def dict_population_iso(data):

    country_codes = data['iso_code'].unique()
    dict_population = {}

    for code in country_codes:
        response = requests.get("https://restcountries.com/v2/alpha/", params={'codes':code})

        # Ignore 404 responses and only include countries with >= 300000 inhabitants
        if response.status_code == 200 and response.json()[0]['population'] >= 300000:
            population = response.json()[0]['population']
            dict_population[code] = population
            print(code)
        else:
            continue

    return dict_population

# Dictionary of iso codes / countries

def dictionary_iso_code(data):

    list_codes = data['iso_code'].unique()
    list_countries = data['location'].unique()

    dict_codes = {'iso_code': list_codes, 'location': list_countries}

    df = pd.DataFrame(dict_codes)

    final_dict = dict(zip(df['iso_code'], df["location"]))

    final_dict = {x: final_dict[x] for x in final_dict.keys() if len(x) < 4}

    return final_dict

## Country level functions
