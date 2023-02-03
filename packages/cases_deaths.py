import pandas as pd
import requests

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

## Data loading

# Transform CSV to dataframe
def csv_to_dataframe(csv):

    print("Start conversion...")

    data = pd.read_csv(csv)

    # Remove first row if description
    if data.iloc[0][0].startswith('#') == True:
        data.drop(index=data.index[0], axis=0, inplace=True)

    # Convert dates to datetime type
    if 'Date_reported' in data.columns:
        data['Date_reported'] = pd.to_datetime(data['Date_reported'])
    elif 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'])

    print("Converted CSV to dataframe")

    return data

## Dictionaries loading

# Generate dictionary with country code + population
def dict_population(data):

    print("Start dictionary...")

    country_codes = data['Country_code'].unique()
    dict_population = {}

    for code in country_codes:
        response = requests.get("https://restcountries.com/v2/alpha/", params={'codes':code})

        # Ignore 404 responses and only include countries with >= 300000 inhabitants
        if response.status_code == 200 and response.json()[0]['population'] >= 300000:
            population = response.json()[0]['population']
            dict_population[code] = population
            print(dict_population)
        else:
            continue

    print("Country code / Population dictionary created")

    return dict_population

# Generate dictionary with country code + country name
def dictionary_country_code(data):

    print("Start dictionary")

    list_codes = data['Country_code'].unique()
    list_countries = data['Country'].unique()

    dict_codes = {'Country_code': list_codes, 'Country': list_countries}

    df = pd.DataFrame(dict_codes)

    final_dict = dict(zip(df['Country_code'], df["Country"]))

    print("Country code / country name dictionary created")

    return final_dict


## Country level functions

# Number of cases (last day)
def new_cases_lastday(data, country):

    data_country = data[data["Country"] == country]
    new_cases_last = data_country["New_cases"].iloc[-1]

    return new_cases_last

# Number of deaths (last day)
def new_deaths_lastday(data,country):

    data_country = data[data["Country"] == country]
    new_deaths_last = data_country["New_deaths"].iloc[-1]

    return new_deaths_last

# Number of new cases weekly (last 7 days)
def new_cases_last_7d(data,country):

    pd.set_option('mode.chained_assignment', None)

    data_country = data[data["Country"] == country]
    new_cases_7d = data_country[["Date_reported", "New_cases"]]
    new_cases_7d["Last_7_days"] = new_cases_7d["New_cases"].rolling(7).sum()

    new_cases_7d.drop(['New_cases'], axis = 1, inplace = True)

    return new_cases_7d

# Number of new cases weekly (7-day rolling average)
def new_cases_7d_average(data,country):

    pd.set_option('mode.chained_assignment', None)

    data_country = data[data["Country"] == country]
    new_cases_7d_average = data_country[["Date_reported", "New_cases"]]
    new_cases_7d_average["7_days_average"] = new_cases_7d_average["New_cases"].rolling(7).sum() / 7
    new_cases_7d_average["7_days_average"] = new_cases_7d_average["7_days_average"].round(decimals=2)

    new_cases_7d_average.drop(['New_cases'], axis = 1, inplace = True)

    return new_cases_7d_average

## Regional level functions




## Global level functions
