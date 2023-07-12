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

    print("Start dictionary")

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

    print("Dictionary of ISO codes / population created")

    return dict_population

# Dictionary of iso codes / countries

def dictionary_iso_code(data):

    print("Start dictionary")

    list_codes = data['iso_code'].unique()
    list_countries = data['location'].unique()

    dict_codes = {'iso_code': list_codes, 'location': list_countries}

    df = pd.DataFrame(dict_codes)

    final_dict = dict(zip(df['iso_code'], df["location"]))

    final_dict = {x: final_dict[x] for x in final_dict.keys() if len(x) < 4}

    print("Dictionary ISO codes / countries created")

    return final_dict

## Country level functions

# Number of weekly vaccinations per country
def new_vaccinations_weekly(data, country):
    data_country = data[data["location"] == country]
    new_vaccinations_weekly = data_country[["date", "daily_vaccinations"]]

    # Ensure "daily_vaccinations" column is treated as numeric
    new_vaccinations_weekly["daily_vaccinations"] = pd.to_numeric(new_vaccinations_weekly["daily_vaccinations"], errors='coerce')

    new_vaccinations_weekly["date"] = pd.to_datetime(new_vaccinations_weekly["date"])

    new_vaccinations_weekly.set_index("date", inplace=True)
    new_vaccinations_weekly = new_vaccinations_weekly.resample('W-MON')["daily_vaccinations"].sum()
    new_vaccinations_weekly = new_vaccinations_weekly.to_frame()
    new_vaccinations_weekly.reset_index(inplace=True)
    new_vaccinations_weekly = new_vaccinations_weekly.rename(columns={"daily_vaccinations": "Weekly_vaccinations"})

    return new_vaccinations_weekly


# Total number of vaccinated people per country
def total_vaccinations_country(data, country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country["people_vaccinated"].iloc[-1]

    return vaccinations_total

# Total % of vaccinated people per country
def total_vaccinations_rate_country(data,country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["people_vaccinated_per_hundred"]]
    vaccinations_total = vaccinations_total.iloc[-1][0]

    return vaccinations_total


# Total % of vaccinated people per country (evolution / month)
def vaccinations_rate_evol_country(data, country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["date", "people_vaccinated_per_hundred"]]

    # Calculate vaccination rate per week
    vaccinations_total = vaccinations_total.groupby(pd.Grouper(key="date", freq="W-MON")).mean().reset_index()

    return vaccinations_total

# Total nb of vaccinated people per country (evolution / month)

def vaccinations_change_evol_country(data, country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["date", "total_vaccinations"]]

    # Clean and convert "total_vaccinations" column to numeric data type
    vaccinations_total["total_vaccinations"] = pd.to_numeric(vaccinations_total["total_vaccinations"], errors="coerce")

    # Convert "date" column to datetime type
    vaccinations_total["date"] = pd.to_datetime(vaccinations_total["date"])

    # Set "date" column as the index
    vaccinations_total.set_index("date", inplace=True)

    # Resample to monthly frequency and calculate the absolute change
    vaccinations_total = vaccinations_total.resample("M").last()
    vaccinations_total["Vaccination_change"] = vaccinations_total["total_vaccinations"].diff()

    return vaccinations_total

# Total nb of vaccinated people per country (monthly)

def vaccinations_monthly_total(data, country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["date", "total_vaccinations"]]

    # Convert "date" column to datetime type
    vaccinations_total["date"] = pd.to_datetime(vaccinations_total["date"])

    # Set "date" column as the index
    vaccinations_total.set_index("date", inplace=True)

    # Resample to monthly frequency and calculate the total vaccinations
    vaccinations_total = vaccinations_total.resample("M").last()

    return vaccinations_total


## Global level functions

# Top 15 countries with highest vaccination rate
def top_15_vaccinations_rate(data, dictionary_population_iso, dictionary_isocodes):
    vaccinations_rates = {}

    for code, country in dictionary_population_iso.items():

        data_country = data[data["iso_code"] == code]

        if pd.isna(data_country["people_vaccinated_per_hundred"].iloc[-1]) == False:

            vaccination_rate_latest = data_country["people_vaccinated_per_hundred"].iloc[-1]

        else:

            vaccination_rate_latest = data_country["people_vaccinated_per_hundred"].iloc[-2]

        # Add vaccination rate to dictionary for given country
        vaccinations_rates[code] = vaccination_rate_latest

    # Sort countries by vaccination rate
    vaccinations_rates_sorted = dict(sorted(vaccinations_rates.items(), key=lambda x:x[1], reverse=True))

    # Select top 15 countries
    top_15_vaccination_rates = {key: vaccinations_rates_sorted[key] for key in list(vaccinations_rates_sorted)[:15]}

    # Turn country codes to country names
    top_15_vaccination_rates = dict((dictionary_isocodes[key], value) for (key, value) in top_15_vaccination_rates.items())

    # Turn to dataframe
    top_15_vaccination_rates_df = pd.DataFrame(list(top_15_vaccination_rates.items()), columns=['Country', 'Vaccination Rate'])


    return top_15_vaccination_rates_df
