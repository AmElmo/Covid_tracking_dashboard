# Import libraries

# Q: give code to import functions from cases_deaths.py and vaccination.py
# q: how to important all functions from a file?
# a:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
import requests
from covid_packages.cases_deaths import *
from covid_packages.vaccination import *


# 1. Fetch data from the web (CRON job). To be pushed in the cloud using Google Cloud Functions

# CRON job function to fetch data from the web
def fetch_file(url, path):
    response = requests.get(url)

    if response.status_code == 200:
        with open(path, 'wb') as file:
            file.write(response.content)
            print('File fetched successfully.')
    else:
        print('File fetching failed.')

    return 'File fetching completed.'

# Calls to the function to trigger its execution

# Cases and deaths data from WHO
fetch_file("https://covid19.who.int/WHO-COVID-19-global-data.csv", "script_data/covid_cases_deaths_who.csv")

# Vaccination data from WHO
fetch_file("https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view&_gl=1*330y71*_ga*MjA0MDYxMzE2Ni4xNjg4NDAwNzUy*_ga_E60ZNX2F68*MTY4ODY1ODc0Ny4yLjEuMTY4ODY1OTI0MS42MC4wLjA.", "script_data/covid_vaccinations_who.csv")


# 2. Apply data manipulation scripts on data

# A. Turn the CSV files into dataframes

# Death & Cases data
data_cases_deaths = csv_to_dataframe("script_data/covid_cases_deaths_who.csv")

# Vaccination data
data_vaccination = csv_to_dataframe("script_data/covid_vaccinations_who.csv")

# B. Build dictionaries

# Generate dictionary with country code + population
dict_country_code_population = dict_population(data_cases_deaths)

# Generate dictionary with country code + country name
dict_country_code_name = dictionary_country_code(data_cases_deaths)

# Generate dictionary of country names + WHO_region
dict_country_name_whoregion = dictionary_country_region(data_cases_deaths)

# Dictionary of iso codes / population (> 300.000)
dict_isocode_population_300k = dict_population_iso(data_vaccination)

# Dictionary of iso codes / countries
dict_isocode_countries = dictionary_iso_code(data_vaccination)

# C. Data on cases and deaths

## Country level functions

# Number of cases (last day) for each country

new_cases_lastday_var = {}

for code, country in dict_country_code_name.items():

    new_cases_last = new_cases_lastday(data_cases_deaths, country)

    new_cases_lastday_var[country] = new_cases_last

print("-- ✅ Number of cases (last day) for each country --")

# Number of deaths (last day)

new_deaths_lastday_var = {}

for code, country in dict_country_code_name.items():

    new_deaths_last = new_deaths_lastday(data_cases_deaths, country)

    new_deaths_lastday_var[country] = new_deaths_last

print("-- ✅ Number of deaths (last day) for each country --")

# Number of new cases weekly (last 7 days)
new_cases_last7day_var = {}

for code, country in dict_country_code_name.items():

    new_cases_last7day = new_cases_last_7d(data_cases_deaths, country)

    new_cases_last7day_var[country] = new_cases_last7day

print("-- ✅ Number of new cases weekly (last 7 days) --")

# Number of new cases weekly (7-day rolling average)
new_cases_last7dayavr_var = {}

for code, country in dict_country_code_name.items():

    new_cases_last7dayavr = new_cases_7d_average(data_cases_deaths, country)

    new_cases_last7dayavr_var[country] = new_cases_last7dayavr

print("-- ✅ Number of new cases weekly (7-day rolling average) --")

# Number of new deaths weekly (last 7 days)
new_deaths_last7day_var = {}

for code, country in dict_country_code_name.items():

    new_deaths_last7day = new_deaths_last_7d(data_cases_deaths, country)

    new_deaths_last7day_var[country] = new_deaths_last7day

print("-- ✅ Number of new deaths weekly (last 7 days) --")

# Number of new deaths weekly (7-day rolling average)
new_deaths_last7dayavr_var = {}

for code, country in dict_country_code_name.items():

    new_deaths_last7dayavr = new_deaths_7d_average(data_cases_deaths, country)

    new_deaths_last7dayavr_var[country] = new_deaths_last7dayavr

print("-- ✅ Number of new deaths weekly (7-day rolling average) --")

# Number of cumulative cases (latest)
cum_cases_latest_var = {}

for code, country in dict_country_code_name.items():

    cum_caseslatest = cum_cases_latest(data_cases_deaths, country)

    cum_cases_latest_var[country] = cum_caseslatest

print("-- ✅ Number of cumulative cases (latest) --")

# Number of cumulative deaths (latest)
cum_deaths_latest_var = {}

for code, country in dict_country_code_name.items():

    cum_deathslatest = cum_deaths_latest(data_cases_deaths, country)

    cum_deaths_latest_var[country] = cum_deathslatest

print("-- ✅ Number of cumulative deaths (latest) --")

print("-- Number of cases (last day) for each country --")
print(new_cases_lastday_var)

print("-- Number of deaths (last day) for each country --")
print(new_deaths_lastday_var)

print("-- Number of new cases weekly (last 7 days) --")
print(new_cases_last7day_var)

print("-- Number of new cases weekly (7-day rolling average) --")
print(new_cases_last7dayavr_var)

print("-- Number of new deaths weekly (last 7 days) --")
print(new_deaths_last7day_var)

print("-- Number of new deaths weekly (7-day rolling average) --")
print(new_deaths_last7dayavr_var)

print("-- Number of cumulative cases (latest) --")
print(cum_cases_latest_var)

print("-- Number of cumulative deaths (latest) --")
print(cum_deaths_latest_var)


# Evolution of new cases (all-time)

evol_cases_alltime_var = {}

for code, country in dict_country_code_name.items():

    evol_cases_alltime = evol_cases_alltime(data_cases_deaths, country)

    evol_cases_alltime_var[country] = evol_cases_alltime

print("-- ✅ Evolution of new cases (all-time) --")

# Evolution of new deaths (all-time)

evol_deaths_alltime_var = {}

for code, country in dict_country_code_name.items():

    evol_deaths_alltime = evol_deaths_alltime(data_cases_deaths, country)

    evol_deaths_alltime_var[country] = evol_deaths_alltime

print("-- ✅ Evolution of new deaths (all-time) --")

# Evolution of cumulative cases (all-time)

evol_cum_cases_var = {}

for code, country in dict_country_code_name.items():

    evol_cum_cases = evol_cum_cases(data_cases_deaths, country)

    evol_cum_cases_var[country] = evol_cum_cases

print("-- ✅ Evolution of cumulative cases (all-time) --")

# Evolution of cumulative deaths (all-time)

evol_cum_deaths_var = {}

for code, country in dict_country_code_name.items():

    evol_cum_deaths = evol_cum_deaths(data_cases_deaths, country)

    evol_cum_deaths_var[country] = evol_cum_deaths

print("-- ✅ Evolution of cumulative deaths (all-time) --")

# New cases (weekly)



def new_cases_weekly(data,country):

    data_country = data[data["Country"] == country]
    new_cases_weekly = data_country[["Date_reported", "New_cases"]]
    new_cases_weekly["Weekly_cases"] = new_cases_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_cases"].sum()

    new_cases_weekly = new_cases_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_cases"].sum()
    new_cases_weekly = new_cases_weekly.to_frame()
    new_cases_weekly.reset_index(inplace=True)
    new_cases_weekly = new_cases_weekly.rename(columns={"New_cases": "Weekly_cases"})

    return new_cases_weekly

# New deaths (weekly)

def new_deaths_weekly(data,country):

    data_country = data[data["Country"] == country]
    new_deaths_weekly = data_country[["Date_reported", "New_deaths"]]
    new_deaths_weekly["Weekly_deaths"] = new_deaths_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_deaths"].sum()

    new_deaths_weekly = new_deaths_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_deaths"].sum()
    new_deaths_weekly = new_deaths_weekly.to_frame()
    new_deaths_weekly.reset_index(inplace=True)
    new_deaths_weekly = new_deaths_weekly.rename(columns={"New_deaths": "Weekly_deaths"})

    return new_deaths_weekly

# New cases (weekly % change)

def new_cases_weekly_change(data,country):

    new_cases_week = new_cases_weekly(data,country)

    new_cases_week.drop(new_cases_week.tail(1).index,inplace=True)

    new_cases_week['Percentage_change'] = new_cases_week['Weekly_cases'].pct_change().round(4) * 100

    new_cases_week.drop(['Weekly_cases'], axis=1, inplace=True)

    return new_cases_week

# New deaths (weekly % change)

def new_deaths_weekly_change(data,country):

    new_deaths_week = new_deaths_weekly(data,country)

    new_deaths_week.drop(new_deaths_week.tail(1).index,inplace=True)

    new_deaths_week['Percentage_change'] = new_deaths_week['Weekly_deaths'].pct_change().round(4) * 100

    new_deaths_week.drop(['Weekly_deaths'], axis=1, inplace=True)

    return new_deaths_week






"""

# Top 10 weeks with most new cases

def top_10_weeks_cases(data,country):

    cases_weekly = new_cases_weekly(data,country)

    cases_weekly = cases_weekly.nlargest(10,'Weekly_cases')

    return cases_weekly

# Top 10 weeks with most new deaths

def top_10_weeks_deaths(data,country):

    deaths_weekly = new_deaths_weekly(data,country)

    deaths_weekly = deaths_weekly.nlargest(10,'Weekly_deaths')

    return deaths_weekly

## Regional level functions

# We will use the same functions defined at country level
# and make use of the function dictionary_country_region() in order to filter out the regions


## Global level functions

# Top 15 countries with most new cases / 100k today / this week

def top_15_new_cases_lastweek(data):

    pd.options.mode.chained_assignment = None

    cases_per_100k = {}

    # Iterate over countries
    for code, pop in dictionary_population.items():

        # Generate weekly cases for each country
        data_country = data[data["Country_code"] == code]
        new_cases_weekly = data_country[["Date_reported", "New_cases"]]
        new_cases_weekly["Weekly_cases"] = new_cases_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_cases"].sum()

        new_cases_weekly = new_cases_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_cases"].sum()
        new_cases_weekly = new_cases_weekly.to_frame()
        new_cases_weekly.reset_index(inplace=True)
        new_cases_weekly = new_cases_weekly.rename(columns={"New_cases": "Weekly_cases"})

        # Select the last week
        last_week = new_cases_weekly['Weekly_cases'].iloc[-1]

        # Calculate the incidence (cases / 100k people) for the country
        incidence = (last_week / (pop / 100000))

        # Add incidence to dictionary for given country
        cases_per_100k[code] = incidence

    # Sort countries by incidence
    cases_per_100k_sorted = dict(sorted(cases_per_100k.items(), key=lambda x:x[1], reverse=True))

    # Select top 15 countries
    top_15_cases_incidence = {key: cases_per_100k_sorted[key] for key in list(cases_per_100k_sorted)[:15]}

    # Turn country codes to country names
    top_15_cases_incidence = dict((dictionary_countrycodes[key], value) for (key, value) in top_15_cases_incidence.items())

    return top_15_cases_incidence

# Top 15 countries with most new deaths / 100k this week

def top_15_new_deaths_lastweek(data):

    pd.options.mode.chained_assignment = None

    deaths_per_100k = {}

    # Iterate over countries
    for code, pop in dictionary_population.items():

        # Generate weekly cases for each country
        data_country = data[data["Country_code"] == code]
        new_deaths_weekly = data_country[["Date_reported", "New_deaths"]]
        new_deaths_weekly["Weekly_deaths"] = new_deaths_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_deaths"].sum()

        new_deaths_weekly = new_deaths_weekly.groupby([pd.Grouper(key="Date_reported", freq="W-MON")])["New_deaths"].sum()
        new_deaths_weekly = new_deaths_weekly.to_frame()
        new_deaths_weekly.reset_index(inplace=True)
        new_deaths_weekly = new_deaths_weekly.rename(columns={"New_deaths": "Weekly_deaths"})

        # Select the last week
        last_week = new_deaths_weekly['Weekly_deaths'].iloc[-1]

        # Calculate the incidence (cases / 100k people) for the country
        incidence = (last_week / (pop / 100000))

        # Add incidence to dictionary for given country
        deaths_per_100k[code] = incidence

    # Sort countries by incidence
    deaths_per_100k_sorted = dict(sorted(deaths_per_100k.items(), key=lambda x:x[1], reverse=True))

    # Select top 15 countries
    top_15_deaths_incidence = {key: deaths_per_100k_sorted[key] for key in list(deaths_per_100k_sorted)[:15]}

    # Turn country codes to country names
    top_15_deaths_incidence = dict((dictionary_countrycodes[key], value) for (key, value) in top_15_deaths_incidence.items())

    return top_15_deaths_incidence

# Top 15 countries with highest total incidence of deaths per 100k
def top_15_total_death_incidence(data):

    total_deaths_per_100k = {}

    for code, pop in dictionary_population.items():

        data_country = data[data["Country_code"] == code]
        cumulative_deaths_latest = data_country["Cumulative_deaths"].iloc[-1]

        # Calculate incidence of deaths in population
        incidence = (cumulative_deaths_latest / (pop / 100000))

        # Add incidence to dictionary for given country
        total_deaths_per_100k[code] = incidence

    # Sort countries by incidence
    deaths_per_100k_sorted = dict(sorted(total_deaths_per_100k.items(), key=lambda x:x[1], reverse=True))

    # Select top 15 countries
    top_15_deaths_incidence = {key: deaths_per_100k_sorted[key] for key in list(deaths_per_100k_sorted)[:15]}

    # Turn country codes to country names
    top_15_deaths_incidence = dict((dictionary_countrycodes[key], value) for (key, value) in top_15_deaths_incidence.items())

    return top_15_deaths_incidence



























# D. Data on vaccinations


# 3. Push data to BigQuery database

# Set the path to your JSON key file
key_path = '/path/to/your/keyfile.json'

# Set the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path


# Create a BigQuery client
client = bigquery.Client(project='your-project-id')

# Create a dataset
dataset_id = 'your-dataset-id'
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset = client.create_dataset(dataset)

# Convert dataframe to BigQuery-compatible schema
schema = pandas_gbq.schema.generate_bq_schema(dataframe)

# Push the dataframe to BigQuery
table_id = 'your-table-id'
pandas_gbq.to_gbq(dataframe, f'{dataset_id}.{table_id}', project_id='your-project-id', if_exists='replace', table_schema=schema)

"""
