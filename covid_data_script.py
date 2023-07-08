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


# Evolution of new cases (all-time)

evol_cases_alltime_var = {}

for code, country in dict_country_code_name.items():

    evol_casesalltime = evol_cases_alltime(data_cases_deaths, country)

    evol_cases_alltime_var[country] = evol_casesalltime

print("-- ✅ Evolution of new cases (all-time) --")

# Evolution of new deaths (all-time)

evol_deaths_alltime_var = {}

for code, country in dict_country_code_name.items():

    evol_deathsalltime = evol_deaths_alltime(data_cases_deaths, country)

    evol_deaths_alltime_var[country] = evol_deathsalltime

print("-- ✅ Evolution of new deaths (all-time) --")

# Evolution of cumulative cases (all-time)

evol_cum_cases_var = {}

for code, country in dict_country_code_name.items():

    evol_cumcases = evol_cum_cases(data_cases_deaths, country)

    evol_cum_cases_var[country] = evol_cumcases

print("-- ✅ Evolution of cumulative cases (all-time) --")

# Evolution of cumulative deaths (all-time)

evol_cum_deaths_var = {}

for code, country in dict_country_code_name.items():

    evol_cumdeaths = evol_cum_deaths(data_cases_deaths, country)

    evol_cum_deaths_var[country] = evol_cumdeaths

print("-- ✅ Evolution of cumulative deaths (all-time) --")

# New cases (weekly)

new_cases_weekly_var = {}

for code, country in dict_country_code_name.items():

    new_casesweekly = new_cases_weekly(data_cases_deaths, country)

    new_cases_weekly_var[country] = new_casesweekly

print("-- ✅ New cases (weekly) --")

# New deaths (weekly)

new_deaths_weekly_var = {}

for code, country in dict_country_code_name.items():

    new_deathsweekly = new_deaths_weekly(data_cases_deaths, country)

    new_deaths_weekly_var[country] = new_deathsweekly

print("-- ✅ New deaths (weekly) --")

# New cases (weekly % change)

new_cases_weekly_change_var = {}

for code, country in dict_country_code_name.items():

    new_cases_weeklychange = new_cases_weekly_change(data_cases_deaths, country)

    new_cases_weekly_change_var[country] = new_cases_weeklychange

print("-- ✅ New cases (weekly % change) --")

# New deaths (weekly % change)

new_deaths_weekly_change_var = {}

for code, country in dict_country_code_name.items():

    new_deaths_weeklychange = new_deaths_weekly_change(data_cases_deaths, country)

    new_deaths_weekly_change_var[country] = new_deaths_weeklychange

print("-- ✅ New deaths (weekly % change) --")


# Top 10 weeks with most new cases

top_10_weeks_cases_var = {}

for code, country in dict_country_code_name.items():

    top_10_weekscases = top_10_weeks_cases(data_cases_deaths, country)

    top_10_weeks_cases_var[country] = top_10_weekscases

print("-- ✅ Top 10 weeks with most new cases --")

# Top 10 weeks with most new deaths

top_10_weeks_deaths_var = {}

for code, country in dict_country_code_name.items():

    top_10_weeksdeaths = top_10_weeks_deaths(data_cases_deaths, country)

    top_10_weeks_deaths_var[country] = top_10_weeksdeaths

print("-- ✅ Top 10 weeks with most new deaths --")


## Regional level functions

# We will use the same functions defined at country level
# and make use of the function dictionary_country_region() in order to filter out the regions


## Global level functions

# Top 15 countries with most new cases / 100k today / this week

top_15_new_cases_lastweek_var = top_15_new_cases_lastweek(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with most new cases / 100k today / this week --")

# Top 15 countries with most new deaths / 100k this week

top_15_new_deaths_lastweek_var = top_15_new_deaths_lastweek(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with most new deaths / 100k this week --")

# Top 15 countries with highest total incidence of deaths per 100k

top_15_total_death_incidence_var = top_15_total_death_incidence(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with highest total incidence of deaths per 100k --")


# D. Data on vaccinations

## Country level functions

# Number of weekly vaccinations per country
def new_vaccinations_weekly(data,country):

    data_country = data[data["location"] == country]
    new_vaccinations_weekly = data_country[["date", "daily_vaccinations"]]
    new_vaccinations_weekly["Weekly_vaccinations"] = new_vaccinations_weekly.groupby([pd.Grouper(key="date", freq="W-MON")])["daily_vaccinations"].sum()

    new_vaccinations_weekly = new_vaccinations_weekly.groupby([pd.Grouper(key="date", freq="W-MON")])["daily_vaccinations"].sum()
    new_vaccinations_weekly = new_vaccinations_weekly.to_frame()
    new_vaccinations_weekly.reset_index(inplace=True)
    new_vaccinations_weekly = new_vaccinations_weekly.rename(columns={"daily_vaccinations": "Weekly_vaccinations"})

    return new_vaccinations_weekly

# Total number of vaccinated people per country
def total_vaccinations_country(data,country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["people_fully_vaccinated"]]

    vaccinations_total = vaccinations_total.iloc[-1][0]

    return vaccinations_total

# Total % of vaccinated people per country
def total_vaccinations_rate_country(data,country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["people_vaccinated_per_hundred"]]
    vaccinations_total = vaccinations_total.iloc[-1][0]

    return vaccinations_total


# Total % of vaccinated people per country (evolution)
def vaccinations_rate_evol_country(data,country):
    data_country = data[data["location"] == country]
    vaccinations_total = data_country[["date","people_vaccinated_per_hundred"]]

    return vaccinations_total


## Global level functions

# Top 15 countries with highest vaccination rate
def top_15_vaccinations_rate(data):
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

    return top_15_vaccination_rates


"""


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
