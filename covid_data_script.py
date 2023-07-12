# Import libraries
import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq
import requests

# Import own packages
from covid_packages.cases_deaths import *
from covid_packages.vaccination import *

# Set enivronment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "covid-dashboard-378011-d39bea98e1ae.json"

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

dataframes = []

for code, country in dict_country_code_name.items():
    new_cases_last = new_cases_lastday(data_cases_deaths, country)
    new_cases_last_df = pd.DataFrame({"New_cases_last_day": [new_cases_last], "Country": [country]})
    new_cases_last_df["Country"] = country
    dataframes.append(new_cases_last_df)

# Concatenate all DataFrames into a single DataFrame
new_cases_lastday_df = pd.concat(dataframes)

# Reset the index
new_cases_lastday_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of cases (last day) for each country --")

# Number of deaths (last day)

dataframes = []

for code, country in dict_country_code_name.items():
    new_deaths_last = new_deaths_lastday(data_cases_deaths, country)
    new_deaths_last_df = pd.DataFrame({"New_deaths_last_day": [new_deaths_last], "Country": [country]})
    new_deaths_last_df["Country"] = country
    dataframes.append(new_deaths_last_df)

# Concatenate all DataFrames into a single DataFrame
new_deaths_lastday_df = pd.concat(dataframes)

# Reset the index
new_deaths_lastday_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of deaths (last day) for each country --")

# Number of new cases weekly (last 7 days)
dataframes = []

for code, country in dict_country_code_name.items():
    new_cases_last7day = new_cases_last_7d(data_cases_deaths, country)

    # Add a 'Country' column to the DataFrame
    new_cases_last7day['Country'] = country

    # If you want to drop the 'Date_reported' column, uncomment the following line
    # new_cases_last7day = new_cases_last7day.drop(['Date_reported'], axis=1)

    dataframes.append(new_cases_last7day)

# Concatenate all DataFrames into a single DataFrame
new_cases_last7day_df = pd.concat(dataframes)

# Reset the index
new_cases_last7day_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of new cases weekly (last 7 days) --")

# Number of new cases weekly (7-day rolling average)
dataframes = []

for code, country in dict_country_code_name.items():
    new_cases_last7dayavr = new_cases_7d_average(data_cases_deaths, country)
    new_cases_last7dayavr["Country"] = country

    dataframes.append(new_cases_last7dayavr)

# Concatenate all DataFrames into a single DataFrame
new_cases_last7dayavr_df = pd.concat(dataframes)

# Reset the index
new_cases_last7dayavr_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of new cases weekly (7-day rolling average) --")

# Number of new deaths weekly (last 7 days)

dataframes = []

for code, country in dict_country_code_name.items():
    new_deaths_last7day = new_deaths_last_7d(data_cases_deaths, country)
    new_deaths_last7day["Country"] = country

    dataframes.append(new_deaths_last7day)

# Concatenate all DataFrames into a single DataFrame
new_deaths_last7day_df = pd.concat(dataframes)

# Reset the index
new_deaths_last7day_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of new deaths weekly (last 7 days) --")

# Number of new deaths weekly (7-day rolling average)
dataframes = []

for code, country in dict_country_code_name.items():
    new_deaths_last7dayavr = new_deaths_7d_average(data_cases_deaths, country)
    new_deaths_last7dayavr["Country"] = country

    dataframes.append(new_deaths_last7dayavr)

# Concatenate all DataFrames into a single DataFrame
new_deaths_last7dayavr_df = pd.concat(dataframes)

# Reset the index
new_deaths_last7dayavr_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of new deaths weekly (7-day rolling average) --")

# Number of cumulative cases (latest)
dataframes = []

for code, country in dict_country_code_name.items():
    cum_caseslatest = cum_cases_latest(data_cases_deaths, country)
    cum_caseslatest_df = pd.DataFrame({"Cum_caseslatest": [cum_caseslatest], "Country": [country]})
    cum_caseslatest_df["Country"] = country
    dataframes.append(cum_caseslatest_df)

# Concatenate all DataFrames into a single DataFrame
cum_caseslatest_df = pd.concat(dataframes)

# Reset the index
cum_caseslatest_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of cumulative cases (latest) --")

# Number of cumulative deaths (latest)

dataframes = []

for code, country in dict_country_code_name.items():
    cum_deathslatest = cum_deaths_latest(data_cases_deaths, country)
    cum_deathslatest_df = pd.DataFrame({"Cum_deathslatest": [cum_deathslatest], "Country": [country]})
    cum_deathslatest_df["Country"] = country
    dataframes.append(cum_deathslatest_df)

# Concatenate all DataFrames into a single DataFrame
cum_deathslatest_df = pd.concat(dataframes)

# Reset the index
cum_deathslatest_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of cumulative deaths (latest) --")


# Evolution of new cases (all-time)

dataframes = []

for code, country in dict_country_code_name.items():
    evol_casesalltime = evol_cases_alltime(data_cases_deaths, country)
    evol_casesalltime["Country"] = country
    dataframes.append(evol_casesalltime)

# Concatenate all DataFrames into a single DataFrame
evol_casesalltime_df = pd.concat(dataframes)

# Reset the index
evol_casesalltime_df.reset_index(drop=True, inplace=True)

print("-- ✅ Evolution of new cases (all-time) --")

# Evolution of new deaths (all-time)

dataframes = []

for code, country in dict_country_code_name.items():
    evol_deathsalltime = evol_deaths_alltime(data_cases_deaths, country)
    evol_deathsalltime["Country"] = country
    dataframes.append(evol_deathsalltime)

# Concatenate all DataFrames into a single DataFrame
evol_deathsalltime_df = pd.concat(dataframes)

# Reset the index
evol_deathsalltime_df.reset_index(drop=True, inplace=True)

print("-- ✅ Evolution of new deaths (all-time) --")

# Evolution of cumulative cases (all-time)

dataframes = []

for code, country in dict_country_code_name.items():
    evol_cumcases = evol_cum_cases(data_cases_deaths, country)
    evol_cumcases["Country"] = country
    dataframes.append(evol_cumcases)

# Concatenate all DataFrames into a single DataFrame
evol_cumcases_df = pd.concat(dataframes)

# Reset the index
evol_cumcases_df.reset_index(drop=True, inplace=True)

print("-- ✅ Evolution of cumulative cases (all-time) --")

# Evolution of cumulative deaths (all-time)

dataframes = []

for code, country in dict_country_code_name.items():
    evol_cumdeaths = evol_cum_deaths(data_cases_deaths, country)
    evol_cumdeaths["Country"] = country
    dataframes.append(evol_cumdeaths)

# Concatenate all DataFrames into a single DataFrame
evol_cumdeaths_df = pd.concat(dataframes)

# Reset the index
evol_cumdeaths_df.reset_index(drop=True, inplace=True)

print("-- ✅ Evolution of cumulative deaths (all-time) --")

# New cases (weekly)

dataframes = []

for code, country in dict_country_code_name.items():
    new_casesweekly = new_cases_weekly(data_cases_deaths, country)
    new_casesweekly["Country"] = country
    dataframes.append(new_casesweekly)

# Concatenate all DataFrames into a single DataFrame
new_casesweekly_df = pd.concat(dataframes)

# Reset the index
new_casesweekly_df.reset_index(drop=True, inplace=True)

print("-- ✅ New cases (weekly) --")

# New deaths (weekly)

dataframes = []

for code, country in dict_country_code_name.items():
    new_deathsweekly = new_deaths_weekly(data_cases_deaths, country)
    new_deathsweekly["Country"] = country
    dataframes.append(new_deathsweekly)

# Concatenate all DataFrames into a single DataFrame
new_deathsweekly_df = pd.concat(dataframes)

# Reset the index
new_deathsweekly_df.reset_index(drop=True, inplace=True)

print("-- ✅ New deaths (weekly) --")

# New cases (weekly % change)

dataframes = []

for code, country in dict_country_code_name.items():
    new_cases_weeklychange = new_cases_weekly_change(data_cases_deaths, country)
    new_cases_weeklychange["Country"] = country
    dataframes.append(new_cases_weeklychange)

# Concatenate all DataFrames into a single DataFrame
new_cases_weeklychange_df = pd.concat(dataframes)

# Reset the index
new_cases_weeklychange_df.reset_index(drop=True, inplace=True)

print("-- ✅ New cases (weekly % change) --")

# New deaths (weekly % change)

dataframes = []

for code, country in dict_country_code_name.items():
    new_deaths_weeklychange = new_deaths_weekly_change(data_cases_deaths, country)
    new_deaths_weeklychange["Country"] = country
    dataframes.append(new_deaths_weeklychange)

# Concatenate all DataFrames into a single DataFrame
new_deaths_weeklychange_df = pd.concat(dataframes)

# Reset the index
new_deaths_weeklychange_df.reset_index(drop=True, inplace=True)

print("-- ✅ New deaths (weekly % change) --")


# Top 10 weeks with most new cases

dataframes = []

for code, country in dict_country_code_name.items():
    top_10_weekscases = top_10_weeks_cases(data_cases_deaths, country)
    top_10_weekscases["Country"] = country
    dataframes.append(top_10_weekscases)

# Concatenate all DataFrames into a single DataFrame
top_10_weekscases_df = pd.concat(dataframes)

# Reset the index
top_10_weekscases_df.reset_index(drop=True, inplace=True)

print("-- ✅ Top 10 weeks with most new cases --")

# Top 10 weeks with most new deaths

dataframes = []

for code, country in dict_country_code_name.items():
    top_10_weeksdeaths = top_10_weeks_deaths(data_cases_deaths, country)
    top_10_weeksdeaths["Country"] = country
    dataframes.append(top_10_weeksdeaths)

# Concatenate all DataFrames into a single DataFrame
top_10_weeksdeaths_df = pd.concat(dataframes)

# Reset the index
top_10_weeksdeaths_df.reset_index(drop=True, inplace=True)

print("-- ✅ Top 10 weeks with most new deaths --")


## Regional level functions

# We will use the same functions defined at country level
# and make use of the function dictionary_country_region() in order to filter out the regions


## Global level functions

# Top 15 countries with most new cases / 100k today / this week

top_15_new_cases_lastweek_df = top_15_new_cases_lastweek(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with most new cases / 100k today / this week --")

# Top 15 countries with most new deaths / 100k this week

top_15_new_deaths_lastweek_df = top_15_new_deaths_lastweek(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with most new deaths / 100k this week --")

# Top 15 countries with highest total incidence of deaths per 100k

top_15_total_death_incidence_df = top_15_total_death_incidence(data_cases_deaths, dict_country_code_population, dict_country_code_name)

print("-- ✅ Top 15 countries with highest total incidence of deaths per 100k --")


# D. Data on vaccinations

## Country level functions

# Number of weekly vaccinations per country

dataframes = []

for code, country in dict_isocode_countries.items():
    total_vaccinations_country_value = new_vaccinations_weekly(data_vaccination, country)
    total_vaccinations_country_value["Country"] = country
    dataframes.append(total_vaccinations_country_value)

# Concatenate all the DataFrames in the list into a single DataFrame
total_vaccinationscountry_df = pd.concat(dataframes)

# Reset the index
total_vaccinationscountry_df.reset_index(drop=True, inplace=True)

print("-- ✅ Number of weekly vaccinations per country --")

# Total number of vaccinated people per country

dataframes = []

for code, country in dict_isocode_countries.items():
    total_vaccinatedpeople_country = total_vaccinations_country(data_vaccination, country)
    dataframes.append(pd.DataFrame({"Country": [country], "Total_vaccinations": [total_vaccinatedpeople_country]}))

# Concatenate all the DataFrames in the list into a single DataFrame
total_vaccinatedpeople_country_df = pd.concat(dataframes)

# Reset the index of the DataFrame
total_vaccinatedpeople_country_df.reset_index(drop=True, inplace=True)

print("-- ✅ Total number of vaccinated people per country --")

# Total % of vaccinated people per country

dataframes = []

for code, country in dict_isocode_countries.items():
    total_percvaccinated_country = total_vaccinations_rate_country(data_vaccination, country)
    dataframes.append(pd.DataFrame({"Country": [country], "Total_vaccinations": [total_percvaccinated_country]}))

# Concatenate all the DataFrames in the list into a single DataFrame
total_percvaccinated_country_df = pd.concat(dataframes)

# Reset the index of the DataFrame
total_percvaccinated_country_df.reset_index(inplace=True)

print("-- ✅ Total % of vaccinated people per country --")

# Total % of vaccinated people per country (evolution / month)

dataframes = []

for code, country in dict_isocode_countries.items():
    # Get the DataFrame for the current country
    vaccinations_changeevol_country = vaccinations_rate_evol_country(data_vaccination, country)
    vaccinations_changeevol_country['Country'] = country
    dataframes.append(vaccinations_changeevol_country)

# Concatenate all the DataFrames in the list into a single DataFrame
vaccinations_changeevol_country_df = pd.concat(dataframes)

# Reset the index of the DataFrame
vaccinations_changeevol_country_df.reset_index(inplace=True)

print("-- ✅ Total % of vaccinated people per country (evolution) --")

# Total nb of vaccinated people per country (evolution / month)

dataframes = []

for code, country in dict_isocode_countries.items():
    # Get the DataFrame for the current country
    vaccinations_nbevol_country = vaccinations_change_evol_country(data_vaccination, country)
    vaccinations_nbevol_country['Country'] = country
    dataframes.append(vaccinations_nbevol_country)

# Concatenate all the DataFrames in the list into a single DataFrame
vaccinations_nbevol_country_df = pd.concat(dataframes)

# Reset the index of the DataFrame
vaccinations_nbevol_country_df.reset_index(inplace=True)

print("-- ✅ Total nb of vaccinated people per country (evolution / month) --")

# Total nb of vaccinated people per country (monthly)

dataframes = []

for code, country in dict_isocode_countries.items():
    # Get the DataFrame for the current country
    vaccinations_monthly_country = vaccinations_monthly_total(data_vaccination, country)
    vaccinations_monthly_country['Country'] = country
    dataframes.append(vaccinations_monthly_country)

# Concatenate all the DataFrames in the list into a single DataFrame
vaccinations_monthly_country_df = pd.concat(dataframes)

# Reset the index of the DataFrame
vaccinations_monthly_country_df.reset_index(inplace=True)

print("-- ✅ Total nb of vaccinated people per country (monthly) --")

## Global level functions

# Top 15 countries with highest vaccination rate

top_15_vaccinations_rate_df = top_15_vaccinations_rate(data_vaccination, dict_isocode_population_300k, dict_isocode_countries)

print("-- ✅ Top 15 countries with highest vaccination rate --")

# q: can you give me the list of all the dataframes that have been created in the script above, as a list [] on a single line?
# a: yes, here it is:
# [new_cases_lastday_df, new_deaths_lastday_df, new_cases_last7day_df, new_cases_last7dayavr_df, new_deaths_last7day_df, new_deaths_last7dayavr_df, cum_caseslatest_df, cum_deathslatest_df, evol_casesalltime_df, evol_deathsalltime_df, evol_cumcases_df, evol_cumdeaths_df, new_casesweekly_df, new_deathsweekly_df, new_cases_weeklychange_df, new_deaths_weeklychange_df, top_10_weekscases_df, top_10_weeksdeaths_df, top_15_new_cases_lastweek_df, top_15_new_deaths_lastweek_df, top_15_total_death_incidence_df, total_vaccinationscountry_df, total_vaccinatedpeople_country_df, total_percvaccinated_country_df, vaccinations_changeevol_country_df, vaccinations_nbevol_country_df, vaccinations_monthly_country_df, top_15_vaccinations_rate_df]

# 3. Push data to BigQuery database

# Authenticate with Google Cloud
key_path = "/path/to/your/service-account-key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Your Google Cloud project ID & dataset ID
project_id = 'covid-dashboard-378011'
dataset_id = "covid_data_script"

# List of your dataframes and their corresponding table names
dataframes = [
    (new_cases_lastday_df, 'new_cases_lastday'),
    (new_deaths_lastday_df, 'new_deaths_lastday'),
    (new_cases_last7day_df, 'new_cases_last7day'),
    (new_cases_last7dayavr_df, 'new_cases_last7dayavr'),
    (new_deaths_last7day_df, 'new_deaths_last7day'),
    (new_deaths_last7dayavr_df, 'new_deaths_last7dayavr'),
    (cum_caseslatest_df, 'cum_caseslatest'),
    (cum_deathslatest_df, 'cum_deathslatest'),
    (evol_casesalltime_df, 'evol_casesalltime'),
    (evol_deathsalltime_df, 'evol_deathsalltime'),
    (evol_cumcases_df, 'evol_cumcases'),
    (evol_cumdeaths_df, 'evol_cumdeaths'),
    (new_casesweekly_df, 'new_casesweekly'),
    (new_deathsweekly_df, 'new_deathsweekly'),
    (new_cases_weeklychange_df, 'new_cases_weeklychange'),
    (new_deaths_weeklychange_df, 'new_deaths_weeklychange'),
    (top_10_weekscases_df, 'top_10_weekscases'),
    (top_10_weeksdeaths_df, 'top_10_weeksdeaths'),
    (top_15_new_cases_lastweek_df, 'top_15_new_cases_lastweek'),
    (top_15_new_deaths_lastweek_df, 'top_15_new_deaths_lastweek'),
    (top_15_total_death_incidence_df, 'top_15_total_death_incidence'),
    (total_vaccinationscountry_df, 'total_vaccinationscountry'),
    (total_vaccinatedpeople_country_df, 'total_vaccinatedpeople_country'),
    (total_percvaccinated_country_df, 'total_percvaccinated_country'),
    (vaccinations_changeevol_country_df, 'vaccinations_changeevol_country'),
    (vaccinations_nbevol_country_df, 'vaccinations_nbevol_country'),
    (vaccinations_monthly_country_df, 'vaccinations_monthly_country'),
    (top_15_vaccinations_rate_df, 'top_15_vaccinations_rate')
    ]

# Loop to push data to BigQuery
for df, table_name in dataframes:
    pandas_gbq.to_gbq(df, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='replace')
    print(f'✅ Pushed{table_name} to {dataset_id}.{table_name}')
