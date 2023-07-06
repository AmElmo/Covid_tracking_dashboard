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
fetch_file("https://covid19.who.int/WHO-COVID-19-global-data.csv", '/raw_data/covid_cases_deaths_who.csv')

# Vaccination data from WHO
fetch_file("https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view&_gl=1*330y71*_ga*MjA0MDYxMzE2Ni4xNjg4NDAwNzUy*_ga_E60ZNX2F68*MTY4ODY1ODc0Ny4yLjEuMTY4ODY1OTI0MS42MC4wLjA.", '/raw_data/covid_vaccinations_who.csv')


# 2. Apply data manipulation scripts on data

# A. Turn the CSV files into dataframes

# Death & Cases data
data_cases_deaths = csv_to_dataframe("/Users/julienberthomier/code/AmElmo/Main_Projects/Covid_tracking_dashboard/csv_files/covid_cases_deaths_who.csv")

# Vaccination data
data_vaccination = csv_to_dataframe("/Users/julienberthomier/code/AmElmo/Main_Projects/Covid_tracking_dashboard/csv_files/vaccination_data_who.csv")

# B. Build dictionaries

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

# Generate dictionary of country names + WHO_region

def dictionary_country_region(data):

    print("Start dictionary")

    combinations = data[['Country', 'WHO_region']].drop_duplicates()

    dict_codes = {'Country': combinations['Country'], 'WHO_region': combinations['WHO_region']}

    df = pd.DataFrame(dict_codes)

    final_dict = dict(zip(df['Country'], df["WHO_region"]))

    print("Country name / WHO_region")

    return final_dict


# C. Data on cases and deaths


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
