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

# Generate dictionary of country names + WHO_region

def dictionary_country_region(data):
    combinations = data[['Country', 'WHO_region']].drop_duplicates()

    dict_codes = {'Country': combinations['Country'], 'WHO_region': combinations['WHO_region']}

    df = pd.DataFrame(dict_codes)

    final_dict = dict(zip(df['Country'], df["WHO_region"]))

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

# Number of new deaths weekly (last 7 days)
def new_deaths_last_7d(data,country):

    pd.set_option('mode.chained_assignment', None)

    data_country = data[data["Country"] == country]
    new_deaths_7d = data_country[["Date_reported", "New_deaths"]]
    new_deaths_7d["Last_7_days"] = new_deaths_7d["New_deaths"].rolling(7).sum()

    new_deaths_7d.drop(['New_deaths'], axis = 1, inplace = True)

    return new_deaths_7d

# Number of new deaths weekly (7-day rolling average)
def new_deaths_7d_average(data,country):

    pd.set_option('mode.chained_assignment', None)

    data_country = data[data["Country"] == country]
    new_deaths_7d_average = data_country[["Date_reported", "New_deaths"]]
    new_deaths_7d_average["7_days_average"] = new_deaths_7d_average["New_deaths"].rolling(7).sum() / 7
    new_deaths_7d_average["7_days_average"] = new_deaths_7d_average["7_days_average"].round(decimals=2)

    new_deaths_7d_average.drop(['New_deaths'], axis = 1, inplace = True)

    return new_deaths_7d_average

# Number of cumulative cases (latest)

def cum_cases_latest(data,country):

    data_country = data[data["Country"] == country]
    cumulative_cases_latest = data_country["Cumulative_cases"].iloc[-1]

    return cumulative_cases_latest

# Number of cumulative deaths (latest)

def cum_deaths_latest(data,country):

    data_country = data[data["Country"] == country]
    cumulative_deaths_latest = data_country["Cumulative_deaths"].iloc[-1]

    return cumulative_deaths_latest

# Evolution of new cases (all-time)

def evol_cases_alltime(data,country):

    data_country = data[data["Country"] == country]
    evolution_new_cases = data_country[["Date_reported", "New_cases"]]
    evolution_new_cases = evolution_new_cases[evolution_new_cases["Date_reported"].dt.dayofweek < 5]

    return evolution_new_cases

# Evolution of new deaths (all-time)

def evol_deaths_alltime(data,country):
    data_country = data[data["Country"] == country]
    evolution_new_deaths = data_country[["Date_reported", "New_deaths"]]
    evolution_new_deaths = evolution_new_deaths[evolution_new_deaths["Date_reported"].dt.dayofweek < 5]

    return evolution_new_deaths

# Evolution of cumulative cases (all-time)

def evol_cum_cases(data,country):

    data_country = data[data["Country"] == country]
    evolution_cumulative_cases = data_country[["Date_reported", "Cumulative_cases"]]

    return evolution_cumulative_cases

# Evolution of cumulative deaths (all-time)

def evol_cum_deaths(data,country):
    data_country = data[data["Country"] == country]
    evolution_cumulative_deaths = data_country[["Date_reported", "Cumulative_deaths"]]

    return evolution_cumulative_deaths

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

# Number of cases (last day)




## Global level functions
