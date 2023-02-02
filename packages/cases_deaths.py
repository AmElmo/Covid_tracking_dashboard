import pandas as pd
import requests

## Data loading

# Transform CSV to dataframe
def csv_to_dataframe(csv):

    data = pd.read_csv(csv)

    # Remove first row if description
    if data.iloc[0][0].startswith('#') == True:
        data.drop(index=data.index[0], axis=0, inplace=True)

    # Convert dates to datetime type
    if 'Date_reported' in data.columns:
        data['Date_reported'] = pd.to_datetime(data['Date_reported'])
    elif 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'])

    return data

## Dictionaries loading

# Generate dictionaries with country code + population
def dict_population(data):

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

    return dict_population




## Country level functions




## Regional level functions




## Global level functions
