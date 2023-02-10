from covid_packages import cases_deaths as cd
from covid_packages import vaccination as vc

import requests

# Download the 2 files from WHO website

## Cases & Deaths data
URL = "https://covid19.who.int/WHO-COVID-19-global-data.csv"
response = requests.get(URL)
# open("cases_deaths_data.csv", "wb").write(response.content)

## Vaccination data
URL = "https://covid19.who.int/who-data/vaccination-data.csv"
response = requests.get(URL)
# open("vaccination_data.csv", "wb").write(response.content)

# Load data as dataframes

cd_data = cd.csv_to_dataframe("cases_deaths_data.csv")

vc_data = cd.csv_to_dataframe("vaccination_data.csv")

# Load dictionaries for cases & deaths
## Generate dictionary with country code + population
dict_code_pop = cd.dict_population(cd_data)

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


# Run manipulation scripts & push to Google Sheet API
