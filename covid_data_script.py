from covid_packages import cases_deaths as cd
from covid_packages import vaccination as vc

import requests

# Download the 2 files from WHO website

## Cases & Deaths data
URL = "https://covid19.who.int/WHO-COVID-19-global-data.csv"
response = requests.get(URL)
open("data_download/cases_deaths_data.csv", "wb").write(response.content)

## Vaccination data
URL = "https://proxy.hxlstandard.org/data.csv?tagger-match-all=on&tagger-01-header=location&tagger-01-tag=%23country%2Bname&tagger-02-header=iso_code&tagger-02-tag=%23country%2Bcode&tagger-03-header=date&tagger-03-tag=%23date&tagger-04-header=total_vaccinations&tagger-04-tag=%23total%2Bvaccinations&tagger-08-header=daily_vaccinations&tagger-08-tag=%23total%2Bvaccinations%2Bdaily&url=https%3A%2F%2Fraw.githubusercontent.com%2Fowid%2Fcovid-19-data%2Fmaster%2Fpublic%2Fdata%2Fvaccinations%2Fvaccinations.csv&header-row=1&dest=data_view&_gl=1*1nmf87q*_ga*MjAyOT"
response = requests.get(URL)
open("data_download/vaccination_data.csv", "wb").write(response.content)

# Load data as dataframes

cd_data = cd.csv_to_dataframe("data_download/cases_deaths_data.csv")

vc_data = cd.csv_to_dataframe("data_download/vaccination_data.csv")

# Load dictionaries for cases & deaths
## Generate dictionary with country code + population
# dict_code_pop = cd.dict_population(cd_data)

## Generate dictionary with country code + country name
dict_code_name = cd.dictionary_country_code(cd_data)

## Generate dictionary of country names + WHO_region
dict_name_who_region = cd.dictionary_country_region(cd_data)

# Load dictionaries for vaccination

## Generate dictionary of iso codes / population (> 300.000)
# dict_iso_pop = vc.dict_population_iso(vc_data)

# Dictionary of iso codes / countries
dict_iso_country =vc. dictionary_iso_code(vc_data)

# Run manipulation scripts & push to Google Sheet API
