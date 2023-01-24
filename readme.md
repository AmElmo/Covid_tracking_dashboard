


# 🤔 Problem

Keeping track of Covid related data is painful. It takes

<p float="left">
  <img src="imgs/sign_language_1.png" width="300" />
  <img src="imgs/sign_language_3.jpeg" width="250" />
</p>

<br/>

# 💡 Solution

 <img src="imgs/sign_language_2.png" width="360" />

 <img src="imgs/image3.gif" width="650" />

<br/>


# 🤖 Stack overview

- Python
- Pandas: for data manipulation and package definition
- Matplotlib and Seaborn: for pre-visualization in Jupyter Notebook
- Tableau: for final visualization
- Google Drive API: to dump aggregated data
- REST Countries API: to get country info)

**COVID-19 Open Data**: Covid-19 dataset created by Google. Not updated since September 15th 2022 - but scripts re-used and adapted for this project.

<br/>


# 🪜 Project steps

## 1. 💽 Data collection

We first looked at Google COVID-19 Open-Data (see [here](https://github.com/GoogleCloudPlatform/covid-19-open-data)). It is a very comprehensive project, unfortunately the scripts written by the community to gather data from each national health ministries were not up to date. Contributions stopped on September 13th 2022 and the repository was turned read-only.

Main data sources:
- For cases and deaths: https://data.humdata.org/dataset/coronavirus-covid-19-cases-and-deaths
- For vaccines: https://data.humdata.org/dataset/covid-19-vaccinations

We have a script with cron-job that picks up the raw CSV data from the WHO (World Health Organization).

## 2. 🧱 Data consolidation and functions definitions

We used Pandas in Jupyter Notebook to explore the data and build aggregation functions on the raw dataset.

The whole logic was then packaged so we can generate the data we need to feed to Tableau.

A total of xxx functions were created across xxx packages.

## 3. ⏳ Data flow

1. We have a CRON job scheduled to pick up the two main csv files from the WHO website every single day at xxx

2. The different packages then run to generate the Google Sheets consolidating the data. Each visualization source is under a separate sheet. The Google Sheet is cleared and re-created every single day.

💡 We picked Google Sheets over a dedicated database (BigQuery or else) because the consolidated data will have relatively low volume of cells (~1m) per sheet. Google Sheet allows for up to 10m cells so we are well within the limits. Performance remains strong.

3. Tableau connects to Google Sheet in order to generate the dashboards.


## 4. 📊 Tableau setup

We connect
