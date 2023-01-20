


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

**COVID-19 Open Data**: Covid-19 dataset created by Google. Not updated since September 15th 2022 - but scripts re-used and adapted for this project.

<br/>


# 🪜 Project steps

## 1. 💽 Data collection

We first looked at Google COVID-19 Open-Data (see [here](https://github.com/GoogleCloudPlatform/covid-19-open-data)). It is a very comprehensive project, unfortunately the scripts written by the community to gather data from each national health ministries were not up to date. Contributions stopped on September 13th 2022 and the repository was turned read-only.


Main data sources:
- For cases and deaths: https://data.humdata.org/dataset/coronavirus-covid-19-cases-and-deaths
- For vaccines: https://data.humdata.org/dataset/covid-19-vaccinations

We have a script with cron-job that picks up the raw CSV data from the WHO (World Health Organization).

## 2. 💽 Data consolidation




## 3. 💽 Tableau setup
