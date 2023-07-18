import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd
from google.cloud import bigquery
import os

client = bigquery.Client(location="US", project="covid-dashboard-378011")

bigquery_ref = 'covid-dashboard-378011.covid_data_script'

key_path = "covid-dashboard-378011-d39bea98e1ae.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Query for the data

query = f"""
SELECT *
FROM {bigquery_ref}.`cum_caseslatest`
"""
df_1 = client.query(query).to_dataframe()


# Build the layout for the Dash app

app = dash.Dash(__name__)

app.layout = html.Div(children=[
    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': df_1['Country'], 'y': df_1['Cum_caseslatest'], 'type': 'bar', 'name': 'Data'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
