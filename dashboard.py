import dash
import dash_core_components as dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# Query for the data

query = """
SELECT *
FROM `my_project.my_dataset.my_table`
"""
df_1 = client.query(query).to_dataframe()


# Build the layout for the Dash app

app = dash.Dash(__name__)

app.layout = html.Div(children=[
    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': df['column1'], 'y': df['column2'], 'type': 'bar', 'name': 'Data'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
