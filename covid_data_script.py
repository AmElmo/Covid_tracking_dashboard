# Import libraries

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import pandas_gbq


# 1. Fetch data from the web (CRON job)

# 2. Apply data manipulation scripts

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
