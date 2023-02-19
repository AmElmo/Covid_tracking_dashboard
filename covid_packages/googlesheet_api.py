import requests
import json

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def clean_sheet_api(SCOPES,SAMPLE_SPREADSHEET_ID,SAMPLE_RANGE_NAME):
    if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_59863169565-0g6psho3kj61kokqdsrqiao73m0o1i4q.apps.googleusercontent.com.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()

    request = sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()

    print("Cleared sheet")



def push_sheet_api(SCOPES,SAMPLE_SPREADSHEET_ID,SAMPLE_RANGE_NAME,VALUES):

    if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_59863169565-0g6psho3kj61kokqdsrqiao73m0o1i4q.apps.googleusercontent.com.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)


    # How the input data should be interpreted.
    value_input_option = ''

    insert_data_option = ''

    value_range_body = {
        "majorDimension": "ROWS",
        "values": VALUES
    }

    # Call the Sheets API
    sheet = service.spreadsheets()

    result = sheet.values().append(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME, valueInputOption="USER_ENTERED", body=value_range_body).execute()

    print(result)
