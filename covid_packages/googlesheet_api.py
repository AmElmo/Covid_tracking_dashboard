import requests
import json

import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient import discovery


<<<<<<< HEAD
def clean_sheet_api(SCOPES,SPREADSHEET_ID,RANGE_NAME):
=======
def clean_sheet_api(SCOPES,SPREADSHEET_ID,SAMPLE_RANGE_NAME):
>>>>>>> e10a2ee10bbd82ebbf81181570833da899b8819c
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

<<<<<<< HEAD
    request = sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
=======
    request = sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
>>>>>>> e10a2ee10bbd82ebbf81181570833da899b8819c

    print("Cleared sheet")



<<<<<<< HEAD
def push_sheet_api(SCOPES,SPREADSHEET_ID,RANGE_NAME,VALUES):
=======
def push_sheet_api(SCOPES,SPREADSHEET_ID,SAMPLE_RANGE_NAME,VALUES):
>>>>>>> e10a2ee10bbd82ebbf81181570833da899b8819c

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
    value_input_option = 'RAW'

    insert_data_option = ''

    value_range_body = {
        "majorDimension": "ROWS",
        "values": VALUES
    }

    # Call the Sheets API
    sheet = service.spreadsheets()

    result = sheet.values().append(spreadsheetId=SPREADSHEET_ID,
<<<<<<< HEAD
                                    range=RANGE_NAME, valueInputOption="USER_ENTERED", body=value_range_body).execute()
=======
                                    range=SAMPLE_RANGE_NAME, valueInputOption="USER_ENTERED", body=value_range_body).execute()
>>>>>>> e10a2ee10bbd82ebbf81181570833da899b8819c

    print(result)
    print("Data pushed to Google Sheet API")

def createSheet(SCOPES,SPEADSHEET_ID,newName):

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

    service = discovery.build('sheets', 'v4', credentials=creds)

    request_body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': newName,
                    }
                }
            }]
        }

<<<<<<< HEAD
    request = service.spreadsheets().batchUpdate(
=======
    request = service.spreadsheets.batchUpdate(
>>>>>>> e10a2ee10bbd82ebbf81181570833da899b8819c
            spreadsheetId=SPEADSHEET_ID,
            body=request_body
        )

    response = request.execute()

    print(response)

def renameSheet(SCOPES, SPREADSHEET_ID, sheetId, newName):

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

    service = discovery.build('sheets', 'v4', credentials=creds)

    batch_update_spreadsheet_request_body = {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheetId,
                    "title": newName,
                }
            }
    }

    request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [batch_update_spreadsheet_request_body]})
    response = request.execute()

    print(response)

    # print(f"Renamed spreadhseet to {newName}")
