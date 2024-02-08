#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import numpy as np
#from src.sheets_utils import download_sheet_as_df
#from src.sheets_utils import upload_df_to_sheet
#from src.sheets_utils import create_new_sheet_from_df

import json
from googleapiclient.errors import HttpError

from datetime import datetime
from datetime import date
import sys
import time
import schedule
from schedule import every, repeat, run_pending

pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 50)

service_account_path = "creds/google__sa.json"
tracking_sheet_id = "1qBU7Kvuuij2fxbqPxebReKMxWgIBmOIE5Gi4ZuX0j_4"
contributor_sheet_id = "14bcgtOEh5ClIYhH7rebN8c_AXEQ6ZcDVLRgwn3ZQW4E"
time_sheet_id = "1ZJq9vqpjZFz1SDZMYSec2uXmD9gSnA994jmVQqU5IsY"
report_sheet_id = "1iP2sVYU_xiGGAp2hmbn_VpakhQpl3cH87qZY86sRUQI"
tasks=None
members=None
time=None
reviews=None
history=None
df=None
summary_df=None
final=None
total_records=0



def download_sheets_as_df(service_account_path, sheet_id, sheet_name):
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Construct the range to read
    sheet_range = f"{sheet_name}!A:DZ"  # Adjust the range A:Z as needed

    # Make the API request
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=sheet_range).execute()
    values = result.get('values', [])

    # Convert to a DataFrame
    if not values:
        #print("No data found.")
        return pd.DataFrame()
    else:
        #return pd.DataFrame.from_records(values[1:],columns=values[0])
        if((sheet_id==time_sheet_id) or (sheet_id==report_sheet_id)):
            new_values = values[1:]
            new_df = pd.DataFrame([row + [None] * (len(values[1]) - len(row)) for row in new_values], columns=values[1])
            return new_df
        else:
            return pd.DataFrame([row + [None] * (len(values[0]) - len(row)) for row in values[1:]], columns=values[0])


def update_df_to_sheet(service_account_path, sheet_id, sheet_name, df):
    """
    Uploads headers and data from a DataFrame to a Google Sheet.
    """
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Construct the range to write
    sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed

    # Convert the DataFrame to a 2D list of values
    values = [df.columns.tolist()] + df.values.tolist()

    # Make the API request
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=sheet_range,
        valueInputOption='RAW', body=body).execute()
    


def get_sheets(service_account_path, sheet_id,sheet_val):
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    values=[]
    # Construct the range to read
    sheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute().get('sheets', [])
    for s in sheet:
        sheet_title = s.get('properties', {}).get('title')
        if(sheet_val in sheet_title):
            # Construct the range to read
            sheet_range = f"{sheet_title}!A:Z"  # Adjust the range A:Z as needed
            # Make the API request
            result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=sheet_range).execute()
            val = result.get('values', [])
            if(val):
                if val[0] and sheet_title not in values:
                    values.append(sheet_title)
    return values




def create_sheet_from_df(service_account_path, sheet_id, sheet_name, df):
    """
    Creates a new sheet and populates it with headers and data from a DataFrame.
    """
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Convert the DataFrame to a 2D list of values
    values = [df.columns.tolist()] + df.values.tolist()
    # Make the API request
    body = {'values': values}
    service.spreadsheets().values().append(
        spreadsheetId=sheet_id, range=sheet_name,
        valueInputOption='RAW', body=body).execute()


def delete_df_from_sheet(service_account_path, sheet_id, sheet_name):
    """
    Creates a new sheet and populates it with headers and data from a DataFrame.
    """
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    # Convert the DataFrame to a 2D list of values
    # Construct the range to write
    if sheet_id==report_sheet_id:
        sheet_range = f"{sheet_name}!A:L"  # Adjust the range A:Z as needed
    else:
        sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed
    #values = [df.columns.tolist()] + df.values.tolist()
    # Make the API request
    #body1 = {'values': values}
    result = service.spreadsheets().values().clear(
                spreadsheetId=tracking_sheet_id,
                range=sheet_range,
                body={}
            ).execute()



def init_report():
    global tasks
    global members
    global time
    global total_tasks
    global reviews
    global history
    global summary_df
    tasks=None
    members=None
    time=None
    tasks_val = 'Conversations_Batch_'
    historical_sheet='historical__personal_corrections'
    #Check sheets available with tasks completed and not delivered
    task_cols = ['task_link','metadata__topic','assigned_to_email','completion_status','modified_question?','duration_mins','completion_date','comments','metadata__type','metadata__target_length','review_status','reviewer_email']
    review_sheet=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score','status_score','age_score','count_score','quality_score'])
    tasks=pd.DataFrame(columns=task_cols)
    summary_df=pd.DataFrame(columns=['Developer ID','Developer Name','Turing email','Current Status','Jibble Time','Total_Num_Tasks','Total_Task_Hours'])
    task_batches = get_sheets(
        service_account_path,
        tracking_sheet_id,
        tasks_val
    )
    #Check sheets available with tasks completed and not delivered
    num_task_sheets = len(task_batches)
    print("Total task sheets to read :",num_task_sheets)
    for s in task_batches:      
        tasks = pd.concat([
            tasks,
        download_sheets_as_df(
            service_account_path,
            tracking_sheet_id,
            s
        )],ignore_index=True)
        #print("Number of tasks : ",len(tasks))
    
    reviews = pd.concat([
                download_sheets_as_df(
                    service_account_path,
                    tracking_sheet_id,
                    'Reviews'
                )], ignore_index=True)

    history = pd.concat([
                download_sheets_as_df(
                    service_account_path,
                    tracking_sheet_id,
                    'historical__personal_corrections'
                )], ignore_index=True)
    members = pd.concat([
            download_sheets_as_df(
                service_account_path,
                contributor_sheet_id,
                'Character.ai_Devs'
            )], ignore_index=True)
    time = pd.concat([
            download_sheets_as_df(
                service_account_path,
                time_sheet_id,
                'Hours sheet'
            )], ignore_index=True)
    #duplicate_rows = current.duplicated(subset=['source','job_id', 'developer_id'])
    #current['is_duplicated']=duplicate_rows
    print('Completed reading task data...')
    print("Number of total tasks : ",len(tasks))
    tasks['completion_date']=pd.to_datetime(tasks['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    #tasks.dropna(subset=['completion_date'], inplace=True)
    print("Number of total tasks after date formatting: ",len(tasks))
    #tasks = tasks[tasks['completion_date'].dt.year == 2024]
    tasks = tasks[tasks['completion_date'].dt.month == 1] #Let us use this later to get monthly info
    print("Number of total tasks completed in Januray,2024: ",len(tasks))
    print('Completed reading reviews data...')
    print("Number of total reviews : ",len(reviews))
    reviews['Timestamp']=pd.to_datetime(reviews['Timestamp'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    #tasks.dropna(subset=['completion_date'], inplace=True)
    print("Number of total reviews after date formatting: ",len(reviews))
    reviews = reviews[reviews['Timestamp'].dt.year == 2024]
    print("Number of total reviews completed in Januray,2024: ",len(reviews))
    print('Completed reading historical data...')
    print("Number of total historical records : ",len(history))
    history['completion_date']=pd.to_datetime(history['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    print("Number of total historical tasks after date formatting: ",len(history))
    history = history[history['completion_date'].dt.year == 2024]
    print("Number of total historical tasks in Januray,2024: ",len(history))
    print('Completed reading member data...')
    print("Number of total members : ",len(members))
    print('Completed reading Jan,2024 timesheet entries')
    print('Number of Jan time sheet entries ..',len(time))
    

def get_totalTime():
    global tasks
    global df 
    tasks['duration_mins'] = pd.to_numeric(tasks['duration_mins'], errors='coerce')
    # Drop rows with NaN values
    tasks.dropna(subset=['duration_mins'], inplace=True)
    # Compute the sum of the 'duration_mins' column
    total_duration = tasks['duration_mins'].sum()
    df = tasks.groupby(['assigned_to_email']).agg(task_count=('task_link', 'count'), task_duration_mins=('duration_mins','sum')).sort_values(by=['task_count'], ascending=[False]).reset_index(level=['assigned_to_email'])
    #print('Number of unique members(df) worked on current batch tasks :',df['assigned_to_email'].nunique())
    print('Total Jan,2024 contributors ',len(df))

def prepare_df():
    global tasks
    global members
    global reviews
    global time
    global history
    global summary_df
    global df
    temp = members[['Dev_ID','dev_name','status','turing_email']].copy()
    temp = temp[~temp['dev_name'].str.contains('#N/A')]
    temp['turing_email'].fillna('NA',inplace=True)
    #print(time.columns)
    time = time[['Developer Names','Grand Total']].copy()
    #print('created a temp df now ',len(temp))
    time.rename(columns={'Developer Names':'dev_name','Grand Total':'Jibble Time'}, inplace=True)
    time.dropna(subset=['dev_name'], inplace=True)
    time = time[~time['dev_name'].str.contains('Grand')]
    time = time[~time['dev_name'].str.contains('Trial')]
    time = time[~time['dev_name'].str.contains('Developer Names')]    
    #print('len of time sheets ',len(time))
    df.rename(columns={'assigned_to_email':'Turing email'}, inplace=True)
    merged = time.merge(temp, on='dev_name', how='left', indicator=True)
    merged.rename(columns={'turing_email':'Turing email'}, inplace=True)
    merged.drop(columns=['_merge'], inplace=True)
    merged1 = merged.merge(df,on='Turing email',how='right',indicator=True)
    both_rows = (merged1['_merge'] == 'both').sum()
    #print('Members in both sheets ',both_rows)
    #print(merged1)
    merged1 = merged1[~merged1['Turing email'].str.contains('https://')]
    merged1.loc[merged1['_merge'] == 'right_only', 'dev_name'] = 'Internal/Anthropic Team'
    merged1.loc[merged1['_merge'] == 'right_only', 'Dev_ID'] = 'NA'
    merged1.loc[merged1['_merge'] == 'right_only', 'status'] = 'NA'
    merged1.loc[merged1['_merge'] == 'right_only', 'Jibble Time'] = 0  
    merged1['task_duration_mins']=merged1['task_duration_mins']/60
    merged1.drop(columns=['_merge'],inplace=True)
    for index,row in merged1.iterrows():
        newRow = {'Developer ID':row['Dev_ID'],'Developer Name':row['dev_name'],'Current Status':row['status'],'Turing email':row['Turing email'],
        'Jibble Time':row['Jibble Time'],'Total_Num_Tasks':row['task_count'],'Total_Task_Hours':row['task_duration_mins']}
        summary_df.loc[len(summary_df.index)] = newRow
    #print(summary_df.columns)
    #print(summary_df)
    total_internal_tasks = summary_df.loc[summary_df['Developer Name'] == 'Internal/Anthropic Team', 'Total_Num_Tasks'].sum()
    total_internal_duration = summary_df.loc[summary_df['Developer Name'] == 'Internal/Anthropic Team','Total_Task_Hours'].sum()
    summary_df = summary_df[~summary_df['Developer Name'].str.contains('Internal/Anthropic Team')]
    #print(total_internal_tasks,total_internal_duration)
    newRow = {'Developer ID':'NA','Developer Name':'Internal/Anthropic Team','Current Status':'NA','Turing email':'NA',
        'Jibble Time':'NA','Total_Num_Tasks':total_internal_tasks,'Total_Task_Hours':total_internal_duration}
    summary_df.loc[len(summary_df.index)] = newRow
    history.rename(columns={'resolved_by_email':'Turing email'}, inplace=True)
    history.dropna(subset=['Turing email'], inplace=True)
    history['resolution_duration'] = pd.to_numeric(history['resolution_duration'], errors='coerce')
    merged2 = merged.merge(history,on='Turing email',how='right',indicator=True)
    both_rows = (merged2['_merge'] == 'both').sum()
    merged2.drop(columns=['_merge'],inplace=True)
    merged2.rename(columns={'dev_name':'Developer Name'}, inplace=True)
    merged2.rename(columns={'Dev_ID':'Developer ID'}, inplace=True)
    merged2.rename(columns={'status':'Current Status'}, inplace=True)
    merged2 = merged2[['Developer ID','Developer Name','Turing email','Current Status','task_link','resolution_duration']].copy()
    hist_df = merged2.groupby(['Turing email']).agg(Total_Historical_Tasks=('task_link', 'count'), Historical_Task_Hours=('resolution_duration','sum')).sort_values(by=['Total_Historical_Tasks'], ascending=[False]).reset_index(level=['Turing email'])
    hist_df['Historical_Task_Hours']=hist_df['Historical_Task_Hours']/60
    merged3 = summary_df.merge(hist_df, on='Turing email', how='left', indicator=True)
    merged3.loc[merged3['_merge'] == 'right_only', 'Developer Name'] = 'Internal/Anthropic Team'
    merged3.loc[merged3['_merge'] == 'right_only', 'Developer ID'] = 'NA'
    merged3.loc[merged3['_merge'] == 'right_only', 'Current Status'] = 'NA'
    merged3['Jibble Time']=pd.to_numeric(merged3['Jibble Time'], errors='coerce')
    merged3['Jibble Time'].fillna(0,inplace=True)
    merged3.loc[merged3['_merge'] == 'right_only', 'Jibble Time'] = 0  
    merged3.drop(columns=['_merge'],inplace=True)
    #print('number of rows with historical data ...',len(merged3))
    merged3['Total_Historical_Tasks'] = pd.to_numeric(merged3['Total_Historical_Tasks'], errors='coerce')
    merged3['Total_Historical_Tasks'].fillna(0,inplace=True)
    merged3['Historical_Task_Hours'] = pd.to_numeric(merged3['Historical_Task_Hours'], errors='coerce')
    merged3['Historical_Task_Hours'].fillna(0,inplace=True)
    merged3['Total_Tasks']=merged3['Total_Num_Tasks']+merged3['Total_Historical_Tasks']
    merged3['Total_Hours']=merged3['Total_Task_Hours']+merged3['Historical_Task_Hours']
    merged3['Total_Hours'] = pd.to_numeric(merged3['Total_Hours'], errors='coerce')
    divide = lambda x,y:1.0 if y == 0 else x / y
    # apply the lambda function to the dataframe
    merged3['Efficiency Ratio'] = merged3.apply(lambda row: divide(row['Total_Hours'], row['Jibble Time']), axis=1)
    #if len(merged3) >1:
    #    delete_df_from_sheet(service_account_path,report_sheet_id,"Jan") 
    create_sheet_from_df(service_account_path,report_sheet_id,"Jan_Summary",merged3)
    print('Updated summary in the doc now')


# Run job every hour 
def main():
    print("Executing the script to add monthly dev data....")
    init_report()
    get_totalTime()
    prepare_df()
       

if __name__ == "__main__":
    main()