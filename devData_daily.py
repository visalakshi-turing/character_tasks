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
#contributor_sheet_id = "14bcgtOEh5ClIYhH7rebN8c_AXEQ6ZcDVLRgwn3ZQW4E"
contributor_sheet_id = "1vJYFBDhk1OgQ2YzV4RtPbMkru-waxlMkBfR-xHtQeGA"
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
        if(sheet_id==time_sheet_id):
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
    sheet_range = f"{sheet_name}!A:DZ"  # Adjust the range A:Z as needed

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
            sheet_range = f"{sheet_title}!A:DZ"  # Adjust the range A:Z as needed
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
    sheet_range = f"{sheet_name}!A:DZ"  # Adjust the range A:Z as needed
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
    #reviews = reviews[reviews['Timestamp'].dt.year == 2024]
    reviews = reviews[reviews['Timestamp'].dt.month == 1]
    print("Number of total reviews completed in Januray,2024: ",len(reviews))
    print('Completed reading historical data...')
    print("Number of total historical records : ",len(history))
    history['completion_date']=pd.to_datetime(history['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    print("Number of total historical tasks after date formatting: ",len(history))
    #history = history[history['completion_date'].dt.year == 2024]
    history = history[history['completion_date'].dt.month == 1]
    print("Number of total historical tasks in Januray,2024: ",len(history))
    print('Completed reading member data...')
    print("Number of total members : ",len(members))
    print('Completed reading Jan,2024 timesheet entries')
    print('Number of Jan time sheet entries ..',len(time))

    #print('Duplicated rows ',len(current[current['is_duplicated']==1]))
    #current = current[~(current['is_duplicated']==1)] 
    #print("Number of Unique Auto Recommendation Rejections : ",len(current))
    

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
    print(total_duration)

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
    time = time.drop(columns=['SL NO','Opp Start date','End Date'])
    #print('created a temp df now ',len(temp))
    time.rename(columns={'Developer Names':'dev_name','Grand Total':'Jibble Time'}, inplace=True)
    time.dropna(subset=['dev_name'], inplace=True)
    time = time[~time['dev_name'].str.contains('Grand')]
    time = time[~time['dev_name'].str.contains('Trial')]
    time = time[~time['dev_name'].str.contains('Developer Names')]
    time = time[~time['dev_name'].str.contains('DevSuccess')]     
    #print('len of time sheets ',len(time))
    merged = time.merge(temp, on='dev_name', how='left', indicator=True)
    merged.rename(columns={'turing_email':'Turing email'}, inplace=True)
    merged.rename(columns={'dev_name':'Developer Name'}, inplace=True)
    merged.rename(columns={'Dev_ID':'Developer ID'}, inplace=True)
    merged.drop(columns=['_merge'], inplace=True)
    df1 = pd.melt(merged, id_vars=['Developer ID','Developer Name','Turing email','status','Jibble Time'],
                var_name='Date',value_name='Jibble_Daily_Effort',value_vars=['1/1', '1/2', '1/3', '1/4', '1/5', '1/6', '1/7',
       '1/8', '1/9', '1/10', '1/11', '1/12', '1/13', '1/14', '1/15', '1/16',
       '1/17', '1/18', '1/19', '1/20', '1/21', '1/22', '1/23', '1/24', '1/25',
       '1/26', '1/27', '1/28', '1/29', '1/30'])
    df1['Date'] = pd.to_datetime(df1['Date']+'/'+str(datetime.now().year),format='%m/%d/%Y',yearfirst=True)
    df1.set_index('Date', inplace=True)
    df1.sort_index(inplace=True)
    df1.reset_index(inplace=True) 
    #print('Now prepare task df.......................')
    task_df=tasks[['task_link','assigned_to_email','completion_status', 'duration_mins', 'completion_date']].copy()
    task_df['duration_mins'] = pd.to_numeric(task_df['duration_mins'], errors='coerce')
    #print(task_df['completion_date'].value_counts())
    #task_df['completion_date']=pd.to_datetime(task_df['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    task_df.rename(columns={'assigned_to_email':'Turing email'}, inplace=True)   
    df2=task_df.groupby(['Turing email','completion_date']).agg(Total_Num_Tasks=('task_link', 'count'),Total_Task_Hours=('duration_mins','sum')).sort_values(by=['completion_date'],ascending=[False]).reset_index(level=['Turing email','completion_date'])
    df2['Total_Task_Hours']=df2['Total_Task_Hours']/60
    df2['Total_Task_Hours'] = pd.to_numeric(df2['Total_Task_Hours'], errors='coerce')
    df2['Total_Task_Hours'].fillna(0,inplace=True)    
    df2['Total_Num_Tasks'] = pd.to_numeric(df2['Total_Num_Tasks'], errors='coerce')
    df2['Total_Num_Tasks'].fillna(0,inplace=True)
    df2[['Total_Task_Hours', 'Total_Num_Tasks']] = df2[['Total_Task_Hours', 'Total_Num_Tasks']].apply(lambda x: round(x, 2))    
    df2['completion_date'] = pd.to_datetime(df2['completion_date'],errors='coerce')
    df2['completion_date'] = df2['completion_date'].dt.strftime('%m/%-d') 
    df2.set_index('completion_date', inplace=True)
    df2.sort_index(inplace=True)
    df2.reset_index(inplace=True)
    history.rename(columns={'resolved_by_email':'Turing email'}, inplace=True)
    history.dropna(subset=['Turing email'], inplace=True)
    history['resolution_duration'] = pd.to_numeric(history['resolution_duration'], errors='coerce')
    #history['completion_date']=pd.to_datetime( history['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    #history['completion_date'] = history['completion_date'].dt.strftime('%m/%-d')
    df3 = history.groupby(['Turing email','completion_date']).agg(Total_Historical_Tasks=('task_link', 'count'), Historical_Task_Hours=('resolution_duration','sum')).sort_values(by=['Total_Historical_Tasks'], ascending=[False]).reset_index(level=['Turing email','completion_date'])
    df3['Historical_Task_Hours']=df3['Historical_Task_Hours']/60
    df3['Historical_Task_Hours'] = pd.to_numeric(df3['Historical_Task_Hours'], errors='coerce')
    df3['Historical_Task_Hours'].fillna(0,inplace=True)    
    df3['Total_Historical_Tasks'] = pd.to_numeric(df3['Total_Historical_Tasks'], errors='coerce')
    df3['Total_Historical_Tasks'].fillna(0,inplace=True)
    df3[['Historical_Task_Hours', 'Total_Historical_Tasks']] = df3[['Historical_Task_Hours', 'Total_Historical_Tasks']].apply(lambda x: round(x, 2))
    df3['completion_date'] = pd.to_datetime(df3['completion_date'],errors='coerce')
    df3['completion_date'] = df3['completion_date'].dt.strftime('%m/%-d') 
    df3.set_index('completion_date', inplace=True)
    df3.sort_index(inplace=True)
    df3.reset_index(inplace=True)
    test_df = df2.merge(df3,on=['completion_date','Turing email'],how='outer')
    #print('Before review : ',test_df.columns)
    #print('Rows : ',len(test_df))
    #Now add review details
    review_df=reviews[['Task Link [Google Colab]','Timestamp', 'Email Address', 'Code Quality', 'Language Quality', 'Author Email']].copy()
    #print('length of review df is ',len(review_df))
    #review_df['Timestamp']=review_df['Timestamp'].astype(str)
    review_df['Language Quality'] = pd.to_numeric(review_df['Language Quality'], errors='coerce')
    review_df['Code Quality'] = pd.to_numeric(review_df['Code Quality'], errors='coerce')
    review_df['Quality Score'] = review_df[['Language Quality','Code Quality']].mean(axis=1)
    review_df.rename(columns={'Author Email':'Turing email'}, inplace=True)
    review_df = review_df.groupby(['Turing email']).agg(Quality_Score=('Quality Score','mean'),Review_Tasks=('Task Link [Google Colab]','count')).sort_values(by=['Quality_Score'], ascending=[False]).reset_index(level=['Turing email'])
    test_df1 = test_df.merge(review_df,on='Turing email', how='left', indicator=True)
    test_df1.drop(columns=['_merge'],inplace=True)
    #print(test_df1.columns)
    test_df1.rename(columns={'completion_date':'Date'}, inplace=True)    
    test_df1['Date'] = pd.to_datetime(test_df1['Date']+'/'+str(datetime.now().year),format='%m/%d/%Y',yearfirst=True)    
    test_df1.set_index('Date', inplace=True)
    test_df1.sort_index(inplace=True)
    test_df1.reset_index(inplace=True) 
    merged_df = df1.merge(test_df1,on=['Turing email','Date'],how='outer')
    merged_df.set_index('Date', inplace=True)
    merged_df.sort_index(inplace=True)
    merged_df.reset_index(inplace=True)
    merged_df.rename(columns={'status':'Current_Developer_Status','Jibble Time':'Total_Jibble_Time',}, inplace=True)
    merged_df['Date']=merged_df['Date'].astype(str)
    merged_df['Total_Num_Tasks']=pd.to_numeric(merged_df['Total_Num_Tasks'], errors='coerce')
    merged_df['Total_Num_Tasks'].fillna(0,inplace=True)
    merged_df['Total_Historical_Tasks']=pd.to_numeric(merged_df['Total_Historical_Tasks'], errors='coerce')
    merged_df['Total_Historical_Tasks'].fillna(0,inplace=True)
    merged_df['Total_Task_Hours']=pd.to_numeric(merged_df['Total_Task_Hours'], errors='coerce')
    merged_df['Total_Task_Hours'].fillna(0,inplace=True)
    merged_df['Historical_Task_Hours']=pd.to_numeric(merged_df['Historical_Task_Hours'], errors='coerce')
    merged_df['Historical_Task_Hours'].fillna(0,inplace=True)    
    merged_df['Total_Jibble_Time']=pd.to_numeric(merged_df['Total_Jibble_Time'], errors='coerce')
    merged_df['Total_Jibble_Time'].fillna(0,inplace=True)
    merged_df['Jibble_Daily_Effort']=pd.to_numeric(merged_df['Jibble_Daily_Effort'], errors='coerce')
    merged_df['Jibble_Daily_Effort'].fillna(0,inplace=True)
    merged_df['Quality_Score']=pd.to_numeric(merged_df['Quality_Score'], errors='coerce')
    merged_df['Quality_Score'].fillna(0,inplace=True)
    merged_df['Review_Tasks']=pd.to_numeric(merged_df['Review_Tasks'], errors='coerce')
    merged_df['Review_Tasks'].fillna(0,inplace=True)
    merged_df['Current_Developer_Status'].fillna('NA',inplace=True)
    merged_df.loc[merged_df['Developer ID'].isna()  | merged_df['Developer Name'].isna(), 'Developer Name'] = 'Internal/Anthropic Team'
    merged_df['Developer ID'].fillna('NA',inplace=True)
    merged_df['Total_Hours'] = merged_df['Total_Task_Hours']+merged_df['Historical_Task_Hours']
    merged_df['Total_Hours']=pd.to_numeric(merged_df['Total_Hours'], errors='coerce')
    merged_df['Total_Hours'].fillna(0,inplace=True)
    merged_df['Total_Tasks'] = merged_df['Total_Num_Tasks']+merged_df['Total_Historical_Tasks']+merged_df['Review_Tasks']
    merged_df['Total_Tasks']=pd.to_numeric(merged_df['Total_Tasks'], errors='coerce')
    merged_df['Total_Tasks'].fillna(0,inplace=True)
    divide = lambda x,y:1.0 if y == 0 else x / y
    # apply the lambda function to the dataframe
    merged_df['Efficiency'] = merged_df.apply(lambda row: divide(row['Total_Hours'], row['Jibble_Daily_Effort']), axis=1)
    create_sheet_from_df(service_account_path,report_sheet_id,"Jan_Daily_Summary",merged_df)
    print("Updated report in the doc...")
 
def main():
    print("Executing the script to add daily dev data of the month....")
    init_report()
    get_totalTime()
    prepare_df()
    

if __name__ == "__main__":
    main()