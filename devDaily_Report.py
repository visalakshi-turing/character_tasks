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
import numpy as np
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
#contributor_sheet_id = "1vJYFBDhk1OgQ2YzV4RtPbMkru-waxlMkBfR-xHtQeGA"
#time_sheet_id = "1ZJq9vqpjZFz1SDZMYSec2uXmD9gSnA994jmVQqU5IsY"
time_sheet_id = "1CXaxu7VYbztPzsZJLTzbAajevBKSFpL_h--9wO0pEA0"
report_sheet_id = "1iP2sVYU_xiGGAp2hmbn_VpakhQpl3cH87qZY86sRUQI"
data_sheet_id = "1v_O33STdi_h7taPd3MkD0fiqRx7rqr_aAQWGnlOfr_w"

tasks=None
members=None
time=None
reviews=None
history=None
df=None
summary_df=None
final=None
data=None
total_records=0
total_num_unique_actions=0
total_num_unique_domains=0




def download_sheets_as_df(service_account_path, sheet_id, sheet_name):
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    # Construct the range to read
    var = 'Conversations'
    if var in sheet_name:
        sheet_range = f"{sheet_name}!A:L"  # Adjust the range A:Z as needed
    elif sheet_id==time_sheet_id:
        sheet_range = f"'{sheet_name}'!A:I"
    else:
        sheet_range = f"'{sheet_name}'!A:Z"  # Adjust the range A:Z as needed

    # Make the API request
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=sheet_range).execute()
    values = result.get('values', [])
    # Convert to a DataFrame
    if not values:
        #print("No data found.")
        return pd.DataFrame()
    else:
        return pd.DataFrame.from_records(values[1:],columns=values[0])
        
        
    '''    
    if(sheet_id==time_sheet_id):
        new_values = values[1:]
        new_df = pd.DataFrame([row + [None] * (len(values[1]) - len(row)) for row in new_values], columns=values[1])
        return new_df
    else:
        return pd.DataFrame([row + [None] * (len(values[0]) - len(row)) for row in values[1:]], columns=values[0])
    '''


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
    global data
    tasks=None
    members=None
    time=None
    reviews=None
    history=None
    data=None
    tasks_val = 'Conversations_Batch_'
    time_val = '_Timesheets'
    historical_sheet='historical__personal_corrections'
    #Check sheets available with tasks completed and not delivered
    task_cols = ['task_link','metadata__topic','assigned_to_email','completion_status','modified_question?','duration_mins','completion_date','comments','metadata__type','metadata__target_length','review_status','reviewer_email','Start Time','End Time']
    #review_sheet=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score','status_score','age_score','count_score','quality_score'])
    tasks=pd.DataFrame(columns=task_cols)
    summary_df=pd.DataFrame(columns=['Developer ID','Developer Name','Turing email','Current Status','Jibble Time','Total_Num_Tasks','Total_Task_Hours'])
    task_batches = get_sheets(service_account_path,tracking_sheet_id,tasks_val)
    time_sheets = get_sheets(service_account_path,time_sheet_id,time_val)
    #Check sheets available with tasks completed and not delivered
    num_task_sheets = len(task_batches)
    print("Total task sheets to read :",num_task_sheets)
    num_time_sheets = len(time_sheets)
    print('Number of time sheets ',num_time_sheets)
    for s in task_batches:      
        tasks = pd.concat([
            tasks,
        download_sheets_as_df(
            service_account_path,
            tracking_sheet_id,
            s
        )],ignore_index=True)
        #print("Number of tasks : ",len(tasks))
    
    #print("Read Member data from source sheet")
    members = pd.concat([
            download_sheets_as_df(
                service_account_path,
                contributor_sheet_id,
                'Character.ai_Devs'
            )], ignore_index=True)

    for s in time_sheets:      
        time = pd.concat([
            time,
        download_sheets_as_df(
            service_account_path,
            time_sheet_id,
            s
        )],ignore_index=True)    
    #print('Completed reading  time sheet data now..')
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

    data = pd.concat([
                download_sheets_as_df(
                    service_account_path,
                    data_sheet_id,
                    "v1 (Jan 25)"
                )], ignore_index=True)

    #duplicate_rows = current.duplicated(subset=['source','job_id', 'developer_id'])
    #current['is_duplicated']=duplicate_rows
    print('Completed reading task data...')
    print("Number of total tasks : ",len(tasks))
    tasks['completion_date']=pd.to_datetime(tasks['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    #tasks.dropna(subset=['completion_date'], inplace=True)
    print("Number of total tasks after date formatting: ",len(tasks))
    tasks = tasks[tasks['completion_date'].dt.year == 2024]
    #tasks = tasks[tasks['completion_date'].dt.month == 1] #Let us use this later to get monthly info
    print("Number of total tasks completed in 2024: ",len(tasks))
    print('Completed reading reviews data...')
    print("Number of total reviews : ",len(reviews))
    reviews['Timestamp']=pd.to_datetime(reviews['Timestamp'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    #tasks.dropna(subset=['completion_date'], inplace=True)
    print("Number of total reviews after date formatting: ",len(reviews))
    reviews = reviews[reviews['Timestamp'].dt.year == 2024]
    #reviews = reviews[reviews['Timestamp'].dt.month == 1]
    print("Number of total reviews completed in 2024: ",len(reviews))
    print('Completed reading historical data...')
    print("Number of total historical records : ",len(history))
    history['completion_date']=pd.to_datetime(history['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False)
    print("Number of total historical tasks after date formatting: ",len(history))
    history = history[history['completion_date'].dt.year == 2024]
    print("Number of total historical tasks : ",len(history))
    print('Completed reading member data...')
    print("Number of total members : ",len(members))
    print('Completed reading 2024 timesheet entries')
    print('Number of time sheet entries ..',len(time))
    print('Number of data sheet entries ',len(data))
    
    

def get_totalTime():
    global tasks
    global df 
    global data
    global total_num_unique_actions
    global total_num_unique_domains
    tasks['duration_mins'] = pd.to_numeric(tasks['duration_mins'], errors='coerce')
    # Drop rows with NaN values
    tasks.dropna(subset=['duration_mins'], inplace=True)
    # Compute the sum of the 'duration_mins' column
    total_duration = tasks['duration_mins'].sum()
    #df = tasks.groupby(['assigned_to_email']).agg(task_count=('task_link', 'count'), task_duration_mins=('duration_mins','sum')).sort_values(by=['task_count'], ascending=[False]).reset_index(level=['assigned_to_email'])
    #print('Number of unique members(df) worked on current batch tasks :',df['assigned_to_email'].nunique()) 
    #print(data.columns)
    data['action'].fillna('NA',inplace=True)
    data['domain__top_level'].fillna('NA',inplace=True)
    total_num_unique_actions = data['action'].nunique()
    total_num_unique_domains = data['domain__top_level'].nunique()
    df = tasks.merge(data,on="task_link",how="left")
    df=df.drop(columns=['batch_id', 'domain__sub_level','use_case__summary'])
    #print('Merged with task data ...' )
    #print(df.columns)



def prepare_df():
    global tasks
    global members
    global reviews
    global time
    global history
    #global summary_df
    global df
    global total_num_unique_actions
    global total_num_unique_domains
    temp = members[['Dev_ID','dev_name','status','turing_email']].copy()
    temp = temp[~temp['dev_name'].str.contains('#N/A')]    
    temp['turing_email'].fillna('NA',inplace=True)
    time = time.drop(columns=['Day','Member Code','Manager(s)','Work Schedule','Worked Hours'])
    time.dropna(subset=['Group'], inplace=True)
    time = time[time['Group'].str.contains('Character.ai')]
    #print('created a temp df now ',len(time))
    time.rename(columns={'Full Name':'dev_name','Tracked Hours':'Jibble Time'}, inplace=True)
    time.dropna(subset=['dev_name'], inplace=True)    
    #print('len of time sheets ',len(time))
    merged = time.merge(temp, on='dev_name', how='left', indicator=True)
    merged.rename(columns={'turing_email':'Turing email'}, inplace=True)
    merged.rename(columns={'dev_name':'Developer Name'}, inplace=True)
    merged.rename(columns={'Dev_ID':'Developer ID'}, inplace=True)
    merged.drop(columns=['_merge'], inplace=True)
    merged['Date'] = pd.to_datetime(merged['Date'],errors='coerce',format='%m/%d/%Y',yearfirst=False,dayfirst=False)
    merged.set_index('Date', inplace=True)
    merged.sort_index(inplace=True)
    merged.reset_index(inplace=True)    
    task_df=df[['task_link','assigned_to_email','completion_status', 'duration_mins','number_of_turns','action','domain__top_level','completion_date']].copy()
    task_df.rename(columns={'completion_date':'Date'}, inplace=True)
    task_df['duration_mins'] = pd.to_numeric(task_df['duration_mins'], errors='coerce')
    task_df['number_of_turns'] = pd.to_numeric(task_df['number_of_turns'], errors='coerce')
    task_df.rename(columns={'assigned_to_email':'Turing email'}, inplace=True)
    #print('task_df columns ',task_df.columns)
    df1=task_df.groupby(['Turing email','Date']).agg(Num_Tasks=('task_link', 'count'),Task_Hours=('duration_mins','sum'),Num_Turns=('number_of_turns','sum'),Num_Unique_Actions=('action','nunique'),Num_Unique_Domains=('domain__top_level','nunique')).reset_index(level=['Turing email','Date'])
    df1['Max_Num_Unique_Actions']=df1.groupby(['Turing email'])['Num_Unique_Actions'].transform(max)
    df1['Max_Num_Unique_Domains']=df1.groupby(['Turing email'])['Num_Unique_Domains'].transform(max)
    df1['Total_Num_Unique_Actions']=total_num_unique_actions
    df1['Total_Num_Unique_Domains']=total_num_unique_domains
    df1['Task_Hours'] = pd.to_numeric(df1['Task_Hours'], errors='coerce')
    df1['Task_Hours'].fillna(0,inplace=True)
    df1['Task_Hours']=df1['Task_Hours']/60
    df1['Num_Turns'] = pd.to_numeric(df1['Num_Turns'], errors='coerce')
    df1['Num_Turns'].fillna(0,inplace=True)
    df1['Num_Tasks'] = pd.to_numeric(df1['Num_Tasks'], errors='coerce')
    df1['Num_Tasks'].fillna(0,inplace=True)
    df1['Num_Unique_Actions'] = pd.to_numeric(df1['Num_Unique_Actions'], errors='coerce')
    df1['Num_Unique_Actions'].fillna(0,inplace=True)
    df1['Num_Unique_Domains'] = pd.to_numeric(df1['Num_Unique_Domains'], errors='coerce')
    df1['Num_Unique_Domains'].fillna(0,inplace=True)
    df1['Max_Num_Unique_Actions'] = pd.to_numeric(df1['Max_Num_Unique_Actions'], errors='coerce')
    df1['Max_Num_Unique_Domains'] = pd.to_numeric(df1['Max_Num_Unique_Domains'], errors='coerce')
    df1['Max_Num_Unique_Actions'].fillna(0,inplace=True)
    df1['Max_Num_Unique_Domains'].fillna(0,inplace=True)
    df1['Total_Num_Unique_Actions'].fillna(0,inplace=True)
    df1['Total_Num_Unique_Domains'].fillna(0,inplace=True)
    df1['Total_Num_Unique_Actions'] = df1['Total_Num_Unique_Actions'].astype(int)
    df1['Total_Num_Unique_Domains'] = df1['Total_Num_Unique_Domains'].astype(int)
    df1[['Task_Hours','Num_Tasks','Num_Turns', 'Num_Unique_Actions','Num_Unique_Domains']] = df1[['Task_Hours','Num_Tasks','Num_Turns','Num_Unique_Actions','Num_Unique_Domains']].apply(lambda x: round(x, 2))    
    df1['Date'] = pd.to_datetime(df1['Date'],errors='coerce',format='%m/%d/%Y',yearfirst=False,dayfirst=False)
    df1.set_index('Date', inplace=True)
    df1.sort_index(inplace=True)
    df1.reset_index(inplace=True)    
    #consider historical task contributions
    history = history[['task_link','resolved_by_email','completion_date','resolution_duration']].copy()
    history.rename(columns={'resolved_by_email':'Turing email','completion_date':'Date'}, inplace=True)
    history.dropna(subset=['Turing email'], inplace=True)
    history['resolution_duration'] = pd.to_numeric(history['resolution_duration'], errors='coerce')
    history['Date']=pd.to_datetime( history['Date'], infer_datetime_format=True,format='%m/%d/%Y',errors='coerce',dayfirst=False,yearfirst=False)
    df2 = history.groupby(['Turing email','Date']).agg(Num_Historical_Tasks=('task_link', 'count'), Historical_Task_Hours=('resolution_duration','sum')).sort_values(by=['Num_Historical_Tasks'], ascending=[False]).reset_index(level=['Turing email','Date'])
    df2['Num_Historical_Tasks'] = pd.to_numeric(df2['Num_Historical_Tasks'], errors='coerce')
    df2['Num_Historical_Tasks'].fillna(0,inplace=True)
    df2['Historical_Task_Hours'] = pd.to_numeric(df2['Historical_Task_Hours'], errors='coerce')
    df2['Historical_Task_Hours'].fillna(0,inplace=True) 
    df2['Historical_Task_Hours']=df2['Historical_Task_Hours']/60   
    df2[['Historical_Task_Hours', 'Num_Historical_Tasks']] = df2[['Historical_Task_Hours', 'Num_Historical_Tasks']].apply(lambda x: round(x, 2))
    df2['Date'] = pd.to_datetime(df2['Date'],errors='coerce',format='%m/%d/%Y',yearfirst=False,dayfirst=False)
    df2.set_index('Date', inplace=True)
    df2.sort_index(inplace=True)
    df2.reset_index(inplace=True)
    print(df2.columns)
    print(df2['Num_Historical_Tasks'].value_counts())
    df3 = df1.merge(df2,on=['Date','Turing email'],how='outer')
    print(df3.columns)

    #Quality score computation
    review_df=reviews[['Task Link [Google Colab]','Timestamp', 'Code Quality', 'Language Quality', 'Author Email']].copy()
    review_df.rename(columns={'Author Email':'Turing email','Timestamp':'Date','Task Link [Google Colab]':'task_link'}, inplace=True) 
    review_df['Date'] = review_df['Date'].dt.strftime('%m/%d/%Y')
    review_df['Date'] = pd.to_datetime(review_df['Date'],format='%m/%d/%Y',yearfirst=False,dayfirst=False,errors='coerce')
    review_df['Language Quality'] = pd.to_numeric(review_df['Language Quality'], errors='coerce')
    review_df['Code Quality'] = pd.to_numeric(review_df['Code Quality'], errors='coerce')
    review_df['QScore'] = review_df[['Language Quality','Code Quality']].mean(axis=1)       
    review_df = review_df.groupby(['Turing email','Date']).agg(Quality_Score=('QScore','mean')).sort_values(by=['Quality_Score'], ascending=[False]).reset_index(level=['Turing email','Date'])
    test_df1 = df3.merge(review_df,on=['Date','Turing email'], how='outer', indicator=True)
    test_df1.drop(columns=['_merge'],inplace=True)   
    test_df1['Date'] = pd.to_datetime(test_df1['Date'],format='%m/%d/%Y',yearfirst=False,dayfirst=False)    
    test_df1.set_index('Date', inplace=True)
    test_df1.sort_index(inplace=True)
    test_df1.reset_index(inplace=True)    
    #Get review tasks
    review_df1=reviews[['Task Link [Google Colab]','Timestamp', 'Email Address']].copy()
    review_df1.rename(columns={'Task Link [Google Colab]':'task_link','Timestamp':'Date','Email Address':'Turing email'}, inplace=True)
    review_df1['Date'] = review_df1['Date'].dt.strftime('%m/%d/%Y')
    review_df1['Date'] = pd.to_datetime(review_df1['Date'],format='%m/%d/%Y',errors='coerce',yearfirst=False,dayfirst=False)
    review_df2 = review_df1.groupby(['Turing email','Date']).agg(Num_Review_Tasks=('task_link','count')).sort_values(by=['Num_Review_Tasks'], ascending=[False]).reset_index(level=['Turing email','Date'])
    review_df2['Num_Review_Tasks']=pd.to_numeric(review_df2['Num_Review_Tasks'],errors='coerce')
    review_df2['Num_Review_Tasks'].fillna(0,inplace=True)    
    review_df2['Date'] = pd.to_datetime(review_df2['Date'],format='%m/%d/%Y',errors='coerce',yearfirst=False,dayfirst=False)    
    review_df2.set_index('Date', inplace=True)
    review_df2.sort_index(inplace=True)
    review_df2.reset_index(inplace=True)
    test_df2 = test_df1.merge(review_df2,on=['Date','Turing email'], how='outer', indicator=True)
    test_df2.drop(columns=['_merge'],inplace=True)  
    merged_df = merged.merge(test_df2,on=['Turing email','Date'],how='outer')
    print(merged_df.columns)    
    merged_df.rename(columns={'status':'Current_Developer_Status','Jibble Time':'Total_Jibble_Time',}, inplace=True)
    merged_df['Turing email'].fillna('NA',inplace=True)
    merged_df['Num_Unique_Actions'].fillna(0,inplace=True)
    merged_df['Num_Unique_Domains'].fillna(0,inplace=True)
    merged_df['Max_Num_Unique_Actions'].fillna(0,inplace=True)
    merged_df['Max_Num_Unique_Domains'].fillna(0,inplace=True)
    merged_df['Total_Num_Unique_Actions'].fillna(0,inplace=True)
    merged_df['Total_Num_Unique_Domains'].fillna(0,inplace=True)
    merged_df['Date']=merged_df['Date'].astype(str)
    merged_df['Num_Turns'] = pd.to_numeric(merged_df['Num_Turns'], errors='coerce')
    merged_df['Num_Turns'].fillna(0,inplace=True)  
    merged_df['Num_Tasks']=pd.to_numeric(merged_df['Num_Tasks'], errors='coerce')
    merged_df['Num_Tasks'].fillna(0,inplace=True)
    merged_df['Num_Historical_Tasks']=pd.to_numeric(merged_df['Num_Historical_Tasks'], errors='coerce')
    merged_df['Num_Historical_Tasks'].fillna(0,inplace=True)
    merged_df['Task_Hours']=pd.to_numeric(merged_df['Task_Hours'], errors='coerce')
    merged_df['Task_Hours'].fillna(0,inplace=True)
    merged_df['Historical_Task_Hours']=pd.to_numeric(merged_df['Historical_Task_Hours'], errors='coerce')
    merged_df['Historical_Task_Hours'].fillna(0,inplace=True)      
    merged_df['Total_Jibble_Time']=pd.to_numeric(merged_df['Total_Jibble_Time'], errors='coerce')
    merged_df['Total_Jibble_Time'].fillna(0,inplace=True)
    merged_df['Quality_Score']=pd.to_numeric(merged_df['Quality_Score'], errors='coerce')
    merged_df['Quality_Score'].fillna(0,inplace=True)
    merged_df['Num_Review_Tasks']=pd.to_numeric(merged_df['Num_Review_Tasks'], errors='coerce')
    merged_df['Num_Review_Tasks'].fillna(0,inplace=True)   
    merged_df['Review_Task_Hours']=merged_df['Num_Review_Tasks']*30
    merged_df['Review_Task_Hours']=pd.to_numeric(merged_df['Review_Task_Hours'], errors='coerce')
    merged_df['Review_Task_Hours'].fillna(0,inplace=True)
    merged_df['Review_Task_Hours']=merged_df['Review_Task_Hours']/60    
    merged_df['Current_Developer_Status'].fillna('NA',inplace=True)
    merged_df['Group'].fillna('NA',inplace=True)
    merged_df.loc[merged_df['Turing email'].isna(), 'Turing email'] = 'NA'
    merged_df.loc[merged_df['Current_Developer_Status']=='Offboarded','Turing email'] = 'NA'
    merged_df.loc[merged_df['Developer ID'].isna()  | merged_df['Developer Name'].isna(), 'Developer Name'] = 'Internal/Anthropic Team'
    merged_df['Developer ID'].fillna('NA',inplace=True)
    merged_df['Total_Hours'] = merged_df['Task_Hours']+merged_df['Historical_Task_Hours']+merged_df['Review_Task_Hours']
    merged_df['Total_Hours']=pd.to_numeric(merged_df['Total_Hours'], errors='coerce')
    merged_df['Total_Hours'].fillna(0,inplace=True)
    merged_df['Total_Tasks'] = merged_df['Num_Tasks']+merged_df['Num_Historical_Tasks']+merged_df['Num_Review_Tasks']
    merged_df['Total_Tasks']=pd.to_numeric(merged_df['Total_Tasks'], errors='coerce')
    merged_df['Total_Tasks'].fillna(0,inplace=True)
    #divide = lambda x,y:0.0 if y == 0 else x / y
   
    # apply the lambda function to the dataframe
    merged_df['Efficiency'] = merged_df.apply(lambda row: np.divide(row['Total_Hours'], row['Total_Jibble_Time'])if row['Total_Jibble_Time'] != 0 else 0, axis=1)
    merged_df['Action_Diversity'] = merged_df.apply(lambda row: np.divide(row['Max_Num_Unique_Actions'],row['Total_Num_Unique_Actions'])if row['Total_Num_Unique_Actions']!=0 else 0,axis=1)
    merged_df['Domain_Diversity'] = merged_df.apply(lambda row: np.divide(row['Max_Num_Unique_Domains'],row['Total_Num_Unique_Domains'])if row['Total_Num_Unique_Domains']!=0 else 0,axis=1)
    merged_df[['Efficiency','Quality_Score','Action_Diversity','Domain_Diversity','Max_Num_Unique_Actions','Max_Num_Unique_Domains']] = merged_df[['Efficiency','Quality_Score','Action_Diversity','Domain_Diversity','Max_Num_Unique_Actions', 'Max_Num_Unique_Domains']].apply(lambda x: round(x, 2))
    final = merged_df.loc[:,['Date', 'Developer ID','Developer Name','Turing email','Group','Current_Developer_Status','Num_Turns','Num_Unique_Actions','Num_Unique_Domains','Max_Num_Unique_Actions','Max_Num_Unique_Domains','Total_Num_Unique_Actions','Total_Num_Unique_Domains', 'Num_Tasks', 'Task_Hours', 'Num_Historical_Tasks', 'Historical_Task_Hours',
            'Num_Review_Tasks','Review_Task_Hours','Total_Tasks', 'Total_Hours','Total_Jibble_Time','Quality_Score','Efficiency','Action_Diversity','Domain_Diversity']]
    #print(final.columns)
    #print(len(final1))
    #final1 = final.drop_duplicates()
    create_sheet_from_df(service_account_path,report_sheet_id,"Daily_Summary_test",final)
    print("Updated report in the doc...")
   

    
   


    
 


 
def main():
    print("Executing the script to add daily dev data of the month....")
    init_report()
    get_totalTime()
    prepare_df()
    

if __name__ == "__main__":
    main()