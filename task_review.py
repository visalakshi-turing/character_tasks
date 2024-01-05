#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import numpy as np
#from src.sheets_utils import download_sheet_as_df
from src.sheets_utils import upload_df_to_sheet
from src.sheets_utils import create_new_sheet_from_df

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
delivery_sheet_id = "1eUif5I8xhHU8fY0X9v8r2JI9hWPh7Dq_9VXpSIHwww4"
delivered_batches=[]
review_batches=[]
num_task_sheets=0
num_delivered_batches=0
is_first_run=0
task_df=None
review_df=None
delivered_df=None
current=None
oldReviews=None
review_sheet=None
reviewSheet = "ReviewQueue"


def download_sheets_as_df(service_account_path, sheet_id, sheet_name):
    # Authenticate with the service account
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Construct the range to read
    sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed

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
        return pd.DataFrame([row + [None] * (len(values[0]) - len(row)) for row in values[1:]], columns=values[0])
    


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


def standardize_date(date):
    """
    Given a date string, standardize the date format to MM/DD/YYYY.
    """
    try:
        # Parse the date string into a datetime object
        standard_date = datetime.strptime(date, "%m/%d/%Y")
    except ValueError:
        try:
            # Attempt to parse other common formats here
            # Example: DD/MM/YYYY
            standard_date = datetime.strptime(date, "%m/%d/%Y")
        except ValueError:
            return ""

    # Format the datetime object into the desired string format
    return standard_date.strftime("%m/%d/%Y")


def get_diff_data(first,second):
    values = set(second['task_link'])
    #print("length of second df : ",len(values))
    first['match'] = first['task_link'].isin(values).astype(int)
    result = first[~(first['match']==1)]
    return result    



def init_review_tasks():
    global review_batches 
    global num_task_sheets
    global delivered_batches
    global task_df
    global review_df
    global delivered_df
    global oldReviews
    global current
    global review_sheet
    global is_first_run
    task_df = None
    delivered_df=None
    current=None
    oldReviews=None    
    tasks_val = 'Conversations_Batch_'
    delivery_val = 'Batch '
    reviews_val = 'Reviews'
    review_batches = get_sheets(
        service_account_path,
        tracking_sheet_id,
        tasks_val
    )
    #Check sheets available with tasks completed and not delivered
    num_task_sheets = len(review_batches)
    delivered_batches = get_sheets(service_account_path,delivery_sheet_id,delivery_val)
    num_delivered_batches = len(delivered_batches)
    if(is_first_run==0):
        current=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status'])
    for s in review_batches:        
        task_df = pd.concat([
            task_df,
        download_sheets_as_df(
            service_account_path,
            tracking_sheet_id,
            s
        )],ignore_index=True)
        #print("Number of tasks : ",len(task_df))
    for d in delivered_batches:          
        delivered_df = pd.concat([
            delivered_df,
        download_sheets_as_df(
            service_account_path,
            delivery_sheet_id,
            d
        )],ignore_index=True)    
    oldReviews = pd.concat([
                download_sheets_as_df(
                    service_account_path,
                    tracking_sheet_id,
                    'Reviews'
                )], ignore_index=True)
    current = download_sheets_as_df(
            service_account_path,
            tracking_sheet_id,
            reviewSheet)
    #Formulate review dataframe 
    delivered_df.drop_duplicates(subset='task_link',keep='first')
    #delivered_df.dropna(subset=["task_link"], axis=0,inplace=True)
    #task_df.dropna(subset=["task_link"], axis=0,inplace=True)    
    task_df = task_df[(task_df["completion_status"]=="Done")]    
    review_sheet=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score','status_score','age_score','count_score','quality_score'])
    review_df=pd.DataFrame(columns=task_df.columns)
    #print("Number of Completed tasks : ",len(task_df))
    #print(" Number of Delivered tasks : ",len(delivered_df))
    values = set(delivered_df['task_link'])
    task_df['match'] = task_df['task_link'].isin(values).astype(int) #Remove tasks delivered
    review_df1 = task_df[~(task_df['match']==1)]
    revList = set(oldReviews['Task Link [Google Colab]'])
    review_df1 = review_df1.assign(task_exists=0)
    review_df1.loc[review_df1['task_link'].isin(revList),'task_exists'] = 1
    review_df2 = review_df1[(review_df1['task_exists']==1)]    
    #review_df1['task_exists'] = review_df1['task_link'].isin(revList).astype(int) #Remove tasks in current Review sheet
    review_df = review_df1[~(review_df1['task_exists']==1)]
    review_df=review_df.assign(total_score=0,status_score=0,age_score=1,count_score=0,quality_score=0)
    


def add_df_to_review_sheet(rs):
    global is_first_run
    global reviewSheet
    global review_sheet
    #rs = rs.iloc[:, :-1]  #No need to store the score    
    rs['today'] = date.today().strftime('%m/%d/%Y')
    rs["completion_date"].replace("NaT","", inplace=True)
    #rs["completion_date"] = rs["completion_date"].replace('NaT', '01/01/2024')
    rs['today']=pd.to_datetime(rs['today'],format='%m/%d/%Y',errors='coerce',dayfirst=False)
    rs['completion_date']=pd.to_datetime(rs['completion_date'],format='%m/%d/%Y',errors='coerce',dayfirst=False)
    diff_days=(rs['today'] - rs['completion_date']).dt.days
    rs['status_score'] = np.where(rs['review_status']=='Reviewed',-sys.maxsize-1,0) 
    rs.loc[rs['quality_score']==0,'quality_score'] = 3
    rs.loc[:,'age_score'] *= (diff_days)*(-10)   
    rs['total_score'] = rs[['total_score', 'count_score', 'quality_score','age_score','status_score']].sum(axis=1,numeric_only=True)
    rs.dropna(subset=['task_link'], inplace=True)
    rs['completion_date'] = rs['completion_date'].astype(str)    
    final = rs[['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score']].copy()
    final=final.sort_values(by='total_score',ascending=True)
    final = final[['task_link','assigned_to_email','completion_date','reviewer_email','review_status']].copy()
    final["completion_date"].replace("NaT","", inplace=True)
    #print('Current length is : ',current.empty,len(current))
    if(is_first_run==0 and (current.empty)):
        #print('first_run not set yet..',len(rs))        
        create_new_sheet_from_df(service_account_path,tracking_sheet_id,"ReviewQueue",final)
        is_first_run=1           
    else:
        res = final[1:].copy(deep=True)
        res.columns = [''] * len(res.columns) 
        create_new_sheet_from_df(service_account_path,tracking_sheet_id,"ReviewQueue",res)
   


def add_tasks_to_review_queue():
    global review_df
    global oldReviews
    global current
    global review_sheet
    global is_first_run 
    try:
        init_review_tasks()
        review_df.loc[review_df['completion_date'].isnull(),'completion_date'] = '12/31/2023' 
        review_df_copy = review_df.copy()
        # Modify the column value using the .loc syntax and the function
        review_df_copy.loc[:,'completion_date'] = review_df_copy.loc[:,'completion_date'].apply(lambda x:standardize_date(x))
        review_df = review_df_copy.copy()
        print('Check Tasks for Review at :',datetime.now())          
        #Add max 30% of total tasks for review
        #print(current.head())
        if(current is not None and len(current.index)!=0):        
            review_df = get_diff_data(review_df,current)
        df=review_df.groupby(['assigned_to_email','completion_date']).agg({'task_link':'count'}).sort_values(by=['task_link','completion_date'],ascending=[False,True]).reset_index(level=['assigned_to_email','completion_date'])
        print('Number of unique members(tasks) worked on current batch tasks :',df['assigned_to_email'].nunique())
        members = df['assigned_to_email'].unique()
        total_tasks = len(review_df)
        print('Number of completed tasks in current batch ',total_tasks)
        review_tasks = int(round(total_tasks*0.3,2))+20
        print('Number of tasks to be considered for review ',review_tasks)
        count=0
        for person in members:  #select one task for each member            
            for index,row in review_df.iterrows():                              
                if row['assigned_to_email']==person and (person not in set(review_sheet['assigned_to_email']) and row['task_link'] not in set(review_sheet['task_link'])):        
                    newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':"",'review_status':"",
                              'status_score':row['status_score'],'count_score':row['count_score']+1,'quality_score':row['quality_score'],'age_score':row['age_score']}
                    review_sheet.loc[len(review_sheet.index)] = newRow                    
                    count=count+1
                    break
            if(count>=review_tasks):
                break
        #print('Added ',count," tasks for review to consider all contributors")
        if(count>=review_tasks):
            add_df_to_review_sheet(review_sheet)
            return
        else:
            oldReviews.dropna(subset=["Email Address"], axis=0,inplace=True)
            oldReviews['Code Quality'] = oldReviews['Code Quality'].astype(int)
            oldReviews['Language Quality'] = oldReviews['Language Quality'].astype(int)            
            oldReviews['time'] = pd.to_datetime(oldReviews['Timestamp'])
            now = pd.Timestamp.now()
            week_ago = now - pd.Timedelta(weeks=1)
            oldReviews_week = oldReviews.loc[oldReviews['time'] >= week_ago] 
            oldReviews['quality_score'] = oldReviews[['Language Quality','Code Quality']].mean(axis=1)
            oldReviews = oldReviews.sort_values(by=['quality_score','time'],ascending=[True,True])
            oldReviews = oldReviews[oldReviews['quality_score']<4.0]
            old_members = oldReviews_week['Email Address'].unique()       
            old_list = list(set(members).intersection(old_members)) #old contributors in current task list
            print('Number of contributors(tasks) with low code quality :',len(old_list))
            old=0
            for person in old_list:  #select one task for each member
                for index1,row1 in review_df.iterrows():
                    if row1['assigned_to_email']==person and (row1['task_link'] not in set(review_sheet['task_link'])):
                        #print('Adding old review data :',row1['task_link'])
                        newRow = {'task_link':row1['task_link'],'assigned_to_email':row1['assigned_to_email'],'completion_date':row1['completion_date'],'reviewer_email':"",'review_status':"",
                        'status_score':row1['status_score'],'count_score':row1['count_score']+1,'quality_score':row1['quality_score'],'age_score':row1['age_score']}
                        review_sheet.loc[len(review_sheet.index)] = newRow
                        count=count+1
                        old=old+1
                        break
                if(count>=review_tasks):
                    break
            print('Added old review low quality member tasks to the sheet ',old+1)
            if(count>=review_tasks):
                add_df_to_review_sheet(review_sheet)
                return
            else:
                review_df['count_score'] = review_df.groupby(['assigned_to_email','completion_date'])['task_link'].transform('count')
                #df=review_df.groupby(['assigned_to_email','completion_date']).agg({"countScore":{"task_link":"count"}}).sort_values(by=['countScore'],ascending=[False]).reset_index(level=['assigned_to_email','completion_date'])
                review_df=review_df[(review_df['count_score']>=4)]
                mem = review_df['assigned_to_email'].unique()                
                print('Number of contributors with high number of tasks :',len(mem))
                high=0
                for person in mem:
                    for index,row in review_df.iterrows():
                        if(row['assigned_to_email']==person and row['task_link'] not in set(review_sheet['task_link'])):
                            newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':"",'review_status':"",
                            'status_score':row['status_score'],'count_score':row['count_score']+1,'quality_score':row['quality_score'],'age_score':row['age_score']}
                            review_sheet.loc[len(review_sheet.index)] = newRow    
                            count=count+1
                            high=high+1
                            break                        
                    if(count>=review_tasks):
                        break
                #Update review sheet and complete current iteration now                
                print('Members with high number of tasks considered for review now : ',high)           
                add_df_to_review_sheet(review_sheet)
                return     
    except HttpError as e:
        error_reason = json.loads(e.content)['error']
        error_details = e.error_details 
        print(error_reason)
        print(error_details)
    except Exception as e:
        error_reason = json.loads(e.content)['error']
        error_details = e.error_details 
        print(error_reason)
        print(error_details)        


#init_review_tasks()

#add_tasks_to_review_queue()

# Run job every hour 
def main():
    print("Execute the script to add completed tasks for review....")
    schedule.every(1).minutes.do(add_tasks_to_review_queue)
    while True:
        schedule.run_pending()       
        time.sleep(1*60*60) #Check hourly updates


if __name__ == "__main__":
    main()