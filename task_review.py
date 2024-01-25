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
total_tasks=0
review_sample=0
tasks_pending=0
members=[]


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
    sheet_range = f"{sheet_name}!A:Z"  # Adjust the range A:Z as needed
    #values = [df.columns.tolist()] + df.values.tolist()
    # Make the API request
    #body1 = {'values': values}
    result = service.spreadsheets().values().clear(
                spreadsheetId=tracking_sheet_id,
                range=sheet_range,
                body={}
            ).execute()



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
    global total_tasks
    global review_sample
    global members
    global tasks_pending
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
        current=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score','status_score','age_score','count_score','quality_score'])
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
        #print("Number of tasks : ",len(delivered_df))   
    oldReviews = pd.concat([
                download_sheets_as_df(
                    service_account_path,
                    tracking_sheet_id,
                    'Reviews'
                )], ignore_index=True)
    current = download_sheets_as_df(
            service_account_path,
            tracking_sheet_id,
            "ReviewQueue")
    #Formulate review dataframe 
    delivered_df.drop_duplicates(subset='task_link',keep='first')
    #delivered_df.dropna(subset=["task_link"], axis=0,inplace=True)
    task_df.dropna(subset=["task_link"], axis=0,inplace=True)
    task_df.dropna(subset=["assigned_to_email"], axis=0,inplace=True)    
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
    review_df['completion_date']=pd.to_datetime(review_df['completion_date'], infer_datetime_format=True,errors='coerce',dayfirst=False,yearfirst=False).fillna('2024-01-01')
    df=review_df.groupby(['assigned_to_email','completion_date']).agg({'task_link':'count'}).sort_values(by=['completion_date','task_link'],ascending=[False,False]).reset_index(level=['assigned_to_email','completion_date'])
    print('Number of unique members(tasks) worked on current batch tasks :',df['assigned_to_email'].nunique())
    members = df['assigned_to_email'].unique()
    total_tasks = len(review_df)
    print('Number of completed tasks in current batch ',total_tasks)
    review_sample = int(round(total_tasks*0.3,2))
    print('Number of tasks to be considered for review ',review_sample)
    print('Number of tasks pending during initializing : ',tasks_pending)
    review_df.sort_values(by='completion_date',inplace=True,ascending=False)
    #print(review_df['completion_date'].head(10))
    current = current.assign(total_score=0,status_score=0,age_score=1,count_score=0,quality_score=0)
    if(current is not None and len(current.index)!=0):
        current = current.replace('', np.nan).dropna(how='all') #Drop empty rows
        if(tasks_pending==0):
            mask=current['review_status'].isin(["Done","In Progress","Reviewed","Reviewing","Completed","In progress"])    
            latest = current[mask]
            pending = current[~(mask)]
            print('Number of reviews completed: ',len(latest),' and review pending -  ',len(current)-len(latest))
            tasks_pending = len(pending)
            #print('pending df length ',tasks_pending)
            if(tasks_pending>0 and tasks_pending<=review_sample):
                tasks_pending=review_sample-tasks_pending
            if(tasks_pending==0):
                tasks_pending=review_sample
            #print('In if pending : ',tasks_pending)
        review_sheet = review_sheet.drop(review_sheet.index)
        review_sheet.assign(total_score=0,status_score=0,age_score=1,count_score=0,quality_score=0)
    else:
        tasks_pending=review_sample
        #print('In else : ',tasks_pending)
        #print('Now tasks to be added from completed tasks : ',tasks_pending)

def get_ageScore(x,y):
    return x*y*(-10)


def get_daysDiff(date_col):
    global review_sheet
    review_sheet[date_col].fillna('01/01/2024',inplace=True)
    #print('Null values in that col after replace.. ',review_sheet[date_col].isna().sum())
    # Convert the date column to pandas datetime format
    review_sheet[date_col] = pd.to_datetime(review_sheet[date_col],errors='coerce',infer_datetime_format=True)
    # Get the current date
    todayDate = datetime.today().date()
    todayDate = pd.Timestamp(todayDate)   
    #todayDate = pd.to_datetime(todayDate,format='%m/%d/%Y',errors='coerce',infer_datetime_format=True,dayfirst=False,yearfirst=False)
    # Subtract the date column from the current date and get the days attribute
    review_sheet['days_diff'] = (todayDate - review_sheet[date_col]).dt.days
    # Return the modified dataframe
    return review_sheet



def add_df_to_review_sheet():
    global is_first_run
    global current
    global review_sheet
    final=None
    review_sheet = get_daysDiff('completion_date')
    review_sheet['age_score'] = review_sheet['days_diff']*(-10)
    #replace future dates with current date
    review_sheet.loc[review_sheet['days_diff']<0,'completion_date']=pd.Timestamp(datetime.today().date())
    review_sheet.dropna(subset=['task_link','assigned_to_email'], inplace=True)
    review_sheet.loc[review_sheet['quality_score']==0,'quality_score'] = 3 
    review_sheet['total_score'] = review_sheet[['total_score', 'count_score', 'quality_score','age_score','status_score']].sum(axis=1,numeric_only=True)
    final=pd.DataFrame(columns=['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score'])
    final = review_sheet[['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score']].copy()
    final=final.sort_values(by='total_score',ascending=False)
    final = final[['task_link','assigned_to_email','completion_date','reviewer_email','review_status']].copy()
    final["completion_date"].fillna('01/01/2024',inplace=True)
    final['completion_date'] = final['completion_date'].astype(str)
    if(is_first_run==0 and (current.empty)):       
        create_sheet_from_df(service_account_path,tracking_sheet_id,"ReviewQueue",final)
        is_first_run=1           
    else:
        #res = final[1:].copy(deep=True)
        #res.columns = [''] * len(res.columns)
        #res = current[['task_link','assigned_to_email','completion_date','reviewer_email','review_status']].copy()
        #Drop existing frame and add updated frame
        delete_df_from_sheet(service_account_path,tracking_sheet_id,"ReviewQueue")
        create_sheet_from_df(service_account_path,tracking_sheet_id,"ReviewQueue",final)   


def add_tasks_to_review_queue():
    global review_df
    global oldReviews
    global current
    global review_sheet
    global is_first_run 
    global tasks_pending
    try:
        init_review_tasks()
        count=0
        print('Check Tasks for Review at :',datetime.now())          
        #Add current data to review frame
        #['task_link','assigned_to_email','completion_date','reviewer_email','review_status','total_score','status_score','age_score','count_score','quality_score'])
        if(current is not None and len(current.index)!=0):        
            mask1 = current['review_status'].isin(["Done","In Progress","Reviewed","Reviewing","Completed","In progress"])
            current_completed = current[(mask1)]
            current_pending = current[~(mask1)]
            #Add current to review sheet
            for index,row in current_completed.iterrows():                   
                newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':row['reviewer_email'],'review_status':row['review_status'],
                        'total_score':0,'status_score':-10000,'age_score':1,'count_score':1,'quality_score':0}
                if(row['task_link'] not in set(review_sheet['task_link'])):
                    review_sheet.loc[len(review_sheet.index)] = newRow                    
                    count=count+1
            for index,row in current_pending.iterrows():                   
                newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':"",'review_status':"",
                'total_score':0,'status_score':0,'age_score':1,'count_score':1,'quality_score':0}
                if(row['task_link'] not in set(review_sheet['task_link'])):
                    review_sheet.loc[len(review_sheet.index)] = newRow                   
                    count=count+1
            #print('Added tasks in current review sheet to review frame and pending added - ',len(current))
            #print('Data read from sheet ----------------------')
            #print(review_sheet.head(20))
            if(tasks_pending==0 or tasks_pending>review_sample):
                add_df_to_review_sheet()
                return   
        print(' Add tasks to review frame now - ',tasks_pending)        
        count=0
        while(count<tasks_pending):
            m=0           
            for person in members:  #select one task for each member            
                for index,row in review_df.iterrows():                              
                    if row['assigned_to_email']==person and (person not in set(review_sheet['assigned_to_email']) and row['task_link'] not in set(review_sheet['task_link'])):        
                        newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':"",'review_status':"",
                                'total_score':row['total_score'],'status_score':row['status_score'],'age_score':row['age_score'],'count_score':row['count_score']+1,'quality_score':row['quality_score']}
                        review_sheet.loc[len(review_sheet.index)] = newRow                    
                        count=count+1
                        m=m+1
                    if(count>=tasks_pending):
                        break
                if(count>=tasks_pending):
                    break
            print('Added ',m,' unique member tasks to frame')          
            if(count>=tasks_pending):
                print('Met review sample size now..')
                tasks_pending=0
                add_df_to_review_sheet()
                return
            else:
                oldReviews.dropna(subset=["Email Address"], axis=0,inplace=True)
                oldReviews['Code Quality'] = oldReviews['Code Quality'].astype(int)
                oldReviews['Language Quality'] = oldReviews['Language Quality'].astype(int)            
                oldReviews['time'] = pd.to_datetime(oldReviews['Timestamp'])
                now = pd.Timestamp.now()
                week_ago = now - pd.Timedelta(weeks=1) 
                oldReviews['quality_score'] = oldReviews[['Language Quality','Code Quality']].mean(axis=1)
                oldReviews = oldReviews.sort_values(by=['quality_score','time'],ascending=[True,True])
                oldReviews = oldReviews[oldReviews['quality_score']<4.0]
                oldReviews = oldReviews.loc[oldReviews['time'] >= week_ago]
                old_members = oldReviews['Email Address'].unique()       
                old_list = list(set(members).intersection(old_members)) #old contributors in current task list
                print('Number of contributors(tasks) with low code quality :',len(old_list))
                old=0
                for person in old_list:  #select one task for each member
                    for index1,row1 in review_df.iterrows():
                        if row1['assigned_to_email']==person and (row1['task_link'] not in set(review_sheet['task_link'])):
                            #print('Adding old review data :',row1['task_link'])
                            newRow = {'task_link':row1['task_link'],'assigned_to_email':row1['assigned_to_email'],'completion_date':row1['completion_date'],'reviewer_email':"",'review_status':"",
                             'total_score':row1['total_score'],'status_score':row1['status_score'],'age_score':row1['age_score'],'count_score':row1['count_score']+1,'quality_score':row1['quality_score']}
                            review_sheet.loc[len(review_sheet.index)] = newRow
                            count=count+1
                            old=old+1            
                        if(count>=tasks_pending):
                            break
                    if(count>=tasks_pending):
                        break
                print('Added old review low quality member tasks to the sheet ',old)
                if(count>=tasks_pending):
                    print('Met review sample size now..')
                    tasks_pending=0
                    add_df_to_review_sheet()
                    return
                else:
                    review_df['count_score'] = review_df.groupby(['assigned_to_email','completion_date'])['task_link'].transform('count')
                    #df=review_df.groupby(['assigned_to_email','completion_date']).agg({"countScore":{"task_link":"count"}}).sort_values(by=['countScore'],ascending=[False]).reset_index(level=['assigned_to_email','completion_date'])
                    high_df=review_df[(review_df['count_score']>=4)]
                    now = pd.Timestamp.now()
                    week_ago = now - pd.Timedelta(weeks=2)
                    high_df = high_df.loc[high_df['completion_date'] >= week_ago]
                    mem = high_df['assigned_to_email'].unique()                
                    print('Number of contributors with high number of tasks :',len(mem))
                    high=0
                    for person in mem:
                        for index,row in review_df.iterrows():
                            if(row['assigned_to_email']==person and row['task_link'] not in set(review_sheet['task_link'])):
                                newRow = {'task_link':row['task_link'],'assigned_to_email':row['assigned_to_email'],'completion_date':row['completion_date'],'reviewer_email':"",'review_status':"",
                                'total_score':row['total_score'],'status_score':row['status_score'],'age_score':row['age_score'],'count_score':row['count_score']+1,'quality_score':row['quality_score']}
                                review_sheet.loc[len(review_sheet.index)] = newRow    
                                count=count+1
                                high=high+1
                                break                      
                            #if(count>=tasks_pending):
                            #    break
                        if(count>=tasks_pending):
                            break
                    #Update review sheet and complete current iteration now
                    if(count>=tasks_pending):               
                        print('Met review sample size now..')
                        tasks_pending=0
                        add_df_to_review_sheet()
                        return
            tasks_pending=tasks_pending-count
            print('Added ',count,' tasks in current iteration')
            print('Yet to add : ',tasks_pending)
            if(tasks_pending<0):
                tasks_pending=0         
            add_df_to_review_sheet()
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
    #schedule.every(1).minutes.do(init_review_tasks)
    schedule.every(1).minutes.do(add_tasks_to_review_queue)
    while True:
        schedule.run_pending()       
        time.sleep(1*60*60) #Check hourly updates

if __name__ == "__main__":
    main()