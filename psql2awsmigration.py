#!/usr/bin/env python
# coding: utf-8

# In[186]:


#import required packages
import boto3
import psycopg2
import pandas as pd
import io


# In[187]:


#Credentials to on-prem Postgres
conn = psycopg2.connect(
   database="dvdrental", user='postgres', password='kernel24', host='localhost', port= '5432'
)
#Acess details for AWS S3
client = boto3.client(
    's3',
    aws_access_key_id = 'XXX', 
    aws_secret_access_key = 'XXa',
    region_name = 'us-east-1'
)
#Establish connection to Postgress
cursor = conn.cursor()

#Execute query to get all tables from Public Schema
cursor.execute("select tablename from pg_tables where schemaname='public'")

#Collect all tables
tables = cursor.fetchall()


# In[191]:


#For each table process Header & Data, collect to Dataframe and write to S3
count=0
for i in tables:
    count=count+1
    #print(str(i[0]))
    #Get all columns for each table
    cursor.execute("select column_name from information_schema.columns "+
                      "where table_schema='public' and table_name='"+str(i[0])+"'")
    columns=''
    #Collect each column to a string
    for j in cursor.fetchall():
        columns=str(columns)+"'"+str(j[0])+"', "
    columns=columns[:-2] #remove last 2 characters from the string -comma and space
    #print(columns)
    #print(str(tables[0]))
    
    #Collect data for each table
    cursor.execute("select * from "+str(i[0]))
    data = cursor.fetchall()
    #print(data1[0])
    
    #header = ['actor_id', 'first_name', 'last_name', 'last_update']
    #Convert the string to List format
    header=list(columns.split(','))
    #print(header)
    #Convert to DataFrame
    df = pd.DataFrame(data,columns = header)
    
    #Write to AWS S3
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)
        response = client.put_object(
            Bucket="dvddatalake", Key=i[0]+".csv", Body=csv_buffer.getvalue()
            )
    #Migration confirmation
    print('Table['+str(count)+']:'+i[0]+' uploaded to S3')
print(str(count)+' tables migrated')

