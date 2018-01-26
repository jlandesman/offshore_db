from sqlalchemy import create_engine
import psycopg2 as pg
import pandas as pd
import os
import urllib
import wget

## Read in login and pass
with open('panama_papers/rootkey.csv') as f:
    login = f.readline().strip('\n')
    passw = f.readline().strip('\n')
    

## Download information
BASE_URL = 'https://data.cityofnewyork.us/api/views'

## Table flags
TABLE_NAMES = dict({#'REAL_PROPERTY_LEGALS'     : '8h5j-fqxa',
                    'REAL_PROPERTY_MASTER'     : 'bnx9-e6tj',
                    'REAL_PROPERTY_PARTIES'    : '636b-3b5g'})

DOWNLOAD_STRING = 'rows.csv?accessType=DOWNLOAD'

## Database connection information
engine_string = 'postgresql+psycopg2://' + login + ':' + passw + '@offshoredb.cwwa5jzhqwm0.us-east-1.rds.amazonaws.com:5432/offshoredb'
engine = create_engine(engine_string)
conn=engine.raw_connection()
cur = conn.cursor()

def preprocess_columns(df):
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.lower()
    return df

## Download function
def download_and_save(table_name, table_url):
    print('Downloading {}'.format(table_name))
    path = os.path.join(BASE_URL,table_url, DOWNLOAD_STRING)
    file_name = str(table_name)+'.csv'
    
    ## Download file
    wget.download(path, file_name)
    
    ## Use pandas to save as pipe delimited file
    df = pd.read_csv(file_name)
    df = preprocess_columns(df)
    
    ## hack to create table in sql, assigning all columns to type text
    df_for_create_table = pd.DataFrame(columns = df.columns)
    
    ## save to csv
    df.to_csv(file_name, sep='|', na_rep = 'nan')
    
    ## Format all columns as text
    col_types = dict()
    for col in df_for_create_table.columns:
        col_types['col'] : 'text'
    
    ## Create empty table
    df_for_create_table.to_sql(table_name.lower(), engine, if_exists='replace', dtype=col_types)


## Upload to db
def upload_to_db(table_name):
    path = str(table_name).upper() + '.csv'
    with open(path) as f:
        next(f) ## Skip header
        cur.copy_from(f, table_name, sep='|')

    conn.commit()
    
## Run
for table_name, table_url in TABLE_NAMES.items():
    download_and_save(table_name, table_url)
    upload_to_db(table_name)