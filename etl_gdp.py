"""
* Code for ETL operations on Country-GDP data
* Created: 8/19/2024 5:35:58 PM
* Author : Omar El-Azab
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime 

data_url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_file_path = './Countries_by_GDP.csv'
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
table_attributes = ["Country", "GDP_USD_millions"]


def extract(url, table_attribs):
    response = requests.get(url)
    if response.status_code == 200:
        web_page = response.text
    web_soup = BeautifulSoup(web_page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)

    web_table = web_soup.find("table", {"class": "wikitable"})
    table_rows = web_table.find_all('tr')

    for row in table_rows:
        col = row.find_all('td')
        if len(col) !=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"]=GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestampformat = '%Y-%h-%d-%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestampformat)
    with open("./etl_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')



log_progress('Preliminaries complete. Initiating ETL process')

df = extract(data_url, table_attributes)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_file_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()


