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

print(extract(data_url, table_attributes))