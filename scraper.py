# Scrapes https://www.wsj.com/market-data/stocks/newfiftytwoweekhighsandlows for the 52 week highs and lows
# Returns both NYSE and NASDAQ highs

import requests
import json
import pandas as pd
import sqlalchemy
import datetime


def json_to_df(movetype, dictionary):
    name_list = []
    volume_list = []
    lastprice_list = []
    change_list = []
    changepercent_list = []

    for i in dictionary["data"][movetype]["instruments"]:
        name_list.append(i['formattedName'])
        volume_list.append(i['formattedVolume'])
        lastprice_list.append(i['lastPrice'])
        change_list.append(i['priceChange'])
        changepercent_list.append(i['percentChange'])

    stockdata_dict = {
        'name': name_list,
        'last_price': lastprice_list,
        'price_change': change_list,
        'percent_change': changepercent_list,
        'volume': volume_list
    }

    dataframe = pd.DataFrame(stockdata_dict)
    return dataframe


# parameters to access JSON data
url = 'https://www.wsj.com/market-data/stocks/'
params = {'id': '{"application":"WSJ","count":100,"region":"US"}',
          'type': "mdc_stockmovers"}
headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0"}

# sending HTML GET request
get_request = requests.get(url, params=params, headers=headers)

if get_request.status_code == 200:
    print("Scrape successful (code 200)")
else:
    print("Scrape failed (code {})".format(get_request.status_code))

# converts json string to dictionary
jsonrequest_dict = json.loads(get_request.text)

# converting dictionary to pd dataframes
decliner_df = json_to_df('decliners', jsonrequest_dict)
gainer_df = json_to_df('gainers', jsonrequest_dict)
active_df = json_to_df('movers', jsonrequest_dict)

# adding current date
decliner_df["date_added"] = [datetime.date.today()] * len(decliner_df)
gainer_df["date_added"] = [datetime.date.today()] * len(gainer_df)
active_df["date_added"] = [datetime.date.today()] * len(active_df)

# connecting to sql
username = ""
password = ""
hostIP = ""
port = ""
database = ""

try:
    engine = sqlalchemy.create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
        username, password, hostIP, port, database))
    connection = engine.connect()
    print(
        "Connection to {0} for user {1} created successfully.".format(hostIP, username))
except Exception as ex:
    print("Connection could not be made due to the following error: \n", ex)

# Inserting dataframes into sql tables
try:
    decliner_df.to_sql(con=connection, name='decliners', if_exists='append',
                       dtype={'name': sqlalchemy.types.VARCHAR(length=500)})
    gainer_df.to_sql(con=connection, name='gainers', if_exists='append',
                     dtype={'name': sqlalchemy.types.VARCHAR(length=500)})
    active_df.to_sql(con=connection, name='most_active', if_exists='append',
                     dtype={'name': sqlalchemy.types.VARCHAR(length=500)})
except Exception as ex:
    print("Table could not be created due to the following error: \n", ex)

# Setting primary keys for each table (this step can also be done in MySQL)
query = ["ALTER TABLE mydb.decliners ADD PRIMARY KEY (name, date_added);",
         "ALTER TABLE mydb.gainers ADD PRIMARY KEY (name, date_added);",
         "ALTER TABLE mydb.most_active ADD PRIMARY KEY (name, date_added);"]

for line in query:
    connection.execute(sqlalchemy.text(line))
