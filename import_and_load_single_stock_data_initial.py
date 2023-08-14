# Import libraries
import psycopg2
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import date
from datetime import timedelta
from datetime import datetime
import matplotlib.pyplot as plt
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Ticker name
ticker_name = 'JPM'

# First trade date of the company 
def first_trade_date(ticker_name_string):
    ticker = yf.Ticker(ticker_name_string)
    first_date_epoch = ticker.info['firstTradeDateEpochUtc']
    first_trade_date = datetime.fromtimestamp(first_date_epoch).date().strftime('%Y-%m-%d')
    return first_trade_date
first_trade_date = first_trade_date('JPM')

# Today
today = datetime.today().strftime('%Y-%m-%d')

# Download data from first trade date to today into a dataframe
stock_data = yf.download(ticker_name, start=first_trade_date, end=today) 
stock_data['Ticker'] = ticker_name
stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
records = stock_data.to_records(index=True)
stock_data = stock_data.reset_index()

# Calculate daily return and add as a column to the table
stock_data['daily_return'] = (stock_data['Close'] / stock_data['Close'].shift(1)) - 1

# Replaced Nan with 0
stock_data['daily_return'].fillna(0,inplace=True)

# Rename columns to start with lowercase letters
stock_data.columns = [column_name.lower() for column_name in stock_data.columns]

#### Insert to PostgreSQL database ######
# Username and password
username = 'tharinduabeysinghe'
password = '#####'

# Create stock prices table
conn = psycopg2.connect(
    database="stocks", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE stock_prices
               (
                Date DATE NOT NULL,
                Open FLOAT NOT NULL,
                High FLOAT NOT NULL,
                Low FLOAT NOT NULL,
                Close FLOAT NOT NULL,
                Adj_Close FLOAT NOT NULL,
                Volume BIGINT NOT NULL,
                Ticker VARCHAR(255),
                Daily_return FLOAT
                );''')
               
conn.close()

# Create an engine and load the data from the dataframe to postgresql database
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:12345@localhost/stocks')
stock_data.to_sql('stock_prices', engine, if_exists='append', index=False)