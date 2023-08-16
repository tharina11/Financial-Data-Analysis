# Import libraries
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import date
from datetime import timedelta
from datetime import datetime
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Create an engine
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')

# The most recent date that data available for
query = '''SELECT max(date)
           FROM stock_prices
        '''
date_price = pd.read_sql_query(query, con=engine)
last_date_db = date_price['max'].to_string(index=False)
last_date = datetime.strptime(last_date_db, '%Y-%m-%d').date()
start_date = last_date + timedelta(days = 1)
start_date

# Last close price in the database
query = '''SELECT close
           FROM stock_prices
           ORDER BY date DESC
           LIMIT 1
        '''
price = pd.read_sql_query(query, con=engine)
last_close = price['close'].item()
last_close

# Ticker name
ticker_name = 'JPM'

# Import data from the most recent date of the table into a dataframe
today = datetime.today().strftime('%Y-%m-%d')
stock_data = yf.download(ticker_name, start=start_date, end=today) 
stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
records = stock_data.to_records(index=True)
stock_data = stock_data.reset_index()

# Calculate daily return and add as a column to the table
stock_data['daily_return'] = (stock_data['Close'] / last_close) - 1

# update column names to match with columns in the table in database
stock_data.columns = [column_name.lower() for column_name in stock_data.columns]

# Load data from the dataframe to the table in postgresql database
# Load data from the dataframe to the table in postgresql database
if not stock_data.empty:
    stock_data.to_sql('stock_prices', engine, if_exists='append', index=False)
else: 
    print('Dataframe is empty')