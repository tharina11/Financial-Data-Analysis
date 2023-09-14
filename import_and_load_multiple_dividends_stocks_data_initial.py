# Import libraries
import yfinance as yf
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

# Tickers list
tickers = ['AAPL', 'MSFT', 'SPG', 'PEP', 'F', 'SHW', 'T', 'ORCL', 'V', 'BAC', 'UNH', 'MRK', 'MCD', 'HD', 
           'JNJ', 'KO', 'JPM', 'AXP', 'SBUX', 'CSCO', 'HPQ', 'MS', 'GS', 'CVS', 'C', 'PFE', 'XOM', 'TGT',
           'WFC', 'CVX', 'PG', 'PRU', 'VZ', 'MMM', 'IBM', 'WBA', 'ABBV']

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# Import data using yahoo finances API for each ticker and load to a dataframe
dataframes = []
for ticker_name in tickers:
    ticker = yf.Ticker(ticker_name)
    first_date_epoch = ticker.info['firstTradeDateEpochUtc']
    first_trade_date = datetime.fromtimestamp(first_date_epoch).date().strftime('%Y-%m-%d')
    stock_data = yf.download(ticker_name, start=first_trade_date, end=today) 
    stock_data['Ticker'] = ticker_name
    stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
    records = stock_data.to_records(index=True)
    stock_data = stock_data.reset_index()
    stock_data['daily_return'] = (stock_data['Close'] / stock_data['Close'].shift(1)) - 1
    stock_data['daily_return'].fillna(0,inplace=True)
    stock_data['Date'] = stock_data['Date'].dt.strftime('%Y-%m-%d')
    div_data = ticker.history(start=first_trade_date, end=today)
    div_data = div_data[['Dividends', 'Stock Splits']].reset_index()
    div_data['Date'] = pd.to_datetime(div_data['Date']).dt.date
    div_data['Date'] = div_data['Date'].astype(str)
    stock_data['Date'] = stock_data['Date'].astype(str)
    stock_data_merged = stock_data.merge(div_data, left_on='Date', right_on='Date',how='left')
    dataframes.append(stock_data_merged)
multiple_stocks_data = pd.concat(dataframes, ignore_index=True)

# Update column names and data types 
multiple_stocks_data['dividends'] = pd.to_numeric(multiple_stocks_data['Dividends'], errors='coerce')
multiple_stocks_data['stock_splits'] = pd.to_numeric(multiple_stocks_data['Stock Splits'], errors='coerce')
multiple_stocks_data.drop(['Stock Splits', 'Dividends'], axis=1, inplace=True)
multiple_stocks_data['Date']=pd.to_datetime(multiple_stocks_data['Date'],format='%Y-%m-%d')
multiple_stocks_data.columns = [column_name.lower() for column_name in multiple_stocks_data.columns]

#### INSERT TO PostgreSQL database ######

username = 'tharinduabeysinghe'
password = '#####'

# Create dividends_stocks_daily table in the stocks database
conn = psycopg2.connect(
    database="stocks", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS dividends_stocks_daily
               (
                Date DATE NOT NULL,
                Open FLOAT NOT NULL,
                High FLOAT NOT NULL,
                Low FLOAT NOT NULL,
                Close FLOAT NOT NULL,
                Adj_Close FLOAT NOT NULL,
                Volume BIGINT NOT NULL,
                Ticker VARCHAR(255),
                Daily_return FLOAT,
                Dividends FLOAT,
                Stock_splits FLOAT
                );''')
               
print("Table created successfully")
conn.close()

# Create an engine and load the data from the dataframe to postgresql database
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
multiple_stocks_data.to_sql('dividends_stocks_daily', engine, if_exists='append', index=False)