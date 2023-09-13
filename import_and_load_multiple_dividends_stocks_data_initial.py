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
    dataframes.append(stock_data)
dividends_stocks_data = pd.concat(dataframes, ignore_index=True)

dividends_stocks_data.columns = [column_name.lower() for column_name in dividends_stocks_data.columns]


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
                Daily_return FLOAT
                );''')
               
print("Table created successfully")
conn.close()

# Create an engine and load the data from the dataframe to postgresql database
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
dividends_stocks_data.to_sql('dividends_stocks_daily', engine, if_exists='append', index=False)