# Import libraries
import pandas as pd
import yfinance as yf
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from numerize import numerize
import psycopg2
from sqlalchemy import create_engine

# Define functions
# Years traded
def years_traded(ticker_name):
    first_trade_date = ticker.info['firstTradeDateEpochUtc']
    first_day = datetime.fromtimestamp(first_trade_date)
    first_day = first_day.date()
    years_traded = relativedelta(date.today(), first_day).years
    return years_traded

# Market cap
def Market_cap_numerized(ticker_name):
    ticker = yf.Ticker(ticker_name)
    market_cap = ticker.info['marketCap']
    market_cap = numerize.numerize(market_cap)
    return market_cap

# Ex dividend date
def Last_dividend_date(ticker_name):
    ticker = yf.Ticker(ticker_name)
    dividend_date = ticker.info['exDividendDate']
    ex_dividend_date = datetime.fromtimestamp(dividend_date).date()
    return ex_dividend_date

# Tickers list
ticker_list = ['AAPL', 'MSFT', 'SPG', 'PEP', 'F', 'SHW', 'T']

# Import data using API
stock_dicts_list = []
    
for ticker_name in ticker_list:
    ticker = yf.Ticker(ticker_name)
    ticker_info = ticker.info
    
    stock_dict = {'ticker': ticker_name,
              'sector': ticker_info['sector'] if 'sector' in ticker_info else None,
              'age': years_traded(ticker_name) if 'firstTradeDateEpochUtc' in ticker_info else None,
              'market_cap': Market_cap_numerized(ticker_name) if 'marketCap' in ticker_info else None,
              'last_dividend_value': ticker_info['lastDividendValue'] if 'lastDividendValue' in ticker_info else None,
              'last_dividend_date': Last_dividend_date(ticker_name) if 'exDividendDate' in ticker_info else None,
              'five_year_avg_div_yield': ticker_info['fiveYearAvgDividendYield'] if 'fiveYearAvgDividendYield' in ticker_info else None,
              'current_price': ticker_info['currentPrice'] if 'currentPrice' in ticker_info else None,
              'trailing_pe': ticker_info['trailingPE'] if 'trailingPE' in ticker_info else None,
              'forward_pe': ticker_info['forwardPE'] if 'forwardPE' in ticker_info else None,
              'fifty_two_week_change': ticker_info['52WeekChange'] if '52WeekChange' in ticker_info else None,
              'payout_ratio': ticker_info['payoutRatio'] if 'payoutRatio' in ticker_info else None,
              'debt_to_equity_ratio': ticker_info['debtToEquity'] if 'debtToEquity' in ticker_info else None,
              'earnings_quarterly_growth': ticker_info['earningsQuarterlyGrowth'] if 'earningsQuarterlyGrowth' in ticker_info else None,
              'book_value': ticker_info['bookValue'] if 'bookValue' in ticker_info else None,
              'beta': ticker_info['beta'] if 'beta' in ticker_info else None,
              'recommendation_key': ticker_info['recommendationKey'] if 'recommendationKey' in ticker_info else None,
              'reccommendation_mean': ticker_info['recommendationMean'] if 'recommendationMean' in ticker_info else None
                     }
    
    stock_dicts_list.append(stock_dict)
print(stock_dicts_list)

# Insert stock data into a pandas dataframe and add imported date into a new column
stock_data = pd.DataFrame.from_records(stock_dicts_list)
stock_data['date_imported']= datetime.today().strftime('%Y-%m-%d')
stock_data = stock_data[['date_imported'] + [col for col in stock_data.columns if col != 'date_imported']]

#### INSERT TO PostgreSQL database ######

# Define username and password of the Posgresql database (use your password)
username = 'tharinduabeysinghe'
password = #######

# Create a stocks database
conn = psycopg2.connect(
   database="postgres", user= username, password= pwd, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cursor = conn.cursor()
sql = '''CREATE DATABASE stocks''';
cursor.execute(sql)
print("Database created successfully!")
conn.close()

# Create dividends_stocks_data in the stocks database
conn = psycopg2.connect(
    database="stocks", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE dividend_stocks_data
               (
                Date_imported VARCHAR(255),
                Ticker VARCHAR(255),
                Sector VARCHAR(255),
                Age INT,
                Market_cap VARCHAR(255),
                Last_dividend_value FLOAT,
                Last_Dividend_Date VARCHAR(255),
                Five_year_avg_div_yield VARCHAR(255),
                Current_Price FLOAT,
                trailing_PE FLOAT,
                Forward_PE FLOAT,
                Fifty_two_Week_Change FLOAT,
                Payout_Ratio FLOAT,
                Debt_to_Equity_Ratio FLOAT,
                Earnings_Quarterly_Growth FLOAT,
                Book_Value FLOAT,
                Beta FLOAT,
                Recommendation_Key VARCHAR(255),
                Reccommendation_Mean FLOAT

                );''')
               
print("Table created successfully")
conn.close()

# Create an engine with sqlalchemy
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
df.to_sql('dividend_stocks_data', engine, if_exists='append', index=False)




