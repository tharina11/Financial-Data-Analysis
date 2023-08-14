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
def years_traded(first_trade_date):
    first_day = datetime.fromtimestamp(first_trade_date)
    first_day = first_day.date()
    years_traded = relativedelta(date.today(), first_day).years
    return years_traded

# Market cap
def Market_cap_numerized(market_cap):
    market_cap = numerize.numerize(market_cap)
    return market_cap

# Ex dividend date
def Last_dividend_date(dividend_date):
    ex_dividend_date = datetime.fromtimestamp(dividend_date).date()
    return ex_dividend_date

# Tickers list
tickers = ['AAPL', 'MSFT', 'SPG', 'PEP', 'F', 'SHW', 'T']

# Import data using yfinances API into a dataframe
ticker_data = []
for ticker_name in tickers:
    ticker = yf.Ticker(ticker_name)
    ticker_info = ticker.info
    df_norm = pd.json_normalize(ticker_info)
    df_norm.insert(0, "ticker", ticker_name, True)
    ticker_data.append(df_norm)

tickers_data_df = pd.concat(ticker_data, axis=0)
tickers_data_df

# Filter necessary data into a new dataframe
columns = ['ticker', 'sector', 'firstTradeDateEpochUtc', 'marketCap', 'lastDividendValue', 
           'exDividendDate', 'fiveYearAvgDividendYield', 'currentPrice', 'trailingPE', 
           'forwardPE', '52WeekChange', 'payoutRatio', 'debtToEquity', 'earningsQuarterlyGrowth', 
           'bookValue', 'beta', 'recommendationKey', 'recommendationMean'
          ]
tickers_dividends_data = tickers_data_df[columns]

# Transform imported data
tickers_dividends_data["exDividendDate"] = tickers_dividends_data["exDividendDate"].apply(Last_dividend_date)
tickers_dividends_data["years_traded"] = tickers_dividends_data["firstTradeDateEpochUtc"].apply(years_traded)
tickers_dividends_data["market_cap_Numerized"] = tickers_dividends_data["marketCap"].apply(Market_cap_numerized)
tickers_dividends_data['date_imported']= datetime.today().strftime('%Y-%m-%d')
tickers_dividends_data = tickers_dividends_data[['date_imported'] + [col for col in tickers_dividends_data.columns if col != 'date_imported']]

# Drop extra column
tickers_dividends_data.drop("firstTradeDateEpochUtc", axis = 1, inplace = True)

# Rename columns
# Rename columns
column_names = {                   
'marketCap' :  'market_cap',         
'lastDividendValue' : 'last_dividend_value',        
'exDividendDate' : 'last_dividend_date',           
'fiveYearAvgDividendYield' : 'five_year_avg_div_yield',
'currentPrice' : 'current_price',
'trailingPE' : 'trailing_pe',
'forwardPE' : 'forward_pe',
'52WeekChange' : 'fifty_two_week_change',
'payoutRatio' : 'payout_ratio',
'debtToEquity' : 'debt_to_equity_ratio',
'earningsQuarterlyGrowth' : 'earnings_quarterly_growth',
'bookValue' : 'book_value',
'recommendationKey' : 'recommendation_key',
'recommendationMean' : 'reccommendation_mean',
'market_cap_Numerized' : 'market_cap_numerized'
}

tickers_dividends_data.rename(columns=column_names, inplace=True)

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

# Create dividends_stocks table in the stocks database
conn = psycopg2.connect(
    database="stocks", user= username, password= password, host='127.0.0.1', port= '5432'
)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''CREATE TABLE dividends_stocks
               (
                date_imported VARCHAR(255),
                ticker VARCHAR(255),
                sector VARCHAR(255),
                market_cap BIGINT,
                last_dividend_value FLOAT,
                last_dividend_date VARCHAR(255),
                five_year_avg_div_yield FLOAT,
                current_price FLOAT,
                trailing_pe FLOAT,
                forward_pe FLOAT,
                fifty_two_week_change FLOAT,
                payout_ratio FLOAT,
                debt_to_equity_ratio FLOAT,
                earnings_quarterly_growth FLOAT,
                book_value FLOAT,
                beta FLOAT,
                recommendation_key VARCHAR(255),
                reccommendation_mean FLOAT,
                years_traded INT,
                market_cap_numerized VARCHAR(255)
                );''')             
conn.close()

# Create an engine and load the data from database to postgresql database
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
tickers_dividends_data.to_sql('dividends_stocks', engine, if_exists='append', index=False)




