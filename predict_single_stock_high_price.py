# Import libraries
import pandas as pd
from datetime import date
from datetime import timedelta
from datetime import datetime
from sklearn import linear_model
from sqlalchemy import create_engine

# Read date and daily high values to a dataframe
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
query = '''SELECT date, high 
           FROM stock_prices 
           WHERE date > '2021-01-01' '''
data = pd.read_sql_query(query, con=engine)
daily_high = pd.DataFrame(data)

# Date of yesterday
today = date.today()
yesterday = today - timedelta(days = 1)

# Set date as index
daily_high_time_index = daily_high.set_index('date')

# five days from yesterday
five_days_from_yesterdy = yesterday - timedelta(days=6)

# Dataframe for last five days
end_date = five_days_from_yesterdy
start_date = yesterday
last_week = daily_high_time_index.loc[end_date: start_date]

# Average high for last five days
current_year_mean = last_week['high'].mean()

# Reset index of last week 
last_week_reset_index = last_week.reset_index()

# Day from last year that corresponds with today
def lastweekoflastyear():
    return datetime.date(datetime.datetime.now().year-1, 12, 28).isocalendar()[1]

date_time_lastyear = datetime.datetime.now() - timedelta(weeks=lastweekoflastyear())
last_year_today = date_time_lastyear.date()

# Day from last year that corresponds with yesterday
last_year_yesterday = last_year_today - timedelta(days = 1)

# Five days from last year's yesterday
five_days_from_last_year_yesterday = last_year_yesterday - timedelta(days=6)

# Last week of last year
start_date = last_year_yesterday
end_date = five_days_from_last_year_yesterday
last_year_week = daily_high_time_index.loc[end_date: start_date]

# Average high of last week of last year
last_year_mean = last_year_week['high'].mean()
last_year_mean

# Ratio of mean highs between last year and current year
year_to_year_ratio = current_year_mean / last_year_mean 

last_year_today_high = daily_high_time_index.loc[last_year_today]['high']
last_year_today_high

# Regression coefficient of last week this year
x = pd.to_datetime(last_week_reset_index['date'], format='%Y-%m-%d')
y = last_week_reset_index['high'].values.reshape(-1, 1)
lm = linear_model.LinearRegression()
model = lm.fit(x.values.reshape(-1, 1),y)
slope = model.coef_
gradient_last_five_days = slope[0][0]

# Prediction for today's high
today_high_prediction_year_adjusted = last_year_today_high * year_to_year_ratio 
gradient_delta = today_high_prediction_year_adjusted * gradient_last_five_days
today_high_prediction_slope_adjusted = today_high_prediction_year_adjusted + gradient_delta
