# Financial-Data-Analysis
This project imports stock data from the Yahoo Finances website. This data will be analyzed to find investment opportunities. The code is written only for educational purposes. 

Data for each stock is imported using a Python API and loaded to PostgreSQL tables. Python and SQL scripts are used in the codebase. Commonly used Python libraries are listed below.

- yfinances
- pandas
- numpy
- datetime
- psycopg2
- sqlalchemy

Specific tasks of each file are described below.

1. import_and_load_single_stock_data_initial.py

Imports prices data from Yahoo Finances website for a single stock and loads into a PostgreSQL database.

2. update_single_stock_data_incremental.py

Updates stock prices table in the database. Imports and inserts only the data from the last date that data available in the stock prices table.

3.import_and_load_dividend_stocks_data_initial.py

Imports data related to dividend return of multiple stocks from Yahoo Finances website and loads into a PostgreSQL database.

4. predict_single_stock_high_price.py

Predicts the highest price of a single stock for the next day for a certain stock.





