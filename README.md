# Stock Data Analysis and Prediction
This project imports stock data from the Yahoo Finances website. The data will be analyzed to find optimum investment opportunities. The codes are written only for educational purposes. 

Data for each stock is imported using a Python API and loaded to PostgreSQL tables. Python and SQL scripts are used in the codebase. Python libraries used in the project are listed below.

- yfinances
- pandas
- numpy
- datetime
- psycopg2
- sqlalchemy
- scikit-learn
- keras

Specific tasks of each file are described below.

1. import_and_load_single_stock_data_initial.py

Imports prices data from Yahoo Finances website for a single stock and loads into a PostgreSQL database.

2. update_single_stock_data_incremental.py

Updates stock prices table in the database. Imports and inserts only the data from the last date that data available in the stock prices table.

3. import_and_load_dividend_stocks_data_initial.py

Imports data related to dividend return of multiple stocks from Yahoo Finances website and loads into a PostgreSQL database.

4. predict_single_stock_high_price.py

Predicts the highest price of a single stock for the next day. Predicts using the seasonality and trend identifiend based on past high data of the same stock.

5. RNN_predict_single_high_price.py

Predicts the highest price of a single stock for the next day. Predicts based on highest prices of the same stock in the last five days using a Recurrent Neural Network.




