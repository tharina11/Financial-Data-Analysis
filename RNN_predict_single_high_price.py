#Import libraries
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from keras.models import Sequential
from keras.layers import Dense, SimpleRNN
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
import math

# Read date and daily high values to a dataframe
engine = create_engine('postgresql+psycopg2://tharinduabeysinghe:#####@localhost/stocks')
query = '''SELECT date, high 
           FROM stock_prices 
           WHERE date > '2021-01-01' '''
data = pd.read_sql_query(query, con=engine)
daily_high = pd.DataFrame(data)

# Set date as index, convert to an array and reshape data
daily_high_time_index = daily_high.set_index('date')
data = np.array(daily_high_time_index.values.astype('float32'))
data = data.reshape(-1, 1)

# Scale the data between minimum and maximum
scaler = MinMaxScaler(feature_range=(0, 1))
data = scaler.fit_transform(data).flatten()

# Define the split percentages for training and testing data
split_percent=0.8
n = len(data)
split = int(n*split_percent)

# Split data to train and test sets
train_data = data[range(split)]
test_data = data[split:]

# Prepare the input X and target Y (Non overlapping Xs)
def get_XY(dat, time_steps):
    # Indices of target array
    Y_ind = np.arange(time_steps, len(dat), time_steps)
    Y = dat[Y_ind]
    # Prepare X
    rows_x = len(Y)
    X = dat[range(time_steps*rows_x)]
    X = np.reshape(X, (rows_x, time_steps, 1))    
    return X, Y

# Define time step as 5 independent (X) points and 1 dependent (Y) point
time_steps = 5
trainX, trainY = get_XY(train_data, time_steps)
testX, testY = get_XY(test_data, time_steps)

# Build a RNN
def create_RNN(hidden_units, dense_units, input_shape, activation):
    model = Sequential()
    model.add(SimpleRNN(hidden_units, input_shape=input_shape, 
                        activation=activation[0]))
    model.add(Dense(units=dense_units, activation=activation[1]))
    model.compile(loss='mean_squared_error', optimizer='adam')
    return model

# Fit RNN to the training data
model = create_RNN(hidden_units=3, dense_units=1, input_shape=(time_steps,1), 
                   activation=['tanh', 'tanh'])
model.fit(trainX, trainY, epochs=20, batch_size=1, verbose=2)

# Print errorsfor training and testing
def print_error(trainY, testY, train_predict, test_predict):    
    # Error of predictions
    train_rmse = math.sqrt(mean_squared_error(trainY, train_predict))
    test_rmse = math.sqrt(mean_squared_error(testY, test_predict))
    # Print RMSE
    print('Train RMSE: %.3f RMSE' % (train_rmse))
    print('Test RMSE: %.3f RMSE' % (test_rmse))    

# Predictions
train_predict = model.predict(trainX)
test_predict = model.predict(testX)
# Mean square error
print_error(trainY, testY, train_predict, test_predict)

# predict stock price high for the next day
last_five_highs = data[-5:].reshape(1,5,1)
y_predicted = model.predict(last_five_highs)
y_new_inverse = scaler.inverse_transform(y_predicted)
print(y_new_inverse)