import numpy as np
import pandas as pd
from keras.models import Sequential
from keras.layers import Dense,LSTM,Dropout
from keras.optimizers import Adam
import pymongo
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
import keras
from numpy import concatenate


def train_test(timestep, nextstep):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['citydb']
    siteid = 'KC1'
    species = 'NO2'
    mycol = mydb['NO2Readings']
    myd = mycol.find({'sensorid': siteid + '-' + species})
    doc = list(myd)
    data = pd.DataFrame.from_dict(doc, orient='columns')
    data.drop(columns=['sensorid'], inplace=True)
    data.drop(columns=['_id'], inplace=True)
    data.columns = ['datetime', 'NO2']
    data.index = pd.to_datetime(data['datetime'])
    data.drop(columns=['datetime'], inplace=True)
    data.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
    data = data.astype('float32')
    data = data.dropna()
    data[data < 0] = 0
    scaler = MinMaxScaler(feature_range=(0, 1))
    data_scaled = scaler.fit_transform(data)

    n_train_hours = 50000
    train = data_scaled[:n_train_hours, :]
    test = data_scaled[n_train_hours:, :]

    train_X = []
    train_y = []
    test_X = []
    test_y = []

    for i in range(len(train) - timestep - nextstep + 1):
        train_X.append(train[i:(i + timestep), :])
        btemp = train[i + timestep:i + timestep + nextstep, 0]
        b = []
        for j in range(len(btemp)):
            b.append(btemp[j])
        train_y.append(b)

    np.random.seed(7)
    np.random.shuffle(train_X)
    np.random.seed(7)
    np.random.shuffle(train_y)
    tf.random.set_seed(7)

    train_X = np.array(train_X, dtype=np.float32)
    train_y = np.array(train_y, dtype=np.float32)

    train_X = np.reshape(train_X, (train_X.shape[0], 720, 1))

    for i in range(len(test) - timestep - nextstep + 1):
        test_X.append(test[i:(i + timestep), :])
        btemp = test[i + timestep:i + timestep + nextstep, 0]
        b = []
        for j in range(len(btemp)):
            b.append(btemp[j])
        test_y.append(b)

    test_X, test_y = np.array(test_X, dtype=np.float32), np.array(test_y, dtype=np.float32)
    test_X = np.reshape(test_X, (test_X.shape[0], 720, 1))

    return train_X, train_y, test_X, test_y, scaler

def rmse(y_true, y_pred):
    return keras.backend.sqrt(keras.backend.mean(keras.backend.square(y_pred - y_true), axis=-1))

def build_model(train_X,train_y):
    model = Sequential()
    model.add(LSTM(40, input_shape=(train_X.shape[1:]), return_sequences=True, ))
    model.add(Dropout(0.1))
    model.add(LSTM(30, return_sequences=True))
    model.add(Dropout(0.1))
    model.add(LSTM(40, return_sequences=True))
    model.add(Dropout(0.1))
    model.add(LSTM(40))
    model.add(Dense(train_y.shape[1]))
    model.compile(optimizer=Adam(lr=LR, amsgrad=True), loss='mse',metrics=[rmse])

    return model

def model_fit(model, train_datas, train_labels,x_test, y_test):    #train_X, train_y, test_X, test_y


    checkpoint_save_path = "./checkpoint/LSTM_stock.ckpt"

    lr_reduce = keras.callbacks.ReduceLROnPlateau('val_loss',
                                                  patience=4,
                                                  factor=0.7,
                                                  min_lr=0.00001)
    best_model = keras.callbacks.ModelCheckpoint(filepath=checkpoint_save_path,
                                                 monitor='val_loss',
                                                 verbose=0,
                                                 save_best_only=True,
                                                 save_weights_only=True,
                                                 mode='min',
                                                 )

    early_stop = keras.callbacks.EarlyStopping(monitor='val_rmse', patience=15)

    history = model.fit(
        train_datas, train_labels,
        validation_data=(x_test, y_test),
        batch_size=BATCHSZ,
        epochs=EACH_EPOCH,
        verbose=2,
        callbacks=[
        best_model,
        early_stop,
        lr_reduce,
                    ]
    )

    return model, history

def model_evaluation(model, test_X, test_y):
    yhat = model.predict(test_X)
    yhat = yhat[:, 0]
    y_normal = yhat.copy()
    y_normal[y_normal < 0] = 0
    test_X = test_X[:, 0, :]
    test_y = test_y[:, 0]

    RMSE_normal = np.sqrt(mean_squared_error(y_normal, test_y))
    R2_SCORE_normal = r2_score(y_normal, test_y)
    print('RMSE_normal: {}\nR2_SCORE_normal: {}\n'.format(RMSE_normal, R2_SCORE_normal))

    yhat = yhat.reshape(len(yhat), 1)
    inv_yhat = concatenate((yhat, test_X[:, 1:]), axis=1)
    inv_y = scaler.inverse_transform(inv_yhat)
    y_pred = inv_y[:, 0]
    y_pred[y_pred < 0] = 0

    test_y = test_y.reshape((len(test_y), 1))
    inv_yact_hat = concatenate((test_y, test_X[:, 1:]), axis=1)
    inv_y = scaler.inverse_transform(inv_yact_hat)
    y_real = inv_y[:, 0]

    y_pred_df = pd.DataFrame(index=y_pred)
    y_pred_df.to_csv(r'./LSTM_pred.csv', encoding='gbk', sep=',')
    y_real_df = pd.DataFrame(index=y_real)
    y_real_df.to_csv(r'./LSTM_real.csv', encoding='gbk', sep=',')

    RMSE = np.sqrt(mean_squared_error(y_pred, y_real))
    R2_SCORE = r2_score(y_pred, y_real)
    print('RMSE: {}\nR2_SCORE: {}\n'.format(RMSE, R2_SCORE))

    return RMSE, R2_SCORE, RMSE_normal, R2_SCORE_normal

if __name__== '__LSTM__':
    TIME_STEP = 8760
    DELAY = 10
    BATCHSZ = 10
    EACH_EPOCH = 3
    LR = 0.001

    train_X, train_y, test_X, test_y, scaler = train_test(timestep=720, nextstep=10)
    model=build_model(train_X,train_y)
    model, history = model_fit(model, train_X, train_y, test_X, test_y)
    RMSE_list, R2_SCORE_list, RMSE_normal_list, R2_SCORE_normal_list = model_evaluation(model, test_X, test_y)