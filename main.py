import numpy as np
import pandas as pd
import pymongo
from sklearn.preprocessing import MinMaxScaler
import joblib
import _thread
import time
import requests
import DB_update.update as update

def get_data(siteid,species,scaler):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient['citydb']
    mycol = mydb[species+'Readings']
    myd = mycol.find({'sensorid': '{}-{}'.format(siteid,species)})
    doc = list(myd)
    data = pd.DataFrame.from_dict(doc, orient='columns')
    data.drop(columns=['sensorid'], inplace=True)
    data.drop(columns=['_id'], inplace=True)
    data.columns = ['datetime', species]
    data.index = pd.to_datetime(data['datetime'])
    data.drop(columns=['datetime'], inplace=True)
    data.replace(to_replace=r'^\s*$', value=np.nan, regex=True, inplace=True)
    data = data.astype('float32')
    data = data.dropna()
    data[data < 0] = 0
    data_scaled = scaler.fit_transform(data)
    return data,data_scaled

def make_prediction(siteid,species,scaler):
    data,data_scaled=get_data(siteid,species,scaler)
    latest_time=str(data[-1:].index[0])
    X=np.array(data_scaled[-720:])
    X=np.reshape(X,(1,720))
    reg=joblib.load('./models/linear-{}.model'.format(species))
    pred_norm=reg.predict(X)
    pred=scaler.inverse_transform(pred_norm)
    temp={"site":siteid,
          "sensor id":'{}-{}'.format(siteid,species),
          "record time":latest_time,
          "values":data.values[-1][-1],
          "1-hour prediction":pred[0][0],
          "2-hour prediction":pred[0][1],
          "3-hour prediction":pred[0][2]}
    #print(temp)
    temp=pd.DataFrame(temp,index=[0])

    return temp

def get_results(monitoring,scaler):
    results=pd.DataFrame()
    for i in range(len(monitoring)):
        if monitoring.loc[i]['NO2'] == 1:
            species = 'NO2'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

        if monitoring.loc[i]['SO2'] == 1:
            species = 'SO2'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

        if monitoring.loc[i]['O3'] == 1:
            species = 'O3'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

        if monitoring.loc[i]['CO'] == 1:
            species = 'CO'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

        if monitoring.loc[i]['PM10'] == 1:
            species = 'PM10'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

        if monitoring.loc[i]['PM25'] == 1:
            species = 'PM25'
            siteid = monitoring.loc[i]['sitecode']
            temp=make_prediction(siteid,species,scaler)
            results=results.append(temp)

    return results

def save_results():
    monitoring = pd.read_csv('monitoring.csv')
    scaler = MinMaxScaler(feature_range=(0, 1))
    results = get_results(monitoring, scaler)
    results.to_csv('results.csv')
    print('Prediction updated')
    time.sleep(10800)

def query_results():
    while 1:
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 200)
        results=pd.read_csv('results.csv')
        check = input('Enter siteid to query results:')
        if str(check) == 'all':
            print(results)
        else:
            print(results[results['site'] == check])

def database_update():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["citydb"]
    monitoring = pd.read_csv('monitoring.csv')
    requests.adapters.DEFAULT_RETRIES = 10
    s = requests.session()
    s.keep_alive = False
    pd.set_option('mode.chained_assignment', None)
    while (1):
        update.update_air(monitoring, mydb,s)
        update.update_weather(mydb)
        time.sleep(21600)

if __name__== '__main__':
    try:
        _thread.start_new_thread(database_update(), ())
        _thread.start_new_thread(save_results, ())
        _thread.start_new_thread(query_results, ())
    except:
        print("Error: unable to start thread")

    while 1:
        pass