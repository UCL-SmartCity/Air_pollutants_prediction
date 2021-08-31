import requests
import pandas as pd
import json
import pymongo
import datetime
import time
import sys
import os

def update_air(mo,mydb,s):
    mycol = mydb['PM25Readings']
    mydoc = mycol.find({'sensorid': 'WMD-PM25'}).sort('recordtime', pymongo.DESCENDING).limit(1)
    r = list(mydoc)
    last_update = datetime.datetime.strptime(r[0]['recordtime'], "%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now()
    starttime = last_update.strftime("%d-%m-%Y")
    endtime = (dt + datetime.timedelta(days=1)).strftime("%d-%m-%Y")

    for i in range(106):
        if mo.loc[i]['NO2']==1:
            senid = mo.loc[i]['sitecode'] + '-NO2'
            url='http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode='+mo.loc[i][
                'sitecode']+'/SpeciesCode=NO2/StartDate='+starttime+'/EndDate='+endtime+'/Json'
            res=s.get(url)
            txt=res.text
            #print(txt)
            li=json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading={'recordtime':item['@MeasurementDateGMT'],
                             'value':item['@Value'],'sensorid':senid}
                    mycol=mydb['NO2Readings']
                    mycol.insert_one(reading)

        if mo.loc[i]['SO2']==1:
            senid = mo.loc[i]['sitecode'] + '-SO2'
            url = 'http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=' + mo.loc[i][
                'sitecode'] + '/SpeciesCode=SO2/StartDate=' + starttime + '/EndDate=' + endtime + '/Json'
            res = s.get(url)
            txt = res.text
            # print(txt)
            li = json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading = {'recordtime': item['@MeasurementDateGMT'],
                               'value': item['@Value'], 'sensorid': senid}
                    mycol = mydb['SO2Readings']
                    mycol.insert_one(reading)

        if mo.loc[i]['O3']==1:
            senid = mo.loc[i]['sitecode'] + '-O3'
            url = 'http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=' + mo.loc[i][
                'sitecode'] + '/SpeciesCode=O3/StartDate=' + starttime + '/EndDate=' + endtime + '/Json'
            res = s.get(url)
            txt = res.text
            # print(txt)
            li = json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading = {'recordtime': item['@MeasurementDateGMT'],
                               'value': item['@Value'], 'sensorid': senid}
                    mycol = mydb['O3Readings']
                    mycol.insert_one(reading)

        if mo.loc[i]['CO']==1:
            senid = mo.loc[i]['sitecode'] + '-CO'
            url = 'http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=' + mo.loc[i][
                'sitecode'] + '/SpeciesCode=CO/StartDate=' + starttime + '/EndDate=' + endtime + '/Json'
            res = s.get(url)
            txt = res.text
            # print(txt)
            li = json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading = {'recordtime': item['@MeasurementDateGMT'],
                               'value': item['@Value'], 'sensorid': senid}
                    mycol = mydb['COReadings']
                    mycol.insert_one(reading)

        if mo.loc[i]['PM10']==1:
            senid = mo.loc[i]['sitecode'] + '-PM10'
            url = 'http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=' + mo.loc[i][
                'sitecode'] + '/SpeciesCode=PM10/StartDate=' + starttime + '/EndDate=' + endtime + '/Json'
            res = s.get(url)
            txt = res.text
            # print(txt)
            li = json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading = {'recordtime': item['@MeasurementDateGMT'],
                               'value': item['@Value'], 'sensorid': senid}
                    mycol = mydb['PM10Readings']
                    mycol.insert_one(reading)

        if mo.loc[i]['PM25']==1:
            senid=mo.loc[i]['sitecode']+'-PM25'
            url = 'http://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode=' + mo.loc[i][
                'sitecode'] + '/SpeciesCode=PM25/StartDate=' + starttime + '/EndDate=' + endtime + '/Json'
            res = s.get(url)
            txt = res.text
            # print(txt)
            li = json.loads(txt)
            for item in li['RawAQData']['Data']:
                rt = item['@MeasurementDateGMT']
                recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
                if recordtime > last_update:
                    reading = {'recordtime': item['@MeasurementDateGMT'],
                               'value': item['@Value'], 'sensorid': senid}
                    mycol = mydb['PM25Readings']
                    mycol.insert_one(reading)

    print('\npollutant update time:'+datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"))

def update_weather(mydb):
    r=run_scrapy()
    mycol = mydb["PressureReadings"]
    last_readings = mycol.find().sort('_id', -1).limit(1)
    latest_data = list(last_readings)
    last_update = datetime.datetime.strptime(latest_data[0]['recordtime'], "%Y-%m-%d %H:%M:%S")

    with open('weather.json') as data:
        weather_data = list(data)

    weather_list = json.loads(weather_data[0])
    weather_df = pd.DataFrame(weather_list)
    for i in range(len(weather_df)):
        weather_df.loc[i]['ds'] = weather_df.loc[i]['ds'][:-8]
    weather_df['ds'] = pd.to_datetime(weather_df['ds'])

    weather_df['ds'] = weather_df['ds'].astype('str')
    weather_df['temp'] = weather_df['temp'].astype('int')
    weather_df['hum'] = weather_df['hum'].astype('int')
    weather_df['pre'] = weather_df['pre'].astype('int')

    tem_df = weather_df[['ds', 'temp']]
    tem_df.columns = ['recordtime', 'value']
    tem_df.loc[:,'sensorid'] = '1-W1-T'
    tem_js = tem_df.to_json(orient="records", force_ascii=False)
    tem_list = json.loads(tem_js)

    hum_df = weather_df[['ds', 'hum']]
    hum_df.columns = ['recordtime', 'value']
    hum_df.loc[:,'sensorid'] = '1-W1-H'
    hum_js = hum_df.to_json(orient="records", force_ascii=False)
    hum_list = json.loads(hum_js)

    pre_df = weather_df[['ds', 'pre']]
    pre_df.columns = ['recordtime', 'value']
    pre_df.loc[:,'sensorid'] = '1-W1-P'
    pre_js = pre_df.to_json(orient="records", force_ascii=False)
    pre_list = json.loads(pre_js)

    for i in tem_list:
        mycol = mydb["TempReadings"]
        rt = i['recordtime']
        recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
        if recordtime > last_update:
            #print(rt)
             mycol.insert_one(i)

    for i in hum_list:
        mycol = mydb["HumidityReadings"]
        rt = i['recordtime']
        recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
        if recordtime > last_update:
            #print(rt)
             mycol.insert_one(i)

    for i in pre_list:
        mycol = mydb["PressureReadings"]
        rt = i['recordtime']
        recordtime = datetime.datetime.strptime(rt, "%Y-%m-%d %H:%M:%S")
        if recordtime > last_update:
            #print(rt)
             mycol.insert_one(i)

    print('\nweather update time:' + datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"))

def run_scrapy():
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    os.system("scrapy crawl weathers --nolog")

if __name__== '__main__':
    #print(os.path.abspath(__file__))
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["citydb"]
    monitoring = pd.read_csv('monitoring.csv')
    requests.adapters.DEFAULT_RETRIES = 10
    s = requests.session()
    s.keep_alive = False
    pd.set_option('mode.chained_assignment', None)
    while (1):
        update_air(monitoring, mydb,s)
        update_weather(mydb)
        time.sleep(21600)