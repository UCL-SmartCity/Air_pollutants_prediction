# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import json


class WeatherPipeline:
    def __init__(self):
        self.f = open("/Users/au_yueng/PycharmProjects/pythonProject11/weather.json","w",encoding="utf-8")

    def process_item(self, item, spider):
        #data = pd.DataFrame(columns=["ds", "temp", "hum", "pre"])
        ds = []
        temp = []
        hum = []
        pre = []
        for i in item["property"]:
            key = list(i.keys())
            if key[0] == "ds":
                ds.append(i["ds"])
            elif key[0] == "temp":
                temp.append(i["temp"])
            elif key[0] == "hum":
                hum.append(i["hum"])
            elif key[0] == "baro":
                pre.append(i["baro"])
        content = {"ds": ds, "temp": temp, "hum": hum, "pre": pre}
        result = json.dumps(content,ensure_ascii= False)

        self.f.write(result)
        #df = pd.DataFrame({"ds": ds, "temp": temp, "hum": hum, "pre": pre})
        #df.to_csv("demo.csv")
        #print(properties)
        #print(data)


        return item
