import scrapy
import js2xml
import json
import xmltodict
from scrapy.http import Request
from weather.items import WeatherItem
import pandas as pd

class WeathersSpider(scrapy.Spider):
    name = 'weathers'
    result=[]
    allowed_domains = ['https://www.timeanddate.com/']
    start_urls = ['https://www.timeanddate.com/weather/uk/london/historic']


    '''
    def start_requests(self):
        months = range(1,7)
        year = 2021
        #urls=[]
        #item=[]

        for month in months:
            url='https://www.timeanddate.com/weather/uk/london/historic?month={}&year={}'.format(month,year)
            yield Request(url, callback=self.parse)
            #print(url)
    #start_urls = urls
    '''

    def parse(self, response):
        dataName = "data"
        scripts = response.xpath('//script/text()').extract()
        script = list(filter(lambda x: (dataName in x), scripts))[0]
        xml = js2xml.parse(script, encoding='utf-8', debug=False)
        xml_tree = js2xml.pretty_print(xml)
        xmlparse = xmltodict.parse(xml_tree)
        #jsonstr = json.dumps(xmlparse, indent=1)
        for obj in xmlparse["program"]["var"]["object"]["property"]:
            if obj["@name"]=="detail":
                detail=obj

        properties=[]
        for data in detail["array"]["object"]:
            for obj in data["property"]:
                property={}
                keys=list(obj.keys())
                item = WeatherItem()
                if (keys[1] == "string"):
                    property[obj["@name"]] = obj["string"]
                elif (keys[1] == "number"):
                    property[obj["@name"]] = obj["number"]["@value"]
                elif (keys[1] == "boolean"):
                    property[obj["@name"]] = str(obj["boolean"])
                properties.append(property)
        #yield properties
        item["property"] = properties


        #print(properties)
        yield item




        #print(properties)
        #print(properties)
        #keys = list(xmlparse["program"]["var"]["object"]["property"].keys())
        #print(detail["array"]["object"][0])
        #print(xml)


        #pass
