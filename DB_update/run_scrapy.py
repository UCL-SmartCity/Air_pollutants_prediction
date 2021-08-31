import sys
import os
from scrapy.cmdline import execute

def r():
    sys.path.append(os.path.dirname(os.path.abspath('/Users/au_yueng/PycharmProjects/pythonProject11/weather/run_scrapy.py')))
    execute(["scrapy", "crawl", "weathers"])
