# -*- coding:utf-8 -*-
import sys
from airport.settings import config
from airport.spider import start_crawl

if __name__ == "__main__":
    try:
        start_crawl(**config)
    except:
        print("机场数据爬虫执行异常=", sys.exc_info())
