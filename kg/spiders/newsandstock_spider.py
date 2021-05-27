import datetime
import json
import os
import random
import re
import time
import scrapy
from pydispatch import dispatcher
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # 使用无头浏览器
from scrapy.http import HtmlResponse
from kg.items import CompanyItem
import sys
import pandas as pd


class newsandstock(scrapy.Spider):  # 需要继承scrapy.Spider类

    name = "news"  # 定义蜘蛛名
    code_filename = '../data/stock_code.txt'
    rootUrl = "http://basic.10jqka.com.cn"
    if 'win32' == sys.platform:
        download_path = "C:/Users/77385/Desktop/data/%s_%s" % (name, datetime.datetime.now().strftime('%Y-%m-%d'))
    else:
        download_path = "/Users/brobear/Downloads/%s_%s" % (name, datetime.datetime.now().strftime('%Y-%m-%d'))
        # download_path = "../data/%s" % name
    online = False

    deal_dir = os.path.join('C:/Users/77385/Desktop/data/', 'news_2021-01-29')

    # 实例化一个浏览器对象
    def __init__(self):
        # 初始化chrome对象
        chrome_options = Options()
        # 设置代理
        # chrome_options.add_argument("--proxy-server=http://127.0.0.1:7890")
        # 一定要注意，=两边不能有空格，不能是这样--proxy-server = http://202.20.16.82:10152
        chrome_options.add_argument('--headless')
        if 'win32' == sys.platform:
            self.browser = webdriver.Chrome(executable_path='../data/chromedriver.exe',
                                            chrome_options=chrome_options)  # win
        else:
            self.browser = webdriver.Chrome(chrome_options=chrome_options)
        # # 查看本机ip，查看代理是否起作用
        # self.browser.get("http://httpbin.org/ip")
        # print(self.browser.page_source)
        super().__init__()
        # 当爬虫关闭时scrapy会发出一个spider_closed的信息,当这个信号发出时就调用closeSpider函数关闭这个浏览器.
        dispatcher.connect(receiver=self.close, signal=scrapy.signals.spider_closed)

    # 整个爬虫结束后关闭浏览器
    def close(self, spider):
        self.browser.quit()

    def start_requests(self):  # 由此方法通过下面链接爬取页面
        # 获取股票code
        with open(self.code_filename, 'r', encoding='utf-8')as f:
            lines = f.readlines()
            codes = [line.split('\t')[0] for line in lines]
            self.code2name = dict()
            self.code2name = {line.split('\t')[0]: line.split('\t')[1][:-1] for line in lines}

        if self.deal_dir is None and not self.online:
            # 下载网页
            if not os.path.exists(self.download_path):
                os.mkdir(self.download_path)
            for code in codes:
                url = '%s/%s/%s.html?a=0' % (
                    self.rootUrl, code, self.name)  # http://basic.10jqka.com.cn/000001/news.html
                file_name = "%s/%s_%s.html" % (self.download_path, code, self.name)
                if not os.path.exists(file_name):
                    yield scrapy.Request(url=url, callback=self.download_parse)  # 爬取到的页面如何处理？提交给parse方法处理
                    time.sleep(1 + random.randint(0, 3))
                # if code in [ "000060", "000062", "000063", "000064"]:
                #     break
        # return

        #
        # todo 解析网页
        for code in codes:
            if self.online:
                url = '%s/%s/%s.html?a=0' % (self.rootUrl, code, self.name)
            else:
                url = 'file://%s/%s_%s.html' % (self.deal_dir, code, self.name)  # 本地
            yield scrapy.Request(url=url, callback=self.parse)  # 爬取到的页面如何处理？提交给parse方法处理
            # if code in ["000005", "000031", "000032", "000033", "000034"]:
            #     break
            # time.sleep(1)

    # 下载网页
    def download_parse(self, response):

        print("-------------------------------------")
        print(response.url)
        code = re.findall(r'/\d{6}/', response.url)[0][1:-1]
        print(code)
        file_name = "%s/%s_%s.html" % (self.download_path, code, self.name)
        print(file_name)
        print("-------------------------------------")
        with open(file_name, 'wb') as f:
            f.write(response.body)

    # todo 解析网页
    def parse(self, response):
        print(response.url)
        if self.online:
            code = re.findall(r'/\d{6}/', response.url)[0][1:-1]
            scrapy_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        else:
            code = re.findall(r'/\d{6}_', response.url)[0][1:-1]
            scrapy_time = time.strftime("%Y-%m-%d %H:%M:%S",
                                        time.localtime(os.path.getmtime(
                                            re.sub("^[C|c]/", 'c:/', response.url[7:], 1)
                                        )))
        # xpath  '//*[@id="linkagedata"]'
        linkagedata = response.xpath('//*[@id="linkagedata"]/text()').extract_first()
        # # 保存数据到数据库
        # yield item
        #
        # 保存
        file_dir = self.deal_dir + '_get_newsandstock'
        if len(self.code2name) > 0:
            file_dir += '(带名字)'
        if not os.path.exists(file_dir):
            os.mkdir(file_dir)
        file_path = os.path.join(file_dir, '%s_%s_newsandstock.csv' % (
        code, self.code2name[code] if code in self.code2name else ''))
        self.get_newsandstock(file_path, linkagedata)
        self.log('保存文件: %s' % file_path)  # 打个日志

    def get_newsandstock(self, file_path, linkagedata):
        data = json.loads(linkagedata)
        data2 = dict()
        for keyi in ['seq', 'ctime', 'curl', 'title', 'source', 'stocks', 'author', 'type']:
            if keyi == 'ctime':
                data2[keyi] = [time.strftime("%Y%m%d", time.localtime(i[keyi])) if keyi in i else '' for i in data]
            else:
                data2[keyi] = [str(i[keyi]) if keyi in i else '' for i in data]

        df = pd.DataFrame(data2)
        if os.path.exists(file_path):
            df2 = pd.read_csv(file_path, sep=',', encoding='utf-8', dtype=str)
            df2.fillna('', inplace=True)
            df = pd.concat([df, df2])
            df.drop_duplicates(subset=['seq', 'ctime', 'curl', 'title', 'source', 'stocks', 'author', 'type'],
                               keep='first', inplace=True)

        df.sort_values(by='ctime', inplace=True)
        df.to_csv(file_path, index=False, sep=',', encoding='utf-8')
