import os
import random
import re
import time
import scrapy
from pydispatch import dispatcher
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # 使用无头浏览器
from scrapy.http import HtmlResponse
from kg.items import CompanyItem, ConceptItem
import sys


class company(scrapy.Spider):  # 需要继承scrapy.Spider类

    name = "concept"  # 定义蜘蛛名
    code_filename = '../data/stock_code.txt'
    rootUrl = "http://basic.10jqka.com.cn"
    if 'win32' == sys.platform:
        download_path = r"C:/Users/77385/Desktop/data/%s" % name
    else:
        download_path = "/Users/brobear/Downloads/%s2" % name
        # download_path = "../data/%s" % name
    online = False

    # 实例化一个浏览器对象
    def __init__(self):
        # 初始化chrome对象
        chrome_options = Options()
        # 设置代理
        # chrome_options.add_argument("--proxy-server=http://202.20.16.82:10152")
        # 一定要注意，=两边不能有空格，不能是这样--proxy-server = http://202.20.16.82:10152
        chrome_options.add_argument('--headless')
        if 'win32' == sys.platform:
            self.browser = webdriver.Chrome(executable_path='../data/chromedriver.exe',
                                            chrome_options=chrome_options)  # win
        else:
            self.browser = webdriver.Chrome(chrome_options=chrome_options)
        # 查看本机ip，查看代理是否起作用
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

        if not self.online:
            # 下载网页
            if not os.path.exists(self.download_path):
                os.mkdir(self.download_path)
            for code in codes:
                url = '%s/%s/%s.html' % (self.rootUrl, code, self.name)
                file_name = "%s/%s_%s.html" % (self.download_path, code, self.name)
                if not os.path.exists(file_name):
                    yield scrapy.Request(url=url, callback=self.download_parse)  # 爬取到的页面如何处理？提交给parse方法处理
                if code in ["000031", "000032", "000033", "000034"]:
                    break
        return
        # TODO
        # if not self.online:
        #     cls.custom_settings={'kg.SeleniumMiddleware.CommonMiddleware': None}
        #     self.update_settings(new_setting)
        #     print(self.settings['DOWNLOADER_MIDDLEWARES'])
        # 解析网页
        for code in codes:
            if self.online:
                url = '%s/%s/%s.html' % (self.rootUrl, code, self.name)
            else:
                url = 'file://%s/%s_%s.html' % (self.download_path, code, self.name)  # 本地
            yield scrapy.Request(url=url, callback=self.parse)  # 爬取到的页面如何处理？提交给parse方法处理
            if code in ["000031", "000032", "000033", "000034"]:
                break

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
        time.sleep(1 + random.randint(0, 20))

    # 解析网页
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
        item = ConceptItem()
        ##规则字典，可以从chrome获得
        xpaths_dict_c = dict()
        ## 常规概念
        xpaths_dict_c['c_name_list'] = '(//div[@id="concept"]//table[@class="gnContent"])[1]/tbody/tr/td[2]/text()'
        item['c_name_list'] = list(map(lambda prod: prod.strip(),
                                       response.xpath(xpaths_dict_c['c_name_list']).extract()
                                       ))
        item['c_top3_list'] = []
        item['c_analysis_list'] = []
        idx = -1
        try:
            for i in range(len(item['c_name_list'])):
                idx += 2
                c_top3_xpath = '(//div[@id="concept"]//table[@class="gnContent"])[1]/tbody/tr[%d]/td[3]/a/text()' % idx
                c_top3 = list(map(lambda prod: prod.strip(),
                                  response.xpath(c_top3_xpath).extract()
                                  ))
                item['c_top3_list'].append(','.join(c_top3))
                c_analysis_xpath = 'string((//div[@id="concept"]//table[@class="gnContent"])[1]/tbody/tr[%d]/td)' % (
                            idx + 1)
                c_analysis = response.xpath(c_analysis_xpath).extract_first().strip()
                if len(c_analysis) == 0:
                    c_analysis_xpath = 'string((//div[@id="concept"]//table[@class="gnContent"])[1]/tbody/tr[%d]/td[4])' % idx
                    c_analysis = response.xpath(c_analysis_xpath).extract_first().strip()
                item['c_analysis_list'].append(c_analysis)
            item['c_type_list'] = ['general'] * len(item['c_name_list'])
        except Exception as e:
            print('error', code, e)

        ## 其他概念
        xpaths_dict_c['c_name_list'] = '(//div[@id="other"]//table[@class="gnContent"])[1]/tbody/tr/td[2]/text()[1]'
        c_name_list_other = list(map(lambda prod: prod.strip(),
                                     response.xpath(xpaths_dict_c['c_name_list']).extract()
                                     ))
        item['c_name_list'] += c_name_list_other
        idx = -1
        try:
            for i in range(len(c_name_list_other)):
                idx += 2
                item['c_top3_list'].append('')
                c_analysis_xpath = 'string((//div[@id="other"]//table[@class="gnContent"])[1]/tbody/tr[%d]/td)' % (
                        idx + 1)
                c_analysis = response.xpath(c_analysis_xpath).extract_first().strip()
                if len(c_analysis) == 0:
                    c_analysis_xpath = 'string((//div[@id="other"]//table[@class="gnContent"])[1]/tbody/tr[%d]/td[3])' % idx
                    c_analysis = response.xpath(c_analysis_xpath).extract_first().strip()
                item['c_analysis_list'].append(c_analysis)
            item['c_type_list'] += ['other'] * len(c_name_list_other)
        except Exception as e:
            print('error', code, e)

        item['code'] = code
        item['scrapy_time'] = scrapy_time

        if len(item['c_name_list']) != len(item['c_top3_list']) or \
                len(item['c_name_list']) != len(item['c_analysis_list']):
            print('error', code)
            return
        theme_points_name = '//*[@id="material"]/div[2]/div[@class="gntc"]/div/span/span/text()'
        nub_name= list(map(lambda prod: prod.strip(),
                                                  response.xpath(theme_points_name).extract()
                                                  ))
        item['theme_points_name_list']=[ i.split(":")[1] for i in nub_name]
        item['theme_points_nub_list'] = [ i.split(":")[0] for i in nub_name]
        theme_points_info = '//*[@id="material"]/div[2]/div[@class="gntc"]/div/div[2]/text()'
        item['theme_points_info_list'] = list(map(lambda prod: prod.strip(),
                                                  response.xpath(theme_points_info).extract()
                                                  ))[::2]

        # 保存数据到数据库
        yield item

        # # 保存
        # filename = 'xxx'
        # with open(filename, 'wb') as f:  # python文件操作，不多说了；
        #     f.write(response.body)  # 刚才下载的页面去哪里了？response.body就代表了刚才下载的页面！
        # self.log('保存文件: %s' % filename)  # 打个日志
