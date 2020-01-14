import os
import random
import re
import time
import scrapy
from pydispatch import dispatcher
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # 使用无头浏览器
from scrapy.http import HtmlResponse
from kg.items import CompanyItem, ConceptItem, HolderItem
import sys


class company(scrapy.Spider):  # 需要继承scrapy.Spider类

    name = "holder"  # 定义蜘蛛名
    code_filename = '../data/stock_code.txt'
    rootUrl = "http://basic.10jqka.com.cn"
    if 'win32' == sys.platform:
        download_path = r"C:/Users/77385/Desktop/data/%s" % name
    else:
        download_path = "/Users/brobear/Downloads/%s" % name
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
                # if code in ["000031", "000032", "000033", "000034"]:
                #     break
        # return
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
            # if code in ["000050", "000051", "000052", "000053"]:
            #     break

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
        item = HolderItem()
        item['code'] = code
        item['scrapy_time'] = scrapy_time

        ##规则字典，可以从chrome获得
        xpaths_dict_h = dict()
        # 股东人数 todo 000002格式不同
        xpaths_dict_h['h_date_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[1]/tbody/tr/th/div/text()'
        # 股东人数越少，则代表筹码越集中，股价越有可能上涨
        xpaths_dict_h[
            'h_number_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/div/text()'  # 股东总人数
        xpaths_dict_h[
            'h_number_rate_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td'  # 较上期变化
        xpaths_dict_h[
            'h_number_A_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # A股东总人数
        xpaths_dict_h[
            'h_number_A_rate_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td'  # A变化
        xpaths_dict_h[
            'h_number_B_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # B股东总人数
        xpaths_dict_h[
            'h_number_H_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # H股东总人数
        xpaths_dict_h[
            'h_stock_number_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # 人均流通股数（股）
        xpaths_dict_h[
            'h_stock_number_rate_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td'  # 人均流通变化
        xpaths_dict_h[
            'h_stock_number_A_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # 人均流通a股股数（股）
        xpaths_dict_h[
            'h_stock_number_A_rate_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td'  # 人均流通A股变化
        xpaths_dict_h[
            'h_industry_avg_list'] = '//*[@id="gdrsTable"]/div/div/div[2]/table[2]/tbody/tr[x]/td/text()'  # 行业平均（户）

        xpaths_dict_h['h_th_list'] = '//*[@id="gdrsTable"]/div/div/div[1]/table[2]/tbody/tr/th/text()'
        th = list(map(lambda prod: prod.strip(),
                      response.xpath(xpaths_dict_h['h_th_list']).extract()
                      ))
        th2key = {'股东总人数(户)': 'h_number_list',
                  '较上期变化': 'h_number_rate_list',
                  'A股股东数（户）': 'h_number_A_list',
                  'A股股东数变化': 'h_number_A_rate_list',
                  'B股股东数(户)': 'h_number_B_list',
                  'H股股东数(户)': 'h_number_H_list',
                  '人均流通股(股)': 'h_stock_number_list',
                  '人均流通变化': 'h_stock_number_rate_list',
                  '人均流通A股(股)': 'h_stock_number_A_list',
                  '人均流通A股变化': 'h_stock_number_A_rate_list',
                  '行业平均(户)': 'h_industry_avg_list'}
        th_k = [th2key[i] for i in th]

        try:
            for k, v in xpaths_dict_h.items():
                if k in th2key.values():
                    if k in th_k:
                        th_idx = th_k.index(k) + 1
                        v = xpaths_dict_h[k].replace('tr[x]', 'tr[%d]' % th_idx)
                        y = list(map(lambda prod: prod.strip(),
                                     response.xpath(v).extract()
                                     ))
                        if 'rate' in k:
                            for xi in range(len(y)):
                                if 'span' in y[xi]:
                                    y[xi] = re.findall(r'-?\d+.\d+%', y[xi])[0]
                                else:
                                    y[xi] = y[xi][4:-5].strip()
                    else:
                        y = ['-'] * len(item['h_date_list'])
                else:
                    y = list(map(lambda prod: prod.strip(),
                                 response.xpath(v).extract()
                                 ))
                item[k] = y
        except Exception as e:
            print('error', code, e)

        # 十大流通股东 floating stockholder

        xpaths_dict_f_h_top10 = dict()
        #
        xpaths_dict_f_h_top10['f_h_top10_date_list'] = '//*[@id="bd_1"]/div[1]/ul/li/a/text()'
        # 持有数量(股)	持股变化(股)	占流通股比例	质押占其直接持股比 实际增减持	股份类型	持股详情
        xpaths_dict_f_h_top10[
            'f_h_top10_name_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/th/a/text()'  #
        xpaths_dict_f_h_top10[
            'f_h_top10_stock_number_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_f_h_top10[
            'f_h_top10_stock_rate_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]'  #
        xpaths_dict_f_h_top10[
            'f_h_top10_stock_percent_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_f_h_top10[
            'f_h_top10_pledge_percent_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_f_h_top10[
            'f_h_top10_stock_actual_up_down_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]'
        xpaths_dict_f_h_top10[
            'f_h_top10_stock_type_list'] = '//*[@id="fher_1"]/table/tbody[1]/tr/td[x]/text()'
        # 持股详情 todo
        date_list = list(map(lambda prod: prod.strip(),
                             response.xpath(xpaths_dict_f_h_top10['f_h_top10_date_list']).extract()
                             ))

        th2key = {
            '机构或基金名称': 'f_h_top10_name_list',
            '持有数量(股)': 'f_h_top10_stock_number_list',
            '持股变化(股)': 'f_h_top10_stock_rate_list',
            '占流通股比例': 'f_h_top10_stock_percent_list',
            '质押占其直接持股比': 'f_h_top10_pledge_percent_list',
            '实际增减持': 'f_h_top10_stock_actual_up_down_list',
            '股份类型': 'f_h_top10_stock_type_list',
            '持股详情': '',
            '机构成本估算(元)': ''}

        try:
            idx = 0
            for datei in date_list:
                idx += 1
                xpaths_dict_f_h_top10['f_h_top10_th_list'] = '//*[@id="fher_%d"]/table/thead/tr/th/text()' % idx
                th = list(map(lambda prod: prod.strip(),
                              response.xpath(xpaths_dict_f_h_top10['f_h_top10_th_list']).extract()
                              ))
                th_k = []
                for thi in th:
                    if len(thi) == 0:
                        continue
                    if thi not in th2key:
                        th_k.append("")
                    else:
                        th_k.append(th2key[thi])
                for k, v in xpaths_dict_f_h_top10.items():
                    if k in th2key.values():
                        if k in th_k:
                            v = v.replace('fher_1', 'fher_%d' % idx)
                            idx2 = th_k.index(k)
                            v = v.replace('td[x]', 'td[%d]' % idx2)
                            y = list(map(lambda prod: prod.strip(),
                                         response.xpath(v).extract()
                                         ))
                            if k == 'f_h_top10_stock_rate_list':
                                x = y
                                for i in range(len(x)):
                                    if '不变' in x[i]:
                                        y[i] = '不变'
                                    elif '新进' in x[i]:
                                        y[i] = '新进'
                                    else:
                                        y[i] = re.findall(r'>-?\d+.\d+[万|亿]*<', x[i])[0][1:-1]
                            elif k == 'f_h_top10_stock_actual_up_down_list':
                                x = y
                                for i in range(len(x)):
                                    if '不变' in x[i]:
                                        y[i] = '不变'
                                    elif '新进' in x[i]:
                                        y[i] = '新进'
                                    elif '%<' in x[i]:
                                        y[i] = re.findall(r'>-?\d+.\d+%<', x[i])[0][1:-1]
                                    else:  # 限售股流通
                                        y[i] = re.findall(r'>[^%><]*</', x[i])[0][1:-2]
                        else:
                            k2 = 'f_h_top10_name_list'
                            v2 = xpaths_dict_f_h_top10[k2]
                            v2 = v2.replace('fher_1', 'fher_%d' % idx)
                            idx2 = th_k.index(k2)
                            v2 = v2.replace('td[x]', 'td[%d]' % idx2)
                            x = list(map(lambda prod: prod.strip(),
                                         response.xpath(v2).extract()
                                         ))
                            y = ['-'] * len(x)  # todo
                    elif k in ['f_h_top10_date_list', 'f_h_top10_th_list']:
                        continue
                    else:
                        v = v.replace('fher_1', 'fher_%d' % idx)
                        y = list(map(lambda prod: prod.strip(),
                                     response.xpath(v).extract()
                                     ))

                    item[k] = item.get(k, []) + y
                if len(y) > 10:
                    print(k)
                k = 'f_h_top10_date_list'
                cc = len(y)
                item[k] = item.get(k, []) + [datei] * cc
                k = 'f_h_top10_order_list'
                item[k] = item.get(k, []) + list(range(1, cc + 1))

        except Exception as e:
            print('error', code, e)

        # 十大股东

        xpaths_dict_h_top10 = dict()
        #
        xpaths_dict_h_top10['h_top10_date_list'] = '//*[@id="bd_0"]/div[1]/ul/li/a/text()'
        # 持有数量(股)	持股变化(股)	占流通股比例	质押占其直接持股比 实际增减持	股份类型	持股详情
        xpaths_dict_h_top10[
            'h_top10_name_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/th/a/text()'  #
        xpaths_dict_h_top10[
            'h_top10_stock_number_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_h_top10[
            'h_top10_stock_rate_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]'  #
        xpaths_dict_h_top10[
            'h_top10_stock_percent_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_h_top10[
            'h_top10_pledge_percent_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]/text()'
        xpaths_dict_h_top10[
            'h_top10_stock_actual_up_down_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]'
        xpaths_dict_h_top10[
            'h_top10_stock_type_list'] = '//*[@id="ther_1"]/table/tbody[1]/tr/td[x]/text()'  #
        # 持股详情 todo
        date_list = list(map(lambda prod: prod.strip(),
                             response.xpath(xpaths_dict_h_top10['h_top10_date_list']).extract()
                             ))

        th2key = {
            '机构或基金名称': 'h_top10_name_list',
            '持有数量(股)': 'h_top10_stock_number_list',
            '持股变化(股)': 'h_top10_stock_rate_list',
            '占总股本比例': 'h_top10_stock_percent_list',
            '质押占其直接持股比': 'h_top10_pledge_percent_list',
            '实际增减持': 'h_top10_stock_actual_up_down_list',
            '股份类型': 'h_top10_stock_type_list',
            '持股详情': ''}

        try:
            idx = 0
            for datei in date_list:
                idx += 1
                xpaths_dict_h_top10['h_top10_th_list'] = '//*[@id="ther_%d"]/table/thead/tr/th/text()' % idx
                th = list(map(lambda prod: prod.strip(),
                              response.xpath(xpaths_dict_h_top10['h_top10_th_list']).extract()
                              ))
                th_k = []
                for thi in th:
                    if len(thi) == 0:
                        continue
                    if thi not in th2key:
                        th_k.append("")
                    else:
                        th_k.append(th2key[thi])
                if code == "000021":
                    print(1)
                for k, v in xpaths_dict_h_top10.items():
                    if k in th2key.values():
                        if k in th_k:
                            v = v.replace('ther_1', 'ther_%d' % idx)
                            idx2 = th_k.index(k)
                            v = v.replace('td[x]', 'td[%d]' % idx2)
                            y = list(map(lambda prod: prod.strip(),
                                         response.xpath(v).extract()
                                         ))
                            if k == 'h_top10_stock_rate_list':
                                x = y
                                for i in range(len(x)):
                                    if '不变' in x[i]:
                                        y[i] = '不变'
                                    elif '新进' in x[i]:
                                        y[i] = '新进'
                                    else:
                                        y[i] = re.findall(r'>-?\d+.\d+[万|亿]*<', x[i])[0][1:-1]
                            elif k == 'h_top10_stock_actual_up_down_list':
                                x = y
                                for i in range(len(x)):
                                    if '不变' in x[i]:
                                        y[i] = '不变'
                                    elif '新进' in x[i]:
                                        y[i] = '新进'
                                    elif '%<' in x[i]:
                                        y[i] = re.findall(r'>-?\d+.\d+%<', x[i])[0][1:-1]
                                    else:  # 限售股流通
                                        y[i] = re.findall(r'>[^%><]*</', x[i])[0][1:-2]
                        else:
                            k2 = 'h_top10_name_list'
                            v2 = xpaths_dict_h_top10[k2]
                            v2 = v2.replace('ther_1', 'ther_%d' % idx)
                            idx2 = th_k.index(k2)
                            v2 = v2.replace('td[x]', 'td[%d]' % idx2)
                            x = list(map(lambda prod: prod.strip(),
                                         response.xpath(v2).extract()
                                         ))
                            y = ['-'] * len(x)  # todo
                    elif k in ['h_top10_date_list', 'h_top10_th_list']:
                        continue
                    else:
                        v = v.replace('ther_1', 'ther_%d' % idx)
                        y = list(map(lambda prod: prod.strip(),
                                     response.xpath(v).extract()
                                     ))

                    item[k] = item.get(k, []) + y
                k = 'h_top10_date_list'
                cc = len(y)
                item[k] = item.get(k, []) + [datei] * cc
                k = 'h_top10_order_list'
                item[k] = item.get(k, []) + list(range(1, cc + 1))

        except Exception as e:
            print('error', code, e)
        # 十大债券持有人
        # todo
        # 控股层级关系
        # todo

        # 保存数据到数据库
        yield item

        # # 保存
        # filename = 'xxx'
        # with open(filename, 'wb') as f:  # python文件操作，不多说了；
        #     f.write(response.body)  # 刚才下载的页面去哪里了？response.body就代表了刚才下载的页面！
        # self.log('保存文件: %s' % filename)  # 打个日志
