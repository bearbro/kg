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


class company(scrapy.Spider):  # 需要继承scrapy.Spider类

    name = "company"  # 定义蜘蛛名
    code_filename = '../data/stock_code.txt'
    rootUrl = "http://basic.10jqka.com.cn"
    if 'win32' == sys.platform:
        download_path = "C:/Users/77385/Desktop/data/%s2" % name
    else:
        download_path = "/Users/brobear/Downloads/%s" % name
        # download_path = "../data/%s" % name
    online = False

    # 实例化一个浏览器对象
    def __init__(self):
        # 初始化chrome对象
        chrome_options = Options()
        # 设置代理
        chrome_options.add_argument("--proxy-server=http://202.20.16.82:10152")
        # 一定要注意，=两边不能有空格，不能是这样--proxy-server = http://202.20.16.82:10152
        chrome_options.add_argument('--headless')
        if 'win32' == sys.platform:
            self.browser = webdriver.Chrome(executable_path='../data/chromedriver.exe',
                                            chrome_options=chrome_options)  # win
        else:
            self.browser = webdriver.Chrome(chrome_options=chrome_options)
        # 查看本机ip，查看代理是否起作用
        self.browser.get("http://httpbin.org/ip")
        print(self.browser.page_source)
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
                if code in ["000002", "000031", "000032", "000033", "000034"]:
                    break
        return
        #TODO
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
            # if code in ["000002", "000031", "000032", "000033", "000034"]:
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
                                           re.sub("^[C|c]/",'c:/',response.url[7:],1)
                                        )))
        item = CompanyItem()
        ##规则字典，可以从chrome获得
        xpaths_dict_c = dict()
        ## 公司详情
        xpaths_dict_c['c_name'] = '//div[@class="bd"]/table/tbody/tr[1]/td[2]/span/text()'
        xpaths_dict_c['c_territory'] = '//div[@class="bd"]/table/tbody/tr[1]/td[3]/span/text()'
        xpaths_dict_c['c_name_en'] = '//div[@class="bd"]/table/tbody/tr[2]/td[1]/span/text()'
        xpaths_dict_c['c_industry'] = '//div[@class="bd"]/table/tbody/tr[2]/td[2]/span/text()'
        xpaths_dict_c['c_name_old'] = '//div[@class="bd"]/table/tbody/tr[3]/td[1]/span/text()'
        xpaths_dict_c['c_website'] = '//div[@class="bd"]/table/tbody/tr[3]/td[2]/span/a/text()'
        xpaths_dict_c['c_main_business'] = '//div[@class="bd"]/div/table/tbody/tr[1]/td/span/text()'
        xpaths_dict_c['c_products'] = 'string(//div[@class="bd"]/div/table/tbody/tr[2]/td/span)'
        # 控股股东  Selenium+Headless Chrome
        xpaths_dict_c['c_shareholder'] = '//div[@class="bd"]/div/table/tbody/tr[3]/td/div/text()'
        xpaths_dict_c['c_actual_controller'] = 'string(//div[@class="bd"]/div/table/tbody/tr[4]/td/div/span)'
        xpaths_dict_c['c_final_controller'] = 'string(//div[@class="bd"]/div/table/tbody/tr[5]/td/div/span)'
        xpaths_dict_c['c_chairman'] = '//div[@class="bd"]/div/table/tbody/tr[6]/td[1]/span/a/text()'
        xpaths_dict_c['c_chairman_secretary'] = '//div[@class="bd"]/div/table/tbody/tr[6]/td[2]/span/a/text()'
        xpaths_dict_c['c_legal_representative'] = '//div[@class="bd"]/div/table/tbody/tr[6]/td[3]/span/a/text()'
        xpaths_dict_c['c_general_manager'] = '//div[@class="bd"]/div/table/tbody/tr[7]/td[1]/span/a/text()'
        xpaths_dict_c['c_registered_capital'] = '//div[@class="bd"]/div/table/tbody/tr[7]/td[2]/span/text()'
        xpaths_dict_c['c_employee_count'] = '//div[@class="bd"]/div/table/tbody/tr[7]/td[3]/span/text()'
        xpaths_dict_c['c_phone'] = '//div[@class="bd"]/div/table/tbody/tr[8]/td[1]/span/text()'
        xpaths_dict_c['c_fax'] = '//div[@class="bd"]/div/table/tbody/tr[8]/td[2]/span/text()'
        xpaths_dict_c['c_postcode'] = '//div[@class="bd"]/div/table/tbody/tr[8]/td[3]/span/text()'
        xpaths_dict_c['c_address'] = '//div[@class="bd"]/div/table/tbody/tr[9]/td/span/text()'
        xpaths_dict_c['c_introduction'] = '//div[@class="bd"]/div/table/tbody/tr[10]/td/p/text()'
        filter_char = re.compile('\\t|\\n|、')
        item['Company'] = dict()
        for k, v in xpaths_dict_c.items():
            try:
                if k == 'c_products':
                    # item[k] = list(map(lambda prod: filter_char.sub('', prod).
                    # strip(), response.xpath(v).extract()))#list
                    item['Company'][k] = response.xpath(v).extract_first().strip() \
                        .replace('\n', '').replace(' ', '').replace('\t', '')
                else:
                    item['Company'][k] = response.xpath(v).extract_first().strip()
            except Exception as e:
                print('error', code, k, e)
        item['Company']['code'] = code
        item['Company']['scrapy_time'] = scrapy_time
        ##高管
        # #name
        # names=response.xpath('//*[@id="ml_001"]/table/tbody/tr/td[@class="tc name"]/a/text()').extract()
        # #assignment
        # assignments=response.xpath('//*[@id="ml_001"]/table/tbody/tr/td[@class="tl"]/text()').extract()
        # shares_numbers=response.xpath('//*[@id="ml_001"]/table/tbody/tr/td/div/span/text()').extract()
        item_P = dict()
        div_ids = ['ml_001', 'ml_002', 'ml_003']
        p_tages = ['董事会', '监事会', '高管']
        for i in range(len(div_ids)):
            div_id = div_ids[i]
            xpaths_dict_p = dict()
            xpaths_dict_p['p_name_list'] = '//div[@id="%s"]/table/tbody/tr/td[@class="tc name"]' \
                                           '/div[contains(@class,"person_table")]/table/thead/tr/td/h3/text()' % div_id
            xpaths_dict_p['p_job_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                          '/table/thead/tr/td[@class="jobs"]/text()' % div_id
            xpaths_dict_p['p_date_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                           '/table/thead/tr/td[@class="date"]/text()' % div_id
            # age sex degree
            xpaths_dict_p['p_intro_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                            '/table/thead/tr/td[@class="intro"]/text()' % div_id
            xpaths_dict_p['p_salary_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                             '/table/thead/tr/td[@class="salary"]/text()' % div_id
            xpaths_dict_p['p_shares_number_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                                    '/table/thead/tr/td/div/span[2]/text()' % div_id
            xpaths_dict_p['p_mainintro_list'] = '//div[@id="%s"]//div[contains(@class,"person_table")]' \
                                                '//td[@class="mainintro"]/div/p/text()' % div_id
            for k, v in xpaths_dict_p.items():
                try:
                    if k == 'p_date_list':
                        item_k = list(map(lambda prod: prod.strip(), response.xpath(v).extract()))
                        item_P['p_notice_date_list'] = item_P.get('p_notice_date_list', []) + item_k[::2]
                        item_P['p_term_date_list'] = item_P.get('p_term_date_list', []) + item_k[1::2]
                    elif k == 'p_mainintro_list':
                        item_k = list(map(lambda prod: prod.strip(), response.xpath(v).extract()))
                        item_P['p_mainintro_list'] = item_P.get('p_mainintro_list', []) + item_k[::2]
                        item_P['p_mainintro_date_list'] = item_P.get('p_mainintro_date_list', []) + item_k[1::2]
                    else:
                        item_P[k] = item_P.get(k, []) + list(
                            map(lambda prod: prod.strip(), response.xpath(v).extract()))
                except Exception as e:
                    print('error', code, k, e)
            item_P['p_type_list'] = item_P.get('p_type_list', []) + [p_tages[i]] * len(item_P['p_mainintro_list'])
        item['Person'] = item_P
        ##发行相关
        xpaths_dict_ci = dict()
        xpaths_dict_ci['s_established_date'] = '//*[@id="publish"]/div[2]/table/tbody/tr[1]/td[1]/span/text()'
        xpaths_dict_ci['s_issue_number'] = '//*[@id="publish"]/div[2]/table/tbody/tr[1]/td[2]/span/text()'
        xpaths_dict_ci['s_issue_price'] = '//*[@id="publish"]/div[2]/table/tbody/tr[1]/td[3]/span/text()'
        xpaths_dict_ci['s_listing_date'] = '//*[@id="publish"]/div[2]/table/tbody/tr[2]/td[1]/span/text()'
        xpaths_dict_ci['s_issue_price_earnings_ratio'] = '//*[@id="publish"]/div[2]/table/tbody/tr[2]/td[2]/span/text()'
        xpaths_dict_ci['s_expected_fundraising'] = '//*[@id="publish"]/div[2]/table/tbody/tr[2]/td[3]/span/text()'
        xpaths_dict_ci['s_first_day_opening_price'] = '//*[@id="publish"]/div[2]/table/tbody/tr[3]/td[1]/span/text()'
        xpaths_dict_ci['s_issuance_rate'] = '//*[@id="publish"]/div[2]/table/tbody/tr[3]/td[2]/span/text()'
        xpaths_dict_ci['s_actual_fundraising'] = '//*[@id="publish"]/div[2]/table/tbody/tr[3]/td[3]/span/text()'
        xpaths_dict_ci['s_lead_underwriter'] = '//*[@id="publish"]/div[2]/table/tbody/tr[4]/td/div[1]/span/text()'
        xpaths_dict_ci['s_listing_sponsor'] = '//*[@id="publish"]/div[2]/table/tbody/tr[4]/td/div[2]/span/text()'
        xpaths_dict_ci['s_history'] = 'string(//*[@id="publish"]/div[2]/table/tbody/tr[5]/td/p[2])'
        item['Stock'] = dict()
        for k, v in xpaths_dict_ci.items():
            try:
                if k == 's_history':
                    item['Stock'][k] = response.xpath(v).extract_first().strip().replace(u'\u3000', ' ')
                    if len(item['Stock'][k]) == 0:
                        v2 = 'string(//*[@id="publish"]/div[2]/table/tbody/tr[5]/td/p[1])'
                        item['Stock'][k] = response.xpath(v2).extract_first().strip().replace(u'\u3000', ' ')
                else:
                    item['Stock'][k] = response.xpath(v).extract_first().strip()
            except Exception as e:
                print('error', code, k, e)
        # 参股控股公司

        xpaths_dict_sc = dict()
        xpaths_dict_sc['c_participating_companies_number'] = '//*[@id="ckg_table"]/caption/span/strong[1]/text()'
        xpaths_dict_sc['c_consolidated_statement_number'] = '//*[@id="ckg_table"]/caption/span/strong[2]/text()'
        xpaths_dict_sc['c_consolidated_statement_name_list'] = '//*[@id="ckg_table"]/tbody/tr/td[2]/p/text()'
        xpaths_dict_sc['c_participation_relationship_list'] = '//*[@id="ckg_table"]/tbody/tr/td[3]/text()'
        xpaths_dict_sc['c_participation_ratio_list'] = '//*[@id="ckg_table"]/tbody/tr/td[4]/text()'
        xpaths_dict_sc['c_investment_amount_list'] = '//*[@id="ckg_table"]/tbody/tr/td[5]/text()'
        xpaths_dict_sc['c_net_profit_list'] = '//*[@id="ckg_table"]/tbody/tr/td[6]/text()'
        xpaths_dict_sc['c_merge_report_list'] = '//*[@id="ckg_table"]/tbody/tr/td[7]/text()'
        xpaths_dict_sc['c_main_business_list'] = '//*[@id="ckg_table"]/tbody/tr/td[8]/text()'
        xpaths_dict_sc['c_announcement_date'] = '//*[@id="ckg_table"]/caption/div/span/text()'
        item['C_holding_C'] = dict()
        for k, v in xpaths_dict_sc.items():
            try:
                if 'list' in k:
                    item['C_holding_C'][k] = [i.strip() for i in response.xpath(v).extract()]
                else:
                    item['C_holding_C'][k] = response.xpath(v).extract_first().strip()
            except Exception as e:
                print('error', code, k, e)

        # 保存数据到数据库
        yield item
        #
        # # 保存
        # filename = 'xxx'
        # with open(filename, 'wb') as f:  # python文件操作，不多说了；
        #     f.write(response.body)  # 刚才下载的页面去哪里了？response.body就代表了刚才下载的页面！
        # self.log('保存文件: %s' % filename)  # 打个日志
