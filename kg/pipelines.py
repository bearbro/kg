# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re

import pymysql


class KgPipeline(object):
    def __init__(self):
        # 连接数据库
        self.connect = pymysql.connect(
            host='192.168.2.9',  # 数据库地址
            port=3356,  # 数据库端口
            db='stock_0033_original',  # 数据库名
            user='bear',  # 数据库用户名
            passwd='admin',  # 数据库密码
            charset='utf8',  # 编码方式
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor();
        self.my_NULL = '-'

    def process_item(self, item, spider):
        print("-------------------------------")
        print(spider.name)
        if spider.name == "stockcode":
            sql = """Create Table If Not Exists Stock_code(
                                    code varchar(255) binary primary key,
                                    name  varchar(255)
                                    ) DEFAULT CHARSET=utf8 """
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql = "insert into stock_code(code,name) value "
            for i in range(len(item['codes'])):
                sql += " ('%s','%s')," % (item['codes'][i], item['names'][i])
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
        elif spider.name == "company":
            print(item['Company']['c_name'])
            scrapy_time = item['Company']['scrapy_time']
            code = item['Company']['code']
            # 公司
            # 创建表格
            col_head = ['c_name', 'c_territory', 'c_name_en', 'c_industry', 'c_name_old', 'c_website',
                        'c_main_business', 'c_products', 'c_actual_controller', 'c_final_controller',
                        'c_chairman', 'c_chairman_secretary', 'c_legal_representative', 'c_general_manager',
                        'c_registered_capital', 'c_employee_count', 'c_phone', 'c_fax', 'c_postcode',
                        'c_address', 'c_introduction']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for i in col_head:
                if i == 'c_introduction':
                    sql_sub += '%s varchar(4096) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists Company(
                        %s
                        primary key(scrapy_time ,code)
                        ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join(col_head)
            sql_sub2 = ""
            for i in col_head:
                if i == 'scrapy_time':
                    sql_sub2 += "'%s', " % scrapy_time
                elif i == 'code':
                    sql_sub2 += "'%s' " % code  # code 是最后一个 不用加','
                elif i in item['Company']:
                    sql_sub2 += "'%s', " % item['Company'][i]
                else:
                    sql_sub2 += "'%s', " % self.my_NULL
            sql = """insert into Company( %s ) values (%s) """ % (sql_sub1, sql_sub2)
            # print(sql)
            try:
                self.cursor.execute(sql)
                # 提交sql语句
                self.connect.commit()
            except Exception as e:
                print(sql)
                print('error',code, e)

            # Person
            # 创建表格
            col_head = ['p_name_list', 'p_job_list', 'p_notice_date_list','p_term_date_list',
                        'p_intro_list', 'p_salary_list', 'p_shares_number_list', 'p_mainintro_list',
                        'p_mainintro_date_list', 'p_type_list']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for ci in col_head:
                i = re.sub(r'_list$', '', ci)
                if i == 'p_mainintro':
                    sql_sub += '%s varchar(4096) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists Person(
                        %s
                        primary key(scrapy_time ,code,p_name,p_intro)
                        ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join([re.sub(r'_list$', '', i) for i in col_head])
            for i in range(len(item['Person']['p_name_list'])):
                sql_sub2 = ""
                for ci in col_head:
                    if ci == 'scrapy_time':
                        sql_sub2 += "'%s', " % scrapy_time
                    elif ci == 'code':
                        sql_sub2 += "'%s' " % code
                    elif ci in item['Person']:
                        sql_sub2 += "'%s', " % item['Person'][ci][i]
                    else:
                        sql_sub2 += "'%s', " % self.my_NULL
                sql = """insert into Person( %s ) values (%s) """ % (sql_sub1, sql_sub2)

                try:
                    self.cursor.execute(sql)
                    # 提交sql语句
                    self.connect.commit()
                except Exception as e:
                    print(sql)
                    print('error',code, item['Person']['p_name_list'][i], e)

            # Stock
            # 创建表格
            col_head = ['s_established_date', 's_issue_number', 's_issue_price', 's_listing_date',
                        's_issue_price_earnings_ratio', 's_expected_fundraising', 's_first_day_opening_price',
                        's_issuance_rate', 's_actual_fundraising', 's_lead_underwriter', 's_listing_sponsor',
                        's_history']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for i in col_head:
                i = re.sub(r'_list$', '', i)
                if i == 's_history':
                    sql_sub += '%s varchar(10240) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists Stock(
                                    %s
                                    primary key(scrapy_time ,code)
                                    ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join(col_head)
            sql_sub2 = ""
            for i in col_head:
                if i == 'scrapy_time':
                    sql_sub2 += "'%s', " % scrapy_time
                elif i == 'code':
                    sql_sub2 += "'%s' " % code  # code 是最后一个 不用加','
                elif i in item['Stock']:
                    sql_sub2 += "'%s', " % item['Stock'][i]
                else:
                    sql_sub2 += "'%s', " % self.my_NULL
            sql = """insert into Stock( %s ) values (%s) """ % (sql_sub1, sql_sub2)
            try:
                self.cursor.execute(sql)
                # 提交sql语句
                self.connect.commit()
            except Exception as e:
                print(sql)
                print('error',code, e)

            # C_holding_C
            # 创建表格
            col_head = [
                'c_participating_companies_number', 'c_consolidated_statement_number',
                'c_consolidated_statement_name_list', 'c_participation_relationship_list',
                'c_participation_ratio_list', 'c_investment_amount_list',
                'c_net_profit_list', 'c_merge_report_list', 'c_main_business_list',
                'c_announcement_date']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for ci in col_head:
                i = re.sub(r'_list$', '', ci)
                if i == '____':
                    sql_sub += '%s varchar(4096) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists C_holding_C(
                        %s
                        primary key(scrapy_time ,code,c_consolidated_statement_name)
                        ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join([re.sub(r'_list$', '', i) for i in col_head])
            for i in range(len(item['C_holding_C']['c_consolidated_statement_name_list'])):
                sql_sub2 = ""
                for ci in col_head:
                    if ci == 'scrapy_time':
                        sql_sub2 += "'%s', " % scrapy_time
                    elif ci == 'code':
                        sql_sub2 += "'%s' " % code
                    elif ci in item['C_holding_C']:
                        if ci.endswith("_list"):
                            sql_sub2 += "'%s', " % item['C_holding_C'][ci][i]
                        else:
                            sql_sub2 += "'%s', " % item['C_holding_C'][ci]
                    else:
                        sql_sub2 += "'%s', " % self.my_NULL
                sql = """insert into C_holding_C( %s ) values (%s) """ % (sql_sub1, sql_sub2)

                try:
                    self.cursor.execute(sql)
                    # 提交sql语句
                    self.connect.commit()
                except Exception as e:
                    print(sql)
                    print('error',code, item['C_holding_C']['c_consolidated_statement_name_list'][i], e)
        elif spider.name == "concept":
            print(item['code'])
            scrapy_time = item['scrapy_time']
            code = item['code']
            # Concept
            # 创建表格
            col_head = ['c_nub_list','c_name_list', 'c_top3_list', 'c_analysis_list', 'c_type_list']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for ci in col_head:
                i = re.sub(r'_list$', '', ci)
                if i == 'c_analysis':
                    sql_sub += '%s varchar(4096) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists Concept(
                                    %s
                                    primary key(scrapy_time ,code,c_name)
                                    ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join([re.sub(r'_list$', '', i) for i in col_head])
            for i in range(len(item['c_name_list'])):
                sql_sub2 = ""
                for ci in col_head:
                    if ci == 'scrapy_time':
                        sql_sub2 += "'%s', " % scrapy_time
                    elif ci == 'code':
                        sql_sub2 += "'%s' " % code
                    elif ci in item:
                        sql_sub2 += "'%s', " % item[ci][i]
                    else:
                        sql_sub2 += "'%s', " % self.my_NULL
                sql = """insert into Concept( %s ) values (%s) """ % (sql_sub1, sql_sub2)

                try:
                    self.cursor.execute(sql)
                    # 提交sql语句
                    self.connect.commit()
                except Exception as e:
                    print(sql)
                    print('error', code, item['c_name_list'][i], e)

            # theme_points
            # 创建表格
            col_head = ['theme_points_name_list','theme_points_nub_list', 'theme_points_info_list']
            col_head += ['scrapy_time', 'code']
            sql_sub = ""
            for ci in col_head:
                i = re.sub(r'_list$', '', ci)
                if i == 'theme_points_info':
                    sql_sub += '%s varchar(4096) ,\n' % i
                elif i in ['scrapy_time', 'code']:
                    sql_sub += '%s varchar(255) not null ,\n' % i
                else:
                    sql_sub += '%s varchar(255) ,\n' % i
            sql = """Create Table If Not Exists Theme_points(
                                    %s
                                    primary key(scrapy_time ,code,theme_points_nub)
                                    ) DEFAULT CHARSET=utf8 """ % sql_sub
            # print(sql)
            self.cursor.execute(sql)
            # 提交sql语句
            self.connect.commit()
            # 插入数据
            sql_sub1 = ", ".join([re.sub(r'_list$', '', i) for i in col_head])
            for i in range(len(item['theme_points_name_list'])):
                sql_sub2 = ""
                for ci in col_head:
                    if ci == 'scrapy_time':
                        sql_sub2 += "'%s', " % scrapy_time
                    elif ci == 'code':
                        sql_sub2 += "'%s' " % code
                    elif ci in item:
                        sql_sub2 += "'%s', " % item[ci][i]
                    else:
                        sql_sub2 += "'%s', " % self.my_NULL
                sql = """insert into Theme_points( %s ) values (%s) """ % (sql_sub1, sql_sub2)

                try:
                    self.cursor.execute(sql)
                    # 提交sql语句
                    self.connect.commit()
                except Exception as e:
                    print(sql)
                    print('error', code, item['theme_points_nub_list'][i], e)
            
        return item  # 必须实现返回
