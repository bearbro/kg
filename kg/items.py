# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class KgItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class HolderItem(scrapy.Item):
    h_date_list = scrapy.Field()
    h_number_list = scrapy.Field()
    h_number_rate_list = scrapy.Field()
    h_stock_number_list = scrapy.Field()
    h_stock_number_rate_list = scrapy.Field()
    h_industry_avg_list = scrapy.Field()

    f_h_top10_date_list = scrapy.Field()
    f_h_top10_name_list = scrapy.Field()
    f_h_top10_stock_number_list = scrapy.Field()
    f_h_top10_stock_rate_list = scrapy.Field()
    f_h_top10_stock_percent_list = scrapy.Field()
    f_h_top10_stock_actual_up_down_list = scrapy.Field()
    f_h_top10_stock_type_list = scrapy.Field()
    f_h_top10_order_list = scrapy.Field()

    h_top10_date_list = scrapy.Field()
    h_top10_name_list = scrapy.Field()
    h_top10_stock_number_list = scrapy.Field()
    h_top10_stock_rate_list = scrapy.Field()
    h_top10_stock_percent_list = scrapy.Field()
    h_top10_stock_actual_up_down_list = scrapy.Field()
    h_top10_stock_type_list = scrapy.Field()
    h_top10_order_list = scrapy.Field()

    code = scrapy.Field()
    scrapy_time = scrapy.Field()


class ConceptItem(scrapy.Item):
    c_nub_list = scrapy.Field()
    c_name_list = scrapy.Field()
    c_top3_list = scrapy.Field()
    c_analysis_list = scrapy.Field()
    c_type_list = scrapy.Field()
    code = scrapy.Field()
    scrapy_time = scrapy.Field()
    theme_points_name_list = scrapy.Field()
    theme_points_nub_list = scrapy.Field()
    theme_points_info_list = scrapy.Field()


class CompanyItem(scrapy.Item):
    # define the fields for your item here like:
    Company = scrapy.Field()
    Person = scrapy.Field()
    Stock = scrapy.Field()
    C_holding_C = scrapy.Field()


class StockcodeItem(scrapy.Item):
    codes = scrapy.Field()
    names = scrapy.Field()
