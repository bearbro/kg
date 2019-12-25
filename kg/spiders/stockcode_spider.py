import random
import time

import scrapy

# 获取股票code
from kg.items import StockcodeItem


class stockcode(scrapy.Spider):  # 需要继承scrapy.Spider类

    name = "stockcode"  # 定义蜘蛛名
    rootUrl = "http://quote.cfi.cn/quotelist.aspx"  # 中财网

    def start_requests(self):  # 由此方法通过下面链接爬取页面
        index = 1
        url = '%s?sortcol=stockcode&sortway=asc&pageindex=%d&sectypeid=1' % (self.rootUrl, index)
        yield scrapy.Request(url=url, callback=self.parse)  # 爬取到的页面如何处理？提交给parse方法处理

    def parse(self, response):
        code_name = response.css('table.table_data tr>td>nobr>a::text').extract()
        item = StockcodeItem()
        item['codes'] = code_name[::2]
        item['names'] = code_name[1::2]
        # 处理name的ST
        item['names'] = [name.replace('*ST', '').replace('ST', '').replace('S', '') for name in
                         code_name[1::2]]

        # 写入磁盘
        filename = '../data/stock_code.txt'
        with open(filename, 'a') as f:  # python文件操作
            for i in range(0, len(code_name), 2):
                f.write('%s\t%s\n' % (code_name[i], code_name[i + 1]))
        self.log('保存到文件: %s' % filename)  # 打个日志

        # 保存数据
        yield item
        texts = response.css('div.pagestr a::text').extract()
        if '下一页' in texts:
            index = texts.index('下一页')
            next_page = self.rootUrl + response.css('div.pagestr a::attr(href)').extract()[index]
            next_page = response.urljoin(next_page)
            # 每爬一个网页的列表随机等待1+秒
            time.sleep(2 + 4 * random.random())
            yield scrapy.Request(next_page, callback=self.parse)

# 股票名字前面有个N是新股上市首日的名称前都会加一个字母N，即英文NEW的意思。
#
# 1、ST，这是对连续两个会计年度都出现亏损的公司施行的特别处理。ST即为亏损股。
#
# 2、*ST，是连续三年亏损，有退市风险的意思，购买这样的股票要有比较好的基本面分析能力。
#
# 3、S*ST，指公司经营连续三年亏损，进行退市预警和还没有完成股改。
#
# 4、SST，指公司经营连续二年亏损进行的特别处理和还没有完成股改。
#
# 5、S，还没有进行或完成股改的股票。
#
# 6、NST，经过重组或股改重新恢复上市的ST股。
#
# 7、PT，退市的股票。
