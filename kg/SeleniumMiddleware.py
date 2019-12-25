import time
import random

from scrapy.http import HtmlResponse


class CommonMiddleware(object):
    '''
    Middleware中的类设置
    '''
    def process_request(self, request, spider):
        spider.browser.get(request.url)
        print("do_js")
        time.sleep(1)
        return HtmlResponse(url=spider.browser.current_url, body=spider.browser.page_source, encoding="gb18030",
                            request=request)
