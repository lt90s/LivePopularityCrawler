# coding: utf-8

from scrapy import Spider

BASE_URL = 'https://www.douyu.com'

DIRECTORIES_CSS_SELECTOR = '#live-list-content a'
DIRECTORY_URL_SELECTOR = 'a::attr(href)'
DIRECTORY_NAME_SELECTOR = 'p::text'

class DouyuSpider(Spider):
    name = 'douyu'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'DouyuSpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'DouyuSpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'DouyuSpider:TotalLiveShow',
        'CLASSIFIED_POPULARITY_REDIS_KEY': 'DouyuSpider:ClassifiedPopularity',
    }

    start_urls = [
        'https://www.douyu.com/directory'
    ]

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()

            self.logger.info('Crawling %s url %s' %(name, url))
            yield response.follow(url, self.parse_directory)




    def parse_directory(self, response):
        pages = response.xpath('//script').re('count: "(.*)"')[0]
        pages = int(pages)
        param = '?page={page}&isAjax=1'
        for page in range(1, pages+1):
            p = param.replace('{page}', str(page))
            yield response.follow(p, self.do_parse)


    def do_parse(self, response):
        for r in response.css('li'):
            item = {}
            item['title'] = r.css('a::attr(title)').extract_first()
            item['url'] = response.urljoin(r.css('a::attr(href)').extract_first())
            s = r.css('div span::text').extract()
            try:
                item['class'] = s[0]
                item['nick'] = s[1]
                item['popularity'] = s[2]
            except:
                continue
            p = item['popularity']
            scale = 1
            if u'万' in p:
                p = p.replace(u'万', '')
                scale = 10000
            p = float(p) * scale
            item['popularity'] = int(p)
            yield item

