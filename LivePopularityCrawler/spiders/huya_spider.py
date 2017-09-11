# coding: utf-8
import json
from scrapy import Spider

BASE_URL = 'http://www.huya.com'

DIRECTORIES_CSS_SELECTOR = '.js-responded-list.narrow a'
DIRECTORY_URL_SELECTOR = 'a::attr(href)'
DIRECTORY_NAME_SELECTOR = 'h3::text'


def ignore_exception(func):
    def wrap(*args, **kwargs):
        try:
            for item in func(*args, **kwargs):
                yield item
        except:
            pass

    return wrap

class HuyaSpider(Spider):
    name = 'huya'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'HuyaSpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'HuyaSpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'HuyaSpider:TotalLiveShow',
        'CLASSIFIED_POPULARITY_REDIS_KEY': 'HuyaSpider:ClassifiedPopularity',
    }

    start_urls = [
        'http://www.huya.com/g'
    ]

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()

            self.logger.info('Crawling %s url %s' %(name, url))
            yield response.follow(url, self.parse_directory)



    @ignore_exception
    def parse_directory(self, response):
        pages = response.css('#js-list-page::attr(data-pages)').extract_first()
        id = response.xpath('//script').re("GID += +'(.*)'")[0]
        url_template = 'http://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&gameId={id}&tagAll=0&page={page}'
        url = url_template.replace('{id}', id)
        for page in range(1, int(pages)+1):
            url = url.replace('{page}', str(page))
            yield response.follow(url, self.api_parse)


    def api_parse(self, response):
        data = json.loads(response.body)
        data = data['data']['datas']
        for d in data:
            item = {}
            item['url'] = BASE_URL + '/' + d['privateHost']
            item['nick'] = d['nick']
            item['popularity'] = int(d['totalCount'])
            item['class'] = d['gameFullName']
            item['title'] = d['roomName']
            yield item

