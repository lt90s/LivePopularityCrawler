# coding: utf-8
import json
from scrapy import Spider, Request


DIRECTORIES_CSS_SELECTOR = 'div .sort-container li'
DIRECTORY_URL_SELECTOR = 'a::attr(href)'
DIRECTORY_NAME_SELECTOR = '.cate-title::text'

class DouyuSpider(Spider):
    name = 'panda'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'PandaSpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'PandauSpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'PandaSpider:TotalLiveShow'
    }

    start_urls = [
        'https://www.panda.tv/cate'
    ]

    HOST_BASE = 'https://www.panda.tv/'

    API_URL = 'https://www.panda.tv/ajax_sort?token=&pageno={page}&pagenum=120&classification={class}'

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()
            name = name.strip()
            self.logger.info('Crawling %s url %s' %(name, url))
            print(url)
            cate = url.split('/')[-1]
            url = self.API_URL.replace('{class}', cate).replace('{page}', '1')
            request = Request(url, callback=self.api_parse)
            request.meta['cate'] = cate
            request.meta['page'] = 1
            yield request

    def api_parse(self, response):
        data = json.loads(response.body)
        data = data['data']['items']
        if not data:
            return

        for d in data:
            item = {}
            item['title'] = d['name']
            item['url'] = self.HOST_BASE + d['id']
            item['nick'] = d['userinfo']['nickName']
            item['class'] = d['classification']['cname']
            item['popularity'] = int(d['person_num'])
            print(json.dumps(item, ensure_ascii=False))
            yield item

        page = response.meta['page'] + 1
        cate = response.meta['cate']
        url = self.API_URL.replace('{class}', cate).replace('{page}', str(page))
        request = Request(url, callback=self.api_parse)
        request.meta['page'] = page
        request.meta['cate'] = cate
        yield request
