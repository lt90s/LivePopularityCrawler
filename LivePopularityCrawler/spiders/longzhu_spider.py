# coding: utf-8
import json
from scrapy import Spider, Request


DIRECTORIES_CSS_SELECTOR = 'div .list-item'
DIRECTORY_URL_SELECTOR = 'div .list-item-thumb a::attr(href)'
DIRECTORY_NAME_SELECTOR = 'div .list-item-tit a::attr(title)'

class LongzhuSpider(Spider):
    name = 'longzhu'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'LongzhuSpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'LongSpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'LongzhuSpider:TotalLiveShow',
        'CLASSIFIED_POPULARITY_REDIS_KEY': 'LongzhuSpider:ClassifiedPopularity',
    }

    start_urls = [
        'http://longzhu.com/games/?from=topbarallgames'
    ]

    HOST_BASE = 'https://www.panda.tv/'

    API_URL = 'http://api.plu.cn/tga/streams?max-results=18&start-index={page}&sort-by=top&filter=0&game={game_id}'

    ITEM_PER_PAGE = 18

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()
            name = name.strip()
            self.logger.info('Crawling %s url %s' %(name, url))
            def wrap_callback(response):
                for item in self.parse_directory(response, name):
                    yield item
            yield response.follow(url, wrap_callback)

    def parse_directory(self, response, name):
        for script in response.xpath('//script'):
            game_id = script.re('.*var gameId = "([0-9]+)"')
            if game_id:
                game_id = game_id[0]
                break
        if not game_id:
            self.logger.info('Failed to extract game_id for category %s' %name)
            return

        url = self.API_URL.format(page=0, game_id=game_id)
        request = Request(url, callback=self.api_parse)
        request.meta['page'] = 0
        request.meta['game_id'] = game_id
        yield request

    def api_parse(self, response):
        data = json.loads(response.body)
        data = data['data']

        for d in data['items']:
            item = {}
            item['title'] = d['channel']['status']
            item['url'] = d['channel']['url']
            item['nick'] = d['channel']['name']
            item['class'] = d['game'][0]['Name']
            item['popularity'] = int(d['viewers'])
            print(json.dumps(item, ensure_ascii=False))
            yield item

        page = response.meta['page'] + 1
        game_id = response.meta['game_id']
        total_page = (data['totalItems'] + self.ITEM_PER_PAGE - 1) / self.ITEM_PER_PAGE
        if page >= total_page:
            return

        url = self.API_URL.format(page=0, game_id=game_id)
        request = Request(url, callback=self.api_parse)
        request.meta['page'] = page
        request.meta['game_id'] = game_id
        yield request
