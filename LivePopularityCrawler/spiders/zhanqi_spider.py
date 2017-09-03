# coding: utf-8
import json
from scrapy import Spider


DIRECTORIES_CSS_SELECTOR = 'div .game-bd li'
DIRECTORY_URL_SELECTOR = 'a::attr(href)'
DIRECTORY_NAME_SELECTOR = 'p::text'

class DouyuSpider(Spider):
    name = 'zhanqi'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'ZhanqiSpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'ZhanqiSpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'ZhanqiSpider:TotalLiveShow'
    }

    ZHANQI_DEFAULT_SIZE = '50'
    ZHANQI_START_PAGE = '1'
    start_urls = [
        'https://www.zhanqi.tv/games'
    ]

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()
            name = name.strip()
            def wrap_callback(response):
                for item in self.parse_directory(response, name):
                    yield item
            self.logger.info('Crawling %s url %s' %(name, url))
            yield response.follow(url, wrap_callback)

    def parse_directory(self, response, name):
        url_template = response.css(
                '.live-list-tabc::attr(data-url)').extract_first()
        url_template = url_template.replace(u'${size}',
                                self.ZHANQI_DEFAULT_SIZE)
        url = url_template.replace(u'${page}', self.ZHANQI_START_PAGE)
        def wrap(response):
            for item in self.parse_api_request(
                    response, url_template, self.ZHANQI_START_PAGE):
                yield item
        yield response.follow(url, wrap)


    def parse_api_request(self, response, url_template, page):
        try:
            data = json.loads(response.body)
            rooms = data['data']['rooms']
            for room in rooms:
                item = {}
                item['nick'] = room['nickname']
                item['url'] = response.urljoin(room['url'])
                item['class'] = room['newGameName']
                item['title'] = room['title']

                p = room['online']
                scale = 1
                if u'万' in p:
                    p = p.replace(u'万', '')
                    scale = 10000
                p = float(p) * scale
                item['popularity'] = int(p)
                yield item
                next_page = str(int(page) + 1)
                url = url_template.replace(u'${page}', next_page)
                def wrap(response):
                    for item in self.parse_api_request(
                            response, url_template, page):
                        yield item

                yield response.follow(url, wrap)
        except Exception as e:
            self.logger.warning(e)
