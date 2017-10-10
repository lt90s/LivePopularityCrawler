# coding: utf-8
import json
from scrapy import Spider
from datetime import datetime, timedelta

DIRECTORIES_CSS_SELECTOR = '.w-head-menu-cnt-main-item div li'
DIRECTORY_URL_SELECTOR = 'a::attr(href)'
DIRECTORY_NAME_SELECTOR = 'a::text'

class DouyuSpider(Spider):
    name = 'yy'

    custom_settings = {
        'TOPN_POPULARITY_REDIS_KEY': 'YySpider:TopNPopularity',
        'TOTAL_POPULARITY_REDIS_KEY': 'YySpider:TotalPopularity',
        'TOTAL_LIVE_SHOW_REDIS_KEY': 'YySpider:TotalLiveShow',
        'CLASSIFIED_POPULARITY_REDIS_KEY': 'YySpider:ClassifiedPopularity',
    }

    start_urls = [
        'http://www.yy.com'
    ]

    def parse(self, response):
        directories = response.css(DIRECTORIES_CSS_SELECTOR)
        for directory in directories:
            url = directory.css(DIRECTORY_URL_SELECTOR).extract_first()
            name = directory.css(DIRECTORY_NAME_SELECTOR).extract_first()
            name = name.strip()
            self.logger.info('Crawling %s url %s' %(name, url))
            yield response.follow(url, self.parse_directory)

    def parse_directory(self, response):
        for more in response.css('.w-video-module.w-video-module-type03'):
            url = more.css('.h-r a::attr(href)').extract_first()
            # try this
            if url is None:
                url = more.css('.more::attr(href)').extract_first()
            name = more.css('.w-video-module-hd span::text').extract_first()
            if name is None:
                name = more.css('.w-video-module-hd a::text').extract_first()

            if url is None and name is not None:
                #print('direct parse %s' %name)
                for item in self.direct_parse(response):
                    yield item
            elif url is not None and name is not None:
                yield response.follow(url, self.parse_directory_more)
            # yy web page format sucks!

    def direct_parse(self, response):
        name = response.css('.w-video-module-hd span::text').extract_first()
        name = name.replace(u'直播', '')
        for content in response.css('.w-video-module-videolist li'):
            title = content.css('a::attr(title)').extract_first()
            url = content.css('a::attr(href)').extract_first()
            url = response.urljoin(url)
            nick = content.css('.intro::text').extract_first()
            popularity = content.css('.usr::text').extract_first()
            if popularity is None:
                continue
            scale = 1
            if u'万' in popularity:
                popularity = popularity.replace(u'万', '')
                scale = 10000
                popularity = float(popularity) * scale
            popularity = int(popularity)
            item = {
                'class': name,
                'title': title,
                'nick': nick,
                'url': url,
                'popularity': popularity
            }
            #print(json.dumps(item, ensure_ascii=False))
            yield item

    def parse_directory_more(self, response):
        try:
            script = response.xpath('//script')
            pages = script.re('var totalPages = ([0-9]+)')[0]
            pages = int(pages)
            module_id = script.re("var moduleId = '(.*)'")[0]
            biz = script.re("var biz = '(.*)'")[0]
            sub_biz = script.re("var subBiz = '(.*)'")[0]
            url_template = 'http://www.yy.com/more/page.action?biz={biz}&subBiz={subBiz}&page={page}&moduleId={id}'

            for page in range(1, pages+1):
                page = str(page)
                url = url_template.replace('{page}', page)
                url = url.replace('{biz}', biz)
                url = url.replace('{subBiz}', sub_biz)
                url = url.replace('{id}', module_id)
                #print(url)
                yield response.follow(url, self.parse_api_request)
        except: # fall back to direct parse, fuck
            for item in self.direct_parse(response):
                yield item

    def parse_api_request(self, response):
        data = json.loads(response.body)
        rooms = data['data']['data']
        for room in rooms:
            item = {}
            start = room['startTime']
            start = datetime.utcfromtimestamp(start) + timedelta(hours=8)
            delta = datetime.now() - start
            # live show going on for 48 hours???
            if delta.days > 1:
                continue
            item['nick'] = room['name']
            item['url'] = response.urljoin(room['liveUrl'])
            try:
                item['class'] = room['newGameName']
            except: #do not handle exception further, let it fail
                item['class'] = self.subBiz2class(room['subBiz'])
            if item['class'] == u'绝地枪神':
                item['class'] = u'绝地求生'

            item['title'] = room.get('title', None) or\
                    room.get('desc') or item['nick']
            try:
                p = room['online']
            except:
                p = str(room['users'])
            scale = 1
            if u'万' in p:
                p = p.replace(u'万', '')
                scale = 10000
            p = float(p) * scale
            item['popularity'] = int(p)
            yield item

    def subBiz2class(self, subBiz):
        mapping = {
            'lol': u'英雄联盟',
            'lvyou': u'旅游',
            'minecraft': u'我的世界',
            'dance': u'热舞',
            'glory': u'王者荣耀',
            'pop': u'唱歌',
            'girl': u'女神',
            'idx': u'喊麦(WTF)',
            'nj': u'户外',
        }
        return mapping.get(subBiz) or subBiz
