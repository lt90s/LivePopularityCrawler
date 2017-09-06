# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import os

import redis
import json

logger = logging.getLogger(__name__)

class LivepopularitycrawlerPipeline(object):
    def process_item(self, item, spider):
        pass


class PipelineBase(object):
    def __init__(self):
        self.url_set = set()

    def duplicate_items(self, item):
        url = item['url']
        if url in self.url_set:
            return True
        else:
            self.url_set.add(url)
            return False

class RedisBasedPipeline(PipelineBase):
    def __init__(self, spider):
        super(RedisBasedPipeline, self).__init__()
        redis_settings = spider.settings.get('REDIS', {})
        host = redis_settings.get('host', 'localhost')
        port = redis_settings.get('port', 6379)
        db = redis_settings.get('db', 0)
        password = redis_settings.get('password', None)
        redis_client = redis.StrictRedis(
                host=host, port=port, db=db, password=password)

        try:
            redis_client.get('foo')
        except Exception as e:
            logger.critical(e)
            os._exit(-1)
        self.redis_client = redis_client


class RedisTopNPopularityPipeline(RedisBasedPipeline):
    '''Top N most popular live show
    The result is store in redis as zset, You can set the key[TOPN_REDIS_KEY]
    in settings.py, default key name is 'topn'.
    N can be set via TOPN in settings.py, default value is 10
    '''

    def __init__(self, topn, topn_key, spider):
        super(RedisTopNPopularityPipeline, self).__init__(spider)
        self.topn = topn
        self.topn_key = topn_key
        self.zset_size = 0
        self.redis_client.delete(topn_key)

    def process_item(self, item, spider):
        if not self.duplicate_items(item):
            json_item = json.dumps(item)
            self.redis_client.zadd(self.topn_key, item['popularity'], json_item)
            if self.zset_size < self.topn:
                self.zset_size += 1
            else:
                # remove smallest
                self.redis_client.zremrangebyrank(self.topn_key, 0, 0)
        return item

    @classmethod
    def from_crawler(cls, spider):
        return cls(
            spider.settings.get('TOPN', 10),
            spider.settings.get('TOPN_POPULARITY_REDIS_KEY', 'TopNPopularity'),
            spider)


class RedisTotalPopularityPipeline(RedisBasedPipeline):
    '''Total popularity of all live shows'''
    def __init__(self, total_key, spider):
        super(RedisTotalPopularityPipeline, self).__init__(spider)
        self.total_key = total_key
        self.redis_client.set(self.total_key, 0)

    def process_item(self, item, spider):
        if not self.duplicate_items(item):
            self.redis_client.incrby(self.total_key, item['popularity'])
            self.redis_client.get(self.total_key)
        return item

    @classmethod
    def from_crawler(cls, spider):
        return cls(
            spider.settings.get(
                'TOTAL_POPULARITY_REDIS_KEY', 'TotalPopularity'),
            spider)



class RedisTotalLiveShowPipeline(RedisBasedPipeline):
    '''Total popularity of all live shows'''
    def __init__(self, total_key, spider):
        super(RedisTotalLiveShowPipeline, self).__init__(spider)
        self.total_key = total_key
        self.redis_client.set(self.total_key, 0)

    def process_item(self, item, spider):
        if not self.duplicate_items(item):
            self.redis_client.incr(self.total_key)
            return item

    @classmethod
    def from_crawler(cls, spider):
        return cls(
            spider.settings.get(
                'TOTAL_LIVE_SHOW_REDIS_KEY', 'TotalLiveShow'),
            spider)



