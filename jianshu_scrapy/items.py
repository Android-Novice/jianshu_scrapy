# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class AuthorIdItem(scrapy.Item):
    author_id = scrapy.Field()

class AuthorItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    following_count = scrapy.Field()
    following_url = scrapy.Field()
    follower_count = scrapy.Field()
    follower_url = scrapy.Field()
    article_count = scrapy.Field()
    word_count = scrapy.Field()
    like_count = scrapy.Field()
    image = scrapy.Field()
    note = scrapy.Field()
    url = scrapy.Field()

class ArticleItem(scrapy.Item):
    id = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    created_at = scrapy.Field()
    read_count = scrapy.Field()
    comment_count = scrapy.Field()
    like_count = scrapy.Field()
    money_count = scrapy.Field()
    url = scrapy.Field()
    author_name = scrapy.Field()
    author_id = scrapy.Field()

class FollowerItem(scrapy.Item):
    following_id = scrapy.Field()
    following_name = scrapy.Field()
    follower_id = scrapy.Field()
    follower_name = scrapy.Field()
