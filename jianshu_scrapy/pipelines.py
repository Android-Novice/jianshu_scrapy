# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import traceback

from jianshu_scrapy.items import AuthorIdItem, AuthorItem, ArticleItem, FollowerItem
from jianshu_scrapy.spiders.jianshu_orm import get_db_session, ParsingItem, User, Article, Follower
from scrapy.exceptions import DropItem

class FilterParsingItemPipeline(object):
    def open_spider(self, spider):
        self.session = get_db_session()

    def process_item(self, item, spider):
        if isinstance(item, AuthorIdItem):
            author_id = item['author_id']
            parsingItem = self.session.query(ParsingItem).filter(ParsingItem.author_id == author_id).first()
            if parsingItem is None:
                parsingItem = ParsingItem(author_id)
                self.session.add(parsingItem)
                self.session.commit()
                self.session.flush()
            raise DropItem('this is parsingitem, handled! ')
        else:
            return item

    def close_spider(self, spider):
        self.session.close()
        self.session.prune()
        del self.session

class FilterAuthorItemPipeline(object):
    def open_spider(self, spider):
        self.session = get_db_session()

    def process_item(self, item, spider):
        if isinstance(item, AuthorItem):
            author_id = item['id']
            user = self.session.query(User).filter(User.id == author_id).first()
            try:
                if user is None:
                    user = User()
                    user.article_count = item['article_count']
                    user.follower_count = item['follower_count']
                    user.follower_url = item['follower_url']
                    user.following_count = item['following_count']
                    user.following_url = item['following_url']
                    user.id = author_id
                    user.image = item['image']
                    user.like_count = item['like_count']
                    user.name = item['name']
                    user.note = item['note']
                    user.url = item['url']
                    user.word_count = item['word_count']
                    if user.follower_count:
                        user.is_follower_complete = 2
                    if user.article_count:
                        user.is_article_complete = 2
                    self.session.add(user)
                    self.session.flush()
                    self.session.commit()
                parsingItem = self.session.query(ParsingItem).filter(ParsingItem.author_id == author_id).first()
                if parsingItem:
                    parsingItem.is_parsed = 2
                    self.session.flush()
                    self.session.commit()
            except Exception as ex:
                logging.error('<JS><Author_Commit>commit author error:\n' + repr(ex))
                logging.error(traceback.format_exc())
                self.session.rollback()
            raise DropItem('handled author: %s' % author_id)
        else:
            return item

    def close_spider(self, spider):
        self.session.close()
        self.session.prune()
        del self.session

class FilterArticleItemPipeline(object):
    def open_spider(self, spider):
        self.session = get_db_session()

    def process_item(self, item, spider):
        if isinstance(item, ArticleItem):
            id = item['id']
            try:
                article = self.session.query(Article).filter(Article.id == id).first()
                if article is None:
                    article = Article(item['id'], item['title'], item['summary'], item['url'], item['created_at'],
                                      item['read_count'], item['comment_count'], item['like_count'], item['money_count'],
                                      item['author_name'])
                    article.author_id = item['author_id']

                    self.session.add(article)
                    self.session.flush()
                    self.session.commit()
            except Exception as error:
                logging.error('<JS><Article_Commit>commit author error:\n' + repr(error))
                logging.error(traceback.format_exc())
                self.session.rollback()
            raise DropItem('handled article: %s' % id)
        else:
            return item

    def close_spider(self, spider):
        self.session.close()
        self.session.prune()
        del self.session

class FilterFollowerItemPipeline(object):
    def open_spider(self, spider):
        self.session = get_db_session()

    def process_item(self, item, spider):
        if isinstance(item, FollowerItem):
            id = item['follower_id']
            try:
                follower = self.session.query(Follower).filter(Follower.follower_id == id).first()
                if follower is None:
                    follower = Follower(id, item['follower_name'], item['following_name'])
                    follower.following_id = item['following_id']
                    self.session.add(follower)
                    self.session.flush()
                    self.session.commit()
            except Exception as error:
                logging.error('<JS><Follower_Commit>commit author error:\n' + repr(error))
                logging.error(traceback.format_exc())
                self.session.rollback()
            raise DropItem('handled follower: %s' % id)
        else:
            return item

    def close_spider(self, spider):
        self.session.close()
        self.session.prune()
        del self.session
