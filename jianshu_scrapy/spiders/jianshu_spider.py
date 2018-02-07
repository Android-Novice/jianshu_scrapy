import logging
import math
import datetime
import traceback
from jianshu_scrapy.items import AuthorIdItem, AuthorItem, ArticleItem, FollowerItem
from jianshu_scrapy.spiders.jianshu_orm import init_mysql, ParsingItem, get_db_session, User
from scrapy import Spider, Request

class jianshu_spider(Spider):
    name = 'jianshu_scrapy'
    allowed_domains = ["http://www.jianshu.com", "https://www.jianshu.com"]
    base_url = 'http://www.jianshu.com'
    author_base_url = base_url + '/u/'
    article_url = base_url + '/u/%s?order_by=shared_at&page=%s'
    follower_url = base_url + '/users/%s/followers?page=%s'
    article_count_per_page = 7
    follower_count_per_page = 9
    article_pageIndex_dic = {}
    follower_pageIndex_dic = {}
    __sync_request_count = 5

    recommend_page_index = 1
    recommend_base_url = base_url + '/recommendations/users?page=%s'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

    def __init__(self):
        super(jianshu_spider, self).__init__()
        init_mysql()
        self.session = get_db_session()
        list = self.session.query(User).filter(User.is_article_complete == 1 or User.is_follower_complete == 1).all()
        if list:
            for item in list:
                if item.is_article_complete == 1:
                    item.is_article_complete = 0
                if item.is_follower_complete == 1:
                    item.is_follower_complete = 0
            self.session.flush()
            self.session.commit()
        list = self.session.query(ParsingItem).filter(ParsingItem.is_parsed == 1).all()
        if list:
            for item in list:
                item.is_parsed = 0
            self.session.flush()
            self.session.commit()

    def make_requests_from_url(self, url):
        pass

    def start_requests(self):
        logging.info('<JS_progress> start request....')
        url = self.recommend_base_url % self.recommend_page_index
        yield Request(url, headers=self.headers)

    def parse(self, response):
        try:
            logging.info('<JS_progress> parse url:' + response.url)
            items = response.xpath('//div[@class="col-xs-8"]/div[@class="wrap"]')
            if len(items) > 0:
                for item in items:
                    author_url = item.xpath('a/@href').extract()[0]
                    author_id = author_url.split('/').pop()
                    parsingItem = AuthorIdItem()
                    parsingItem['author_id'] = author_id
                    logging.info('<JS><parsingitem> author_id: %s' % author_id)
                    yield parsingItem

                self.recommend_page_index += 1
                url = self.recommend_base_url % self.recommend_page_index
                yield Request(url, headers=self.headers, callback=self.parse)

                request = self.start_request_author()
                if request:
                    logging.info('<js_scrapy> start next author....4')
                    yield request
        except Exception as ex:
            logging.error('<JS><Author_Commit>parse error:\n' + repr(ex))
            logging.error(traceback.format_exc())

    def parse_author(self, response):
        try:
            logging.info('<JS_progress> parse_author url:' + response.url)
            header_part = response.xpath('//div[@class="main-top"]')
            if header_part:
                try:
                    # 头像
                    imageElm = header_part[0].xpath('a[@class="avatar"]/img/@src').extract()[0]
                    author_image = 'http:' + imageElm
                    # 名字
                    author_name = header_part[0].xpath('div[@class="title"]/a[@class="name"]/text()').extract()[0]
                    authorElem = header_part[0].xpath('div[@class="title"]/a[@class="name"]/@href').extract()[0]
                    author_url = self.base_url + authorElem
                    author_id = author_url.split('/').pop()

                    extraElms = header_part[0].xpath('div[@class="info"]/ul/li/div[@class="meta-block"]')
                    # 作者关注的人数和url
                    following_url = self.base_url + extraElms[0].xpath('a/@href').extract()[0]
                    author_following_count = int(extraElms[0].xpath('a/p/text()').extract()[0])
                    # 关注作者的人数
                    follower_url = self.base_url + extraElms[1].xpath('a/@href').extract()[0]
                    author_follower_count = int(extraElms[1].xpath('a/p/text()').extract()[0])
                    # 文章数量
                    article_url = self.base_url + extraElms[2].xpath('a/@href').extract()[0]
                    author_article_count = int(extraElms[2].xpath('a/p/text()').extract()[0])
                    # 字数
                    author_word_count = int(extraElms[3].xpath('p/text()').extract()[0])
                    # 点赞数
                    author_like_count = int(extraElms[4].xpath('p/text()').extract()[0])
                    # 个人介绍
                    author_note = \
                        response.xpath('//div[@class="description"]/div[@class="js-intro"]').xpath(
                            'string(.)').extract()
                    if author_note:
                        author_note = author_note[0]
                    else:
                        author_note = ''
                    logging.info(
                        'Author: %s,\n Following: %s, \nFollowers: %s, \nArticle: %s, \nWords: %s, \nLike: %s, \nFollowing_Url: %s, \nFollower_url: %s, \nArticle_url: %s' % (
                            author_name, author_following_count, author_follower_count, author_article_count,
                            author_word_count, author_like_count, following_url, follower_url, article_url))
                    author = AuthorItem()
                    author['id'] = author_id
                    author['image'] = author_image
                    author['name'] = self._cut_long_str(self._replace_spacial_char(author_name), 100)
                    author['url'] = author_url
                    author['following_url'] = following_url
                    author['following_count'] = author_following_count
                    author['follower_url'] = follower_url
                    author['follower_count'] = author_follower_count
                    author['article_count'] = author_article_count
                    author['word_count'] = author_word_count
                    author['like_count'] = author_like_count
                    author['note'] = self._cut_long_str(self._replace_spacial_char(author_note), 255)
                    yield author
                except Exception as error:
                    logging.error('<JS> parsing_author error:' + repr(error))
                    logging.error(traceback.format_exc())

                request = self.start_request_article()
                if request:
                    logging.info('<js_scrapy> start next author\'s article....5')
                    yield request
                request = self.start_request_follower()
                if request:
                    logging.info('<js_scrapy> start next author\'s follower....6')
                    yield request

            request = self.request_next_author()
            if request:
                logging.info('<js_scrapy> start next author....7')
                yield request
        except Exception as ex:
            logging.error('<JS><Author_Commit>parse_author error:\n' + repr(ex))
            logging.error(traceback.format_exc())

    def parse_author_error(self, failure):
        try:
            logging.error(
                '<JS_Author> response error: %s \n %s' % (failure.value.response.status, failure.value.response.url))
            status = failure.value.response.status
            url = failure.value.response.url
            author_id = url.split('/').pop()
            parsingItem = self.session.query(ParsingItem).filter(ParsingItem.author_id == author_id).first()
            if parsingItem:
                parsingItem.is_parsed = status
                self.session.flush()
                self.session.commit()
            request = self.request_next_author()
            if request:
                logging.info('<js_scrapy> start next author....7')
                yield request
        except Exception as ex:
            logging.error('<JS><Author_Commit>parse_author_error error:\n' + repr(ex))
            logging.error(traceback.format_exc())

    def parse_articles(self, response):
        try:
            logging.info('<JS_progress> parse_articles url:' + response.url)
            parsed_count = 0
            cur_author_id = ''
            cur_page_index = ''
            if '/u/' in response.url and '?order' in response.url:
                cur_author_id = response.url[response.url.index('/u/') + 3:response.url.index('?order')]
                cur_page_index = response.url[response.url.rfind('=') + 1:]
                cur_page_index = int(cur_page_index) + 1
                self.article_pageIndex_dic[cur_author_id] -= 1

                try:
                    articlesElem = response.xpath(
                        '//div[@id="list-container"]/ul[@class="note-list"]/li/div[@class="content"]')
                    author = \
                        response.xpath(
                            '//div[@class="main-top"]/div[@class="title"]/a[@class="name"]/text()').extract()[0]
                    author_id = \
                        response.xpath(
                            '//div[@class="main-top"]/div[@class="title"]/a[@class="name"]//@href').extract()[0]
                    author_id = author_id.split('/').pop()
                    if len(articlesElem):
                        for articleElem in articlesElem:
                            articleItem = self.parse_article_item(articleElem, author_id, author)
                            if articleItem:
                                parsed_count += 1
                                yield articleItem
                except Exception as error:
                    logging.error('<JS><parse_articles> error: ' + repr(error))
                    logging.error(traceback.format_exc())
            else:
                if 'users/' in response.url and '/timeline' in response.url:
                    cur_author_id = response.url[response.url.index('users/') + 6:response.url.index('/timeline')]
                else:
                    logging.error('<JS><article_warning> over, url: ' + response.url)
            if cur_author_id:
                if parsed_count > 0 and self.article_pageIndex_dic[cur_author_id] > 0:
                    url = self.article_url % (cur_author_id, cur_page_index)
                    logging.info('<js_scrapy> start next page articles....8')
                    yield Request(url, headers=self.headers, callback=self.parse_articles, dont_filter=True)
                else:
                    author = self.session.query(User).filter(User.id == cur_author_id).first()
                    if author:
                        author.is_article_complete = 2
                        self.session.commit()
                        if cur_author_id in self.article_pageIndex_dic.keys():
                            self.article_pageIndex_dic.pop(cur_author_id)
                    request = self.request_next_author_article()
                    if request:
                        logging.info('<js_scrapy> start next author\'s articles....9')
                        yield request
        except Exception as ex:
            logging.error('<JS><Author_Commit>parse_articles error:\n' + repr(ex))
            logging.error(traceback.format_exc())

    def parse_article_item(self, articleElem, author_id, author):
        try:
            urlElem = articleElem.xpath('a[@class="title"]/@href').extract()
            if len(urlElem) == 0:
                return None
            article_url = self.base_url + urlElem[0]
            article_id = article_url.split('/').pop()
            if article_id is None:
                return None
            titleElem = articleElem.xpath('a[@class="title"]').xpath('string(.)').extract()
            if len(titleElem) == 0:
                return None
            article_title = self._cut_long_str(self._replace_spacial_char(titleElem[0]), 100)
            summaryElem = articleElem.xpath('p[@class="abstract"]/text()').extract()
            if len(summaryElem) == 0:
                return None
            article_summary = self._cut_long_str(self._replace_spacial_char(summaryElem[0].strip()), 255)
            extraElems = articleElem.xpath('div[@class="meta"]/a[@target="_blank"]')
            if len(extraElems) != 2:
                return None
            read_count = extraElems[0].xpath('string(.)').extract()
            if len(read_count) > 0 and read_count[0].strip().isdecimal():
                read_count = int(read_count[0])
            else:
                return None
            comment_count = extraElems[1].xpath('string(.)').extract()
            if len(comment_count) > 0 and comment_count[0].strip().isdecimal():
                comment_count = int(comment_count[0])
            else:
                return None
            extraElems = articleElem.xpath('div[@class="meta"]/span')
            if len(extraElems) == 0:
                return None
            like_count = extraElems[0].xpath('string(.)').extract()
            if len(like_count) > 0 and like_count[0].strip().isdecimal():
                like_count = int(like_count[0])
            else:
                return None
            money_count = 0
            if len(extraElems) == 2:
                money_count = extraElems[1].xpath('string(.)').extract()
                if len(money_count) > 0 and money_count[0].strip().isdecimal():
                    money_count = int(money_count[0])
                else:
                    return None
            created_at = \
                articleElem.xpath(
                    'div[@class="author"]/div[@class="info"]/span[@class="time"]/@data-shared-at').extract()[0]
            created_at = datetime.datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S+08:00')
            print(
                'title: %s, \nsummary:%s, \nurl:%s, \ntime:%s, \nread: %s, \ncomment:%s, \nlike:%s, \nmoney:%s' % (
                    article_title, article_summary, article_url, created_at, read_count, comment_count,
                    like_count, money_count))
            articleItem = ArticleItem()
            articleItem['id'] = article_id
            articleItem['title'] = article_title
            articleItem['summary'] = article_summary
            articleItem['created_at'] = created_at
            articleItem['read_count'] = read_count
            articleItem['comment_count'] = comment_count
            articleItem['like_count'] = like_count
            articleItem['money_count'] = money_count
            articleItem['url'] = article_url
            articleItem['author_name'] = author
            articleItem['author_id'] = author_id
            return articleItem
        except Exception as ex:
            logging.error('<JS> parsing_article_item Error: ' + repr(ex))
            logging.error(traceback.format_exc())
        return None

    def parse_followers(self, response):
        try:
            logging.info('<JS_progress> parse_followers url:' + response.url)
            cur_author_id = ''
            cur_page_index = ''
            if 'users/' in response.url and '/followers' in response.url:
                cur_author_id = response.url[response.url.index('users/') + 6:response.url.index('/followers')]
                cur_page_index = response.url[response.url.rfind('=') + 1:]
                cur_page_index = int(cur_page_index) + 1
                self.follower_pageIndex_dic[cur_author_id] -= 1
                author_name = response.xpath(
                    '//div[@class="main-top"]/div[@class="title"]/a[@class="name"]/text()').extract()[0]
                followersElem = response.xpath(
                    '//div[@id="list-container"]/ul[@class="user-list"]/li/div[@class="info"]/a[@class="name"]')
                if len(followersElem):
                    for followerElem in followersElem:
                        try:
                            follower_id = followerElem.xpath('@href').extract()[0].split('/').pop()
                            follower_name = followerElem.xpath('text()').extract()[0]
                            follower_name = self._replace_spacial_char(follower_name)

                            item = FollowerItem()
                            item['follower_id'] = follower_id
                            item['follower_name'] = follower_name
                            item['following_id'] = cur_author_id
                            item['following_name'] = author_name
                            yield item

                            parsingItem = AuthorIdItem()
                            parsingItem['author_id'] = follower_id
                            yield parsingItem
                        except Exception as error:
                            logging.error('<JS><parse_followers> error: ' + repr(error))
                            logging.error(traceback.format_exc())

                request = self.start_request_author()
                if request:
                    logging.info('<js_scrapy> start next author....1')
                    yield request
            else:
                logging.error('<JS><follower_warning> over, url: ' + response.url)

            if cur_author_id:
                if self.follower_pageIndex_dic[cur_author_id] > 0:
                    url = self.follower_url % (cur_author_id, cur_page_index)
                    logging.info('<js_scrapy> start next page followers....2')
                    yield Request(url, headers=self.headers, callback=self.parse_followers, dont_filter=True)
                else:
                    author = self.session.query(User).filter(User.id == cur_author_id).first()
                    if author:
                        author.is_follower_complete = 2
                        self.session.commit()
                        if cur_author_id in self.follower_pageIndex_dic.keys():
                            self.follower_pageIndex_dic.pop(cur_author_id)
                    request = self.request_next_author_follower()
                    if request:
                        logging.info('<js_scrapy> start next author\'s followers....3')
                        yield request
        except Exception as ex:
            logging.error('<JS><Author_Commit>parse_followers error:\n' + repr(ex))
            logging.error(traceback.format_exc())

    def start_request_follower(self):
        parsing_count = self.session.query(User).filter(User.is_follower_complete == 1).count()
        need_parsing_count = self.session.query(User).filter(User.is_follower_complete == 0).count()
        if parsing_count < self.__sync_request_count and need_parsing_count > 0:
            return self.request_next_author_follower()
        return None

    def request_next_author_follower(self):
        author = self.session.query(User).filter(User.is_follower_complete == 0).limit(1).first()
        if author:
            author.is_follower_complete = 1
            self.session.commit()
            page_index = min(math.ceil(author.follower_count / self.follower_count_per_page), 150)
            self.follower_pageIndex_dic.setdefault(author.id, page_index)
            url = self.follower_url % (author.id, 1)
            logging.info('<JS_progress> request follower url:' + url)
            return Request(url, headers=self.headers, callback=self.parse_followers, dont_filter=True)
        return None

    def start_request_article(self):
        parsing_count = self.session.query(User).filter(User.is_article_complete == 1).count()
        need_parsing_count = self.session.query(User).filter(User.is_article_complete == 0).count()
        if parsing_count < self.__sync_request_count and need_parsing_count > 0:
            return self.request_next_author_article()
        return None

    def request_next_author_article(self):
        author = self.session.query(User).filter(User.is_article_complete == 0).limit(1).first()
        if author:
            author.is_article_complete = 1
            self.session.commit()
            page_index = min(math.ceil(author.article_count / self.article_count_per_page), 150)
            self.article_pageIndex_dic.setdefault(author.id, page_index)
            url = self.article_url % (author.id, 1)
            logging.info('<JS_progress> request article url:' + url)
            return Request(url, headers=self.headers, callback=self.parse_articles, dont_filter=True)
        return None

    def request_next_author(self):
        parsingItem = self.session.query(ParsingItem).filter(ParsingItem.is_parsed == 0).first()
        if parsingItem:
            author_url = self.author_base_url + parsingItem.author_id
            logging.info('<parsing_author> ' + author_url)
            parsingItem.is_parsed = 1
            self.session.flush()
            self.session.commit()
            logging.info('<JS_progress> request author url:' + author_url)
            return Request(author_url, headers=self.headers, callback=self.parse_author, dont_filter=True,
                           errback=self.parse_author_error)
        return None

    def start_request_author(self):
        parsingCount = self.session.query(ParsingItem).filter(ParsingItem.is_parsed == 1).count()
        needParsingCount = self.session.query(ParsingItem).filter(ParsingItem.is_parsed == 0).count()
        if parsingCount < self.__sync_request_count and needParsingCount > 0:
            return self.request_next_author()
        return None

    def _replace_spacial_char(self, src_text):
        str_list = list(src_text)
        index = -1
        for i in str_list:
            index += 1
            if ord(i) > 120000:
                print('****************************************************************src: %s, ord: %s' % (i, ord(i)))
                str_list[index] = ''
        new_text = ''.join(str_list)
        del str_list
        return new_text

    def _cut_long_str(self, src_text, max_len):
        if len(src_text) < max_len:
            return src_text
        return ''.join(src_text[0:max_len])
