from scrapy.cmdline import execute
import logging
import datetime
logging.basicConfig(filename='D:\crawler_%s.log' %datetime.datetime.now().strftime('%Y%m%d_%H%M%S'), level=logging.DEBUG, format='%(asctime)s - %(levelname)s -%(message)s',datefmt='%m/%d/%Y %H:%M:%S %p')
logging.debug('Test....')

execute(['scrapy', 'crawl', 'jianshu_scrapy'])