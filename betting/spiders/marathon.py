import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
from datetime import datetime

if __name__ == '__main__':
  from ORM import Operations

else:
  from betting.spiders.ORM import Operations

MONTHS = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
MONTH_DICT = {value:idx for idx, value in enumerate(MONTHS)}

def parse_date(date_list):
  date = datetime.now()
  if len(date_list) == 1:
    hour, minute = date_list[0].split(":")
    return datetime(date.year, date.month, date.day, int(hour), int(minute), 0)

  elif len(date_list) == 3:
    day, month, time = date_list
    month = MONTH_DICT[month]
    hour, minute = time.split(":")

    return datetime(date.year, month, int(day), int(hour), int(minute), 0)

  return date

class RootSpider(scrapy.Spider):
  name = "root"
  football_matches = []

  def start_requests(self):
    start_urls = ['https://www.marathonbet.com/en/all-events.htm?cpcids=all']

    for url in start_urls:
      yield scrapy.Request(url=url,
        callback=self.start_page_parser,
        errback=self.errbacktest,
        meta={'root': url})

  def start_page_parser(self, response):
    #football
    football_urls = [x for x in response.xpath("//td/a[@class='category-label-link']/@href").extract()
      if 'betting/Football/' in x]

    for football_url in football_urls:
      yield response.follow(url=football_url,
        callback=self.football,
        errback=self.errbacktest)

  def football(self, response):
    events = response.xpath("//div[@data-event-treeid]")

    matches = []
    for event in events:
      try:
        match = {}
        match['id'] = event.xpath(".//@data-event-treeid").extract_first()
        match['home'], match['away'] = event.xpath(".//span[@data-member-link]/text()").extract()

        match['time'] = event.xpath(".//td[@class='date']/text()").extract_first().split()
        match['time'] = parse_date(match['time'])

        match['_1'], match['_x'], match['_2'] = event.xpath(
          ".//span[@data-selection-price]/text()").extract()[0:3]

        match['created'] = datetime.now().replace(microsecond=0)

        self.football_matches.append(match)
      except Exception as e:
        print(response.url, e)


  def errbacktest(self, failiure):
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    [Operations.SaveMarathonFootball(football_match) for football_match in self.football_matches]

if __name__ == "__main__":
  process = CrawlerProcess({
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
      'LOG_LEVEL': 'ERROR'
  })

  process.crawl(RootSpider)
  process.start()
