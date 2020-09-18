import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
import json
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
  basic_key = "//span[@data-selection-key='{}@{}']/text()"
  data_key_path = "//span[@data-selection-key]/@data-selection-key"
  teams = "//div[contains(@class, 'member-name') and contains(@class, 'nowrap')]/a/span/text()"
  time = "//td[@class='date']/text()"
  _1 = "Match_Result.1"
  _x = "Match_Result.draw"
  _2 = "Match_Result.3"
  _h1 = "To_Win_Match_With_Handicap2.HB_H"
  _h2 = "To_Win_Match_With_Handicap2.HB_A"
  _u = "Total_Goals.Under_2.5"
  _o = "Total_Goals.Over_2.5"

  def start_requests(self):
    if getattr(self,'test_match', '') != '':
      yield scrapy.Request(url=self.test_match,
        callback=self.football_match_parser,
        errback=self.errbacktest,
        meta={'root': self.test_match, 'proxy': 'p.webshare.io:20001'})

      return

    start_urls = ['https://www.marathonbet.com/en/all-events.htm?cpcids=all']

    for url in start_urls[0:1]:
      yield scrapy.Request(url=url,
        callback=self.start_page_parser,
        errback=self.errbacktest,
        meta={'root': url, 'proxy': 'p.webshare.io:20000'})

  def start_page_parser(self, response):
    #football
    football_league_urls = [x for x in response.xpath("//td/a[@class='category-label-link']/@href").extract()
      if 'betting/Football/' in x]

    for football_league_url in football_league_urls[0:5]:
      yield response.follow(url=football_league_url,
        callback=self.football_league_page_parser,
        errback=self.errbacktest,
        meta={'proxy': 'p.webshare.io:20000'})

  def football_league_page_parser(self, response):
    # extract all league name
    league_name = ' '.join(response.xpath("//h1[@class='category-label ']/span/text()").extract())
    league_id = int(re.match('.*?([0-9]+)$', response.url).group(1))

    # query or save league name from database
    league = Operations.SaveLeague({'id': league_id, 'value': league_name})

    # find all match urls
    football_match_urls = response.xpath("//a[@class='member-link']/@href").extract()

    # follow all match urls
    #for football_match_url in football_match_urls[0:1]:
    for football_match_url in football_match_urls[0:5]:

      yield response.follow(url=football_match_url,
        callback=self.football_match_parser,
        errback=self.errbacktest,
        meta={'league': league.Id, 'proxy': 'p.webshare.io:20000'})

  def football_match_parser(self, response):
    timestamp = datetime.now()
    teams = response.xpath(self.teams).extract()
    data = {}
    data['match'] = {}
    data['match']['league'] = response.meta.get('league')
    data['match']['competition'] = {}
    data['match']['competition']['id'] = int(response.url.split("+")[-1])
    data['match']['competition']['date'] = parse_date(response.xpath(self.time).extract_first().split())
    data['match']['home'] = {'value': teams[0]}
    data['match']['away'] = {'value': teams[1]}
    data['match']['competition']['collections'] = []

    # We start by searching all these data-mutable-id's in divs
    table_divs = response.xpath("//div[@data-preference-id]")
    table_divs = [x for x in table_divs if len(x.xpath(".//td[@data-mutable-id]")) > 0]

    for table in table_divs:
      unit = {}
      unit['id'] = table.xpath(".//@data-preference-id").get()
      unit['value'] = table.xpath(".//div[@class='name-field']/text()").get().strip()
      unit['headers'] = [x.strip() for x in table.xpath(".//tr/th/text()").extract()]

      table_data = table.xpath(".//table")[1].xpath(".//td[@data-sel]")
      data_mutable_ids = [x.xpath(".//@data-mutable-id").extract_first() for x in table_data]
      data_sel = [json.loads(x.xpath(".//@data-sel").extract_first()) for x in table_data]
      unit['prices'] = []
      for _id, sel in zip(data_mutable_ids, data_sel):
        item = {}
        item['id'] = _id
        item['price'] = round(float(sel['epr']), 2)
        item['sn'] = sel['sn']
        item['mn'] = sel['mn']
        item['time'] = timestamp
        unit['prices'].append(item)

      data['match']['competition']['collections'].append(unit)

    start = time()
    Operations.SaveFootball(data)
    print("Save: {}".format(time() - start))


  def errbacktest(self, failiure):
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    pass #[Operations.SaveMarathonFootball(football_match) for football_match in self.football_matches]

if __name__ == "__main__":
  process = CrawlerProcess({
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
      'LOG_LEVEL': 'ERROR'
  })

  process.crawl(RootSpider)
  process.start()
