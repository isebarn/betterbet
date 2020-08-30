import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
from datetime import datetime
from scrapy_selenium import SeleniumRequest

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
    start_urls = ['https://www.marathonbet.com/en/all-events.htm?cpcids=all']

    for url in start_urls:
      yield scrapy.Request(url=url,
        callback=self.start_page_parser,
        errback=self.errbacktest,
        meta={'root': url})

  def scrape_float_field(self, response, data_key, field):
    field = response.xpath(self.basic_key.format(data_key, field)).extract_first(0)
    return float(field)

  def football_match_parser(self, response):

    try:
      _id = int(response.url.split("+")[-1])
      data_key = response.xpath(self.data_key_path).extract_first().split("@")[0]
      match = {}
      match['id'] = _id
      match['league_id'] = response.meta.get('league_id')
      match['home'], match['away'] = response.xpath(self.teams).extract()

      match['time'] = response.xpath(self.time).extract_first().split()
      match['time'] = parse_date(match['time'])

      match['_1'] = self.scrape_float_field(response, data_key, self._1)
      match['_x'] = self.scrape_float_field(response, data_key, self._x)
      match['_2'] = self.scrape_float_field(response, data_key, self._2)
      match['_h1'] = self.scrape_float_field(response, data_key, self._h1)
      match['_h2'] = self.scrape_float_field(response, data_key, self._h2)
      match['_u'] = self.scrape_float_field(response, data_key, self._u)
      match['_o'] = self.scrape_float_field(response, data_key, self._o)
      match['created'] = datetime.now().replace(microsecond=0)

      Operations.SaveFootball(match)

    except Exception as e:
      Operations.SaveFootballError("{}: {}".format(response.url, str(e)))

      return


  def football_league_page_parser(self, response):

    # extract all league name
    league_name = ' '.join(response.xpath("//h1[@class='category-label ']/span/text()").extract())
    league_id = int(re.match('.*?([0-9]+)$', response.url).group(1))

    # query or save league name from database
    league = Operations.GetOrCreateFootballLeague(league_id, league_name)

    # find all match urls
    football_match_urls = response.xpath("//a[@class='member-link']/@href").extract()

    # follow all match urls
    for football_match_url in football_match_urls:

      yield response.follow(url=football_match_url,
        callback=self.football_match_parser,
        errback=self.errbacktest,
        meta={'league_id': league.Id})

  def start_page_parser(self, response):
    #football
    football_league_urls = [x for x in response.xpath("//td/a[@class='category-label-link']/@href").extract()
      if 'betting/Football/' in x]

    for football_league_url in football_league_urls:
      yield response.follow(url=football_league_url,
        callback=self.football_league_page_parser,
        errback=self.errbacktest)

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
