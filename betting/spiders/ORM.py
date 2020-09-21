import os
import json
from sqlalchemy import ForeignKey, ForeignKeyConstraint, desc, create_engine, func, Column, SmallInteger, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker, aliased, relationship, joinedload, lazyload
import datetime
from pprint import pprint

#engine = create_engine("postgres://betting:betting123@192.168.1.35:5432/betting", echo=False)
engine = create_engine(os.environ.get('BETTING_DATABASE'), echo=False)
Base = declarative_base()

def json_object(_object):
  data = dict(_object.__dict__)
  data.pop('_sa_instance_state', None)
  return data

def json_child_list(data, name):
  if name in data:
    data[name] = [_object.json() for _object in data[name]]

def json_child_object(data, name):
  if name in data:
    data[name] = data[name].json()

# Collection is a generic Market type
class Collection(Base):
  __tablename__ = 'collection'

  Id = Column('id', String, primary_key=True)
  Value = Column('value', String)

  def __init__(self, data):
    self.Id = data['id']
    self.Value = data['value']

  def json(self):
    data = json_object(self)
    return data

# Market is a specific collection has a collection and headers
# and belongs to a specific competition
class Market(Base):
  __tablename__ = 'market'

  Id = Column('id', Integer, primary_key=True)
  Headers = Column('headers', ARRAY(String))
  Collection = Column('collection', String, ForeignKey('collection.id'))
  Competition = Column('competition', Integer, ForeignKey('competition.id'))
  price_fields = relationship('PriceField', lazy="joined")
  collection = relationship("Collection", lazy="joined")

  def __init__(self, data):
    self.Headers = data['headers']
    self.Collection = data['id']
    self.Competition = data['competition']

  def json(self):
    data = json_object(self)
    json_child_list(data, 'prices')
    json_child_object(data, 'collection')
    return data

class PriceField(Base):
  __tablename__ = 'price_field'

  Id = Column('id', BigInteger, primary_key=True)
  Market = Column('market', Integer, ForeignKey('market.id'))
  MN = Column('mn', String)
  SN = Column('sn', String)
  prices = relationship('Price', lazy="joined")

  def __init__(self, data):
    self.Id = data['price_field_id']
    self.Market = data['market']
    self.MN = data['mn']
    self.SN = data['sn']

  def json(self):
    data = json_object(self)
    json_child_list(data, 'prices')

    return data

class Price(Base):
  __tablename__ = 'price'

  Id = Column('id', BigInteger, primary_key=True)
  Epoch = Column('time_id', BigInteger, primary_key=True)
  PriceField = Column('price_field', Integer, ForeignKey('price_field.id'))
  Value = Column('value', Float)
  Time = Column('time', DateTime)

  def __init__(self, data):
    self.Id = data['price_id']
    self.Epoch = int(data['time'].timestamp())
    self.PriceField = data['price_field_id']
    self.Value = data['price']
    self.Time = data['time']

  def json(self):
    data = json_object(self)
    #json_child_list(data, 'increments')
    #data['CurrentPrice'] = data['Value'] + sum([x['Value'] for x in data['increments']])
    return data

class Competitor(Base): # Fullham, Serena Williams
  __tablename__ = 'competitor'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, data):
    self.Value = data['value']

  def json(self):
    data = json_object(self)
    return data

class Competition(Base):
  __tablename__ = 'competition'

  Id = Column('id', Integer, primary_key=True)
  Date = Column('time', DateTime)
  markets = relationship('Market', lazy="joined")

  def __init__(self, data):
    self.Id = data['id']
    self.Date = data['date']

  def json(self):
    data = json_object(self)
    json_child_list(data, 'markets')

    return data

class Match(Base):
  __tablename__ = 'match'

  Id = Column('id', Integer, primary_key=True)
  Competition = Column('competition', Integer, ForeignKey('competition.id'))
  Home = Column('home', Integer, ForeignKey('competitor.id'))
  Away = Column('away', Integer, ForeignKey('competitor.id'))
  League = Column('league', Integer, ForeignKey('league.id'))
  competition = relationship('Competition', lazy="joined")
  home = relationship('Competitor', lazy="joined", primaryjoin= Home == Competitor.Id)
  away = relationship('Competitor', lazy="joined", primaryjoin= Away == Competitor.Id)

  def __init__(self, data):
    self.Competition = data['competition']['id']
    self.Home = data['home']['id']
    self.Away = data['away']['id']
    self.League = data['league']

  def json(self):
    data = json_object(self)
    json_child_object(data, 'competition')
    json_child_object(data, 'home')
    json_child_object(data, 'away')
    return data

class League(Base):
  __tablename__ = 'league'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)
  matches = relationship('Match', lazy="joined")

  def __init__(self, data):
    self.Id = data['id']
    self.Value = data['value']

  def json(self):
    data = json_object(self)
    json_child_list(data, 'matches')
    return data

class Football(Base):
  __tablename__ = 'football'

  Id = Column('id', Integer, primary_key=True)
  Match = Column('match', Integer, ForeignKey('match.id'))
  match = relationship('Match', lazy="joined")

  def __init__(self, data):
    self.Match = data['match']['id']

  def json(self):
    data = json_object(self)
    json_child_object(data, 'match')

    return data


class Operations:
  def SaveAndUpdateCollectionsAndPrices(data):
    for collection in data['match']['competition']['collections']:
      Operations.SaveCollection(collection)

      # markets need competition
      collection['competition'] = data['match']['competition']['id']
      Operations.SaveMarket(collection)

      for price in collection['prices']:
        price['collection'] = collection['id']
        price['market'] = collection['market']
        Operations.SavePriceField(price)

        Operations.SavePrice(price)

    session.commit()

  def SaveLeague(data):
    league = session.query(League).filter_by(Id=data['id']).first()
    if league == None:
      league = League(data)
      session.add(league)
      session.commit()

    return league

  def SaveFootball(data):
    competition = session.query(Competition).filter_by(Id=data['match']['competition']['id']).first()
    # If first time scraping, create new stuff
    if competition is None:
      Operations.SaveCompetition(data['match']['competition'])
      Operations.SaveCompetitor(data['match']['home'])
      Operations.SaveCompetitor(data['match']['away'])
      Operations.SaveMatch(data['match'])

      Operations.SaveAndUpdateCollectionsAndPrices(data)

      football = Football(data)
      session.add(football)
      session.commit()

    else:
      Operations.SaveAndUpdateCollectionsAndPrices(data)



  def SavePrice(data):
    if session.query(Price).filter_by(Id=data['price_id']).scalar() == None:
      session.add(Price(data))

    else:
      prices = session.query(PriceField).get(data['price_field_id']).json()['prices']
      prices.sort(key=lambda x: x['Id'])

      if round(prices[0]['Value'],2) != round(data['price'],2):
        pprint(data)
        #session.add(Price(data))


  def SavePriceField(data):
    if session.query(PriceField.Id
        ).filter_by(Id=data['price_field_id']).scalar() == None:

      session.add(PriceField(data))
      session.flush()

  def SaveMarket(data):
    market = session.query(Market).filter_by(Collection=data['id'], Competition=data['competition']).first()
    if market == None:
      market = Market(data)
      session.add(market)
      session.flush()

    data['market'] = market.Id


  def SaveCollection(data):
    if session.query(Collection.Id).filter_by(Id=data['id']).scalar() == None:
      session.add(Collection(data))

  def SaveMatch(match_data):
    match = session.query(Match).filter_by(Competition=match_data['competition']['id']).first()
    if match == None:
      match = Match(match_data)
      session.add(match)
      session.flush()

    match_data['id'] = match.Id

  def SaveCompetition(competition_data):
    competition = session.query(Competition).filter_by(Id=competition_data['id']).first()
    if competition == None:
      session.add(Competition(competition_data))

  def SaveCompetitor(competitor_data):
    competitor = session.query(Competitor).filter_by(Value=competitor_data['value']).first()
    if competitor == None:
      competitor = Competitor(competitor_data)
      session.add(competitor)
      session.flush()

    competitor_data['id'] = competitor.Id

  def QueryFootball():
    return list(
      map(Football.json,
          session.query(Football)
          .options(lazyload('match.competition.markets'))
          .all()))

  def QueryLeagueList():
    return list(
      map(League.json,
        session.query(League)
        .options(lazyload('matches'))
        .all()))

  def QueryLeague(league, start_date = None, end_date = None):
    return list(map(Match.json, session.query(Match
      ).options(lazyload('competition.markets')
      ).filter_by(League=league).all()))

  def QueryCompetition(competition):
    return session.query(Competition
      ).options(lazyload('markets.prices')
      ).get(competition).json()

  def QueryPrices(market, collection):
    return list(map(Price.json, session.query(Price
      ).filter_by(Market=market, Collection=collection
      ).all()))

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


def read_file(filename):
  file = open(filename, "r")
  return json.load(file)

def options_table(data):
  for price in data:
    price_value = price['Value']
    day = price['Time'].day
    yield {'Value': price_value, 'Time': price['Time'], 'SN': price['SN'], 'increment': 0}
    for increment in price['increments']:
      if increment['Value'] == 0: continue
      price_value += increment['Value']
      yield {
        'Value': price_value,
        'Time': increment['Time'],
        'increment': increment['Value'],
        'Day': increment['Time'].day != day
        }
      day = increment['Time'].day

if __name__ == "__main__":
  data = session.query(PriceField).get(-1489739615).json()['prices']
  pprint(data)
  pass
  ''' MG431_-635082837Football 229
  from test import get_data
  data = get_data()
  Operations.SaveFootball(data)
  '''
  #data = Operations.QueryPrices(2271, 'MG636_-48400775Football')
  #pprint(list(val_options_table(data)))
  #pprint(Operations.QueryLeague(21520, datetime.datetime.now()))
  #pprint(Operations.QueryCompetition(10103623))
  #data = Operations.QueryPrices(7712, 'MG652_-678920046Football')
  #data = list(options_table(data))
  #pprint(data)


  # view increments for a collection
  # select * from price_field pf join price p on p.price_field = pf.id join price_increment pi on pi.price = p.id where pf.collection = 