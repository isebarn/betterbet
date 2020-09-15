import os
import json
from sqlalchemy import ForeignKey, ForeignKeyConstraint, desc, create_engine, func, Column, SmallInteger, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker, aliased, relationship, joinedload, lazyload
import datetime
from pprint import pprint

engine = create_engine(os.environ.get('BETTING_DATABASE'), echo=False)
Base = declarative_base()

def json_object(_object):
  data = dict(_object.__dict__)
  data.pop('_sa_instance_state', None)
  return data


# Collection is a generic Market type
class Collection(Base):
  __tablename__ = 'collection'

  Id = Column('id', String, primary_key=True)
  Value = Column('value', String)

  def __init__(self, data):
    self.Id = data['id']
    self.Value = data['value']

# Market is a specific collection has a collection and headers
# and belongs to a specific competition
class Market(Base):
  __tablename__ = 'market'

  Id = Column('id', Integer, primary_key=True)
  Headers = Column('headers', ARRAY(String))
  Collection = Column('collection', String, ForeignKey('collection.id'))
  Competition = Column('competition', Integer, ForeignKey('competition.id'))
  prices = relationship('Price', lazy="joined")

  def __init__(self, data):
    self.Headers = data['headers']
    self.Collection = data['id']
    self.Competition = data['competition']

  def json(self):
    data = json_object(self)
    data['prices'] = [price.json() for price in data['prices']]
    return data

class PriceField(Base):
  __tablename__ = 'price_field'

  Id = Column('id', String, primary_key=True)
  Collection = Column('collection', String, ForeignKey('collection.id'), primary_key=True)

  def __init__(self, data):
    self.Id = data['id']
    self.Collection = data['collection']

class Price(Base):
  __tablename__ = 'price'

  Id = Column('id', Integer, primary_key=True)
  PriceField = Column('price_field', String)
  Collection = Column('collection', String)
  Market = Column('market', Integer, ForeignKey('market.id'))
  Time = Column('time', DateTime)
  Value = Column('value', Float)
  MN = Column('mn', String)
  SN = Column('sn', String)
  increments = relationship('PriceIncrement', lazy="joined")
  __table_args__ = (ForeignKeyConstraint([PriceField, Collection],
                                           ['price_field.id', 'price_field.collection']), {})

  def __init__(self, data):
    self.PriceField = data['id']
    self.Collection = data['collection']
    self.Market = data['market']
    self.Time = data['time']
    self.Value = data['price']
    self.MN = data['mn']
    self.SN = data['sn']

  def json(self):
    data = json_object(self)
    data['increments'] = [increment.json() for increment in data['increments']]
    data['CurrentPrice'] = data['Value'] + sum([x['Value'] for x in data['increments']])
    return data


class PriceIncrement(Base):
  __tablename__ = 'price_increment'

  Id = Column('id', Integer, primary_key=True)
  Price = Column('price', Integer, ForeignKey('price.id'))
  Value = Column('value', Float)
  Time = Column('time', DateTime)

  def __init__(self, data):
    self.Price = data['Id']
    self.Value = data['increment']
    self.Time = data['update']

  def json(self):
    data = json_object(self)
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
    data['markets'] = [market.json() for market in data['markets']]
    return data

class Match(Base):
  __tablename__ = 'match'

  Id = Column('id', Integer, primary_key=True)
  Competition = Column('competition', Integer, ForeignKey('competition.id'))
  Home = Column('home', Integer, ForeignKey('competitor.id'))
  Away = Column('away', Integer, ForeignKey('competitor.id'))
  competition = relationship('Competition', lazy="joined")
  home = relationship('Competitor', lazy="joined", primaryjoin= Home == Competitor.Id)
  away = relationship('Competitor', lazy="joined", primaryjoin= Away == Competitor.Id)

  def __init__(self, data):
    self.Competition = data['competition']['id']
    self.Home = data['home']['id']
    self.Away = data['away']['id']

  def json(self):
    data = json_object(self)
    data['competition'] = data['competition'].json()
    data['home'] = data['home'].json()
    data['away'] = data['away'].json()
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
    data['match'] = data['match'].json()

    return data


class Operations:
  def SaveFootball(data):
    competition = session.query(Competition).filter_by(Id=data['match']['competition']['id']).first()
    # If first time scraping, create new stuff
    if competition is None:
      Operations.SaveCompetition(data['match']['competition'])
      Operations.SaveCompetitor(data['match']['home'])
      Operations.SaveCompetitor(data['match']['away'])
      Operations.SaveMatch(data['match'])

      for collection in data['match']['competition']['collections']:
        Operations.SaveCollection(collection)

        # markets need competition
        collection['competition'] = data['match']['competition']['id']
        Operations.SaveMarket(collection)

        for price in collection['prices']:
          price['collection'] = collection['id']
          Operations.SavePriceField(price)

          price['market'] = collection['market']
          Operations.SavePrice(price)

      football = Football(data)
      session.add(football)
      session.commit()

    # now just figure out the increments
    else:
      new_prices = {x['id']:x for market in data['match']['competition']['collections'] for x in market['prices']}
      prices = {x['PriceField']: x for market in competition.json()['markets'] for x in market['prices']}

      for _id, value in new_prices.items():
        if _id in prices and new_prices[_id]['price'] != prices[_id]['CurrentPrice']:
          prices[_id]['increment'] = new_prices[_id]['price'] - prices[_id]['CurrentPrice']
          prices[_id]['increment'] = round(prices[_id]['increment'], 2)
          prices[_id]['update'] = new_prices[_id]['time']
          Operations.SavePriceIncrement(prices[_id])

      session.commit()

  def SavePriceIncrement(data):
    session.add(PriceIncrement(data))

  def SavePrice(data):
    if session.query(Price.Id
        ).filter_by(PriceField=data['id'], Market=data['market']
        ).scalar() == None:

      session.add(Price(data))

  def SavePriceField(data):
    if session.query(PriceField.Id
        ).filter_by(Id=data['id'], Collection=data['collection']
        ).scalar() == None:

      session.add(PriceField(data))

  def SaveMarket(data):
    if session.query(Market.Id
        ).filter_by(Collection=data['id'], Competition=data['competition']
        ).scalar() == None:

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



Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

#FOOTBALL_TEAM_CACHE = {team.Value: team for team in  Operations.QueryFootballTeam()}
#FOOTBALL_LEAGUE_CACHE = {league.Id: league for league in session.query(FootballLeague).all()}

def read_file(filename):
  file = open(filename, "r")
  return json.load(file)

if __name__ == "__main__":
  '''
  from test import get_data
  data = get_data()
  Operations.SaveFootball(data)
  '''




  #pprint(new_prices)
  #Operations.SaveFootball(data)
  pprint(session.query(Football).first().json())
