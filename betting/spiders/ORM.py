import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import sessionmaker, aliased, relationship, joinedload, lazyload
from datetime import datetime
from pprint import pprint

engine = create_engine(os.environ.get('BETTING_DATABASE'), echo=False)
Base = declarative_base()

class FootballError(Base):
  __tablename__ = 'football_error'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, value):
    self.Value = value

class FootballTeam(Base):
  __tablename__ = 'football_team'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, value):
    self.Value = value

  def json_object(self):
    data = dict(self.__dict__)
    data.pop('_sa_instance_state', None)
    return data

class FootballOdds(Base):
  __tablename__ = 'football_odds'
  keys = ['_1','_x','_2','_h1','_h2','_u','_o']

  Id = Column('id', Integer, primary_key=True)
  Match = Column('match', Integer, ForeignKey('football_match.id'))
  _1 = Column('_1', Integer)
  _X = Column('_x', Integer)
  _2 = Column('_2', Integer)
  _H1 = Column('_h1', Integer)
  _H2 = Column('_h2', Integer)
  _U = Column('_u', Integer)
  _O = Column('_o', Integer)
  Time = Column('time', DateTime)
  match = relationship('FootballMatch', lazy="joined", back_populates="odds")

  def __init__(self, match_data, match):
    self.normalize_data_to_integers(match_data, match)
    self.Match = match_data['id']
    self._1 = match_data['_1']
    self._X = match_data['_x']
    self._2 = match_data['_2']
    self._H1 = match_data['_h1']
    self._H2 = match_data['_h2']
    self._U = match_data['_u']
    self._O = match_data['_o']
    self.Time = match_data['created']

  def json_object(self):
    data = dict(self.__dict__)
    data['Time'] = data['Time'].isoformat()
    data.pop('_sa_instance_state', None)
    data.pop('match', None)
    return data

  def normalize_data_to_integers(self, match_data, match):
    for key in self.keys:
      # sum up all the odds
      value = sum(getattr(odds, key.upper()) for odds in match.odds)/100

      # add the odds to the original value
      value += getattr(match, key.upper())

      # subtract to find the new value
      match_data[key] -= value
      match_data[key] = int(round(match_data[key] * 100,2))

class FootballLeague(Base):
  __tablename__ = 'football_league'

  # https://www.marathonbet.com/en/betting/Football/Internationals/
  # UEFA+Nations+League/League+C/Group+Stage+-+6402506
  # this has an id of 6402506
  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)
  matches = relationship('FootballMatch', lazy="select")

  def __init__(self, data):
    self.Id = data['id']
    self.Value = data['value']

  def json_object(self):
    data = dict(self.__dict__)
    data.pop('_sa_instance_state', None)
    data['matches'] = [match.json_object() for match in data['matches']]
    return data

class FootballMatch(Base):
  __tablename__ = 'football_match'

  Id = Column('id', Integer, primary_key=True)
  Home = Column('home', Integer, ForeignKey('football_team.id'))
  Away = Column('away', Integer, ForeignKey('football_team.id'))
  League = Column('league', Integer, ForeignKey('football_league.id'))
  _1 = Column('_1', Float)
  _X = Column('_x', Float)
  _2 = Column('_2', Float)
  _H1 = Column('_h1', Float)
  _H2 = Column('_h2', Float)
  _U = Column('_u', Float)
  _O = Column('_o', Float)
  odds = relationship(FootballOdds, lazy="joined", back_populates="match")
  _home = relationship(FootballTeam, lazy="joined", primaryjoin= Home == FootballTeam.Id)
  _away = relationship(FootballTeam, lazy="joined", primaryjoin= Away == FootballTeam.Id)
  _league = relationship(FootballLeague, lazy="joined")


  Time = Column('time', DateTime)
  Created = Column('created', DateTime)

  def __init__(self, match):
    self.Id = match['id']
    self.Home = Operations.GetOrCreateFootballTeam(match['home']).Id
    self.Away = Operations.GetOrCreateFootballTeam(match['away']).Id
    self.League = match['league_id']
    self._1 = match['_1']
    self._X = match['_x']
    self._2 = match['_2']
    self._H1 = match['_h1']
    self._H2 = match['_h2']
    self._U = match['_u']
    self._O = match['_o']
    self.Time = match['time']
    self.Created = match['created']

  def json_object(self):
    data = dict(self.__dict__)
    data.pop('_sa_instance_state', None)
    data['Created'] = data['Created'].isoformat()
    data['Time'] = data['Time'].isoformat()
    data['odds'] = [odd.json_object() for odd in data['odds']]
    data['_home'] = data['_home'].json_object()
    data['_away'] = data['_away'].json_object()
    return data


class Operations:

  def private_session():
    PrivateSession = sessionmaker()
    PrivateSession.configure(bind=engine)
    return PrivateSession()

  def GetOrCreateFootballTeam(team_name):
    if team_name in FOOTBALL_TEAM_CACHE:
      return FOOTBALL_TEAM_CACHE[team_name]

    else:
      team = FootballTeam(team_name)
      session.add(team)
      session.commit()

      FOOTBALL_TEAM_CACHE[team_name] = team
      return team

  def GetOrCreateFootballLeague(league_id, league_name=None):
    if league_id in FOOTBALL_LEAGUE_CACHE:
      return FOOTBALL_LEAGUE_CACHE[league_id]

    else:
      league = FootballLeague({'id': league_id, 'value': league_name})
      session.add(league)
      session.commit()

      FOOTBALL_TEAM_CACHE[league_id] = league
      return league

  def SaveFootballLeague(data):
    if session.query(FootballLeague.Id).filter_by(Id=data['id']).scalar() == None:
      session.add(FootballLeague(data))
      session.commit()

  def QueryFootballLeagues():
    data = [dict(league.__dict__) for league in FOOTBALL_LEAGUE_CACHE.values()]
    [league.pop('_sa_instance_state') for league in data]
    return data

  def QueryFootballLeague(league_id):
    return Operations.private_session().query(FootballLeague
            ).filter_by(Id=league_id
            ).options(joinedload(FootballLeague.matches)
            ).first().json_object()

  def QueryFootballTeam():
    return session.query(FootballTeam).all()


  def SaveFootball(match_data):
    match = session.query(FootballMatch).filter_by(Id=match_data['id']).options(joinedload('odds')) .first()
    if match == None:
      match = FootballMatch(match_data)
      session.add(match)
      session.commit()

    else:
      match_odds = FootballOdds(match_data, match)

      for key in match_odds.keys:
        if match_data[key] != 0:
          session.add(match_odds)
          session.commit()

          break

  def QueryFootballOdds(match):
    return session.query(FootballOdds).filter_by(Match=match).all()

  def QueryFootballMatch(json = True):
    return [(match.json_object() if json else match) for match in session.query(FootballMatch).all()]

  def SaveFootballError(data):
    session.add(FootballError(data))
    session.commit()

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

FOOTBALL_TEAM_CACHE = {team.Value: team for team in  Operations.QueryFootballTeam()}
FOOTBALL_LEAGUE_CACHE = {league.Id: league for league in session.query(FootballLeague).all()}

if __name__ == "__main__":
  pprint(Operations.QueryFootballLeagues())
