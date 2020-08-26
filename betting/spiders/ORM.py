import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, Column, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased
from datetime import datetime
from pprint import pprint

engine = create_engine(os.environ.get('BETTING_DATABASE'), echo=False)
Base = declarative_base()


class MarathonFootballTeam(Base):
  __tablename__ = 'marathon_football_team'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, value):
    self.Value = value

class MarathonFootballMatchOdds(Base):
  __tablename__ = 'marathon_football_match_odds'

  Id = Column('id', Integer, primary_key=True)
  Match = Column('match', Integer, ForeignKey('marathon_football_match.id'))
  _1 = Column('_1', Float)
  _x = Column('_x', Float)
  _2 = Column('_2', Float)
  Time = Column('time', DateTime)

  def __init__(self, match_data):
    self.Match = match_data['id']
    self._1 = match_data['_1']
    self._x = match_data['_x']
    self._2 = match_data['_2']
    self.Time = match_data['created']

class MarathonFootballMatch(Base):
  __tablename__ = 'marathon_football_match'

  Id = Column('id', Integer, primary_key=True)
  Home = Column('home', Integer, ForeignKey('marathon_football_team.id'))
  Away = Column('away', Integer, ForeignKey('marathon_football_team.id'))
  Time = Column('time', DateTime)

  Created = Column('created', DateTime)

  def __init__(self, match):
    self.Id = match['id']
    self.Home = Operations.GetOrCreateMarathonFootballTeam(match['home']).Id
    self.Away = Operations.GetOrCreateMarathonFootballTeam(match['away']).Id
    self.Time = match['time']
    self.Created = match['created']

class Operations:

  def QueryMarathonFootballTeam():
    return session.query(MarathonFootballTeam).all()

  def GetOrCreateMarathonFootballTeam(team_name):

    if team_name in MARATHON_FOOTBALL_TEAM_CACHE:
      return MARATHON_FOOTBALL_TEAM_CACHE[team_name]

    else:
      team = MarathonFootballTeam(team_name)
      session.add(team)
      session.commit()
      MARATHON_FOOTBALL_TEAM_CACHE[team_name] = team

    return team

  def SaveMarathonFootball(match_data):
    match = session.query(MarathonFootballMatch.Id).filter_by(Id=match_data['id']).first()
    if match == None:
      match = MarathonFootballMatch(match_data)
      session.add(match)
      session.commit()

    if '_1' in match_data and '_x' in match_data and '_2' in match_data:
      match_odds = MarathonFootballMatchOdds(match_data)
      session.add(match_odds)
      session.commit()

  def QueryMarathonFootballMatchOdds(match):
    return session.query(MarathonFootballMatchOdds).filter_by(Match=match).all()

  def QueryMarathonFootballMatch():
    match = aliased(MarathonFootballMatch)
    home = aliased(MarathonFootballTeam)
    away = aliased(MarathonFootballTeam)

    matches = session.query(match, home, away
      ).join(home, home.Id == match.Home
      ).join(away, away.Id == match.Away).all()

    return [{'id': match[0].Id,
          'time': match[0].Time,
          'created': match[0].Created,
          'home': match[1].Value,
          'away': match[2].Value,
          'odds': [{'home': odds._1, 'draw': odds._x, 'away': odds._2, 'time': odds.Time}
            for odds in Operations.QueryMarathonFootballMatchOdds(match[0].Id)]}
            for match in matches]

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()
MARATHON_FOOTBALL_TEAM_CACHE = {team.Value: team for team in  Operations.QueryMarathonFootballTeam()}
if __name__ == "__main__":
  pprint(Operations.QueryMarathonFootballMatch())

