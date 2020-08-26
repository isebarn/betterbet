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

class MarathonFootballMatch(Base):
  __tablename__ = 'marathon_football_match'

  Id = Column('id', Integer, primary_key=True)
  Home = Column('home', Integer, ForeignKey('marathon_football_team.id'))
  Away = Column('away', Integer, ForeignKey('marathon_football_team.id'))
  Time = Column('time', DateTime)
  _1 = Column('_1', Float)
  _x = Column('_x', Float)
  _2 = Column('_2', Float)
  Created = Column('created', DateTime)

  def __init__(self, match):
    self.Id = match['id']
    self.Home = Operations.GetOrCreateMarathonFootballTeam(match['home']).Id
    self.Away = Operations.GetOrCreateMarathonFootballTeam(match['away']).Id
    self.Time = match['time']
    self._1 = match['_1']
    self._x = match['_x']
    self._2 = match['_2']
    self.Created =  datetime.now().replace(microsecond=0)

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

  def SaveMarathonFootball(match):
    if session.query(MarathonFootballMatch.Id).filter_by(Id=match['id']).scalar() == None:
      session.add(MarathonFootballMatch(match))
      session.commit()

  def QueryMarathonFootballMatch():
    match = aliased(MarathonFootballMatch)
    home = aliased(MarathonFootballTeam)
    away = aliased(MarathonFootballTeam)

    matches = session.query(match, home, away
      ).join(home, home.Id == match.Home
      ).join(away, away.Id == match.Away).all()

    return [{'id': match[0].Id,
          'time': match[0].Time,
          '_1': match[0]._1,
          '_x': match[0]._x,
          '_2': match[0]._2,
          'created': match[0].Created,
          'home': match[1].Value,
          'away': match[2].Value} for match in matches]

Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()
MARATHON_FOOTBALL_TEAM_CACHE = {team.Value: team for team in  Operations.QueryMarathonFootballTeam()}
if __name__ == "__main__":
  pprint(Operations.QueryMarathonFootballMatch())

