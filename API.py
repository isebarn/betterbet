from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS, cross_origin
from json import loads
import os

import betting.spiders.ORM as ORM
from time import sleep
from Tables import tables

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"*": {"origins": os.environ.get('WEB')}})

@app.route('/matches')
def root():
  return jsonify(ORM.Operations.QueryFootball())

@app.route('/matches/competition')
def competition():
  competition = loads(request.args.get('competition', default = None, type = str))
  return jsonify(ORM.Operations.QueryCompetition(competition))

@app.route('/matches/competition/prices')
def prices():
  market = request.args.get('market')
  collection = request.args.get('collection')
  data = ORM.Operations.QueryPrices(market, collection)
  return jsonify(tables(collection, data))


@app.route('/leagues')
def leagues():
  return jsonify(ORM.Operations.QueryLeagueList())

@app.route('/leagues/league')
def league():
  league = loads(request.args.get('league', default = None, type = str))
  return jsonify(ORM.Operations.QueryLeague(league))
