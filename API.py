from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS, cross_origin
from json import loads
import os

import betting.spiders.ORM as ORM
from time import sleep

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"*": {"origins": os.environ.get('WEB')}})

@app.route('/')
def root():
  return jsonify(ORM.Operations.QueryFootballMatch())

@app.route('/leagues')
def leagues():
  return jsonify(ORM.Operations.QueryFootballLeagues())

@app.route('/leagues/league')
def league():
  league = loads(request.args.get('league', default = None, type = str))
  return jsonify(ORM.Operations.QueryFootballLeague(league))