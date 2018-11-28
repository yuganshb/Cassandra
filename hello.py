# -*- coding: utf-8 -*-
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


from flask import Flask, flash, redirect, render_template, request, session, abort
import os

app = Flask (__name__)

#fun var

	
@app.route("/")
def index():

	return render_template('test.html')

@app.route("/query1" , methods = ['POST','GET'])
def query1():
	author = request.form['query1']

	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query1 where author = ?""")

	open_read = session.execute(query,(author,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page

@app.route("/query2" , methods = ['POST','GET'])
def query2():
	keyword = request.form['query2']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query2 where keywords = ?""")

	open_read = session.execute(query,(keyword,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page

@app.route("/query3" , methods = ['POST','GET'])
def query3():
	hashtag = request.form['query3']
	print(hashtag)
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query3 where hashtgs = ?""")

	open_read = session.execute(query,(hashtag,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page


@app.route("/query4" , methods = ['POST','GET'])
def query4():
	author = request.form['query4']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query4 where mentions = ?""")

	open_read = session.execute(query,(author,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page

@app.route("/query5" , methods = ['POST','GET'])
def query5():
	date = request.form['query5']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query5 where date = ?""")

	open_read = session.execute(query,(date,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page

@app.route("/query6" , methods = ['POST','GET'])
def query6():
	location = request.form['query6']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("twitter_key_space")

	query = session.prepare(""" SELECT * from table_query6 where location = ?""")

	open_read = session.execute(query,(location,))
	page = ''

	for row in open_read:
	
		page += '<p>%s</p>' % repr(row)

	return page


if __name__ == "__main__":
    app.run(host = '127.0.0.1',port = 8014)