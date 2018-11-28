# -*- coding: utf-8 -*-
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime


from flask import Flask, flash, redirect, render_template, request, session, abort
import os

app = Flask (__name__)

#fun var

	
@app.route("/")
def index():

	return render_template('test.html')


@app.route("/query5" , methods = ['POST','GET'])
def query5():
	hashtag = request.form['query5']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("midsem")

	query = session.prepare(""" SELECT * from table_query5 where hashtag = ?""")

	open_read = session.execute(query,(hashtag,))
	page = " <style> table, th, td { border: 1px solid black; } </style>"

	page = page + "<table style = \"width:100%\"> <tr> <th> HASHTAG </th> <th> DATE </th> <th> TWEET COUNT </th> </tr> "

	for row in open_read:
	
		page = page + "<tr> "
		page += "<td> %s </td>" % repr(row.hashtag)
		page += "<td> %s </td>" % repr(row.date)
		page += "<td> %s </td>" % repr(row.tweet_count)
		page += "</tr>"

	
	page += "</table>"
	return page

@app.route("/query6" , methods = ['POST','GET'])
def query6():
	date = request.form['query6']
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.set_keyspace("midsem")

	query = session.prepare(""" SELECT * from table_query8 where date = ?""")

	open_read = session.execute(query,(date,))
	page = " <style> table, th, td { border: 1px solid black; } </style>"

	page = page + "<table style = \"width:50%\"> <tr> <th> HASHTAG </th> <th> DATE </th> </tr> "

	for row in open_read:
	
		page = page + "<tr> "
		page += "<td> %s </td>" % repr(row.date)
		page += "<td> %s </td>" % repr(row.hashtag_mention)
		page += "</tr>"

	
	page += "</table>"
	return page


if __name__ == "__main__":
    app.run(host = '127.0.0.1',port = 8010)