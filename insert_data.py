import logging
import json
import os

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime

KEYSPACE = "twitter_key_space"


def ins_table1():
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	log.info("setting keyspace...")
	session.set_keyspace(KEYSPACE)

	files = os.listdir('./workshop_dataset1')
	i=0

	for file in files:
		
		a = open('workshop_dataset1/' + file)
		data = json.load(a)

    	for key,value in data.items():
		
			
			#query = session.prepare("INSERT INTO table_query1(datetime , author , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
			#d = datetime.strptime(value[fields_tab1[0]],"%Y-%m-%d %H:%M:%S")

			i=i+1
			#session.execute(query,(d,value[fields_tab1[1]],value[fields_tab1[2]],value[fields_tab1[3]],
			#	value[fields_tab1[4]],value[fields_tab1[5]],value[fields_tab1[6]]))

	

	print("No of rows :- " , i)

ins_table1()