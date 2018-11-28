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

KEYSPACE = "midsem"

def query5():

	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	#session.execute("DROP KEYSPACE midsem");


	#session.execute("""
	 #   CREATE KEYSPACE %s
	  #  WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
	   # """ % KEYSPACE )


	

	log.info("setting keyspace...")
	session.set_keyspace(KEYSPACE)

	
	session.execute("drop table table_query5");


	session.execute("""

    	CREATE TABLE table_query5( 
         
        hashtag text,
        date text,
        tweet_count counter,
        Primary key(hashtag,date)
        )WITH CLUSTERING ORDER BY(date DESC) 
        
        """)


	files = os.listdir('./workshop_dataset1')

	for file in files:

		a = open('workshop_dataset1/' + file)

		data = json.load(a)

		for key,value in data.items():

			if(value['hashtags'] is not None):
			
				for hstgs in value['hashtags']:
					
					if(len(hstgs) > 0):

						if(value['date'] <= '2018-01-17' and value['date'] >= '2018-01-15'):

							query = session.prepare("UPDATE table_query5 set tweet_count = tweet_count + 1 where hashtag = ? and date = ?")
							session.execute(query,(hstgs,value['date']))

query5()









