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
KEYSPACE = "midsem"

def query8():

	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	#session.execute("DROP KEYSPACE midsem");


	#session.execute("""
	#    CREATE KEYSPACE %s
	 #   WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
	  #  """ % KEYSPACE )


	log.info("setting keyspace...")
	session.set_keyspace(KEYSPACE)

	
	session.execute("""

		CREATE TABLE table_query8( 
         
        date date,
        hashtag_mention text,

        Primary key(date,hashtag_mention)
        ) 
        
        """)



	files = os.listdir('./workshop_dataset1')

	for file in files:

		a = open('workshop_dataset1/' + file)

		data = json.load(a)

		for key,value in data.items():

			if(value['hashtags'] is not None and value['mentions'] is not None):

				for hstg in value['hashtags']:

					if(len(hstg) > 0):

						for ment in value['mentions']:

							if(len(ment) > 0):

								query = session.prepare("""INSERT INTO table_query8(date,hashtag_mention) values(?,?)""")
								
								d = datetime.strptime(value['date'],"%Y-%m-%d")
								hm = hstg+ment
								session.execute(query,(d,hm))



query8()

       