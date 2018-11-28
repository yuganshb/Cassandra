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

KEYSPACE = "twitter_key_space"

def main():
	
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()

	session.execute("""
    	CREATE KEYSPACE %s
    	WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
    	""" % KEYSPACE )


	log.info("setting keyspace...")
	session.set_keyspace(KEYSPACE)

	session.execute("""

    	CREATE TABLE table_query1( 
         
        datetime timestamp, 
        author text, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        Primary key(author,datetime)
        )WITH CLUSTERING ORDER BY(datetime DESC) 
        
        """)

	session.execute("""

        CREATE TABLE table_query2( 
        like_count int, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        keywords text, 
        Primary key(keywords, like_count)
        )WITH CLUSTERING ORDER BY(like_count DESC)  
        
        """)

	session.execute("""

        CREATE TABLE table_query3( 
        hashtgs text, 
        datetime timestamp, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        Primary key(hashtgs,datetime)
        )WITH CLUSTERING ORDER BY(datetime DESC) 
        
        """)

	session.execute("""

        CREATE TABLE table_query4( 
        datetime timestamp, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        mentions text, 
        Primary key(mentions,datetime)
        )WITH CLUSTERING ORDER BY(datetime DESC) 
        
        """)

	session.execute("""

        CREATE TABLE table_query5( 
        date date, 
        like_count int, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        Primary key(date,like_count)
        )WITH CLUSTERING ORDER BY(like_count DESC) 
        
        """)

	session.execute("""

        CREATE TABLE table_query6( 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        Primary key(location)
        ) 
        
        """)
	session.execute("""

        CREATE TABLE table_query7( 
        hashtags text, 
        date date, 
        location text, 
        tid text, 
        tweet_text text, 
        author_id text, 
        lang text, 
        Primary key(date,hashtags)
        ) 
        
        """)


if __name__ == "__main__":
    main()



