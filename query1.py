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


    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)
    
    session.execute("""

        CREATE TABLE IF NOT EXISTS table_query1( 
        quote_count int, 
        reply_count int, 
        hashtags list<text>, 
        datetime timestamp, 
        date date, 
        like_count int, 
        verified text, 
        sentiment int, 
        author text, 
        location text, 
        tid text, 
        retweet_count int, 
        type text, 
        media_list text, 
        quoted_source_id text, 
        url_list list<text>, 
        tweet_text text, 
        author_profile_image text, 
        author_screen_name text, 
        author_id text, 
        lang text, 
        keywords_processed_list list<text>, 
        retweet_source_id text, 
        mentions list<text>, 
        replyto_source_id text,
        Primary key(author , datetime)
        ) 
        
        """)

    files = os.listdir('./workshop_dataset1')

    for file in files:

    
        a = open('workshop_dataset1/' + file)

        data = json.load(a)

        for key in data:

            if(data[key]['media_list'] != None):
                data[key]['media_list'] = ""
        
    
            data2 = json.dumps(data[key])
        
            query = session.prepare ("INSERT INTO twitter_data JSON ?")
            session.execute(query,(data2,))


            
if __name__ == "__main__":
    main()