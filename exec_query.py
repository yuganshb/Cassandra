import logging
import json
import os
from datetime import datetime

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

    '''rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        log.info("dropping existing keyspace...")
        session.execute("DROP KEYSPACE " + KEYSPACE)
    

    log.info("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)'''

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)
    
    '''session.execute("""

        CREATE TABLE twitter_data( 
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
        Primary key(tid)
        ) 
        
        """)
    '''

    fields_tab1 = ["datetime" , "author" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab2 = ["keyword" , "like_count" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab3 = ["hashtgs" , "datetime" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab4 = ["datetime" , "mentions" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab5 = ["date" , "like_count" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab6 = ["tid" , "tweet_text" , "author_id" , "location" ,"lang"]
    fields_tab7 = ["date" , "tid" , "tweet_text" , "author_id" , "location" ,"lang"]

    files = os.listdir('./workshop_dataset1')
    i=0

    for file in files:

    
        a = open('workshop_dataset1/' + file)

        data = json.load(a)

        for key,value in data.items():

            #query1 = session.prepare("INSERT INTO table_query1(datetime , author , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
            #d = datetime.strptime(value[fields_tab1[0]],"%Y-%m-%d %H:%M:%S")

            #session.execute(query1,(d,value[fields_tab1[1]],value[fields_tab1[2]],value[fields_tab1[3]],
             #  value[fields_tab1[4]],value[fields_tab1[5]],value[fields_tab1[6]]))

            #if(value['keywords_processed_list'] is not None):

             #   for kywds in value['keywords_processed_list']:
              #      if(len(kywds) > 0):

               #         query2 = session.prepare("INSERT INTO table_query2(keywords , like_count , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
                #        session.execute(query2,(kywds,value[fields_tab2[1]],value[fields_tab2[2]],value[fields_tab2[3]],
                 #           value[fields_tab2[4]],value[fields_tab2[5]],value[fields_tab2[6]]))
           # else:
            #    query2 = session.prepare("INSERT INTO table_query2(keywords , like_count , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
             #   session.execute(query2,("null",value[fields_tab2[1]],value[fields_tab2[2]],value[fields_tab2[3]],
              #      value[fields_tab2[4]],value[fields_tab2[5]],value[fields_tab2[6]]))'''

            '''if(value['hashtags'] is not None):

                for hstgs in value['hashtags']:
                    if(len(hstgs) > 0):

                        query3 = session.prepare("INSERT INTO table_query3(hashtgs , datetime , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
                        d = datetime.strptime(value['datetime'],"%Y-%m-%d %H:%M:%S")

                        session.execute(query3,(hstgs,d,value[fields_tab3[2]],value[fields_tab3[3]],
                            value[fields_tab3[4]],value[fields_tab3[5]],value[fields_tab3[6]]))
            else:
                query3 = session.prepare("INSERT INTO table_query3(hashtgs , datetime , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
                d = datetime.strptime(value['datetime'],"%Y-%m-%d %H:%M:%S")
                session.execute(query3,("null",d,value[fields_tab3[2]],value[fields_tab3[3]],
                    value[fields_tab3[4]],value[fields_tab3[5]],value[fields_tab3[6]]))'''

            '''if(value['mentions'] is not None):

                for ment in value['mentions']:
                    if(len(ment) > 0):

                        query4 = session.prepare("INSERT INTO table_query4(datetime , mentions , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
                        d = datetime.strptime(value['datetime'],"%Y-%m-%d %H:%M:%S")

                        session.execute(query4,(d,ment,value[fields_tab4[2]],value[fields_tab4[3]],
                            value[fields_tab4[4]],value[fields_tab4[5]],value[fields_tab4[6]]))
            else:
                query4 = session.prepare("INSERT INTO table_query4(datetime , mentions , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
                d = datetime.strptime(value['datetime'],"%Y-%m-%d %H:%M:%S")
                session.execute(query4,(d,"null",value[fields_tab4[2]],value[fields_tab4[3]],
                    value[fields_tab4[4]],value[fields_tab4[5]],value[fields_tab4[6]]))'''


            

            query5 = session.prepare("INSERT INTO table_query5(date , like_count , tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?,?,?)")
            d = datetime.strptime(value['date'],"%Y-%m-%d")

            session.execute(query5,(d,value[fields_tab5[1]],value[fields_tab5[2]],value[fields_tab5[3]],
                value[fields_tab5[4]],value[fields_tab5[5]],value[fields_tab5[6]]))


            if(value['location'] != None):

                query6 = session.prepare("INSERT INTO table_query6(tid , tweet_text , author_id , location,lang) VALUES(?,?,?,?,?)")

                session.execute(query6,(value[fields_tab6[0]],value[fields_tab6[1]],value[fields_tab6[2]],value[fields_tab6[3]],
                    value[fields_tab6[4]]))

            















     
    
    print(i)
if __name__ == "__main__":
    main()