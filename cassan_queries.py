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

    session.set_keyspace(KEYSPACE)

    rows = session.execute("Select author from twitter_data")

    for r in rows:
    	print(r.author)


if __name__ == "__main__":
    main()


