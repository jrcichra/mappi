#!/usr/bin/python3

import socket
import logging
import pymysql
import os

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# Change to match your LAN
PREFIX = "10.0.0"


def downOthers(ips):
    sql = "UPDATE devices SET state = 'down' where last_seen < NOW() - INTERVAL 1 HOUR and ip_address not in ("
    for ip in ips:
        sql += "'{}',".format(ip)
    sql = sql[:-1]
    sql += ")"
    return sql


def getUpsert(o, ip):
    sql = "INSERT INTO devices (ip_address,first_seen,"
    sql += ", ".join(o.keys())
    sql += ") VALUES ('" + ip + "',now(),"
    for key in o.keys():
        sql += "'{}',".format(o[key])
    sql = sql[:-1]
    sql += ") ON DUPLICATE KEY UPDATE last_seen=now(),"
    for key in o.keys():
        sql += "{}='{}',".format(key, o[key])
    sql = sql[:-1]
    return sql

### MAIN ###


logging.info("Starting a round of mappi.")
entities = {}           # hash of things on the network
# Loop through all the hosts
ip = 1
while ip < 255:
    entity = {}     # object for our db
    fullip = "{}.{}".format(PREFIX, ip)
    try:
        h = socket.gethostbyaddr(fullip)[0]
        logging.debug("h={}".format(h))
        entity['hostname'] = h
        if os.system("ping {} -c 1".format(fullip)) == 0:
            entity['state'] = 'up'
        else:
            entity['state'] = 'down'
        # add this entity to our list of up entities
        entities[fullip] = entity
    except Exception as e:
        logging.error("Error on ip: {} - {}".format(fullip, e))

    # Inc
    ip += 1

# We've looped through all our entities, lets try and upsert them in a single transaction

queries = []  # array of query strings
found = 0     # devices we found

for e in entities:
    queries.append(getUpsert(entities[e], e))
    found += 1
logging.info("Found {} devices on this round.".format(found))

# Establish DB connection:

db = pymysql.connect("localhost", "mappi", "password", "mappi",
                     cursorclass=pymysql.cursors.DictCursor)
try:
    cursor = db.cursor()
    for query in queries:
        logging.debug("About to execute: {}".format(query))
        cursor.execute(query)
except:
    db.rollback()
    db.close()
    raise
else:
    db.commit()
    db.close()

logging.info("Finished a round of mappi.")
