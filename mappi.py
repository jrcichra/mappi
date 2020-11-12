#!/usr/bin/python3

import socket
import logging
import pymysql
import os

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# Change to match your LAN
PREFIX = "10.0.0"
DOMAIN = "PK5001Z"
# Enable if you want to write out a coreDNS hosts file based on the DB data
COREDNS = True
COREDNSPATH = "/home/pi/.mappihosts"


def getUpsert(o, ip):
    sql = "INSERT INTO devices (ip_address,first_seen,"
    sql += ", ".join(o.keys())
    sql += f") VALUES ('{ip}',now(),"
    for key in o.keys():
        sql += f"'{o[key]}',"
    sql = sql[:-1]
    sql += ") ON DUPLICATE KEY UPDATE last_seen=now(),"
    for key in o.keys():
        sql += f"{key}='{o[key]}',"
    sql = sql[:-1]
    return sql

### MAIN ###


logging.info("Starting a round of mappi.")
entities = {}           # hash of things on the network
# Loop through all the hosts
ip = 1
while ip < 255:
    entity = {}     # object for our db
    fullip = f"{PREFIX}.{ip}"
    try:
        h = socket.gethostbyaddr(fullip)[0]
        logging.debug(f"h={h}")
        entity['hostname'] = h
    except Exception as e:
        logging.error(f"Couldn't get hostname for ip: {fullip} - {e}")
    if os.system(f"ping {fullip} -c 1") == 0:
        entity['state'] = 'up'
    else:
        entity['state'] = 'down'
    # add this entity to our list of entities
    entities[fullip] = entity
    # Inc
    ip += 1

# We've looped through all our entities, lets try and upsert them in a single transaction

queries = []  # array of query strings
found = 0     # devices we found

for e in entities:
    queries.append(getUpsert(entities[e], e))
    found += 1
logging.info(f"Found {found} devices on this round.")

# Establish DB connection:

db = pymysql.connect("localhost", "mappi", "test", "mappi",
                     cursorclass=pymysql.cursors.DictCursor)
try:
    cursor = db.cursor()
    for query in queries:
        logging.debug(f"About to execute: {query}")
        cursor.execute(query)
except:
    db.rollback()
    db.close()
    raise
else:
    db.commit()

# Find hostnames that have more than one IP. If IPs > 1, delete all that are down


# If they have COREDNS support on, write out a file that coreDNS can serve
if COREDNS:
    with open(COREDNSPATH, 'w') as f:
        cursor.execute("SELECT ip_address, hostname FROM devices")
        rows = cursor.fetchall()
        for row in rows:
            ip = row['ip_address']
            host = row['hostname']
            f.write(f"{ip}    {host}    {host.replace(f'.{DOMAIN}','')}\n")

db.close()
logging.info("Finished a round of mappi.")
