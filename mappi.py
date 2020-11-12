#!/usr/bin/python3

import subprocess
import threading
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
COREDNSPATH = "/home/pi/coredns/hosts"
# Prepend this hosts file
PREHOSTS = "/home/pi/coredns/prehosts"


def getUpsert(o, ip):
    sql = "INSERT INTO devices (ip_address,first_seen,"
    sql += ", ".join(o.keys())
    sql += f") VALUES ('{ip}',now(),"
    for key in o.keys():
        sql += f"'{o[key]}',"
    sql = sql[:-1]
    sql += ") ON DUPLICATE KEY UPDATE "
    if o['state'] == 'up':
        sql += 'last_seen=now(),'
    for key in o.keys():
        sql += f"{key}='{o[key]}',"
    sql = sql[:-1]
    return sql


def check_ip(entities, ip):
    entity = {}     # object for our db
    fullip = f"{PREFIX}.{ip}"
    h = subprocess.check_output(
        f"dig +short @10.0.0.1 -x {fullip}", shell=True)[:-2].decode()
    if h != '':
        logging.debug("fullip={}, h={}".format(fullip, h))
        entity['hostname'] = h
    else:
        logging.error(f"Couldn't get hostname for ip: {fullip}")
    if os.system(f"ping {fullip} -c 1") == 0:
        entity['state'] = 'up'
    else:
        entity['state'] = 'down'
    # add this entity to our list of entities
    entities[fullip] = entity
    ### MAIN ###


logging.info("Starting a round of mappi.")
entities = {}           # hash of things on the network
threads = []            # array of threads to wait for
# Loop through all the hosts
ip = 1
while ip < 255:
    t = threading.Thread(target=check_ip, args=(entities, ip))
    threads.append(t)
    t.start()
    # Inc
    ip += 1
# wait for all the threads to complete
for thread in threads:
    thread.join()

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
    # Find hostnames that have more than one IP. If IPs > 1, clear the hostname on those which are down
    clear_old = """
        update devices set hostname = null where ip_address in (
            select down.ip_address from devices d
            left join (select hostname,state,ip_address from devices where state = 'down') down
            on d.hostname = down.hostname
            and d.state <> down.state
            where down.hostname is not null
        )
    """
    cursor.execute(clear_old)
except:
    db.rollback()
    db.close()
    raise
else:
    db.commit()


# If they have COREDNS support on, write out a file that coreDNS can serve
if COREDNS:
    with open(COREDNSPATH, 'w') as f:
        cursor.execute(
            "SELECT ip_address, hostname FROM devices WHERE hostname IS NOT NULL ORDER BY CAST(REPLACE(ip_address,'.','') AS INT)")
        rows = cursor.fetchall()
        with open(PREHOSTS, 'r') as r:
            for line in r.readlines():
                f.write(line)

        for row in rows:
            ip = row['ip_address']
            host = row['hostname']
            f.write(f"{ip}    {host}    {host.replace(f'.{DOMAIN}','')}\n")

db.close()
logging.info("Finished a round of mappi.")
