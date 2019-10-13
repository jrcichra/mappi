import nmap
import json
import logging
import pymysql

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def getMacAddress(host, ip):
    try:
        mac = host['addresses']['mac']
        return mac
    except KeyError as e:
        try:
            # There's the possibility of more than one key in this...
            macs = list(host['vendor'].keys())
            if len(macs) >= 1:
                if len(macs) > 1:
                    logging.warn(
                        "There is more than one mac address under vendor for ip: {}".format(ip))
                mac = macs[0]
                return mac
            else:
                return None

        except KeyError as e:
            return None


def getHostName(host, ip):
    try:
        hostname = host['hostnames'][0]['name']
        return hostname
    except KeyError as e:
        return None


def getHostNameType(host, ip):
    try:
        host_type = host['hostnames'][0]['type']
        return host_type
    except KeyError as e:
        return None


def getState(host, ip):
    try:
        state = host['status']['state']
        return state
    except KeyError as e:
        return None


def getVendor(host, ip):
    # Need this for the key
    mac = getMacAddress(host, ip)
    if mac is None:
        return None
    else:
        try:
            vendor = host['vendor'][mac]
            return vendor
        except KeyError as e:
            return None


def downOthers(ips):
    sql = "UPDATE devices SET state = 'down' where last_seen < NOW() - INTERVAL 1 HOUR and ip_address not in ("
    for ip in ips:
        sql += "'{}',".format(ip)
    sql = sql[:-1]
    sql += ")"
    return sql


def getUpsert(o, ip):
    sql = "INSERT INTO devices (ip_address,"
    sql += ", ".join(o.keys())
    sql += ") VALUES ('" + ip + "',"
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
n = nmap.PortScanner()  # nmap object
entities = {}           # hash of things on the network
# Scan the network
n.scan(hosts='10.0.0.0/24', arguments='-sP -PE -PA21,22,23,80,3389')
# Loop through all the hosts
for ip in n.all_hosts():
    host = n[ip]    # extract nmap object
    entity = {}     # object for our db

    mac = getMacAddress(host, ip)
    if mac != None and mac != '':
        hostname = getHostName(host, ip)
    if hostname != None and hostname != '':
        entity['hostname'] = hostname

    hostname_type = getHostNameType(host, ip)
    if hostname_type != None and hostname_type != '':
        entity['hostname_type'] = hostname_type

    state = getState(host, ip)
    if state != None and state != '':
        entity['state'] = state

    vendor = getVendor(host, ip)
    if vendor != None and vendor != '':
        entity['vendor'] = vendor

    # add this entity to our list of entities
    entities[ip] = entity

# We've looped through all our entities, lets try and upsert them in a single transaction

queries = []  # array of query strings
found = 0     # devices we found

for e in entities:
    queries.append(getUpsert(entities[e], e))
    found += 1
logging.info("Found {} devices on this round.".format(found))

queries.append(downOthers(list(entities.keys())))

# Establish DB connection:

db = pymysql.connect("localhost", "mappi", "password", "mappi",
                     cursorclass=pymysql.cursors.DictCursor)
try:
    cursor = db.cursor()
    for query in queries:
        cursor.execute(query)
except:
    db.rollback()
    db.close()
    raise
else:
    db.commit()
    db.close()

logging.info("Finished a round of mappi.")
