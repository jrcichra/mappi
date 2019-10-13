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
                return 'UN:KN:OW:N:{}'.format(ip)

        except KeyError as e:
            return 'UN:KN:OW:N:{}'.format(ip)


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


def downOthers(macs):
    sql = "UPDATE devices SET state = 'down' where last_seen < NOW() - INTERVAL 1 HOUR and mac_address not in ("
    for mac in macs:
        sql += "'{}',".format(mac)
    sql = sql[:-1]
    sql += ")"
    return sql


def getUpsert(o, mac):
    sql = "INSERT INTO devices (mac_address,"
    sql += ", ".join(o.keys())
    sql += ") VALUES ('" + mac + "',"
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
    # Determine the mac address, this is our PK
    mac = getMacAddress(host, ip)
    if mac is None:
        # skip this host
        logging.error(
            "I didn't find any mac addresses...can't continue with this ip: {}".format(ip))
        continue
    else:
        # the mac will be our key for the entity array
        # get the other properties
        hostname = getHostName(host, ip)
        if hostname != None and hostname != '':
            entity['hostname'] = hostname

        hostname_type = getHostNameType(host, ip)
        if hostname_type != None and hostname_type != '':
            entity['hostname_type'] = hostname_type

        entity['ip_address'] = ip

        state = getState(host, ip)
        if state != None and state != '':
            entity['state'] = state

        vendor = getVendor(host, ip)
        if vendor != None and vendor != '':
            entity['vendor'] = vendor

        # add this entity to our list of entities
        entities[mac] = entity

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
