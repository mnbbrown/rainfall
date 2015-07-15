import requests
from StringIO import StringIO
import psycopg2
import datetime
import os

pgdsn = os.environ.get('PGDSN')
if pgdsn is None:
    raise ValueError("The env variable PGDSN must be set")

try:
    conn = psycopg2.connect(pgdsn)
except:
    raise ValueError("Unable to connect to db using: {0}".format(pgdsn))

STATIONS_GIST = "https://gist.githubusercontent.com/mnbbrown/5c1bb65ca70108806af6/raw/220d5fcbc15a44945ae4d4ce7e9ec3960a5c987c/gistfile1.txt"


def get_stations():
    r = requests.get(STATIONS_GIST)
    lines = r.iter_lines()
    [next(lines) for _ in range(4)]
    stations = []
    for l in lines:
        if len(l) is not 109:
            continue
        site = l[0:7].strip()
        name = l[8:49].strip()
        lat = l[49:59].strip()
        lng = l[59:68].strip()
        date = l[77:85]
        if datetime.datetime.strptime(date, '%b %Y') > datetime.datetime(month=11, year=2014, day=30):
            stations.append((site,name,float(lat),float(lng),))
    with conn.cursor() as cur:
        for s in stations:
            print "Inserting", s[1]
            try:
                cur.execute("INSERT INTO stations (id,name,location) VALUES (%s, %s, ST_Point(%s,%s))", s)
            except Exception, e:
                print "Error inserting", s[1], e.pgerror
            conn.commit()

get_stations()
conn.close()
