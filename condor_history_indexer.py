#!/bin/env python

import sys
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

typ='OSG'

print typ

d = datetime.now()
ind="condor_history-"+str(d.year)+"."+str(d.month)+"."+str(d.day)

def is_Int(s):
    try:
        r=float(s)
        if r==int(r):
            return True
        else: 
            return False
    except ValueError:
        return False
        
def is_number(s):
    try:
        r=float(s) 
        return True
    except ValueError:
        return False
        
print sys.argv[1]
condorfile = open(sys.argv[1])
lines=condorfile.readlines()

aLotOfData=[]
data = {
    '_index': ind,
    '_type': typ
}
for l in lines:
    if l[:4]=='*** ':
        aLotOfData.append(data)
        data = {
            '_index': ind,
            '_type': typ
        }
        continue
    l=l.strip()
    si=l.find(' = ')
    key=l[:si]
    value=l[si+3:]
    value=value.strip('"')
    if is_Int(value):
        r=int(float(value))
        #if key in data: print key, "previous value: ", data[key], "\tnew value:", r 
        data[key]=r
        if key=='EnteredCurrentStatus':
            data['timestamp']=r*1000
    elif is_number(value):
        r=float(value)
        #if key in data: print key, "previous value: ", data[key], "\tnew value:", r 
        data[key]=r        
    else:
        #if key in data: print key, "previous value: ", data[key], "\tnew value:", value 
        data[key]=value[:32765]


print 'jobs loaded:', len(aLotOfData)

print "make sure we are connected right..."
import requests
res = requests.get('http://uct2-es-door.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'uct2-es-door.mwt2.org', 'port':9200}])

try:
    res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
    print "inserted:",res[0],'\tErrors:',res[1]
except es_exceptions.ConnectionError as e:
    print 'ConnectionError ', e
except es_exceptions.TransportError as e:
    print 'TransportError ', e
except helpers.BulkIndexError as e:
    print e[0]
    for i in e[1]:
        print i

