#
# mongo Operations
#
import sys, os, time, importlib
from enum import Enum
# from datetime import datetime
from urlparse import urlparse
import mongoengine as meng
# from mongoengine.connection import get_db
# from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from mongoengine.context_managers import switch_collection
from extras_mongoengine.fields import StringEnumField
import config
from pkg_resources import importlib_bootstrap
from cups import Dest

CHUNK = 3
WAITSLEEP = 4

PID = os.getpid()               #FIXME: not guaranteed unique across machines!

class hkstate(Enum):
    open = 'open'
    working = 'working'
    done = 'done'

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    state = StringEnumField(hkstate, default = 'open')

connect2db_cnt = 0

def connect2db(col, uri):
    global connect2db_cnt, con
    parts = urlparse(uri)
    db = os.path.basename(parts[2])
    if connect2db_cnt:
        alias = col._class_name + "_"+ db
#         print "DBG alias:", alias
        con = meng.connect(db, alias=alias, host=uri)
        switch_db(col, alias).__enter__()
    else:
        con = meng.connect(db, host=uri)
    connect2db_cnt += 1
    return con

#
# drop collections and reset
#
def mongoo_reset(srccol, destcol):
    print "dropping housekeeping collection:", housekeep._get_collection_name()
    housekeep.drop_collection()
    print "dropping destination collection:", destcol
    destcol.drop_collection()
#
# set up housekeeping
#
def mongoo_init(srccol, destcol, key, query):
    if housekeep.objects.count() == 0:
        print "initializing housekeeping for", housekeep._get_collection_name()
        q = srccol.objects(**query).only(key).order_by(key)
        tot = q.count()
        keys = [x.num for x in q]                   #FIXME -- in memory!
        for i in range(0, tot, CHUNK):
            hk = housekeep()
            hk.start = keys[i]
            hk.end = keys[min(i+CHUNK-1, len(keys)-1)]
            hk.save()
        init = True
    else:
        raise Exception("TODO: incremental init")

#
# Process what we can
#
def mongoo_process(srccol, destcol, key, query, cb):
    # get pymongo collection for dest/housekeep
    db = meng.connection.get_db(destcol._class_name+"_"+destcol.objects._collection.database.name)
    pmhk = db[srccol._class_name+"_"+destcol._class_name]
    while housekeep.objects(state = 'open').count():
        #
        # tricky pymongo stuff mongoengine doesn't support.
        # find an open chunk, update it to state=working
        # must be done atomically to avoid contention with other processes
        #
        # update housekeep.state with find and modify
        raw = pmhk.find_and_modify({'state': 'open'}, {'$set': {'state': 'working'}})
        #if raw==None, we lost the race with another process, so duck out for now
        if raw != None:
            #reload as mongoengine object -- _id is .start (because set as primary_key)
            hko = housekeep.objects(start = raw['_id'])[0]
            #get data pointed to by housekeep
            query[key + "__gte"] = hko.start
            query[key + "__lte"] = hko.end
            cursor = srccol.objects(**query)
            print "mongo_process: %d elements in chunk %s" % (cursor.count(), hko.start)
            cb(cursor, destcol)
            hko.state = 'done'
            hko.save()
        else:
            print "race lost -- skipping"
        print "sleep..."
        time.sleep(WAITSLEEP)

if __name__ == "__main__":
    if config.source == config.dest:
        raise Exception("Source and destination must be different collections")
    print "pid:", PID
    src_dest = config.source + "_" + config.dest
    goo = importlib.import_module(src_dest)
    source = getattr(goo, config.source)
    dest = getattr(goo, config.dest)

    print "source database, collection:", config.src_db, source
    print "destination database, collection:", config.dest_db, dest
    
    connect2db(source, config.src_db)
    connect2db(dest, config.dest_db)
    connect2db(housekeep, config.dest_db)
    hk_colname = source._class_name + '_' + dest._class_name
    switch_collection(housekeep, hk_colname).__enter__()

    if hasattr(goo, 'QUERY'):
        query = goo.QUERY
    else:
        query = {}

    if 'reset' in sys.argv[1:]:
        print "drop housekeep(%s) and %s at %s, sure?" % (hk_colname, config.dest, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, dest)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest)

    elif 'init' in sys.argv[1:]:
        mongoo_init(source, dest, goo.KEY, query)
        
    elif 'process' in sys.argv[1:]:
        mongoo_process(source, dest, goo.KEY, query, goo.process)
