#
# mongo Operations
#
# mongoo.py src_db source dest_db dest cmd
#
import sys, os, time, datetime, importlib, argparse
from enum import Enum
# from datetime import datetime
from urlparse import urlparse
import mongoengine as meng
# from mongoengine.connection import get_db
# from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from mongoengine.context_managers import switch_collection
from extras_mongoengine.fields import StringEnumField

#we need class defs from science (at least for pp)
# PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") )     #science is parallel
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../") )        #we are located in science/venv/src
sys.path.append(PYBASE)
sys.path.append(os.getcwdu())
from utils.pp import pp

MYID = "%05d-%s" % (os.getpid(), repr(time.time()*1000000)[-8:-2])

def t0():
    global T
    print MYID, "---PROFILING---"
    T = time.time()

def t1():
    print MYID, "---%s seconds---" % (time.time() - T)

class hkstate(Enum):
    open = 'open'
    working = 'working'
    fail = 'fail'
    done = 'done'

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()                             # total # of entries to do
    good = meng.IntField(default = 0)                   # entries successfully processed
    bad = meng.IntField(default = 0)                    # entries we failed to process to completion
    log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = StringEnumField(hkstate, default = 'open')
    meta = {'indexes': ['state']}

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
    print MYID, "dropping housekeeping collection:", housekeep._get_collection_name()
    housekeep.drop_collection()
    print MYID, "dropping destination collection:", destcol
    destcol.drop_collection()
    time.sleep(1)
#
# set up housekeeping
#
def mongoo_init(srccol, destcol, key, query, chunk=3):
    if housekeep.objects.count() == 0:
        print MYID, "initializing housekeeping for", housekeep._get_collection_name()
        q = srccol.objects(**query).only(key).order_by(key)
    else:
        last = housekeep.objects().order_by('-start')[0].end
        print MYID, "last partition field in housekeep:", last
        query[key + "__gt"] = last
        q = srccol.objects(**query).only(key).order_by(key)
        print MYID, "added %d entries to %s" % (q.count(), housekeep._get_collection_name())
    tot = q.count()

    while q.count() > 0:
        hk = housekeep()
        hk.start = getattr(q[0], key)
        hk.end =  getattr(q[min(chunk-1, q.count()-1)], key)
        hk.save()
        query[key + "__gt"] = hk.end
        q = srccol.objects(**query).only(key).order_by(key)
    init = True

#
# Process what we can
#
def mongoo_process(srccol, destcol, key, query, cb):
    while housekeep.objects(state = 'open').count():
        #
        # tricky pymongo stuff mongoengine doesn't support.
        # find an open chunk, update it to state=working
        # must be done atomically to avoid contention with other processes
        #
        # update housekeep.state with find and modify
        raw = housekeep._collection.find_and_modify({'state': 'open'}, {'$set': {'state': 'working'}})
        #if raw==None, someone scooped us
        if raw != None:
            #reload as mongoengine object -- _id is .start (because set as primary_key)
            hko = housekeep.objects(start = raw['_id'])[0]
            #get data pointed to by housekeep
            query[key + "__gte"] = hko.start
            query[key + "__lte"] = hko.end
            cursor = srccol.objects(**query)
            print MYID, "%s mongo_process: %d elements in chunk %s-%s" % (datetime.datetime.now().strftime("%H:%M:%S:%f"), cursor.count(), hko.start, hko.end)
            hko.total = cursor.count()
            hko.good, hko.bad, hko.log = cb(cursor, destcol, MYID)
            hko.state = 'done'
            hko.save()
        else:
            print MYID, "race lost -- skipping"
        print MYID, "sleep..."
        sys.stdout.flush()
        time.sleep(WAITSLEEP)
    print MYID, "mongo_process over"

if __name__ == "__main__":
    par = argparse.ArgumentParser(description = "Mongo Operations")
    par.add_argument("src_db")
    par.add_argument("source")
    par.add_argument("dest_db")
    par.add_argument("dest")
    par.add_argument("cmd")
    par.add_argument("--chunk", type=int, default = 3)
    par.add_argument("--multi", type=int, default = 1)
    par.add_argument("--sleep", type=float, default = 1)
    config = par.parse_args()
#     if len(sys.argv) > 1 and 'config=' in sys.argv[1]:
#         config = importlib.import_module(sys.argv[1][7:])
#     else:
#         import config

    if config.dest_db == '.':
        config.dest_db = config.src_db    
    if config.source == config.dest:
        raise Exception("Source and destination must be different collections")
    print "MYID:", MYID
    print MYID, "CMD:", sys.argv
    src_dest = config.source + "_" + config.dest
    goo = importlib.import_module(src_dest)
    source = getattr(goo, config.source)
    dest = getattr(goo, config.dest)
    WAITSLEEP = config.sleep

    print MYID, "source database, collection:", config.src_db, source
    print MYID, "destination database, collection:", config.dest_db, dest
    
    connect2db(source, config.src_db)
    connect2db(dest, config.dest_db)
    connect2db(housekeep, config.dest_db)
    hk_colname = source._class_name + '_' + dest._class_name
    switch_collection(housekeep, hk_colname).__enter__()

    if hasattr(goo, 'QUERY'):
        query = goo.QUERY
    else:
        query = {}

    t0()
        
    if 'reset' == config.cmd:
        print MYID, "drop housekeep(%s) and %s at %s, sure?" % (hk_colname, config.dest, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, dest)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest, MYID)

    elif 'init' == config.cmd:
        mongoo_init(source, dest, goo.KEY, query, config.chunk)
        if hasattr(goo, 'init'):
            goo.init(source, dest, MYID)
        
    elif 'process' == config.cmd:
        if config.multi > 1:
            for i in range(config.multi):
                do = "python %s %s %s %s %s --chunk=%d --sleep=%d process" % (sys.argv[0], 
                        config.src_db, config.source, config.dest_db, config.dest, config.chunk, config.sleep)
                if config.multi > 1:
                    do += " &"
                print MYID, "doing:", do
                os.system(do)
        else:
            mongoo_process(source, dest, goo.KEY, query, goo.process)

    elif 'status' == config.cmd:
        print MYID, "----------- TRACKING STATUS ------------"
        print MYID, "%s done, %s not" % (housekeep.objects(state = 'done').count(), housekeep.objects(state__ne = 'done').count())
        bad = 0
        good = 0
        tot = 0
        for h in housekeep.objects:
            bad += h.bad
            good += h.good
            tot += h.total
            if h.total != h.good:
                print MYID, "Some badness found for %s-%s:" % (h.start, h.end)
                print MYID, "%d are good, %d are bad." % (h.good, h.bad)
                for badd in h.log:
                    print MYID, "----------------------------------------"
                    print MYID, badd
                    print MYID, "----------------------------------------"
#         pp(housekeep.objects)
        print MYID, "total good: %d bad: %d sum: %d expected total: %d" % (good, bad, good+bad, tot)         

    elif 'wait' == config.cmd:
        print MYID, "----------- WAITING FOR PROCESSES TO COMPLETE ------------"
        tot = housekeep.objects.count()
        done = housekeep.objects(state = 'done').count()
        while done < tot:
            time.sleep(WAITSLEEP)
            print MYID, "%s still waiting: %d out of %d complete" % (datetime.datetime.now().strftime("%H:%M:%S:%f"), done, tot)
            done = housekeep.objects(state = 'done').count()
        print MYID, "----------- THE WAITING GAME IS OVER ------------"

    elif 'dev' == config.cmd:
        WAITSLEEP = 0
        print MYID, "drop housekeep(%s) and %s at %s, sure?" % (hk_colname, config.dest, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, dest)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest)
        mongoo_init(source, dest, goo.KEY, query)
        mongoo_process(source, dest, goo.KEY, query, goo.process)

    else:
        print "usage:"
        print "mongoo.py reset   #erase all destination data for complete reprocessing"
        print "mongoo.py init    #initialize for processing"
        print "mongoo.py process #process data"
        print "mongoo.py dev     #reset, init, process in single thread for development tests"
        print "mongoo.py status"
        exit()
        
    t1()
