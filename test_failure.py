import os, sys, time, random
import mongoengine as meng
import qmmap

class qmmap_src(meng.Document):
    num = meng.IntField(primary_key = True)
  
class qmmap_dest(meng.Document):
    val = meng.IntField(primary_key = True)

#this gets called (if defined) before processing each worker
def init(source, dest):         #type(source)=cursor, type(dest)=collection
    print("process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name))
    
def process(source):
    #test random process fail
    if random.random() < .15:
        print("UNFATHOMABLE FAILURE", file=sys.stderr)
        time.sleep(15)
        print("EXEUNT WORKER", file=sys.stderr)
        return

    gs = qmmap.toMongoEngine(source, qmmap_src)
    
    gd = qmmap_dest(val = gs.num * 10)
    print(os.getpid(), "  processed %s" % gs.num)
    time.sleep(.5) #slow for testing
    return gd.to_mongo()
 
if __name__ == "__main__":
    import pymongo, time
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()

    if input("drop qmmap_src, qmmap_dest, housekeeping(qmmap_src_qmmap_dest)?")[:1] == 'y':
        db.qmmap_src.drop()
        db.qmmap_dest.drop()
        db.qmmap_src_qmmap_dest.drop()

    for i in range(10):
        db.qmmap_src.save({'_id': i})

    qmmap.mmap(process, "qmmap_src", "qmmap_dest", init=init, multi=10, verbose=3, timeout=10)

    for o in qmmap_dest.objects:
        print(o.val, end=' ')
    print()

    #inspect housekeeping collection for fun & profit
    good = 0
    total = 0
    for hk in db.qmmap_src_qmmap_dest.find():
        good += hk['good']
        total += hk['total']
    print("%d succesful operations out of %d" % (good, total))
