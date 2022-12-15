import os, sys, time, random, argparse
import mongoengine as meng
import qmmap
from bson import ObjectId
random.seed(12346)
class qmmap_src(meng.Document):
    s1 = meng.StringField()
    s2 = meng.StringField()
    qmmap_job = meng.StringField()

class qmmap_dest(meng.Document):
    s = meng.StringField()

# if defined, this will run at start of each chunk
def init(source_cursor, destination_collection):
#     print "processing %d documents from %s to %s" % (source_cursor.count(), source_cursor.collection.name, destination_collection.name)
    pass

def process(source):
    gs = qmmap.toMongoEngine(source, qmmap_src)
    s = ""
    for i in range(len(gs.s1)):
        s += gs.s1[i]
        s += gs.s2[i]
    gd = qmmap_dest(s = s)
    return gd.to_mongo()

def randstring(size):
    return "".join(chr(random.randint(65, 65+25)) for i in range(size))

def make_random_input(num, size, job=None):
    """Randomize an input data set in the qmmap_src collection
    @num: number of elements
    @size: size, in characters, of each string in the input
    """
    for i in range(num):
        src = qmmap_src()
        src.s1 = randstring(size)
        src.s2 = randstring(size)
        if job:
            if random.random() >= .5:
                src.qmmap_job = job
        src.id = ObjectId(hex(random.randint(0, 2**96))[2:].zfill(24))
        src.save()


if __name__ == "__main__":
    import pymongo, time
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    meng.connect("test")

    par = argparse.ArgumentParser(description = "qmmap test large data set")
    par.add_argument("processes", type=int, nargs='?', default=1)
    par.add_argument("size", type=int, nargs='?', default=500)
    par.add_argument("num", type=int, nargs='?', default=10000)
    par.add_argument("--verbose", type=int, default=1)
    par.add_argument("--skipdata", action='store_true')
    par.add_argument("--init_only", action='store_true')
    par.add_argument("--make_input_only", action='store_true')
    par.add_argument("--process_only", action='store_true')
    par.add_argument("--timeout", type=int, default=120)
    par.add_argument("--sleep", type=int, default=60)
    par.add_argument("--chunk_size", type=int, default=None)
    par.add_argument("--job", default=None)
    par.add_argument("--incremental", action='store_true')
    par.add_argument("--log", action='store_true')

    config = par.parse_args()

    if config.make_input_only:
        if not config.skipdata:
            qmmap_src.objects.delete()
        make_random_input(config.num, config.size, config.job)
        sys.exit(0)

    if not config.process_only:
        if not config.skipdata:
            if input("drop qmmap_src, qmmap_dest?")[:1] == 'y':
                db.qmmap_src.drop()
                db.qmmap_dest.drop()

            print("Generating test data, this may be slow...")
            make_random_input(config.num, config.size, config.job)
        else:
            if input("drop qmmap_dest?")[:1] == 'y':
                db.qmmap_dest.drop()

    print("Running mmap...")
    t = time.time()
    qmmap.mmap(process, "qmmap_src", "qmmap_dest", init=init,
        multi=config.processes, verbose=config.verbose, init_only=config.init_only,
        process_only=config.process_only, timeout=config.timeout,
        sleep=config.sleep, job=config.job, incremental=config.incremental,
        log=config.log, chunk_size=config.chunk_size)
    print("time processing:", time.time() - t, "seconds")
    print("representative output:")
#     print list(db.qmmap_dest.find())
    try:
        for o in qmmap_dest.objects.limit(3):
            print(o.s[:20])
    except RuntimeError:
        pass
    print()

    #inspect housekeeping collection for fun & profit
    good = 0
    total = 0
    hkcol = config.job if config.job else "qmmap_src_qmmap_dest"
    for hk in db[hkcol].find():
        good += hk['good']
        total += hk['total']
    print("%d succesful operations out of %d" % (good, total))
