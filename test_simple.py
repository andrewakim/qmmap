def process(source):
    print("  processed %s" % source['_id'])
    return {'_id': source['_id']*10}

if __name__ == "__main__":
    import os, pymongo, qmmap, time

    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()

    if input("drop qmmap_src, qmmap_dest, housekeeping(qmmap_src_qmmap_dest)?")[:1] == 'y':
        db.qmmap_src.drop()
        db.qmmap_dest.drop()
        db.qmmap_src_qmmap_dest.drop()

    for i in range(10):
        db.qmmap_src.save({'_id': i})

    ret = qmmap.mmap(process, "qmmap_src", "qmmap_dest", multi=None)
    
    print("output:", list(ret.find()))
