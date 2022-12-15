# assumes mongodb running locally, database named 'test', function input is a
# pymongo object

import sys, os
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..") ) 
sys.path.insert(0, PYBASE)

from mongoengine import Document, IntField, connect
import pymongo 

from qmmap import toMongoEngine, connectMongoEngine, mmap


con=connect("test")
connectMongoEngine(con)

class qmmap_in(Document):
    num = IntField(primary_key = True)

class qmmap_out(Document):
    val = IntField(primary_key = True)

def func(source):
    gs = toMongoEngine(source, qmmap_in)
    gd = qmmap_out(val = gs.num * 10)
    return gd.to_mongo()

db = pymongo.MongoClient().test
for i in range(10):
    db.qmmap_in.save({'_id': i})

ret = mmap(func, "qmmap_in", "qmmap_out", reset=True)

try:
    for o in qmmap_out.objects:
        print(o.val, end=' ')
except RuntimeError:
    pass