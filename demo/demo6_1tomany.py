# assumes mongodb running locally, database named 'test', function input is a
# pymongo object

import sys, os
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..") ) 
sys.path.insert(0, PYBASE)

import pymongo
from mongoengine import Document, IntField, connect

from qmmap import toMongoEngine, connectMongoEngine, mmap


connect("test") ###not needed if multi>1

class qmmap_in(Document):
    num = IntField(primary_key = True)
    extra = IntField()

class qmmap_out(Document):
    val = IntField(primary_key = True)
    comp = IntField()

def init(source, dest):
    print ("initialize for chunk here")

def func(source):
    gs = toMongoEngine(source, qmmap_in)
    times10 = gs.num * 10
    if gs.num == 4:
        gd1 = qmmap_out(val=times10*20, comp=times10 + gs.extra)
        gd2 = qmmap_out(val=times10*30, comp=times10 - gs.extra)
        ret = [gd1.to_mongo(), gd2.to_mongo()]
    else:
        gd = qmmap_out(val=times10, comp=times10 + gs.extra)
        ret = gd.to_mongo()
    return ret

db = pymongo.MongoClient().test
for i in range(10):
    db.qmmap_in.save({'_id': i, 'extra': i + 1})

ret = mmap(func, "qmmap_in", "qmmap_out", multi=2, sleep=2, reset=True,  init=init)

try:
    for o in qmmap_out.objects:
        print(o.val, o.comp)
except RuntimeError:
    pass