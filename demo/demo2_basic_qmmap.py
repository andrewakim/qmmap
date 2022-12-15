# assumes mongodb running locally, database named 'test', function input is a
# pymongo object
import sys, os
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..") ) 
sys.path.insert(0, PYBASE)

import pymongo
from qmmap import mmap

db = pymongo.MongoClient().test

for i in range(10):
    db.qmmap_in.save({'_id': i})

def func(source):
    return {'_id': source['_id']*10}

ret = mmap(func, "qmmap_in", "qmmap_out")
print((list(ret.find())))
