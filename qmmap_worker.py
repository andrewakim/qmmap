#!/usr/bin/env python
#
# qmmap worker invoked as command-line script
#
import sys, os, importlib, argparse, json, pymongo
from qmmap import do_chunks, connectMongoEngine, housekeep
from mongoengine.context_managers import switch_collection

par = argparse.ArgumentParser(description = "qmmap worker process")
par.add_argument("module_abspath")
par.add_argument("module")
par.add_argument("function")
par.add_argument("source")
par.add_argument("dest")
par.add_argument("--src_uri", type=str, default = "mongodb://127.0.0.1/test")
par.add_argument("--dest_uri", type=str, default = "mongodb://127.0.0.1/test")
par.add_argument("--init", type=str, default = "")
par.add_argument("--query", type=str, default = "{}")
par.add_argument("--key", type=str, default = "_id")
par.add_argument("--verbose", type=int, default = 0)

config = par.parse_args()

query = json.loads(config.query)

sys.path.insert(0, config.module_abspath)
module = importlib.import_module(config.module)
cb = getattr(module, config.function)
init = getattr(module, config.init) if config.init else None

source_db = pymongo.MongoClient(config.src_uri).get_default_database()
source = source_db[config.source]

dest_db = pymongo.MongoClient(config.dest_uri).get_default_database()
dest = dest_db[config.dest]

connectMongoEngine(dest)
hk_colname = source.name + '_' + dest.name
switch_collection(housekeep, hk_colname).__enter__()

# print "DEBUG start worker", os.getpid()
do_chunks(init, cb, source, dest, query, config.key, config.verbose)
# print "DEBUG end worker", os.getpid()
