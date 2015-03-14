#
# test mongoo
#
import sys, os, importlib
import mongoengine as meng
if len(sys.argv) > 1 and 'config=' in sys.argv[1]:
    config = importlib.import_module(sys.argv[1][7:])
else:
    import config

from goosrc_goodest import goosrc
from mongoo import connect2db

connect2db(goosrc, config.src_db)
if raw_input("drop goosrc?")[:1] == 'y':
    goosrc.drop_collection()

for i in range(0, int(sys.argv[-1]), 3):
    g = goosrc(num = i)
    g.save()
    if i == 6:
        g = goosrc(num = i)
        g.save()
        g = goosrc(num = i)
        g.save()
print "created %d objects" % goosrc.objects.count()
