# QMmap

## Distributed MongoDB Map

### from hiQ Labs (www.hiqlabs.com)

QMmap is a lightweight library that enables asynchronous, parallel processing of MongoDB documents using a simple, map-like interface.

*The following examples were created in IPython, an excellent tool. Cut and paste these examples into your IPython notebook, or run the Python file indicated.*

Python's `map` function takes a callback function and applies it to a list, returning a list:

```Python
# demo/demo1_basic_map.py

src = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
def func(v):
    return v*10

list(map(func, src))

```
*IPython output:*

    [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

QMmap provides a similar function, ```mmap```, that operates on MongoDB collections:

```Python
# demo/demo2_basic_qummap.py
# assumes mongodb running locally, database named 'test', function input is a
# pymongo object

import pymongo
from qmmap import mmap

db = pymongo.MongoClient().test

for i in range(10):
    db.qmmap_in.save({'_id': i})

def func(source):
    return {'_id': source['_id']*10}

ret = mmap(func, "qmmap_in", "qmmap_out")
list(ret.find())
```
    [{u'_id': 0}, {u'_id': 10}, {u'_id': 20}, {u'_id': 30}, {u'_id': 40}, {u'_id': 50}, {u'_id': 60}, {u'_id': 70}, {u'_id': 80}, {u'_id': 90}]

QMmap has helper functions to support mongoengine classes:

```Python
# demo/demo3_mongoengine_qmmap.py

from mongoengine import Document, IntField, connect
from qmmap import toMongoEngine, connectMongoEngine, mmap
connect("test")

class qmmap_in(Document):
    num = IntField(primary_key = True)

class qmmap_out(Document):
    val = IntField(primary_key = True)

def init(source, dest):
    connectMongoEngine(dest)

def func(source):
    gs = toMongoEngine(source, qmmap_in)
    gd = qmmap_out(val = gs.num * 10)
    return gd.to_mongo()

ret = mmap(func, "qmmap_in", "qmmap_out")

for o in qmmap_out.objects:
    print (o.val)
```

    0 10 20 30 40 50 60 70 80 90

Alternatively, if your processing function is written in a way that converts a mongoengine object to another mongoengine object, simply add a `qmmapify` decorator, passing in the input class.  If you want to preserve the mongoengine-to-mongoengine interface of that function, create a second function, and pass that second one to QMmap.  See below, where the second function (`qmmap_func`) with the dectorator simply calls the first.

```Python
# demo/demo4_
import qmmap

class qmmap_in(Document):
    num1 = IntField()
    num2 = IntField()

class qmmap_out(Document):
    val = IntField()

def func(qmmap_in_obj):
    output = qmmap_out()
    output.val = qmmap_in_obj.num1 + qmmap_in_obj.num2
    return output

@qmmap.qmmapify(qmmap_in)
def qmmap_func(qmmap_in_obj):
    return func(qmmap_in_obj)

ret = qmmap.mmap(qmmap_func, "qmmap_in", "qmmap_out")
```

You can then continue to have `func` operate in the "mongoengine" world while passing `qmmap_func` to qmmap. (

We can leverage multiple CPU's by specifying the ```multi``` parameter:

```Python
# demo/demo4_mongoengine_multicore_qmmap.py
# same as demo3 except for some multicore options specified
# ...
ret = mmap(func, "qmmap_in", "qmmap_out", multi=2, sleep=2, reset=True)
```

## "init" function

You may optionally pass an "init" function parameter to `qmmap.mmap` (if called from the command line, you give it the function's name, and it must be in the same module as the processing `function`). If specified, this init function will be called at the beginning of every chunk.

Its parameters are

- `source`, a pymongo cursor that iterates over the documents in the input chunk
- `dest`, a pymongo cursor into the destination collection

### Context variables

If you return a dictionary from the init function, it will be available to every invocation of the processing function, via the variable `qmmap.context`. This is useful for computations that you want to be done only once.


## Typical use cases and recommendations

### Collection transformations

In the simplest case, just pass a function that computes the output document given the input document.

### Transformation with filter

As above, but you can return `None` for objects that should not be passed on to the output collection.

Alternately, you can use the `query` parameter to specify a pymongo query, and QMmap will only operate on values satisfying the query.  However, it must do the query twice (once when setting up housekeeping and again when iterating over the chunk), so it's only recommended for queries that can make good use of indexes; otherwise, it's best to implement the logic in the processor function itself.

### Ensure operations on certain documents happen together.

Whatever field you set for the `key` parameter of `mmap`, the housekeeping step will ensure that all documents with the same value the `key` paramter are placed in the same chunk.  (By default, it chunks by primary key.)

### Ensure that operations happen in a specific order.

You can specify an attribute in the `sort` parameter of mmap. If you do, mmap will iterate over each chunk only after having sorted by that parameter.

#### Example: Insertion in timestamp order

Let's say you want to compute a list of diffs over time for a user's account balance given snapshots over time. There are many documents in the input collection that correspond to the same user. It's easier to compute these diffs when you know you're always inserting a new account balance in chronological order.

To best handle that case with QMmap, you will want to ensure that all user shapshots for the same user occur in the same chunk (as chunks may be processed in any order), and that they are operated on in order of timestamp. Therefore, set the `key` parameter as the user id and the `sort` parameter as the time stamp.  Then, your processing function can always assume that it has the snapshots in chronological order, so it can safely compute the diff against the last known snapshot for that user.

## Command line invocation

To invoke from the command line, use `qmcli.py`; you *must* pass four required arguments in this order:

```
  module                Python module containing the function you wish to
                        apply to the input collection, e.g. foo.bar[...]
  function              Function within `module` that you wish to apply to
                        each member of theinput collection
  source_col            input/source collection, sent to `module`.`function`
                        for processing
  dest_col              output/destination collection to write output
                        documents of `module`.`function` to
```

Other deviations from the default arguments can be specified as indicated in `qmcli.py --help`.  Importantly, you may draw the settings from a JSON file using `--jsonconfig` or a python module with `pyconfig`. Assuming the file in the example code above is in `processors/convert.py`, you could invoke it from the command line as:

```Bash
qmcli.py --multi=2 processors.convert qmmap_func qmmap_in qmmap_out
```

## Example benchmarks

Run ``test.py`` to see multiple CPU's work for real.

```Python
python test.py
```

```
drop qmmap_in, qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
Generating test data, this may be slow...
Running mmap...
time processing: 16.4323170185 seconds
representative output:
BQZWVQTIEWZWHNERPLCP
FSLTFLDAYTKHWCHKWTTX
BRYOCRKDJGTBZCKMMSIG
```

On my machine using one process, this took about 16 seconds. Let's try to use all 4 cores:

```Python
python test.py 4 --skipdata --sleep 1
```

```
drop qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
Running mmap...
time processing: 8.48818993568 seconds
representative output:
BQZWVQTIEWZWHNERPLCP
FVPKESQNSFVIHUQQOJCX
UXSIJIMOOHGBFWGGSENP
```

Larger data sets should provide even better results; speedup should approach the number of CPU's available.

Multiple separate machines can operate on the same data, as a compute cluster. To accomplish this, we break up the processing into an initialization phase which runs first, then run a process phase on multiple nodes:

```Python
run test.py 4 --skipdata --init_only
```

```
drop qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
Running mmap...
time processing: 0.20853805542 seconds
representative output:

0 succesful operations out of 10000
```

Now we start processing on two "nodes" (for now, we will test in two IPython shells on the same machine):

Shell 1:
```Python
run test.py 2 --skipdata --process_only --verbose=0
```

```
Running mmap...
time processing: 6.0252699852 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

Shell 2 (start this right away):
```Python
run test.py 2 --skipdata --process_only --verbose=0
```

```
Running mmap...
time processing: 4.03092908859 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

## Notes on multi-node runs

Once a job is set up, you can continue to add workers on other nodes by invoking mmap with `--process_only`. One suggested model for doing so (e.g. in a Jenkins workflow) is as follows:

1) Allocate nodes such that the number of executors equals the number of cores on the node.

2) Set up a chain of jobs: first a job runs the initialization state (`qmcli.py` with `--init_only`)

3) That initialization is followed by then two jobs simultaneously: a `--manage_only` job and a spawning of multiple instances of a `--process_only` job (perhaps using the Build Flow plugin if on Jenkins

4) There should be as many `--process_only` jobs as there are executors on the available nodes, which in turn is equal to the number of cores.
