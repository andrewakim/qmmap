#
# mongo Operations
#
import sys, os, importlib, datetime, time, traceback, __main__
import socket
from pprint import pformat
import bson
import pymongo
from pymongo.read_preferences import ReadPreference
from multiprocessing import Process
import mongoengine as meng
from mongoengine.context_managers import switch_collection

NULL = open(os.devnull, "w")
BATCH_SIZE = 600  # Set input batch size; mongo will limit it if it's too much

def is_shell():
    return sys.argv[0] == "" or sys.argv[0][-8:] == "/ipython"

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()  # total # of entries to do
    good = meng.IntField(default = 0)  # entries successfully processed
#     bad = meng.IntField(default = 0)                    # entries we failed to process to completion
#     log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = meng.StringField(default = 'open')
    # Globally unique identifier for the process, if any, that is working on
    # this chunk, to know if something else is working on it
    procname = meng.StringField(default = 'none')
#     git = meng.StringField()                                 # git commit of this version of source_destination
    tstart = meng.DateTimeField()  # Time when job started
    time = meng.DateTimeField()  # Time when job finished
    meta = {'indexes': ['state', 'time']}

class qmmap_log(meng.DynamicDocument):
    def begin(self, query, srccnt, destcnt, multi, log):
        if multi:
            chunks = housekeep.objects.count()
            self.chunks_allocated = chunks
            if chunks:
                first_chunk = housekeep.objects[0]
                self.first_chunk_size = first_chunk.total
                self.first_processed = first_chunk.start
                last_chunk = housekeep.objects[chunks-1]
                self.last_chunk_size = last_chunk.total
                self.last_processed = last_chunk.end
        self.start = datetime.datetime.utcnow()
        self.query = str(query)
        self.source_count_begin = srccnt
        self.dest_count_begin = destcnt
        if log != True:
            for k, v in log.items():
                setattr(self, k, v)
        self.save()

    def finish(self, destcnt, multi):
        self.finished = datetime.datetime.utcnow()
        if multi:
            tot = 0
            try:
                for x in housekeep.objects:
                    tot += x.good
            except RuntimeError:
                pass
            self.records_processed = tot
            self.chunks_allocated = housekeep.objects.count()
            self.chunks_processed = housekeep.objects(state='done').count()
        self.dest_count_end = destcnt
        self.save()

    def __repr__(self):
        return pformat(self.to_mongo().to_dict())

def _connect(srccol, destcol, dest_uri=None, job=None):
    connectMongoEngine(destcol, dest_uri)
    if job == None:
        hk_colname = srccol.name + '_' + destcol.name
    else:
        hk_colname = job
    log_colname = hk_colname + "_log"
    switch_collection(housekeep, hk_colname).__enter__()
    switch_collection(qmmap_log, log_colname).__enter__()

def _init(srccol, destcol, key, query, chunk_size, verbose):
    housekeep.drop_collection()

    if verbose & 2: print("housekeeping query: %s" % query)
    q = srccol.find(query, [key]).sort([(key, pymongo.ASCENDING)]) ### sort sorta stopped working :/
    cnt = q.count()
    if cnt==0:
        print ("Zero rows to process: exiting")
        return 0
    if verbose & 2: print("initializing %d entries, housekeeping for %s" % (cnt, housekeep._get_collection_name()))
    i = 0
    gtotal=0
    # tot = min(chunk_size, cnt)
    # t0 = time.time()
    # while tot > 0:
    #     if verbose & 2:
    #         t = time.time()-t0
    #         est = t * cnt / chunk_size
    #         print("housekeeping: %d time: %f est total time for housekeeping: %f seconds (%f hours)" % (i, t, est, est/3600))
    #         t0 = time.time()
    #     i +=1
    #     hk = housekeep()
    #     hk.start = q[gtotal][key]
    #     if verbose & 2: print("  start:", hk.start)
    #     hk.end =  q[gtotal + min(chunk_size-1, tot-1)][key]
    #     if verbose & 2: print("  end:", hk.end)
    #     if (hk.start == None or hk.end == None):
    #         if verbose & 2: print("ERROR: key field has None. start: %s end: %s" % (hk.start, hk.end), file=sys.stderr)
    #         raise Exception("key error")
    #     hk.total = min(chunk_size, cnt-gtotal)
    #     if verbose & 2: print("  total:", hk.total)
    #     gtotal+=hk.total
    #     hk.save()
    #     tot = min(chunk_size, cnt-gtotal)
    #     if verbose & 2: sys.stdout.flush()
    while True:
        hk = housekeep()
        hk.total = min(chunk_size, cnt - gtotal)
        if verbose & 2: print("  total:", hk.total)
        if hk.total == 0:
            break
        hk.start = q.next()[key]
        if verbose & 2: print("  start:", hk.start)
        if hk.total==1:
            hk.end = hk.start
        else:
            for n in range(hk.total-2):
                nx = q.next()
                if verbose & 4: print("  next:", nx[key])
            hk.end = q.next()[key]
        if verbose & 2: print("  end:", hk.end)
        hk.save()
        gtotal += hk.total
    return gtotal


def _is_okay_to_work_on(hkstart):
    """Returns whether a chunk, identified by its housekeeping start value, is okay
to work on, i.e. whether its status is "working" and this process is assigned to it
    """
    if not hkstart:  # Can ignore if specific chunk not specified
        return True
    chunk = housekeep.objects.get(start=hkstart)
    # If it's been reset to open, or being worked on by another node, no good
    state = chunk.state
    if state == "done":
        print("Chunk {0} is already finished".format(hkstart))
        sys.stdout.flush()
        return False
    if state == "open":
        print("Chunk {0} had been reset to open".format(hkstart))
        sys.stdout.flush()
        return False
    if state == "working" and chunk.procname != procname():
        print("Chunk {0} was taken over by {1}, moving on".format(
            hkstart, chunk.procname))
        sys.stdout.flush()
        return False
    return True


def _doc_size(doc):
    """Returns the size, in bytes of a Mongo object
    @doc: Mongo document in native Mongo format
    """
    return len(bson.BSON.encode(doc))


def _copy_cursor(cursor):
    """Returns a new cursor with the same properites that won't affect the original
    @cursor: any cursor that hasn't already been iterated over

    @return: new cursor with same attributes
    """
    new_cursor = cursor.collection.find()
    new_cursor.__dict__.update(cursor.__dict__)
    return new_cursor


def _write_bulk(bulk):
    """Execute bulk write `bulk` and note the errors
    """
    try:
        bulk.execute()
    except:
        _print_proc("***BULK WRITE EXCEPTION (process)***")
        _print_proc(traceback.format_exc())
        _print_proc("***END EXCEPTION***")


def _process(init, proc, src, dest, verbose, hkstart=None):
    """Run process `proc` on cursor `src`.
    @hkstart: primary key of houskeeping chunk that this is processing, if you are
using one and which to avoid collisions
    """
    if not verbose & 1:
        oldstdout = sys.stdout
        sys.stdout = NULL
    global context
    if init:
        try:
            # Pass a copy of the source and destination cursors so they won't
            # affect iteration in the rest of _process
            context = init(_copy_cursor(src), dest)#_copy_cursor(dest))
        except:
            _print_proc("***EXCEPTION (process)***")
            _print_proc(traceback.format_exc())
            _print_proc("***END EXCEPTION***")
            return 0
    good = 0
    # After you've accumulated this many bytes of objects, execute the bulk
    # write and start it over
    WRITE_THRESHOLD = 10000000
    inserts = 0
    # Before starting, check if some other process has taken over; in that
    # case, exit early with -1
    if not _is_okay_to_work_on(hkstart):
        return -1
    bulk = dest.initialize_unordered_bulk_op()
    src.batch_size(BATCH_SIZE)
    insert_size = 0  # Size, in bytes, of all objects to be written
    insert_count = 0 # Number of inserts
    for doc in src:
        try:
            ret = proc(doc)
            if ret != None:
                if type(ret) not in (list, tuple):
                    rets = (ret,)
                else:
                    rets = ret
                for ret in rets:
                    # If doing housekeeping, save for bulk insert since that will know
                    # whether these would be duplicate inserts
                    if hkstart:
                        # if _id in ret, search by that and upsert/update_one;
                        # assume that all non-_id, non-$ keys need to be updated with
                        # the $set operator
                        if '_id' in ret:
                            bulk.find({'_id': ret['_id']}).upsert().update_one(
                                {'$set': ret}
                            )
                        elif '__MATCH_AND_UPSERT__' in ret:
                            if ret['__MATCH_AND_UPSERT__'] == True:
                                q = ret
                            else:
                                q = ret['__MATCH_AND_UPSERT__']
                            del ret['__MATCH_AND_UPSERT__']
                            bulk.find(q).upsert().update_one(
                                {'$set': ret}
                            )
                        else:
                            # if no _id, do simple insert
                            bulk.insert(ret)
                        insert_size += _doc_size(ret)
                        insert_count += 1
                        # If past the threshold, do another check and write
                        if insert_size > WRITE_THRESHOLD:
                            if not _is_okay_to_work_on(hkstart):
                                return -1
                            print("Writing to chunk {0} : {1} docs totaling " \
                                "{2} bytes".format(hkstart, insert_count, insert_size))
                            sys.stdout.flush()
                            _write_bulk(bulk)
                            bulk = dest.initialize_unordered_bulk_op()
                            insert_size = 0
                            insert_count = 0
                    else:
                        # No housekeeping checks, so save immediately with DB check
                        dest.save(ret)
                    inserts += 1
            good += 1
        except:
            _print_proc("***EXCEPTION (process)***")
            _print_proc(traceback.format_exc())
            _print_proc("***END EXCEPTION***")
    # After processing, check again if okay to insert
    sys.stdout.flush()
    if not _is_okay_to_work_on(hkstart):
        return -1
    if hkstart:  # Do bulk insert only if doing housekeeping
        if insert_count > 0:
            _print_proc("Writing to chunk {0} : {1} docs totaling {2} " \
                "bytes".format(hkstart, insert_count, insert_size))
            _write_bulk(bulk)
        else:
            _print_proc("No bulk writes to do at end of chunk" \
                " {0}".format(hkstart))
    if not verbose & 1:
        sys.stdout = oldstdout
    sys.stdout.flush()
    return good


context = {}


def do_chunks(init, proc, src_col, dest_col, query, key, sort, verbose, sleep=60):
    while housekeep.objects(state = 'done').count() < housekeep.objects.count():
        tnow = datetime.datetime.utcnow()
        raw = housekeep._collection.find_and_modify(
            {'state': 'open'},
            {
                '$set': {
                    'state': 'working',
                    'tstart': tnow,
                    'procname': procname(),
                }
            }
        )
        # if raw==None, someone scooped us
        if raw != None:
            raw_id = raw['_id']
            #reload as mongoengine object -- _id is .start (because set as primary_key)
            hko = housekeep.objects(start = raw_id)[0]
            # Record git commit for sanity
#             hko.git = git.Git('.').rev_parse('HEAD')
#             hko.save()
            # get data pointed to by housekeep
            qq = {'$and': [query, {key: {'$gte': hko.start}}, {key: {'$lte': hko.end}}]}
            # Make cursor not timeout, using version-appropriate paramater
            if pymongo.version_tuple[0] == 2:
                cursor = src_col.find(qq, timeout=False)
            elif pymongo.version_tuple[0] == 3:
                cursor = src_col.find(qq, no_cursor_timeout=True)
            else:
                raise Exception("Unknown pymongo version")
            # Set the sort parameters on the cursor
            if sort[0] == "-":
                cursor = cursor.sort(sort[1:], pymongo.DESCENDING)
            else:
                cursor = cursor.sort(sort, pymongo.ASCENDING)
            if verbose & 2: print("mongo_process: %d elements in chunk %s-%s" % (cursor.count(), hko.start, hko.end))
            sys.stdout.flush()
            # This is where processing happens
            hko.good =_process(init, proc, cursor, dest_col, verbose,
                hkstart=raw_id)
            # Check if another job finished it while this one was plugging away
            hko_later = housekeep.objects(start = raw_id).only('state')[0]
            if hko.good == -1:  # Early exit signal
                print("Chunk at %s lost to another process; not updating" % raw_id)
                sys.stdout.flush()
            elif hko_later.state == 'done':
                print("Chunk at %s had already finished; not updating" % raw_id)
                sys.stdout.flush()
            else:
                hko.state = 'done'
                hko.procname = 'none'
                hko.time = datetime.datetime.utcnow()
                hko.save()
        else:
            # Not all done, but none were open for processing; thus, wait to
            # see if one re-opens
            print('Standing by for reopening of "working" job...')
            sys.stdout.flush()
            time.sleep(sleep)


def _num_not_at_state(state):
    """Helper for consisely counting the number of housekeeping objects at a
given state
    """
    return housekeep.objects(state__ne=state).count()


# balance chunk size vs async efficiency etc
# otherwise try for at least 10 chunks per proc
#
def _calc_chunksize(count, multi, chunk_size=None):
    if chunk_size != None:
        return chunk_size
    cs = count/(multi*10.0)
    cs = max(cs, 10)
    if count / float(cs * multi) < 1.0:
        cs *= count / float(cs * multi)
        cs = max(1, int(cs))
    return int(cs)



def procname():
    """Utility for getting a globally-unique process name, which needs to combine
hostname and process id
@returns: string with format "<fully qualified hostname>:<process id>"."""
    return "{:>18}:{}".format(socket.getfqdn(), os.getpid())


def mmap(   cb,
            source_col,
            dest_col,
            init=None, 
            reset=False,
            source_uri="mongodb://127.0.0.1/test", 
            dest_uri="mongodb://127.0.0.1/test",
            query={},
            key='_id',
            sort='_id',
            verbose=1,
            multi=None,
            wait_done=True,
            init_only=False,
            process_only=False,
            manage_only=False,
            chunk_size=None,
            timeout=120,
            sleep=60,
            log=False, #True (create log), or instance of qmmap_log
            incremental=False,
            delete_temp_collections=False,
            job=None,
            ignore_job=False,
            # **kwargs,
            ):

    # Two different connect=False idioms; need to set it false to wait on
    # connecting in case of process being spawned.
    if pymongo.version_tuple[0] == 2:
        dbs = pymongo.MongoClient(
            source_uri, read_preference=ReadPreference.SECONDARY_PREFERRED,
            _connect=False,
        ).get_default_database()
        dbd = pymongo.MongoClient(dest_uri, _connect=False).get_default_database()
    else:
        dbs = pymongo.MongoClient(
            source_uri, read_preference=ReadPreference.SECONDARY_PREFERRED,
            connect=False,
        ).get_default_database()
        dbd = pymongo.MongoClient(dest_uri, connect=False).get_default_database()
    dest = dbd[dest_col]
    stot = None
    if multi == None:  # don't use housekeeping, run straight process
        print ("multi==None disables logging and incremental")
        if reset:
            print(("Dropping all records in destination db" +
                   "/collection {0}/{1}").format(dbd, dest.name), file=sys.stderr)
            dest.remove({})
        source = dbs[source_col].find(query)
        # if log:
        #     q = log
        #     log = qmmap_log()
        #     log.begin(query, stot, dest.count(), multi, q)
        _process(init, cb, source, dest, verbose)
    else:
        _connect(dbs[source_col], dest, dest_uri, job=job)
        if incremental:
            q = qmmap_log.objects(finished__exists=True, last_processed__exists=True).order_by("-finished")
            if q.limit(1).count(with_limit_and_skip=True):
                last = q[0].last_processed
                if query != {}:
                    print ("WARNING: incremental does not play well with a filtered query")
                if '_id' in query:
                    raise Exception("_id replaced -- FIXME")
                query['_id'] = {'$gt': last}
                if verbose & 2:
                    print ("query modified for incremental:", query)
            else:
                print ("No existing log file, ignoring incremental flag")
                incremental=False
#
# Note: we explicitly ALWAYS query for qmmmap_job even if it is None -- in which case
# we process everything without a job the old way
# IOW, backwards compatibility FTW
#
        if not ignore_job:
            query['qmmap_job'] = job
        print("query modified for job:", query)
        if manage_only:
            manage(timeout, sleep)
        elif not process_only:
            computed_chunk_size = _calc_chunksize(
                dbs[source_col].find(query).count(), multi, chunk_size)
            if verbose & 2: print("chunk size:", computed_chunk_size)
            if reset:
                print(("Dropping all records in destination db" +
                    "/collection {0}/{1}").format(dbd, dest.name), file=sys.stderr)
                dest.remove({})
            stot=_init(dbs[source_col], dest, key, query, computed_chunk_size, verbose)
            if log and stot:
                q = log
                log = qmmap_log()
                log.begin(query, stot, dest.count(), multi, q)
                if incremental and 'first_processed' in log:
                    delta=log.first_processed.generation_time - last.generation_time
                    if delta.total_seconds() < 60:
                        print("WARNING: potential out-of-order ObjectID's %s and %s are only %s seconds apart" %
                          (last, log.first_processed, delta))
        # Now process code, if one of the other "only_" options isn't turned on
        if not manage_only and not init_only and stot:
            args = (init, cb, dbs[source_col], dest, query, key, sort, verbose,
                sleep)
            if verbose & 2:
                print("Chunking with arguments %s" % (args,))
            if is_shell():
                print(("WARNING -- can't generate module name. Multiprocessing will be emulated..."), file=sys.stderr)
                do_chunks(*args)
            else:
                if multi > 1:
                    for j in range(multi):
                        if verbose & 2:
                            print("Launching subprocess %s" % j)
                        proc = Process(target=do_chunks, args=args)
                        proc.start()
                else:
                    do_chunks(*args)
            if wait_done:
                manage(timeout, sleep)
                #wait(timeout, verbose & 2)
    if log and stot:
        log.finish(dest.count(), multi)
    if delete_temp_collections:
        if incremental:
            print ("WARNING: --incremental is not compatible with --delete_temp_collections so not deleting")
        else:
            housekeep.drop_collection()
            qmmap_log.drop_collection()
    return dbd[dest_col]

def toMongoEngine(pmobj, metype):
    meobj = metype._from_son(pmobj)
    meobj.validate()
    return meobj


def qmmapify(meng_class):
    """Decorator for turning a `process` function writeen for mongoengine objects,
to a process function written for pymongo objects (and therefore compatible with
QMmap.
    params:
    @meng_class: mongoengine class for the type that the mongoengine function
    expects as an argument
    """
    def pymongo_process_fn(meng_process_fn):
        def wrapper(pymongo_source):
            input_meng_obj = toMongoEngine(pymongo_source, meng_class)
            output_meng_obj = meng_process_fn(input_meng_obj)
            # If it returned an object at all, convert that to pymongo
            if output_meng_obj:
                return output_meng_obj.to_mongo()
            else:
                return None
        return wrapper
    return pymongo_process_fn


def connectMongoEngine(pmcol, conn_uri=None):
    if pymongo.version_tuple[0] == 2:     #really? REALLY?
        #host = pmcol.database.connection.HOST
        #port = pmcol.database.connection.PORT
        host = pmcol.database.connection.host
        port = pmcol.database.connection.port
    else:
        host = pmcol.database.client.HOST
        port = pmcol.database.client.PORT
    # Can just use the connection uri, which has credentials
    if conn_uri:
        return meng.connect(pmcol.database.name, host=conn_uri)
    return meng.connect(pmcol.database.name, host=host, port=port)

def remaining():
    return housekeep.objects(state__ne = "done").count()

def wait(timeout=120, verbose=True):
    t = time.time()
    r = remaining()
    rr = r
    while r:
#         print "DEBUG r %f rr %f t %f" % (r, rr, time.time() - t)
        if time.time() - t > timeout:
            if verbose: print("TIMEOUT reached - resetting working chunks to open", file=sys.stderr)
            q = housekeep.objects(state = "working")
            if q:
                q.update(state="open", procname='none')
        if r != rr:
            t = time.time()
        if verbose: print(r, "chunks remaning to be processed; %f seconds left until timeout" % (timeout - (time.time() - t))) 
        time.sleep(1)
        rr = r
        r = remaining()


def _print_proc(log_str):
    """Utility function for writing to STDERR with procname prepended
    @log_str: string to write
    """
#     print >> sys.stderr, procname(), log_str
#make atomic so no interrupted output lines:
    sys.stderr.write("%s %s\n" % (procname(), log_str) )
    sys.stderr.flush()


def _print_progress():
    q = housekeep.objects(state = 'done').only('time')
    tot = housekeep.objects.count()
    done = q.count()
    if done > 0:
        pdone = 100. * done / tot
        q = q.order_by('time')
        first = q[0].time
        q = q.order_by('-time')
        last = q[0].time
        if first and last:  # guard against lacking values
            tdone = float((last-first).total_seconds())
            ttot = tdone*tot / done
            trem = ttot - tdone
            print("%s still waiting: %d out of %d complete (%.3f%%). %.3f seconds complete, %.3f remaining (%.5f hours)" \
            % (datetime.datetime.utcnow().strftime("%H:%M:%S:%f"), done, tot, pdone, tdone, trem, trem / 3600.0))
        else:
            print("No progress data yet")
    else:
        print("%s still waiting; nothing done so far" % (datetime.datetime.utcnow(),))
sys.stdout.flush()


def manage(timeout, sleep=120):
    """Give periodic status, reopen dead jobs, return success when over;
    combination of wait, status, clean, and the reprocessing functions.
    sleep = time (sec) between status updates
    timeout = time (sec) to give a job until it's restarted
    """
    T0 = time.time()
    num_not_done = _num_not_at_state('done')
    tot=num_not_done
    print("Managing job's execution; currently {0} remaining".format(num_not_done))
    sys.stdout.flush()
    # Keep going until none are state=working or done
    while _num_not_at_state('done') > 0:
        # Sleep before management step
        time.sleep(sleep)
        _print_progress()
        # Now, iterate over state=working jobs, restart ones that have gone
        # on longer than the timeout param
        tnow = datetime.datetime.utcnow()  # get time once instead of repeating
        # Iterate through working objects to see if it's too long
        hkwquery = []
        try:
            for h in housekeep.objects(state='working').all():
                hkwquery.append(h)
        except RuntimeError:
            pass
        for hkw in hkwquery:
            # .tstart must have time value for state to equal 'working' at all
            time_taken = (tnow - hkw.tstart).total_seconds()
            print(("Chunk on {0} starting at {1} has been working for {2} " +
                "sec").format(hkw.procname, hkw.start, time_taken))
            sys.stdout.flush()
            if time_taken > timeout:
                print(("More than {0} sec spent on chunk {1} ;" +
                    " setting status back to open").format(
                    timeout, hkw.start))
                sys.stdout.flush()
                hkw.state = "open"
                hkw.procname = 'none'
                hkw.save()
    print("----------- PROCESSING COMPLETED ------------")
    T1 = time.time()
    try:
        print ("Total time taken: %f seconds (%f hours); %d chunks at %f sec per chunk" % (T1-T0, (T1-T0)/3600, tot, (T1-T0)/tot))
    except:
        print ("Not enough data to profile")
