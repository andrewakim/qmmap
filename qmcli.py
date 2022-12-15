#!/usr/bin/env python

import argparse
from bson.json_util import loads
import importlib
import json
import os
import sys

import qmmap


def main():
    par = argparse.ArgumentParser(description="""
Command line wrapper for QMmap utility, which distributes the work of processing
one collection into other by use of a function.
""")
    par.add_argument("--verbose", type=int, default = 1, help=
        "How much debugging info: 1, 2, or 3 (default=1)"
    )
    par.add_argument("--multi", type=int, default = None, help=
        "How many subprocesses to spawn on this node for this job; affects " \
        "chunking of housekeeping collection"
    )
    par.add_argument("--reset", action='store_true', help=
        "Drop output collection during housekeeping setup stage"
    )
    par.add_argument("--init_only", action='store_true', help=
        "Only set up housekeeping collection (optionall drop output collection)"
    )
    par.add_argument("--process_only", action='store_true', help=
        "Only run the processing stage of QMmap: query Housekeeping for " \
        "available chunks and process them"
    )
    par.add_argument("--manage_only", action='store_true', help=
        "Only run the 'manage' stage of QMmap: report progress and reopen chunks" \
        "being processed if they have taken longer than `timeout` seconds"
    )
    par.add_argument("--source_uri", default="mongodb://127.0.0.1/test", help=
        "URI to the input DB, in MongoDB format:\n" \
        "mongodb://[username:password@]host[:port]/database"
    )
    par.add_argument("--dest_uri", default="mongodb://127.0.0.1/test", help=
        "URI to the output DB, in MongoDB format:\n" \
        "mongodb://[username:password@]host[:port]/database"
    )
    par.add_argument("--chunk_size", type=int, default=None, help=
        "Specify CHUNK_SIZE as the size of each chunk; default is to compute" \
        "based on QMmap's algorithm based on input collection size and number" \
        "of processes"
    )
    par.add_argument("--timeout", type=int, default=120, help=
        "How long, in seconds, to allow a processor to work on a chunk before " \
        "assuming it has died and should be released for other processes to work " \
        "on it; default = 120 seconds"
    )
    par.add_argument("--sleep", type=float, default=60, help=
        "Report status of the job every SLEEP seconds; default = 60"
    )
    par.add_argument("--query", type=str, default="{}", help=
        "Query to restrict what values of the input collection to operate on; " \
        "default = {} (i.e. all of it)"
    )
    par.add_argument("--init", type=str, default=None, help=
        "String indicating the function in `module` to run on each chunk before " \
        "processing it; default = None (no such function)"
    )
    par.add_argument("--jsonconfig", type=str, default=None, help=
        "Path to JSON file for specifying this utility's parameters; overrides " \
        "any which were specified from the command line"
    )
    par.add_argument("--pyconfig", type=str, default=None, help=
        "Python module (e.g. foo.bar[...] which specifies this utility's " \
        "parameters; overrides any specified from the command line or via " \
        "--jsonconfig"
    )
    par.add_argument("--key", type=str, default="_id", help=
        "Key/field to use for chunking; when setting up chunks, this will ensure" \
        "that no two input documents with the same value for `key` will be placed" \
        "in different chunks; default: _id (primary key)"
    )
    par.add_argument("--sort", type=str, default="_id", help=
        "Key/field to sort by *within* each chunk; this ensures that, when a " \
        "chunk is processed, the items will be processed in ascending order by" \
        "`sort` key (put a '-' in front to sort descending e.g. '-name'); " \
        "default: _id"
    )
    par.add_argument("module", help=
        "Python module containing the function you wish to apply to the input " \
        "collection, e.g. foo.bar[...]"
    )
    par.add_argument("function", help=
        "Function within `module` that you wish to apply to each member of the" \
        "input collection"
    )
    par.add_argument("source_col", help=
        "input/source collection, sent to `module`.`function` for processing"
    )
    par.add_argument("dest_col", help=
        "output/destination collection to write output documents of " \
        "`module`.`function` to"
    )
    clargs = par.parse_args()
    sys.path.insert(0, os.getcwd())
    module = importlib.import_module(clargs.module)
    cb = getattr(module, clargs.function)
    arg_dict = vars(clargs)
    arg_dict['cb'] = cb
    del arg_dict['function']
    # If user has specified a config file, overwrite arg_dict values to the given
    # ones
    if clargs.jsonconfig:
        with open(clargs.jsonconfig, "r") as f:
            config_dict = json.loads(f.read())
        arg_dict.update(config_dict)
    if clargs.pyconfig:
        configmod = importlib.import_module(clargs.pyconfig)
        config_dict = {
            x: y for x, y in vars(configmod).items() if not x.startswith("__")
        }
        arg_dict.update(config_dict)
    if clargs.init:
        init_fn = getattr(module, clargs.init)
        arg_dict['init'] = init_fn
    # Remove the parsed params that aren't handled by qmmap.mmap
    del arg_dict['module']
    del arg_dict['pyconfig']
    del arg_dict['jsonconfig']
    # Convert query to python dict
    arg_dict['query'] = loads(arg_dict['query'])
    qmmap.mmap(**arg_dict)


if __name__ == "__main__":
    main()
