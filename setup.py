#!/usr/bin/env python
from distutils.core import setup

setup(name='qmmap',
      version='0.001',
      description='Parallel MongoDB Map',
      url='https://github.com/hiqlabs/qmmap/',
      scripts=["qmcli.py"],
      py_modules=["qmmap"],
     )
