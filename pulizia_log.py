#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Pulire file log
'''

import os,sys, getopt
import inspect, os.path


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import time



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

logpath=logfile='{}/log'.format(path)


#giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/pulizia_log_folder.log'.format(logpath)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)



now = time.time()

for f in os.listdir(logpath):
    if f not in ('README.md'):
        f = os.path.join(logpath, f)
        if os.stat(f).st_mtime < now - 14 * 86400: 
            if os.path.isfile(f):
                #print(f)
                os.remove(os.path.join(path, f))