#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per allineare SIT con SIT prog:
- piazzole eliminate da SIT e non da SIT prog
- 
- 

'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle

#import openpyxl
#from pathlib import Path


import xlsxwriter


import psycopg2

import datetime

from urllib.request import urlopen
import urllib.parse

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/log/test_error.log'.format(path, giorno_file)
errorfile='{}/log/error.log'.format(path, giorno_file)


# Create a custom logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.INFO)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)





cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)


def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('OK logger INFO level')
    logger.warning('This is a warning 1')
    logger.error('This is an error')
    logger.info('OK logger INFO level 2')
    count=len(open(errorfile).readlines(  ))
    if  count >0:
        print('''C'è un problema''')
        



if __name__ == "__main__":
    main()  