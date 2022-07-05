#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga il WS di IDEA che registra i conferimenti
'''


import os, sys, getopt, re  # ,shutil,glob

import argparse
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path

import datetime


import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

#sys.path.append('../')
from credenziali import *
#from recupera_token import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/sit_test.log'.format(path)
errorfile='{}/error_sit_test.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''


# Create a custom logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)




def main():
    #################################################################
    logger.info("Recupero il token")


    #url_auth_test="http://amiupostgres/SIT_AUTH_TEST/Authentication.aspx"
    #response = requests.get(url_auth_test, params={'user':'DSI/Marzocchi', 'password':'Amiuamiu7'})
    #logger.debug(response.status_code)
    #exit()
    #################################################################
    '''logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()'''
    #conn.autocommit = True
    #################################################################
    hed = {"Authorization": "Bearer " + token1}

    logger.debug(hed)
    url_test= 'http://amiupostgres/SIT_API_TEST/api/aste/1418020002/percorsi/22146'
    response = requests.get(url_test, headers=hed)
    #response.json()
    logger.debug(response.status_code)

if __name__ == "__main__":
    main()  