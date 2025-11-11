#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica le variazioni e manda CSV a assterritorio@amiu.genova.it giornalmemte con la sintesi delle stesse 
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import *


#import requests

import logging
import email, smtplib, ssl
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from invio_messaggio import *


from crea_dizionario_da_query import *

import locale
#locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')

import csv

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

'''#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    filename=logfile,
    level=logging.DEBUG)
'''


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/test_tappe.log'.format(path)
errorfile='{}/log/error_test_tappe.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)







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
    
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True



    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    
    
    
    # PRIMA VERIFICO SE CI SIANO DIFFERENZE CHE GIUSTIFICHINO IMPORTAZIONE
    curr1 = conn.cursor()
    sel_sit='''select now()
    from etl.v_tappe vt 
    limit 1 '''
    try:
        curr1.execute(sel_sit, ())
        #logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
        #curr1.rowfactory = makeDictFactory(curr1)
        tappe_sit=curr1.fetchall()
    except Exception as e:
        logger.error(sel_sit )
        logger.error(e)
    
    
    cur1 = con.cursor()
    sel_uo='''SELECT SYSDATE FROM DUAL
    ''' 
    try:
        cur1.execute(sel_uo, ())
        #cur1.rowfactory = makeDictFactory(cur1)
        tappe_uo=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    curr1.close()  
    cur1.close()      

    
    logger.debug(tappe_sit[0][0])
    logger.debug(tappe_uo[0][0])

    
                
    
   
    
    
    

    # CHIUDO LE CONNESSIONI 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()


if __name__ == "__main__":
    main()