#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script è utile per accoppiare i servizi alle UT su UO in maniera massiva e ordinata evitando delle cavolate

Da usare con cautela

Lo script fa pulizia sulle seguenti 3 tabelle: 

- TODO - ORDINE SERVIZI UO (per un servizio, definita SERVUZIO BASE deve essere popolato a mano)    
 
- TODO - FORZA_LAVORO_UO

- DAT_FORZA_LAVORO_UO

'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter
#from xlsxwriter.utility import xl_rowcol_to_cell

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail


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
import calendar

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
logfile='{}/log/servizi_su_uo.log'.format(path)
errorfile='{}/log/error_servizi_su_uo.log'.format(path)
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


    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    
    

    ###############################################################################
    # DAT_FORZA_LAVORO_UO
    
    query1='''SELECT ID_UO, ID_SERVIZIO, TURNO, max(QUANTITA), min(QUANTITA), count(QUANTITA)
        FROM DAT_FORZA_LAVORO_UO dflu  
        GROUP BY ID_SERVIZIO, ID_UO, TURNO 
        HAVING count(QUANTITA) > 1
        ORDER BY ID_UO
    ''' 
    try:
        cur.execute(query1)
        #cur.rowfactory = makeDictFactory(cur)
        dflu=cur.fetchall()
    except Exception as e:
        logger.error(query1)
        logger.error(e)
        
        
    for dd in dflu:
        delete1= '''DELETE FROM UNIOPE.DAT_FORZA_LAVORO_UO
        WHERE ID_UO = :id_uo AND ID_SERVIZIO = :id_serv AND TURNO = :tt
        '''
        
        insert1= '''INSERT INTO UNIOPE.DAT_FORZA_LAVORO_UO
        (ID_UO, ID_SERVIZIO, TURNO, QUANTITA)
        VALUES (:id_uo, :id_serv, :tt, 0)
        '''
        
        try:
            cur.execute(delete1, (dd[0], dd[1], dd[2]))
        except Exception as e:
            logger.debug(delete1, (dd[0], dd[1], dd[2]))
            logger.error(e)
        
        try:
            cur.execute(insert1, (dd[0], dd[1], dd[2]))
        except Exception as e:
            logger.debug(insert1, (dd[0], dd[1], dd[2]))
            logger.error(e)
            #logger.debug(insert1, data)


    
    con.commit()
    cur.close()
    con.close()
    
    


if __name__ == "__main__":
    main()