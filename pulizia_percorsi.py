#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script controlla la presenza all'interno dei percorsi attivi di elementi di piazzole eliminate 
e ripulisce i percorsi 

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/pulizia_percorsi.log'.format(path)
errorfile='{}/log/pulizia_percorsi.log'.format(path)
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


# libreria per invio mail
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



################################
# DA CAMBIARE 
nome_db='sit'
#################################



def main():
    
    # connessione a PostgreSQL
    logging.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    curr1 = conn.cursor()
    curr2 = conn.cursor()
    #conn.autocommit = True



    # cerco i percorsi attivi su SIT
    query_el='''SELECT eap2.id_elemento, eap2.id_asta_percorso, ap.id_percorso, e.id_piazzola  
        FROM elem.elementi_aste_percorso eap2
        join elem.elementi e on e.id_elemento= eap2.id_elemento  
        join elem.aste_percorso ap on ap.id_asta_percorso = eap2.id_asta_percorso 
        where eap2.id_elemento in (
        select distinct id_elemento from elem.elementi e where id_elemento in (
        select id_elemento from elem.elementi_aste_percorso eap) and  id_piazzola in (
        select id_piazzola from elem.piazzole where data_eliminazione is not null
        ))'''

    try:
        curr.execute(query_el)
        elementi=curr.fetchall()
    except Exception as e:
        logging.error(query_el)
        logging.error(e)
    
    #controllo lunghezza
    logging.debug(len(elementi))
    
    
    for ee in elementi:
        
        # elimino gli elementi

        query_delete= '''DELETE from elem.elementi_aste_percorso eap 
where id_elemento = %s and id_asta_percorso = %s'''
        try:
            curr1.execute(query_delete, [ee[0], ee[1]])
        except Exception as e:
            logging.error(query_delete)
            logging.error(e)
        

        descr='Rimosso elemento {0} da piazzola {1} da script python'.format(ee[0], ee[3])

        query_log='''INSERT INTO util.sys_history
("type", "action", description, datetime, id_user, id_piazzola, id_percorso, id_elemento)
VALUES('PERCORSO', 'UPDATE_ELEM', %s,
 CURRENT_TIMESTAMP, 0, %s, %s, %s);
 '''
        try:
            curr2.execute(query_log, (descr, ee[3],ee[2], ee[0]))
        except Exception as e:
            logging.error(query_log)
            logging.error(e)
        
        conn.commit()
        
    curr2.close()    
    curr1.close()
    curr.close()

if __name__ == "__main__":
    main()   