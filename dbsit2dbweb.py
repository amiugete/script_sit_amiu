#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi - Roberta Fagandini

'''
Lo script si occupa di trasferire dati da DB SIT a DB web_db
Per pubblicazione mappe web su Lizmap

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import requests
from requests.exceptions import HTTPError

import json


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#print('path={0}'.format(path))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

# Create a custom logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


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

# libreria per scrivere file csv
import csv


#from refresh_viste_materializzate_lastposition import move_mv_amiugis



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
      
       
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    
    
    curr = conn.cursor()
    curr1 = conn.cursor()


    # due viste differenti
    # _s (solo svuotamenti)     
    
    
    #sitData = ['geo.piazzola','geo.grafostradale']
    sitData = {'geo.piazzola':('geometry(point,3003)', 'geo.mv_cons_raccolta_60dd_pref'), 'geo.grafostradale':('geometry(linestring,3003)', 'geo.mv_cons_spazzamento_60dd_pref')}
    new_aste = 0
    for sd in sitData:

        if 'grafostradale' in sd:
            query_check_aste = "select id_via from elem.aste a where data_ultima_modifica >= (now() - interval '1 day')::date"
            try:
                curr.execute(query_check_aste)
                lista_aste=curr.fetchall()
                if lista_aste == []:
                    logger.info('Non ci sono nuove aste quindi non faccio il trasferimento dati')
                    new_aste += 1
            except Exception as e:
                logger.error(query_check_aste)
                logger.error(e)
            curr.close()
            conn.close()
        else:
            new_aste = 0

        conn_web = psycopg2.connect(dbname=db_web,
                                port=port,
                                user=user_webroot,
                                password=pwd_webroot,
                                host=host_amiugis)
            
        curr_web = conn_web.cursor()

        if new_aste == 0:
            logger.info('Inizio trasferimento tabella {} su amiugis'.format(sd))
            
                
            # ora creo la tabella su amiugis per questioni di performance
            query_dblink='''select dblink_connect('conn_dblink{0}', 'sit')'''.format(sd.split('.')[1])
            try:
                curr_web.execute(query_dblink)
            except Exception as e:
                logger.error(query_dblink)
                logger.error(e)
            
            
            
            query_dblink1='''truncate {0}'''.format(sd) 

            try:
                curr_web.execute(query_dblink1)
            except Exception as e:
                logger.error(query_dblink1)
                logger.error(e)

            query_dblink2='''insert into {0} (id, geoloc)
            select * from dblink('conn_dblink{1}', 'select id, geoloc from {0};') 
            AS t1(id BIGINT, geoloc {2})'''.format(sd, sd.split('.')[1], sitData[sd][0])

            try:
                curr_web.execute(query_dblink2)
            except Exception as e:
                logger.error(query_dblink2)
                logger.error(e)
                
            """     
            query_dblink3='''ALTER TABLE {0} 
            ADD CONSTRAINT {1}_pk PRIMARY KEY ({2})'''.format(sd, sd.split('.')[1], 'id')

            try:
                curr_web.execute(query_dblink3)
            except Exception as e:
                logger.error(query_dblink3)
                logger.error(e)

            query_dblink4='''CREATE INDEX {0}_geom_idx
            ON {1}
            USING GIST ({2})'''.format(sd.split('.')[1], sd, 'geoloc')
            
            try:
                curr_web.execute(query_dblink4)
            except Exception as e:
                logger.error(query_dblink4)
                logger.error(e) 
            """
                
            query_dblink5='''select dblink_disconnect('conn_dblink{0}')'''.format(sd.split('.')[1])
            
            try:
                curr_web.execute(query_dblink5)
            except Exception as e:
                logger.error(query_dblink5)
                logger.error(e)
            
            logger.info('Fine aggiornamento tabella {} su amiugis'.format(sd))
        

        logger.info('AGGIORNAMENTO vista {0}'.format(sitData[sd][1]))

        ### REFRESH VISTA MATERIALIZZATA POSIZIONE LAST 30 DD ###
        query_refresh = 'REFRESH MATERIALIZED VIEW CONCURRENTLY {0};'.format(sitData[sd][1])
        try:
            curr_web.execute(query_refresh)
            logger.debug('La vista {0} è stata aggiornata correttamente'.format(sitData[sd][1]))
        except Exception as e:
            logger.error(query_refresh)
            logger.error(e)

            
        # faccio commit
        conn_web.commit()

        # CHIUSURA connessione
        curr_web.close()
        conn_web.close()




    


    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()      