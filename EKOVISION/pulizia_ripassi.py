#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script interroga le schede di lavoro e fornisce un elenco di quelle da cancellare

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


import requests
from requests.exceptions import HTTPError

import json

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/pulizia_ripassi.log'.format(path)
errorfile='{}/log/error_pulizia_ripassi.log'.format(path)
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



    
     

def main():
      
    #
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()

    
    query1='''select codice_modello_servizio, ordine, codice, ripasso, data_inizio, data_fine
    from anagrafe_percorsi.v_percorsi_elementi_tratti 
        where codice_modello_servizio in ( 
            select codice_modello_servizio from anagrafe_percorsi.v_percorsi_elementi_tratti 
            where (data_inizio!=data_fine  or data_fine is null) and ripasso > 2
        ) and codice in 
        (select codice from anagrafe_percorsi.v_percorsi_elementi_tratti 
            where (data_inizio!=data_fine  or data_fine is null) and ripasso > 2)
        order by codice_modello_servizio, codice,  ripasso'''
    
    
    try:
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        curr.execute(query1)
        lista_variazioni=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

    percorso_con_problemi=[]
    
    rip=0 
    cod_percorso=''
    for vv in lista_variazioni:
        logger.debug(rip)
        # controllo la data fine
        if vv[5] is None:
            schema='UPDATE elem.elementi_aste_percorso '
        else:
            schema='UPDATE history.elementi_aste_percorso '
        if cod_percorso!='' and vv[0]==cod_percorso and vv[2]==id_elemento:
            rip+=1
        else:
            rip=0
        cod_percorso=vv[0]
        id_elemento=vv[2]
        id
        query_update='''{} set ripasso = %s 
        where id_elemento=%s and 
        id_asta_percorso in 
            (select id_asta_percorso from elem.aste_percorso
            where id_percorso in (
                select id_percorso from elem.percorsi p 
                where cod_percorso = %s
                )
            )'''.format(schema)
        curr.execute(query_update, (rip, int(vv[2]), vv[0]))
        logger.debug('ripasso={0}, id_elemento={1}, cod_percorso={2}'.format(rip, int(vv[2]), vv[0]))
        conn.commit()
        
            
        
    
    
    
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      