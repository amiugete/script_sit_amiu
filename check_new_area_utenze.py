#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script verifica se sono state aggiunte nuove aree alla tabella etl.aree_ecopunti4326 e/o alla tabella etl.aree_4326
rispettivamente caricate su progetti lizmap utenze_ecopunti e utenze_piazzole con strumento di editing attivo

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
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
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
    tabelle = ['etl.aree_4326','etl.aree_ecopunti_4326']

    receiver_email = 'assterritorio@amiu.genova.it'
    subject = "Nuove richieste di estrazione utenze per ecopunti/piazzole"

    body_mail = ''

    for t in tabelle:
        logger.info('Verifico se sono state inserite nuove aree in {}.'.format(t))
        query= 'select * from {} ae where date(ae.data_disegno) = now()::date and ae.mail is not true and ae.def is true'.format(t)
    
    
        try:
            curr.execute(query)
            lista_aree_def=curr.fetchall()
        except Exception as e:
            logger.error(query)
            logger.error(e)

        # verifico se sono state inserite aree non definitive e nel caso lo scrivo nella mail
        query_nodef= 'select * from {} ae where date(ae.data_disegno) = now()::date and ae.mail is not true and ae.def is not true'.format(t)
        try:
            curr.execute(query_nodef)
            lista_aree_nodef=curr.fetchall()
        except Exception as e:
            logger.error(query_nodef)
            logger.error(e)
    
        if len(lista_aree_nodef) != 0:
            logger.debug('Numero aree nodef: {}'.format(len(lista_aree_nodef)))
            string = '<p><b>ATTENZIONE!</b> Sono state aggiunte aree per cui non è stata spuntata la checkbox <i>"Invia richiesta ad Assterritorio"</i> nella tabella {}, verificare con il territorio!</p><br>'.format(t)
        else:
            string = ''

        logger.info('Numero nuove aree: {}'.format(len(lista_aree_def)))
    
        #if len(lista_aree) != 0:
        for la in lista_aree_def:
            body_mail += '''
            <p>
            L'area con id <b>{}</b> e nome <b>{}</b> è stata aggiunta alla tabella <b>{}</b><br><br>{}
            E' possibile consultare il progetto Lizmap Utenze ecopunti al seguente link: <br>
            <a href="https://amiugis.amiu.genova.it/mappenew2/lizmap/www/index.php/view/map?repository=repository1&project=utenze_ecopunti4326" target="_blank">Visualizza la mappa</a>
            </p><br><p></p>
            '''.format(la[0], la[1], t, string)
        
            query2= 'update {} ae set mail = true where id= %s'.format(t)
            try:
                curr.execute(query2, (la[0],))
                conn.commit()
            except Exception as e:
                logger.error(query2)
                logger.error(e)
            
            logger.info("Aggiorno la tabella {} mettendo mail=true".format(t))
    
    if body_mail:
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email #debug_mail
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Nuova area per estrazione utenze aggiunta"
                        
        message.attach(MIMEText(f'''
            <html>
            <head></head>
            <body>
            <p>Mail generata automaticamente dal codice <b>check_new_area_utenze.py</b></p>
            {body_mail}
            </body>
            </html>
            ''', "html"))

        # aggiunto allegato (usando la funzione importata)
        #allegato(message, file_ut, nome_file_ut)
        
        #text = message.as_string()

        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)
    else:
        logger.info("Nessuna nuova area trovata, nessuna mail inviata.")

    logger.info("chiudo le connessioni in maniera definitiva")


    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      