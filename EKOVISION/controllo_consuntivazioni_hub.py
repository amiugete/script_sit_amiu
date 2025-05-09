#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa del controllo che arrivino dati dai totem all'HUB 



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob


import requests
from requests.exceptions import HTTPError

import json

#import getopt  # per gestire gli input

#import pymssql

#from datetime import date, datetime, timedelta, today

import datetime

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


from descrizione_percorso import *  
    
     

def main():
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
    
    
    footer_mail='''<hr>
<p>Questa è una mail automatica inviata dagli script di lettura e trasferimento a Ekovision. 
<br>
Si prega di NON RISPONDERE alla presente mail. 
In caso di problemi con l'applicativo scrivere 
a assterritorio@amiu.genova.it
</p>'''
        
    # Mi connetto a HUB (PostgreSQL) per poi recuperare le mail
  


    nome_db=db_consuntivazione
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_consuntivazione,
                        password=pwd_consuntivazione,
                        host=host_hub)
    
    currc = connc.cursor()
    
    
    
    # spazzamento
    query_select='''select coalesce(max(datainsert), to_date('20231120','YYYYMMDD'))
from spazzamento.effettuati'''
    try:
        currc.execute(query_select)
        max_date=currc.fetchall()
    except Exception as e:
        logging.error(e)
    
    for dd in max_date:
        max_data=dd[0] 
        
    
    if (datetime.datetime.now() - max_data) > datetime.timedelta(hours=24):
        logger.warning("interval = {0}".format(datetime.datetime.now() - max_data))
        receiver_email='andrea.gava@wingsoft.it'
        mail_cc='assterritorio@amiu.genova.it, pianar@amiu.genova.it'
        
        
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'no_reply@amiu.genova.it'
        message["To"] = receiver_email
        #message["To"] = mail_cc
        ####################################################
        message["Subject"] = 'WARNING - Ultima consuntivazione spazzamento registrato > 24 ore'
        message["Bcc"] = mail_cc  # Recommended for mass emails
        message.preamble = "Ultima consuntivazione spazzamento > 24 ore"

        body='''L'ultima consuntivazione spazzamento scaricata sull'HUB
        risale al <b>{0}</b>.
        <br><br>Verificare la correttezza dei dati
        {1}
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(max_data, footer_mail)
            
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))

        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(parentdir)
        immagine(message,logoname)
        
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        if invio==200:
            logger.info('Messaggio inviato')

        else:
            logger.error('Problema invio mail. Error:{}'.format(invio))
           
    
    # raccolta
    query_select='''select coalesce(max(inser), to_date('20231120','YYYYMMDD'))
from raccolta.effettuati_amiu'''
    try:
        currc.execute(query_select)
        max_date=currc.fetchall()
    except Exception as e:
        logging.error(e)
    
    for dd in max_date:
        max_data=dd[0] 
        
    
    if (datetime.datetime.now() - max_data) > datetime.timedelta(hours=24):
        logger.warning("interval = {0}".format(datetime.datetime.now() - max_data))
        receiver_email='andrea.gava@wingsoft.it'
        mail_cc='assterritorio@amiu.genova.it, pianar@amiu.genova.it'
        
        
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'no_reply@amiu.genova.it'
        message["To"] = receiver_email
        #message["To"] = mail_cc
        ####################################################
        message["Subject"] = 'WARNING - Ultima consuntivazione raccolta registrata > 24 ore'
        message["Bcc"] = mail_cc  # Recommended for mass emails
        message.preamble = "Ultima consuntivazione raccolta > 24 ore"

        body='''L'ultima consuntivazione raccolta scaricata sull'HUB
        risale al <b>{0}</b>.
        <br><br>Verificare la correttezza dei dati
        {1}
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(max_data, footer_mail)
            
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))

        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(parentdir)
        immagine(message,logoname)
        
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        if invio==200:
            logger.info('Messaggio inviato')

        else:
            logger.error('Problema invio mail. Error:{}'.format(invio))
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    





if __name__ == "__main__":
    main()      