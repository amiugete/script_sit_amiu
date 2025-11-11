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
    
def crea_messaggio(subject, body_html, to_email, cc_email=''):
    message = MIMEMultipart()
    message["From"] = 'no_reply@amiu.genova.it'
    message["To"] = to_email
    message["Subject"] = subject
    if cc_email:
        message["Bcc"] = cc_email
    message.preamble = subject
    message.attach(MIMEText(body_html, "html"))
    logoname='{}/img/logo_amiu.jpg'.format(parentdir)
    immagine(message, logoname)
    return message

def invia_email(subject, body_html, to_email, cc_email=''):
    message = crea_messaggio(subject, body_html, to_email, cc_email)
    logger.info("Invio mail in corso...")
    invio = invio_messaggio(message)
    logger.info(f"Risultato invio: {invio}")
    if invio == 200:
        logger.info('Messaggio inviato correttamente')
    else:
        logger.error(f'Errore durante l\'invio: {invio}')

def main():
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.datetime.today()
    #oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    #oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    #logger.debug('Oggi {}'.format(oggi))
    
    
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
        
        
        if max_data != datetime.datetime(2023, 11, 20): 
            subjectS = 'WARNING - Ultima consuntivazione spazzamento registrato > 24 ore'
            bodyS='''L'ultima consuntivazione spazzamento scaricata sull'HUB
            risale al <b>{0}</b>.
            <br><br>Verificare la correttezza dei dati
            {1}
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(max_data, footer_mail)
        else:
            
            subject = 'WARNING - Ultima consuntivazione spazzamento con data NULLA'

            body='''       
            L'ultima consuntivazione raccolta scaricata sull'HUB
            risale al <b>{0}</b>.
            <br> Ciò significa che quando ha girato lo script ossia alle {2} 
            la query ha restituito valore nullo.
            Verificare la correttezza dei dati. E' possibile sia un falso positivo, 
            ma se il problema non ci fosse è possibile però che ci 
            siano problemi sulla sincronizzazione da totem a hub (lenta?) 
            {1}
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(max_data, footer_mail, giorno)
            
            
        invia_email(subjectS, bodyS, 
                to_email='andrea.gava@wingsoft.it', 
                cc_email='assterritorio@amiu.genova.it, pianar@amiu.genova.it')

     
   
   
   
   
   
    
    
    
    
    
    # raccolta
    query_selectr='''select coalesce(max(inser), to_date('20231120','YYYYMMDD'))
from raccolta.effettuati_amiu'''
    try:
        currc.execute(query_selectr)
        max_dater=currc.fetchall()
    except Exception as e:
        logging.error(e)
    
    for dd in max_dater:
        max_data=dd[0] 
  
     
    if (datetime.datetime.now() - max_data) > datetime.timedelta(hours=24):
        logger.info("max data = {0}".format(max_data))
        logger.warning("interval = {0}".format(datetime.datetime.now() - max_data))
        
        
        
        if max_data != datetime.datetime(2023, 11, 20): 
            subject = 'WARNING - Ultima consuntivazione raccolta registrata > 24 ore'
            #message.preamble = "Ultima consuntivazione raccolta > 24 ore"

            body='''L'ultima consuntivazione raccolta scaricata sull'HUB
            risale al <b>{0}</b>.
            <br><br>Verificare la correttezza dei dati
            {1}
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(max_data, footer_mail)
        else: 
            subject = 'WARNING - Ultima consuntivazione raccolta con data NULLA'

            body='''L'ultima consuntivazione raccolta scaricata sull'HUB
            risale al <b>{0}</b>. 
            <br> Ciò significa che quando ha girato lo script ossia alle {2} 
            la query ha restituito valore nullo.
            Verificare la correttezza dei dati. E' possibile sia un falso positivo, 
            ma se il problema non ci fosse è possibile però che ci 
            siano problemi sulla sincronizzazione da totem a hub (lenta?) 
            {1}
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(max_data, footer_mail, oggi)
        
        
            
    
        invia_email(subject, body, 
            to_email='andrea.gava@wingsoft.it', 
            cc_email='assterritorio@amiu.genova.it, pianar@amiu.genova.it')
    else:
        logger.info('Tutto Ok, ultima consuntivazione raccolta del {}'.format(max_data))

    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    





if __name__ == "__main__":
    main()      