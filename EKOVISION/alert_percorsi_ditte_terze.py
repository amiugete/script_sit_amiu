#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Marzocchi

'''
Lo script si occupa di veriificare se nella data odierna sono state fatte delle consuntivazioni più vecchie di 3 giorni. 

Gira tutte le mattine e per il giorno precedente mi dice i percorsi che sono stati consuntivati su Ekovision per date èiù vecchie di 3 giorni 

Richiesta di Ufficio di Caruso
'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import requests
from requests.exceptions import HTTPError

import json


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


#import requests

import logging

import inspect

filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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


# per vedere se il percorso era previsto quel giorno
from tappa_prevista import tappa_prevista

    
     

def main():
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    

    
    #id_scheda = 398690
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    
    
    query='''
    SELECT DISTINCT as2.ID_SERVIZIO_STAMPA, ss.DESCRIZIONE AS famiglia, as2.DESC_SERVIZIO, au.DESC_UO, 
see.ID_SCHEDA, see.CODICE_SERV_PRED, aspu.DESCRIZIONE, 
to_char(to_date(see.DATA_PIANIF_INIZIALE, 'YYYYMMDD'),'DD/MM/YYYY') AS DATA_PERCORSO,
to_char(TO_TIMESTAMP(SUBSTR(see.NOMEFILE, 20, 11), 'YYYYMMDD_HH24'), 'DD/MM/YYYY') AS GIORNO_EDIT_SCHEDA
FROM SCHEDE_ESEGUITE_EKOVISION see 
JOIN ANAGR_SER_PER_UO aspu ON aspu.ID_PERCORSO = see.CODICE_SERV_PRED 
    AND to_date(see.DATA_PIANIF_INIZIALE, 'YYYYMMDD') BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE 
JOIN anagr_uo au ON au.ID_UO = aspu.ID_UO 
JOIN ANAGR_SERVIZI as2 ON as2.ID_SERVIZIO = aspu.ID_SERVIZIO 
JOIN SERVIZIO_STAMPA ss ON ss.ID_SERVZIO_STAMPA = as2.ID_SERVIZIO_STAMPA 
WHERE au.ID_UO IN (
119,	/*RASTRELLO*/
120,	/*REVETRO*/
121,	/*ATI SOC. COOP */
164,	/*RTI CONSORZIO OMNIA - ATI SOC. COOP*/
167,	/*HUMANA*/
163,	/*OMNIA*/
166,	/*GENOVA INSIEME*/
169		/*COOP MARIS*/
) AND see.DATA_PIANIF_INIZIALE >= '20241021' /*data partenza del sistema*/
AND 
/*INTERVALLO > 3 giorni*/
(TO_DATE(SUBSTR(see.NOMEFILE, 20, 8), 'YYYYMMDD') - TO_DATE(see.DATA_PIANIF_INIZIALE, 'YYYYMMDD')) > 3
AND 
SUBSTR(see.NOMEFILE, 20, 8) = to_char((trunc(sysdate)-1),'YYYYMMDD')
    '''
    
    
    
    #testo_mail=''
    
    try:
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        cur.execute(query)
        lista_percorsi_dt=cur.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

    messaggio='<ul>'       
    for lp in lista_percorsi_dt:
        messaggio='''{0}
            <li>Servizio:{1}
            <br>UO: {2}
            <br>Percorso: {3} - {4} (id_scheda:{5}) del <b> {6}</b></li>
            '''.format(messaggio, lp[2], lp[3], lp[5], lp[6],lp[4],lp[7])

    messaggio='{}</ul>'.format(messaggio)
    
    if messaggio != '<ul></ul>':
        logger.debug(messaggio)
        
        subject = "Alert schede Ekovision vecchie modificate"
            
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'

        to_mail='mauro.caruso@amiu.genova.it, Servizi.esternalizzati@amiu.genova.it'
    
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'noreply@amiu.genova.it'
        message["To"] = to_mail
        message["Bcc"] = receiver_email
        #message["CCn"] = debug_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Chiusura schede di lavoro"


        body='''Di seguito si riportano dei percorsi modificati ieri con data più vecchia di 3 giorni. 
        <br>{0}
        <br><br><hr>
        AMIU<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico. 
        In caso di dubbi contattare i vostri referenti'''.format(messaggio )
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        

        
        
        text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
    else:
        logger.info('Non ho trovato nessun vecchio percorso consuntivato ieri')    
        
        
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    
    cur.close()
    con.close()



if __name__ == "__main__":
    main()      