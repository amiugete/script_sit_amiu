#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
invio mail al primo di ogni mese


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import xlsxwriter

import psycopg2

import cx_Oracle
import locale
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')



currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


import requests
from requests.exceptions import HTTPError

import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION
import pysftp

import json



filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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


import fnmatch



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
        
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    num_giorno=datetime.today().weekday()
    logger.debug('Giorno della settimana{}'.format(num_giorno))
    
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=(datetime.today()- timedelta(days = 30)).strftime('%Y%m%d')
    logger.debug(giorno_file)
    mese_mail=(datetime.today()- timedelta(days = 30)).strftime('%B %Y')
    logger.debug(mese_mail)
    #exit()
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    
    
    curr = conn.cursor()
    
    
    query_mail_ut='''select string_agg(trim(mail), ', ') as mail_ut from topo.ut
where id_zona  in (1,2, 3, 5, 6)'''
    
    query_mail_zone='''select string_agg(trim(mail), ', ') as mail_zone from topo.zone_amiu
where id_zona  in (1,2, 3, 5, 6)'''         
      
    query_all_mail='''
    select replace(string_agg(mail, ', '),';',',') as mail from (
	select string_agg(trim(mail), ', ') as mail from topo.ut
	where id_zona  in (1,2, 3, 5, 6)
	union
	select string_agg(trim(mail), ', ') as mail from topo.zone_amiu za 
	where id_zona  in (1,2, 3, 5, 6)
) as mail0      '''    

    try:
        curr.execute(query_mail_ut)
        lista_mail_ut=curr.fetchall()
    except Exception as e:
        logger.error(query_mail_ut)
        check_error=1
        logger.error(e)


    for mu in lista_mail_ut:
        mail_ut=mu[0]
    
    try:
        curr.execute(query_mail_zone)
        lista_mail_zone=curr.fetchall()
    except Exception as e:
        logger.error(query_mail_zone)
        check_error=1
        logger.error(e)


    for mz in lista_mail_zone:
        mail_zone=mz[0]
    
    
    #logger.debug(mail_ut)
    #exit()
    #logger.debug(mail_zone)
    
    
    
   
    testo_mail='''Buongiorno, 
    <br><br>nell'ambito dell'attività di consuntivazione dei servizi tramite Ekovision, per vincoli normativi, si rende necessario <i>chiudere</i> tutte le schede per evitare, almeno mese per mese, che possano essere modificate. Lo scopo è quello di inviare dati ufficiali a Città Metropolitana ed ARERA. 
    <br><br>
    <b>E' quindi necessario che tutte le schede di {} siano <i>salvate come eseguite</i> (ossia consuntivate) da voi.
Pertanto si richiede gentilmente di controllare le schede ancora aperte e salvarle come eseguite, indicando eventuali causali nel caso di servizio non effettuato.
</b> In caso di problematiche vi ricordiamo che i colleghi di assterritorio@amiu.genova.it sono a vostra disposizione per supporto.
<br><br> 
A scanso di equivoci si ricorda che:
<ul>
<li> le schede ancora aperte comportano che il servizio risulti non consuntivato sul portale di città metropolitana, con le relative conseguenze </li>
<li> nel caso un servizio non fosse effettuato, è sempre necessario utilizzare l'apposita spunta “non effettuato” con la causale. In questi casi è possibile utilizzare il totem o il backoffice anche se non è assolutamente necessario. E' sufficiente consuntivare il servizio usando Ekovision.</li>
</ul>

Una volta chiuse, a differenza di quelle salvate come effettuate, le schede non saranno più modificabili, salvo richiesta a capi zona o alla Direzione del territorio, che avrà la facoltà di riaprirle ma solo per valide motivazioni.
'''.format(mese_mail)
    
    
    
    
    subject = "Chiusura schede {}".format(mese_mail)
            
    ##sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = 'noreply@amiu.genova.it'
    message["To"] = mail_ut
    message["CC"] = '{}, {}'.format(mail_zone, 'Mario.Bianchi@amiu.genova.it, Alessia.Magni@amiu.genova.it, roberto.longo@amiu.genova.it, alessandro.rapetti@amiu.genova.it')
    message["Bcc"] = '{}'.format(receiver_email)
    #message["CCn"] = debug_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "Chiusura schede di lavoro"


    body='''{0}
    <br><br><hr>
    AMIU<br>
    <img src="cid:image1" alt="Logo" width=197>
    <br>Questa mail è stata creata in automatico. 
    In caso di dubbi contattare i vostri referenti'''.format(testo_mail, )
                        
    # Add body to email
    message.attach(MIMEText(body, "html"))


    #aggiungo logo 
    logoname='{}/img/logo_amiu.jpg'.format(path1)
    immagine(message,logoname)
    
    

    
    
    text = message.as_string()

    logger.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logger.info(invio)
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      