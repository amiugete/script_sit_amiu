#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica se sono state inserite su DB (treg_edok.istanze_tari):
    - pratiche con valore di ufficio anomalo nella giornata di ieri
    - pratiche con stato acquisito create più di tre giorni fa

Invia le eventuali anomalie ai colleghi ddella TARI che dovrebbero così intercettare le pratcihe che EDOK per qualche ragione 
fa vedere solo all'utente admin
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import csv

import psycopg2

import cx_Oracle

from datetime import date, datetime, timedelta

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

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


def main():
    logger.info('Il PID corrente è {0}'.format(os.getpid()))


    oggi = datetime.now()
    mese = oggi.month
    anno = oggi.year
    ieri = date.today() - timedelta(days=1)

    logger.info (ieri.strftime("%d/%m/%Y"))

    # Se dicembre, passa a gennaio dell'anno successivo
    if mese == 12:
        next_mese = 1
        anno += 1
    else:
        next_mese = mese + 1


    logging.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
            port=port,
            user=user,
            password=pwd,
            host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    # verifico se ieri abbiamo inserito a DB pratiche con valori di Ufficio anomali
    query_ist = '''
        select it.cusprotocollo, it.filtersd1, it.filtersd2, date  
from treg_edok.istanze_tari it 
where it.filtersd2 not in ('Genova', 'Cogoleto', 'Arenzano') and it.date::date = CURRENT_DATE - 1 
order by date desc
    '''
    try:
        curr.execute(query_ist)
        lista_anomalie=curr.fetchall()
        logger.debug(lista_anomalie)
    except Exception as e:
        logger.error(lista_anomalie)
        logger.error(e)

    # verifico se ci sono pratiche con stato acquisito più vecchie di 3 giorni
    query_ist_old = '''
        select cusprotocollo, filtersd1, filtersd2, "date", 
        cusdataassegnazione, statusid, statusdescription, 
        statusvalue, updatetime, id_archivio 
        from treg_edok.istanze_tari it 
        where it.statusdescription ilike 'Acquisito' and it.date::date < CURRENT_DATE -3 and filtersd1 <> 'Sportello Telematico'
        order by date
    '''
    try:
        curr.execute(query_ist_old)
        lista_ist_old=curr.fetchall()
        logger.debug(lista_ist_old)
    except Exception as e:
        logger.error(lista_ist_old)
        logger.error(e)

    messaggio = '''ALERT AUTOMATICO EDOK<br><br>'''
    
    invio_mail = 0

    # se ci sono pratiche con ufficio anomalo aggiunte ieri, compongo il testo della mail
    if len(lista_anomalie)>0:
        invio_mail += 1        
        messaggio = '''{0}Ieri {1} sono state inserite pratiche con valore di <b>Ufficio</b> anomalo. 
        <br>Di seguito l'elenco: <ul>'''.format(messaggio, ieri.strftime("%d/%m/%Y"))
        
        for a in lista_anomalie:
            logger.debug(a)
            messaggio='{0}<li>Protocollo: {1} - Ufficio: {2}</li>'.format(messaggio,a[0], a[2])
        messaggio='{}</ul>'.format(messaggio)

    # se ci sono pratiche con stato acquisito più vecchie di 3 giorni, compongo il testo della mail 
    if len(lista_ist_old)>0:
        invio_mail += 1      
        messaggio='''
        {}Ci sono pratiche ancora <b>non assegnate</b> da più di 3 giorni.
        <br>Di seguito l'elenco: <ul>'''.format(messaggio)
        
        for io in lista_ist_old:
            logger.debug(io)
            messaggio='{0}<li>Protocollo: {1} - Ufficio: {2} - Data: {3}</li>'.format(messaggio, io[0], io[2], io[3])

        messaggio='{}</ul>'.format(messaggio)

    #se si verifica una delle due anomalie verificate, invio la mail
    if invio_mail > 0:

        receiver_email='Vezzosi@amiu.genova.it, Matteo.Relli@amiu.genova.it, Manuela.Marchese@amiu.genova.it'
        debug_email='roberta.fagandini@amiu.genova.it'


        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["CC"] = "assterritorio@amiu.genova.it"
        message["Subject"] = "Anomalie pratiche EDOK"
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Pratiche con valore Ufficio anomalo"

        body='''{0}
        <br><br><hr>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico. 
        Per qualsiasi chiarimento contattare assterritorio@amiu.genova.it'''.format(messaggio)
                        
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        

            
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)


    else:
        logger.info("Non sono al momento presenti anomalie sui dati EDOK caricati su DB")

if __name__ == "__main__":
    main()