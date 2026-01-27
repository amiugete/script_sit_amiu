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


def invia_mail(subject, messaggio_html):
    receiver_email = 'Vezzosi@amiu.genova.it, Matteo.Relli@amiu.genova.it, Manuela.Marchese@amiu.genova.it'
    #debug_email = "roberta.fagandini@amiu.genova.it"
    cc_email = "assterritorio@amiu.genova.it"

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["CC"] = cc_email
    message["Subject"] = subject
    message.preamble = subject

    body = f'''
    {messaggio_html}
    <br><br><hr>
    <img src="cid:image1" alt="Logo" width="197">
    <br>Questa mail è stata creata in automatico.
    Per qualsiasi chiarimento contattare assterritorio@amiu.genova.it
    '''

    message.attach(MIMEText(body, "html"))

    logoname = f'{path1}/img/logo_amiu.jpg'
    immagine(message, logoname)

    logger.info(f"Invio mail: {subject}")
    invio=invio_messaggio(message)
    logging.info(invio)
    return invio



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

    query_tipo_pratica ="""
        select distinct it.cusmetadatitipopratica
        from treg_edok.istanze_tari it
        left join treg_edok.mapping_prestazione mp on it.cusmetadatitipopratica = mp.cod_edok
        where mp.cod_edok is null and it.cusmetadatitipopratica is not null
    """
    try:
        curr.execute(query_tipo_pratica)
        lista_tipo_pratica=curr.fetchall()
        logger.debug(lista_tipo_pratica)
    except Exception as e:
        logger.error(lista_tipo_pratica)
        logger.error(e)

    # se ci sono pratiche con ufficio anomalo aggiunte ieri, compongo il testo della mail
    if len(lista_anomalie)>0:
        
        messaggio = f'''
            <b>ALERT AUTOMATICO EDOK</b><br><br>
            Ieri {ieri.strftime("%d/%m/%Y")} sono state inserite pratiche con valore di
            <b>Ufficio</b> anomalo.<br>
            Di seguito l'elenco:
            <ul>
        '''

        for a in lista_anomalie:
            messaggio += f'<li>Protocollo: {a[0]} - Ufficio: {a[2]}</li>'

        messaggio += '</ul>'
        
        oggetto= "ANOMALIA EDOK Pratiche con Ufficio anomalo"

        invia_mail(oggetto, messaggio)

    # se ci sono pratiche con stato acquisito più vecchie di 3 giorni, compongo il testo della mail 
    if len(lista_ist_old)>0:
        messaggio0 = '''
        <b>ALERT AUTOMATICO EDOK</b><br><br>
        Ci sono pratiche ancora <b>non assegnate</b> da più di 3 giorni.
        <br>Di seguito l'elenco:
        <ul>
        '''

        for io in lista_ist_old:
            messaggio0 += f'<li>Protocollo: {io[0]} - Ufficio: {io[2]} - Data: {io[3].strftime("%d/%m/%Y")}</li>'

        messaggio0 += '</ul>'

        oggetto0= "ALLERT EDOK Pratiche non assegnate da oltre 3 giorni"

        invia_mail(oggetto0, messaggio0)

    if len(lista_tipo_pratica)>0:
        messaggio1 = '''
        <b>ALERT AUTOMATICO EDOK</b><br><br>
        Sono state trovate pratiche con tipo pratica non mappato.
        <br>Di seguito l'elenco dei tipi pratica:
        <ul>
        '''

        for tp in lista_tipo_pratica:
            messaggio1 += f'<li>{tp[0]}</li>'

        messaggio1 += '</ul>'

        messaggio1 += '''
            Si prega di comunicare il prima possibile, inviando una mail ad assterritorio@amiu.genova.it, se si tratta di tipi pratica da inviare ad ARERA, 
            in caso affermativo specificare la tipologia AREARA corrispondente tra quelle di seguito elencate:
            <ul>
                <li>Richiesta di attivazione servizio</li>
                <li>Richiesta di variazione servizio</li>
                <li>Richiesta di cessazione servizio</li>
                <li>Richiesta di informazioni scritte</li>
                <li>Richiesta di rettifica degli importi addebitati</li>
            </ul>
        '''

        oggetto1= "ALLERT EDOK Pratiche con tipo pratica non mappato"

        invia_mail(oggetto1, messaggio1)
    

if __name__ == "__main__":
    main()