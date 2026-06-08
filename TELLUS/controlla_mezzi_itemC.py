#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa di verificare che targa e sportello letti in automatico da Tellus siano corretti,
confrontandoli con quelli presenti nell'anagrafe dei mezzi (InfoPM).

In caso di discrepanze, invia una mail di segnalazione direttamente a Tellus.
'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

from collections import defaultdict

import locale

import xlsxwriter

import psycopg2

import cx_Oracle

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


import uuid


    
     

def main():
      

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

    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    
    
    mese_anno_oggi=oggi.strftime('%Y%m')
    
    
    


    query_mezzi_sit = '''select targa, sportello, 
data_installazione, 
data_primo_messaggio, 
data_ultimo_messaggio, 
collaudato 
from tellus.mezzi_itemd'''
    
    
    

    
    
    
    
    # TODO: spostare controllo su SIT
    # select delle schede presenti in treg_eko.consunt_ekovision
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    curr = conn.cursor()



    try:
        curr.execute(query_mezzi_sit)
        mezzi_sit = curr.fetchall()
    except Exception as e:
        logger.error('Errore esecuzione query mezzi SIT: {}'.format(e))

    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    query_mezzi_infopm = '''
    SELECT *
FROM v_auto_ekovision@info a 
WHERE sportello LIKE '%' || :d1 || '%'
AND REPLACE(trim(targa), ' ', '') = :d2
    '''
    
    text_warning = ''
    
    for mezzo in mezzi_sit:
        try:
            cur.execute(query_mezzi_infopm, {
                "d1": mezzo[1],
                "d2": mezzo[0]
            })
            mezzi_infopm = cur.fetchall()
        except Exception as e:
            logger.error('Errore esecuzione query mezzi infopm: {}'.format(e))
        
        
        if len(mezzi_infopm) == 0 and mezzo[0].strip() != 'TEST' :
            #logger.debug('Sono qua dentro')
            text_warning += f'<li>mezzo con targa <b>{mezzo[0]}</b> e sportello <b>{mezzo[1]} </b>: non trovato in infopm</li>'

    
    if text_warning != '':
        logger.warning(text_warning)
        warning_message_mail('''Buongiorno,<br> oggi sono state trovate le seguenti anomalie sui dati letti tramite WebService<br><ul>'''+ text_warning +'''</ul><br>
                             Si prega di verificare e correggere i dati.<br><br>Grazie<br><br>''', 
                             'assistenza@tellus.it, pianar@amiu.genova.it',
                             os.path.basename(__file__), 
                             logger, 
                             'Controllo mezzi item D - Discrepanze trovate')
    else :
        logger.info('Controllo mezzi item D - Nessuna discrepanza trovata')
        logger.debug(text_warning)
    
    
    #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()      