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
logfile='{}/log/preconsunsuntivazione.log'.format(path)
errorfile='{}/log/error_preconsuntivazione.log'.format(path)
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



def tappa_prevista(day,frequenza_binaria):
    '''
    Data una data e una frequenza dice se la tappa è prevista sulla base di quella frequenza o no
    '''
    # settimanale
    if frequenza_binaria[0]=='S':
        if int(frequenza_binaria[day.weekday()+1])==1:
            return 1
        elif int(frequenza_binaria[day.weekday()+1])==0:
            return -1
        else:
            return 404
    # mensile (da finire)
    elif frequenza_binaria[0]=='M':
        # calcolo la settimana (week_number) e il giorno della settimana (day of week --> dow)
        week_number = (day.day) // 7 + 1
        dow=day.weekday()+1
        string='{0}{1}'.format(week_number,dow)
        # verifico se il giorno sia previsto o meno
        if string in frequenza_binaria:
            return 1
        else: 
            return -1
    
     

def main():
      

    cp='0201007701'
    data_check='20231128'

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

    
    # prima di tutto faccio un controllo sulle schede di lavoro per verificare se sono state generate anche per i nuovi percorsi

    # PARAMETRI GENERALI WS
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
        
    
    
   
        

    params={'obj':'schede_lavoro',
        'act' : 'r',
        'sch_lav_data': data_check,
        'cod_modello_srv': cp, 
        'flg_includi_eseguite': 1,
        'flg_includi_chiuse': 1
        }

    response = requests.post(eko_url, params=params, data=data_json, headers=headers)
    #response.json()
    #logger.debug(response.status_code)
    try:      
        response.raise_for_status()
        check=0
        # access JSOn content
        #jsonResponse = response.json()
        #print("Entire JSON response")
        #print(jsonResponse)
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        check=1
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        logger.error(response.json())
        check=1
    if check<1:
        letture = response.json()
        logger.info(letture)
        if len(letture['schede_lavoro']) > 0 : 
            id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
            ora_inizio_lav=letture['schede_lavoro'][0]['ora_inizio_lav']
            ora_inizio_lav_2=letture['schede_lavoro'][0]['ora_inizio_lav_2']
            ora_fine_lav=letture['schede_lavoro'][0]['ora_fine_lav']
            ora_fine_lav_2=letture['schede_lavoro'][0]['ora_fine_lav_2']
            logger.info('Id_scheda:{}'.format(id_scheda))
    
    body_mail='''E' arrivata una consuntivazione da totem per il percorso {} in data {}.
    <br>Non esistendo la scheda per il giorno in questione è stata creata in automatico.
    La nuova scheda ha ID {}'''.format(cp, data_check, id_scheda)           
    creazione_scheda_mail(body_mail,'roberto.marzocchi@amiu.genova.it; vobbo@libero.it', os.path.basename(__file__), logger)

    curr.close()
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      