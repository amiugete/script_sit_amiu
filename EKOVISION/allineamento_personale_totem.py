#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa di scaricare elenco mezzi da EKOVISION e di scriverlo sul DB del totem

Ci serve per sapere ID ekovision e targa del mezzo associato

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



def empty_to_none(obj):
    '''
    Questa funzione scende in qualsiasi struttura (dict, list, tuple) e sostituisce le stringhe vuote con None.
    
    '''

    if isinstance(obj, dict):
        return {k: empty_to_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [empty_to_none(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(empty_to_none(v) for v in obj)
    else:
        return None if obj == "" else obj
     

def main():
      


    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    try:
        logger.debug(len(sys.argv))
        if sys.argv[1]== 'prod':
            test=0
        else: 
            logger.error('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        logger.info('Non ci sono parametri, sono in test')
        test=1

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    # Mi connetto al nuovo DB consuntivazione  
    if test ==1:
        nome_db= db_totem_test
    elif test==0:
        nome_db=db_totem
    else:
        logger.error(f'La variabilie test vale {test}. Si tratta di un valore anomalo. Mi fermo qua')
        exit()
        
    logger.info('Connessione al db {} su {}'.format(nome_db, host_totem))
    conn_c = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)

    curr_c = conn_c.cursor()
    
    
    

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    logger.info('Provo a leggere i dati del personale')
    
    
    params2={'obj':'personale',
            'act' : 'r',
            'data': '{}'.format(oggi.strftime('%Y%m%d')),
            }
    
    response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
    letture2 = response2.json()
    letture2 = empty_to_none(letture2)
    logger.info(letture2)
    logger.info('Letti {} record del personale'.format(len(letture2['data'][0]['personale'])))
    
    # scrivo i dati sul DB del totem 
        
    k=0
    while k <len(letture2['data'][0]['personale']):
        #logger.debug(k)
        logger.debug(letture2['data'][0]['personale'][k]['id'])
        query_upsert_personale= '''
        INSERT INTO totem.personale_ekovision (
            id_ekovision, 
            nome, 
            cognome, 
            cf, 
            matricola, 
            dt_nascita, 
            id_categoria_lavoratore, 
            id_sede_trasp, 
            des_sede_trasp, 
            update_data
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
            now())
            ON CONFLICT (id_ekovision) /* or you may use [DO NOTHING;] */ 
            DO UPDATE  SET nome=EXCLUDED.nome, cognome=EXCLUDED.cognome, 
            cf=EXCLUDED.cf, matricola=EXCLUDED.matricola, dt_nascita=EXCLUDED.dt_nascita,
            id_categoria_lavoratore=EXCLUDED.id_categoria_lavoratore, 
            id_sede_trasp=EXCLUDED.id_sede_trasp, des_sede_trasp=EXCLUDED.des_sede_trasp;
        ''' 
        try:
            curr_c.execute(query_upsert_personale, 
                           (
                           letture2['data'][0]['personale'][k]['id'],   
                           letture2['data'][0]['personale'][k]['nome'],
                           letture2['data'][0]['personale'][k]['cognome'],
                           letture2['data'][0]['personale'][k]['cf'],
                           letture2['data'][0]['personale'][k]['matricola'],
                           letture2['data'][0]['personale'][k]['dt_nascita'],
                           letture2['data'][0]['personale'][k]['id_categ_lavoratore'],
                           letture2['data'][0]['personale'][k]['id_sede_trasp'],
                           letture2['data'][0]['personale'][k]['des_sede_trasp']
                           ))
        except Exception as e:
            logger.error(query_upsert_personale)
            logger.error(e)
        k+=1     
    
    conn_c.commit()
    logger.info('Aggiornati/scritti {} record del personale'.format(k)) 
    
    
    #exit()
    
    
     # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
 
    

    #currc1.close()
    conn_c.close()
    





if __name__ == "__main__":
    main()      