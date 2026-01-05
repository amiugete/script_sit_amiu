#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa dell'update delle descrzioni delle schede di lavoro già esistenti in EKOVISION
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


import uuid


    
     

def main():
      

    url_ws_eko=eko_url_test

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }

    
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db_test
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()


    # seleziono i percorsi da aggiornare 
    select_cod_percorso=""" SELECT cod_percorso,  new_desc
    FROM etl.update_descrizioni ud 
    WHERE ud.update_fatto is not true """


    try:
        curr.execute(select_cod_percorso)
        lista_percorsi=curr.fetchall()
    except Exception as e:
        logger.error(select_cod_percorso)
        logger.error(e)
        
    
    for cp in lista_percorsi:
        cod_percorso=cp[0]
        new_desc=cp[1]
        logger.info('Aggiorno la descrizione del percorso {}'.format(cod_percorso))
        
        #exit()
        
        logger.debug(oggi)
        
        
        check_error=0

        
        #exit()
        gg=0
        
        while gg <= 14-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            logger.debug(day)
            # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
            
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': day,
                'cod_modello_srv': cod_percorso, 
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1
                }
            response = requests.post(url_ws_eko, params=params, data=auth_data_eko, headers=headers)
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
    
    
                if len(letture['schede_lavoro']) > 0 : 
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    logger.debug('Id_scheda non eseguita:{}'.format(id_scheda))
                    
    
                    logger.info(f'Provo a leggere i dettagli della scheda {id_scheda}')
                    
                    
                    params2={'obj':'schede_lavoro',
                            'act' : 'r',
                            'id': '{}'.format(id_scheda),
                            'flg_esponi_consunt':1
                            }
                    
                    # salvo i dettagli nella variabile letture2
                    response2 = requests.post(url_ws_eko, params=params2, data=auth_data_eko, headers=headers)
                    letture2 = response2.json()
                    # rimuovo l key "status"
                    del letture2["status"]
                    del letture2['schede_lavoro'][0]['trips']
                    del letture2['schede_lavoro'][0]['risorse_tecniche']
                    del letture2['schede_lavoro'][0]['filtri_rfid']    
                    #logger.info(letture2)
                    #logger.debug(letture2)
                    check_inserimento=0
                    letture2['schede_lavoro'][0]['descr_scheda_lav']=f'[{cod_percorso}] {new_desc}'
                    letture2['schede_lavoro'][0]['servizi'][0]['descrizione']=f'[{cod_percorso}] {new_desc}'
                    
                    
                    guid = uuid.uuid4()
                    params2={'obj':'schede_lavoro',
                            'act' : 'w',
                            'ruid': '{}'.format(str(guid)),
                            'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                            }
                    #exit()
                    response2 = requests.post(url_ws_eko, params=params2, data=auth_data_eko, headers=headers)
                    try:
                        result2 = response2.json()
                        if result2['status']=='error':
                            check_error+=1
                            logger.error('Id_scheda = {}'.format(id_scheda))
                            logger.error(result2)
                    except Exception as e:
                        logger.error(e)
                        warning_message_mail('Problema scheda {}'.format(id_scheda[0]), 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    
                    
                    
            gg+=1        
                    
    
        if check_error==0:
            # se non ci sono stati errori marco l'update come fatto
            update_fatto=""" UPDATE etl.update_descrizioni ud 
            SET update_fatto= true
            WHERE ud.cod_percorso= %s and ud.update_fatto is not true"""
            try:
                curr.execute(update_fatto, (cod_percorso,))
                conn.commit()
                logger.info('Update della tabella etl.update_descrizioni andato a buon fine per il percorso {}'.format(cod_percorso))
            except Exception as e:
                logger.error(update_fatto)
                logger.error(e)

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()


if __name__ == "__main__":
    main()      