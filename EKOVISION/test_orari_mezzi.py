#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della pulizia dell'elenco percorsi generato dai JOB spoon realizzati per Ekovision

In particolare fa: 

- controllo ed eliminazione percorsi duplicati (non dovrebbe più servire a valle di una modifica al job)
- versionamento dei percorsi come da istruzioni 


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



    
     

def main():
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
      
    # PARAMETRI WS
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    
    id_scheda=117060
    
    logger.debug('Id scheda = {}'.format(id_scheda))


        
        
        
        
    logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda))
    
    
    params2={'obj':'schede_lavoro',
            'act' : 'r',
            'id': '{}'.format(id_scheda),
            }
    
    response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
    #letture2 = response2.json()
    letture2 = response2.json()
    logger.debug(letture2)
    
    
    
    if len(letture2['schede_lavoro'][0]['risorse_tecniche'])>0:
        if letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id_giustificativo'] == 0:
            tt=0
            while  tt<len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                targa=letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['targa']
                logger.debug(targa)
                cur2 = con.cursor()
                
                query_sportello='''SELECT trim(SPORTELLO) FROM V_AUTO_EKOVISION@INFO WHERE trim(REPLACE(TARGA, ' ', '')) = trim(:t1) '''
                try:
                    cur2.execute(query_sportello, (letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['targa'], ))
                    sspp=cur2.fetchall()
                except Exception as e:
                    logger.error(query_sportello)
                    logger.error(e)
                if len(sspp)>0:
                    for sp in sspp:
                        sportello= sp[0]
                    logger.debug(sportello)
                    cur2.close()
                    cur2 = con.cursor()
                    durata=0
                    data_ora_start='{} {}'.format(
                        letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['data_inizio'],
                        letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['ora_inizio']
                        )
                    data_ora_fine='{} {}'.format(
                        letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['data_fine'],
                        letture2['schede_lavoro'][0]['risorse_tecniche'][tt]['ora_fine']
                        )
                    
                    fmt='%Y%m%d %H%M%S'
                    data_ora_start_ok = datetime.strptime(data_ora_start, fmt)
                    data_ora_fine_ok = datetime.strptime(data_ora_fine, fmt)
                    logger.debug(data_ora_start_ok)
                    logger.debug(data_ora_fine_ok)
                    # calcolo differenza in minuti ()
                    durata+=(data_ora_fine_ok - data_ora_start_ok).total_seconds() / 60.0
                    logger.debug(durata)
                    exit()
                

    
        #exit()

            
            
            
        
        
    

    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()
    
    




if __name__ == "__main__":
    main()      