#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''



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
      


    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    

    

    

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    
    
    # 0213236202 scheda aperta con 2 persone
    # 0213236102 scheda eseguita con 2 persone 

    # qua provo il WS
    params={'obj':'schede_lavoro',
        'act' : 'r',
        'sch_lav_data': '20250901',
        'cod_modello_srv': '0201259302',
        'flg_includi_eseguite': 1,
        'flg_includi_chiuse': 1
        }
    
    # provo il WS solo con la data 
    """params={'obj':'schede_lavoro',
        'act' : 'r',
        'sch_lav_data': '20240101',
        'flg_includi_eseguite': 1,
        'flg_includi_chiuse': 1
        }"""
    response = requests.post(eko_url, params=params, data=auth_data_eko, headers=headers)
    #response.json()
    logger.debug(response.status_code)
    try:      
        response.raise_for_status()
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
        #logger.info(letture)
        logger.info(len(letture['schede_lavoro']))
        logger.debug(letture['schede_lavoro'])
        exit()
        
        if len(letture['schede_lavoro']) == 0:
            #va creata la scheda di lavoro
            logger.info('Andrebbe creata la scheda di lavoro')
            giason={
                        "crea_schede_lavoro": [
                        {
                            "data_srv": "20231024",
                            "cod_modello_srv": "0500110701",
                            "cod_turno_ext": "997"
                        }
                        ]
                        } 
            params2={'obj':'crea_schede_lavoro',
                    'act' : 'w',
                    'ruid': '0000004',
                    'json': json.dumps(giason)
                    }
            exit()
            response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
            letture2 = response2.json()
            logger.info(letture2)
            try: 
                id_scheda=letture['crea_schede_lavoro'][0]['id']
            except Exception as e:
                logger.error(e)
        elif len(letture['schede_lavoro']) > 0 : 
            id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
            turno=letture['schede_lavoro'][0]['cod_turno_ext']
            in_lavorazione= letture['schede_lavoro'][0]['flg_in_lavorazione']
            eseguita=letture['schede_lavoro'][0]['flg_eseguito']
            chiusa= letture['schede_lavoro'][0]['flg_chiuso'] 
            logger.info(id_scheda)
            logger.info(turno)




if __name__ == "__main__":
    main()      