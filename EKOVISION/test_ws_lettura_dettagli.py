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
      


    

    
    test= {"name": "école '& c/o aaa", 
        "location": "New York"}
    
    json_data = json.dumps(test , ensure_ascii=False).encode('utf-8')
    
    #print(json_data)
    
    #exit()
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    

    
    id_scheda = 694482 #696205 #478360 #600883  #484922 #502441   #423341 OK #   423319 da problemi
    
    
    

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    
    logger.info('Provo a leggere i dettagli della scheda')
    
    
    params2={'obj':'schede_lavoro',
            'act' : 'r',
            'id': '{}'.format(id_scheda),
            }
    
    response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
    #letture2 = response2.json()
    letture2 = response2.json()

    #logger.info(letture2)
    logger.info(letture2["schede_lavoro"][0]["trips"])
    trips=letture2["schede_lavoro"][0]["trips"]
    # ciclo sulle aste 
    componenti_eko=[]
    tr=0
    while tr < len(trips):
        waypoints=letture2['schede_lavoro'][0]['trips'][tr]['waypoints']
        wid=0
        while wid < len(waypoints):
            works=letture2['schede_lavoro'][0]['trips'][tr]['waypoints'][wid]['works'] 
            # ciclo sugli elementi
            cc=0
            while cc < len(works):
                list=[]
                list.append(int(letture2['schede_lavoro'][0]['trips'][tr]['waypoints'][wid]['works'][cc]['id_object']))
                list.append(int(letture2['schede_lavoro'][0]['trips'][tr]['waypoints'][wid]['pos']))
                list.append(int(letture2['schede_lavoro'][0]['trips'][tr]['waypoints'][wid]['works'][cc]['data_inizio']))
                list.append(int(letture2['schede_lavoro'][0]['trips'][tr]['waypoints'][wid]['works'][cc]['data_fine']))
                componenti_eko.append(list)
                cc+=1
            wid+=1
        tr+=1
    
    logger.info(len(componenti_eko))
    
    
    
    
    




if __name__ == "__main__":
    main()      