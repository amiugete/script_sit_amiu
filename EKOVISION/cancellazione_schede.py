#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script interroga le schede di lavoro e fornisce un elenco di quelle da cancellare

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

filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/cancellazione_schede.log'.format(path)
errorfile='{}/log/error_cancellazione_schede.log'.format(path)
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
f_handler.setLevel(logging.INFO)


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
      

    #cp='0206001401'

    ccpp=['0102007702']

  
    
    schede_cancellare=''
    for cp in ccpp:
        logger.info('Controllo il percorso {}'.format(cp))
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
            
        
        
    
            
              
        gg=1
        while gg <= 14-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            #logger.debug(day)
            # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
            
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': day,
                'cod_modello_srv': cp
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
                #logger.info(letture)
                if len(letture['schede_lavoro']) > 0 : 
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    logger.info('Id_scheda:{}'.format(id_scheda))
                    
                    if schede_cancellare=='':
                        schede_cancellare='{}'.format(id_scheda)
                    else:
                        schede_cancellare='{},{}'.format(schede_cancellare,id_scheda)
            gg+=1
        
        print(schede_cancellare)
    
    # provo a mandare la mail
    try:
        if schede_cancellare!='':
            # Create a secure SSL context
            context = ssl.create_default_context()



        # messaggio='Test invio messaggio'


            subject = "ELIMINAZIONE SCHEDE LAVORO - Percorsi frequenza variata"
            
            ##sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            debug_email='roberto.marzocchi@amiu.genova.it'

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = debug_email
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Cambio frequenze"


            body='''
            Elenco schede da cancellare: <br>
            {0}
            <br><br>
            AMIU Assistenza Territorio<br>
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(schede_cancellare)
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))


            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(path1)
            immagine(message,logoname)
            
            

            
            
            text = message.as_string()

            logger.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            logger.info(invio)
    except Exception as e:
        logger.error(e) # se non fossi riuscito a mandare la mail
    curr.close()
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      