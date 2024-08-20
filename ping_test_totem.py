
#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione spazzamento:



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import requests
from requests.exceptions import HTTPError

import json


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


import openpyxl




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

# libreria per scrivere file csv
import csv



    
     

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
  

    nome_db=db_consuntivazione
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_consuntivazione,
                        password=pwd_consuntivazione,
                        host=host_hub)
    
    currc= connc.cursor()
    
    select_query='''SELECT descrizione, ip_address FROM totem.ip_totem'''
    
    try:
        currc.execute(select_query)
        lista_totem=currc.fetchall()
    except Exception as e:
        logger.error(select_query)
        logger.error(e)


    lista_ut=[]
    hostname=[]
    
    for cc in lista_totem:
        lista_ut.append(cc[0])
        hostname.append(cc[1])
    """
    lista_ut=["U.T. ALBARO",
"U.T. ALTA VALBISAGNO",
"U.T. BASSA VALBISAGNO",
"U.T. CASTELLETTO",
"U.T. CENTRO COMMERCIALE",
"U.T. CENTRO STORICO",
"U.T. FOCE",
"U.T. LEVANTE (QUARTO)",
"U.T. MEDIO LEVANTE ",
"U.T. NERVI",
"U.T. OREGINA",
"U.T. PEGLI e PRA/VOLTRI",
"U.T. SAMPIERDARENA",
"U.T. SESTRI",
"U.T. VALPOLCEVERA",
"U.T. ARENZANO",
"U.T. CAMPOLIGURE",
"U.T. COGOLETO (SATER)",
"U.T. MONTEBRUNO",
"U.T. SAVIGNONE",
"U.T. ALTA VALPOLCEVERA",
"RIMESSA VOLPARA",
"RIMESSA SESTRI",
"RIMESSA VOLPARA 2",
"RIMESSA SESTRI 2",
"UT CENTRO STORICO 2", 
"Totem prova D'annunzio"]
    
    
    hostname = ["192.168.56.3",
"192.168.60.244",
"192.168.60.212",
"192.168.60.227",
"192.168.56.75",
"192.168.62.111",
"192.168.62.185",
"192.168.60.83",
"192.168.60.155",
"192.168.57.34",
"192.168.60.57",
"192.168.59.91",
"192.168.60.28",
"192.168.60.194",
"192.168.58.52",
"192.168.57.15",
"192.168.60.36",
"192.168.40.201",
"192.168.60.99",
"192.168.59.228",
"192.168.59.115",
"172.24.10.158",
"192.168.33.87",
"172.17.1.24",
"192.168.33.25",
"192.168.62.113", 
"172.24.2.43"]
    
    """
    
    
    logger.debug(len(lista_ut))
    logger.debug(len(hostname))
    
    if len(lista_ut)!=len(hostname):
        logger.error('Problema array input')
        exit()
    
    result_test=[]
    
    for hh in hostname:
        response = os.system("ping -c 4 -W 60 {} > {}/log/ping_output.txt".format(hh, path))
        result_test.append(response)

    
    logger.debug(result_test)
    

    logger.debug(sum(result_test))

    if sum(result_test)>0:
        testo_mail='Ci sono problemi con i seguenti totem:<ul>'
        i=0
        while i < len(result_test):
            if result_test[i]!=0:
                testo_mail = '{0}<li>{1} - IP: {2} - Error {3} </li>'.format(testo_mail, lista_ut[i], hostname[i], result_test[i])
            i+=1
        testo_mail='{0}</ul>'.format(testo_mail)
        context = ssl.create_default_context()



    # messaggio='Test invio messaggio'


        subject = "ALERT MAIL - Totem offline"
        
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it; asstelefoni@amiu.genova.it'
        debug_email='riccardo.piana@amiu.genova.it'

        mail_test='roberto.marzocchi@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Cc"] = debug_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Cambio frequenze"


        body='''Mail automatica per il monitoraggio (ogni 2 ore) dello stato dei totem
        <br><br> {0}
        <br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(testo_mail)
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path)
        immagine(message,logoname)
        
        

        
        
        text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        
        
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it; pianar@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
        
        
        
if __name__ == "__main__":
    main()      