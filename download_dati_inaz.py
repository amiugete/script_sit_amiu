#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) scarico dati da SFTP da INAZ

    - mediscopio
    - sap




-------

Per ottenere il file con i requisiti bisognerebbe avere una cartella dedicata e lanciare il comando

pipreqs /path 

il comando produce il file requirements.txt
'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta



currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


import requests
from requests.exceptions import HTTPError

import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION
import pysftp

import csv



filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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


import fnmatch



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
        
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    giorno_file=datetime.today().strftime('%Y%m%d')
        
    
    
    cartella_inaz_sftp='HR/OUTPUT/'    
    logger.info('Leggo e scarico file SFTP da cartella {}'.format(cartella_inaz_sftp))
    

    path_output='{0}/inaz_output'.format(path)
    if not os.path.exists(path_output):
        os.makedirs(path_output)
    path_log='{0}/log'.format(path)
    if not os.path.exists(path_log):
        os.makedirs(path_log)    
        
    # lista file da importare
        # nome 
        # formato
        # check se file presente o meno    
    lista_file= ['sap', 'mediscopio']
    lista_formati=['txt', 'xlsx']
    file_presente=[0,0]
    
    
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_inaz_sftp, username=user_inaz_sftp,
    password=pwd_inaz_sftp, port= port_inaz_sftp,  cnopts=cnopts)

        
        with srv.cd(cartella_inaz_sftp): #chdir to public
            #print(srv.listdir('./'))
            for filename in srv.listdir('./'):
                #logger.debug(filename)
                k=0
                while k < len(lista_file):
                    if fnmatch.fnmatch(filename, "{}_{}.{}".format(lista_file[k], giorno_file, lista_formati[k])):
                        srv.get(filename, path + "/inaz_output/" + filename)
                        logger.info('Scaricato file {}'.format(filename))
                        file_presente[k]=1
                        
                    k+=1
                           
                        
        
                            
 
                                                
        
        # Closes the connection
        srv.close()
        logger.info('Connessione chiusa')
    except Exception as e:
        logger.error('Problema connessione spazio SFTP di INAZ')
        logger.error(e)
    
    
    
    
    
    k=0
    while k < len(file_presente):
        if file_presente[k]==0:
            messaggio = 'Su spazio SFTP di INAZ non è presente file {}_{}{}'.format(lista_file[k], giorno_file, lista_formati[k])
            logger.warning(messaggio)
            warning_message_mail(messaggio, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        k+=1
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("Ho conlcuso l'attività")





if __name__ == "__main__":
    main()      
    