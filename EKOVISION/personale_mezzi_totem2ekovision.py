#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Script che usando i WS di Ekovision scrive il personale e i mezzi associati a un percorso usando il totem


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


from descrizione_percorso import *  
    
     

def main():
      
    
    
    try:
        if sys.argv[1]== 'prod':
            test=0
        elif sys.argv[1]== 'test':
            test=1
        else: 
            print('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        test=1
        
        
    
    path=os.path.dirname(sys.argv[0]) 
    nome=os.path.basename(__file__).replace('.py','')
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    if test==0:
        logfile='{0}/log/{1}.log'.format(path,nome)
        errorfile='{0}/log/error_{1}.log'.format(path,nome)
    else: 
        logfile='{0}/log/{1}_test.log'.format(path,nome)
        errorfile='{0}/log/error_{1}_test.log'.format(path,nome)
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
    f_handler.setLevel(logging.INFO)


    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)


    cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    c_handler.setFormatter(cc_format)
    f_handler.setFormatter(cc_format)
    
    if test==1:
        logger.info('Ambiente di TEST')
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
        
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
    
    
    select_entry_totem='''select id_percorso as cod_percorso, 
to_char(datalav, 'YYYYMMDD') as data_percorso,
r.codice, 
vpes.codice_badge,
pe.id_ekovision as id_persona_eko,
r.sportello,
mi.sportello,
mi.targa,
me.id_ekovision as id_mezzo_eko
from servizi.registrazioni r
left join totem.v_personale_ekovision_step1 vpes  on vpes.codice_badge::varchar = r.codice
left join totem.personale_ekovision pe 
	on trim(upper(pe.cognome)) = trim(upper(vpes.cognome)) and  
	trim(upper(pe.nome)) = trim(upper(vpes.nome))
left join totem.mezzi_infopm mi on mi.sportello::int = r.sportello::int 
left join totem.mezzi_ekovision me on trim(upper(me.targa)) = trim(upper(mi.targa)) 
where send_ekovision is not true'''
    
    
    try:
        curr_c.execute(select_entry_totem)
        lista_entry_totem=curr_c.fetchall()
    except Exception as e:
        logger.error(select_entry_totem)
        logger.error(e)
    
    
    for et in lista_entry_totem:
        
        cp=et[0]
        data_percorso=et[1]
        id_persona=et[4]
        id_mezzo=et[8]
        logger.info('Aggiorno il percorso {} del giorno {}'.format(cp, data_percorso))
    
    
    
        # da completare!!
        
        
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    #currc1.close()
    conn_c.close()
    





if __name__ == "__main__":
    main()