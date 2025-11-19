#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Script per portare i dati dai servizi dal SIT al nuovo DB di consuntivazione 

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


from descrizione_percorso import *  
    
     

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

    # Mi connetto al SIT 
    nome_db=db 
    logger.info('Connessione al db {} su {}'.format(nome_db, host_hub))
    
    conn_s = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    
    curr_s = conn_s.cursor()
    curr_c = conn_c.cursor()
    
    

    # seleziono i dati da copiare
    query_select_su_sit='''SELECT cod_percorso as id_percorso, 
		versione,
		vspe.descrizione, id_turno, durata, 
        vspe.id_tipo, at2.descrizione as desc_tipo,
        freq_testata as id_frequenza, vspe.cod_sede as id_presa_servizio, 
        vst.id_rimessa_sit, 
        vst.desc_rimessa, 
        vst.id_uo_sit, 
        vst.desc_ut,
        data_inizio_validita, data_fine_validita
        FROM anagrafe_percorsi.v_servizi_per_ekovision vspe
        left join anagrafe_percorsi.v_sedi_totem vst on vst.cod_sede = vspe.cod_sede
        left join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = vspe.id_tipo 
        where (cod_percorso in (
        select distinct cod_percorso from anagrafe_percorsi.elenco_percorsi ep  
        where data_fine_validita >= now()::date or data_ultima_modifica >= now()::date - interval '1' day
        )  or data_fine_validita >= now()::date - interval '1' month)
        and data_inizio_validita <= now()::date
        order by cod_percorso,versione'''
    
    try:
        curr_s.execute(query_select_su_sit)
        elenco_dati_copiare=curr_s.fetchall()
    except Exception as e:
        logger.error(query_select_su_sit)
        logger.error(e)
    
    logger.info(f"Trovati {len(elenco_dati_copiare)} record da copiare.")
    
    curr_c = conn_c.cursor()
    upsert=''' INSERT INTO servizi.servizi_per_ekovision (
        id_percorso, versione, descrizione, 
        id_turno, durata,
        id_tipo, desc_tipo,
        id_frequenza, id_presa_servizio,
        id_rimessa_sit, desc_rimessa,
        id_uo_sit, desc_ut,
        data_inizio_validita, data_fine_validita
        ) 
        VALUES( 
        %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s
        ) 
        ON CONFLICT (id_percorso, versione) /* or you may use [DO NOTHING;] */
        DO UPDATE  SET descrizione=EXCLUDED.descrizione, id_turno=EXCLUDED.id_turno,
        durata=EXCLUDED.durata, id_tipo=EXCLUDED.id_tipo, 
        desc_tipo=EXCLUDED.desc_tipo, id_frequenza=EXCLUDED.id_frequenza,
        id_presa_servizio=EXCLUDED.id_presa_servizio, 
        id_rimessa_sit=EXCLUDED.id_rimessa_sit, desc_rimessa=EXCLUDED.desc_rimessa,
        id_uo_sit=EXCLUDED.id_uo_sit, desc_ut=EXCLUDED.desc_ut,
        data_inizio_validita=EXCLUDED.data_inizio_validita, data_fine_validita=EXCLUDED.data_fine_validita'''
    
    # faccio upsert
    for row in elenco_dati_copiare:
        
    
        try:
            curr_c.execute(upsert, row)
        except Exception as e:
            logger.error(upsert)
            logger.error(f"Errore su ID {row[0]}: {e}")
    
    
    # faccio commit
    conn_c.commit()
    logger.info("Dati copiati con successo ✅")

    
    


    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    curr_s.close()
    

    #currc1.close()
    conn_c.close()
    
    conn_s.close()




if __name__ == "__main__":
    main()