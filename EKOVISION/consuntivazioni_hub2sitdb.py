#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Script temporaneo per portare i dati dall'HUB al nuovo DB di consuntivazione 

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

    # Mi connetto all'HUB
    nome_db=db_consuntivazione
    logger.info('Connessione al db {} su {}'.format(nome_db, host_hub))
    conn_h = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_consuntivazione,
                        password=pwd_consuntivazione,
                        host=host_hub)
    
    curr_h = conn_h.cursor()
    curr_c = conn_c.cursor()
    
    
    # SPAZZAMENTO
    # per prima cosa recupero il max(id) che ho su  nuovo db
    query_select = '''select coalesce(max(id),0) from spazzamento.effettuati'''
    
    try:
        curr_c.execute(query_select)
        max_id=curr_c.fetchone()[0]
    except Exception as e:
        logger.error(query_select)
        logger.error(e)
    
    
    logger.debug(f"Max ID attuale: {max_id}")
    #exit()
    
    curr_c.close()
    # seleziono i dati da copiare
    query_select_su_hub='''select e.id,
    tappa, 
    ct.id as id_causale, 
    /*causale,*/
    datainsert,
    datalav,
    codice, 
    punteggio
    from spazzamento.effettuati e
 join spazzamento.causali_testi ct on upper(trim(e.causale)) = upper(trim(ct.descrizione))
 where e.id > %s'''
    
    try:
        curr_h.execute(query_select_su_hub, (max_id,))
        elenco_dati_copiare=curr_h.fetchall()
    except Exception as e:
        logger.error(query_select_su_hub)
        logger.error(e)
    
    logger.info(f"Trovati {len(elenco_dati_copiare)} record da copiare.")
    #exit()
    # riapro il cursore
    curr_c = conn_c.cursor()
    upsert=''' INSERT INTO spazzamento.effettuati (
                id, tappa,
                id_causale, datainsert,
                datalav, codice,
                punteggio) 
            values 
            (%s, %s,
            %s, %s,
            %s, %s,
            %s
            )
            ON CONFLICT (id) 
            DO UPDATE  
            SET tappa=EXCLUDED.tappa, id_causale=EXCLUDED.id_causale,
            datainsert=EXCLUDED.datainsert, datalav=EXCLUDED.datalav,
            codice=EXCLUDED.codice, punteggio=EXCLUDED.punteggio'''
    
    # faccio upsert
    for row in elenco_dati_copiare:
        
    
        try:
            curr_c.execute(upsert, row)
        except Exception as e:
            logger.error(upsert)
            logger.error(f"Errore su ID {row[0]}: {e}")
    
    
    # faccio commit
    conn_c.commit()
    #logger.info("Dati copiati con successo ✅")

    
    
    # rifare la vista v_effettuati prima di cambiare credenziali su altro script
    
    
    
    curr_c.close()
    curr_h.close()
    # andrà fatta stessa cosa per la raccolta
    curr_h = conn_h.cursor()
    curr_c = conn_c.cursor()
    
    # per prima cosa recupero il max(id) che ho su  nuovo db
    query_select = '''select coalesce(max(id),0) from raccolta.effettuati_amiu'''
    
    try:
        curr_c.execute(query_select)
        max_id=curr_c.fetchone()[0]
    except Exception as e:
        logger.error(query_select)
        logger.error(e)
    
    
    logger.debug(f"Max ID attuale: {max_id}")
    #exit()
    
    curr_c.close()
    
    
    # seleziono i dati da copiare
    query_select_su_hub='''select e.id,
    id_tappa::bigint, 
    inser,
    datalav, 
    codice,
    ct.id::int as id_causale, 
    fatto, 
    e.nota_via 
    from raccolta.effettuati_amiu e
 join raccolta.causali_testi ct on upper(trim(e.causale)) = upper(trim(ct.descrizione))
 where e.id > %s'''
    
    try:
        curr_h.execute(query_select_su_hub, (max_id,))
        elenco_dati_copiare=curr_h.fetchall()
    except Exception as e:
        logger.error(query_select_su_hub)
        logger.error(e)
    
    logger.info(f"Trovati {len(elenco_dati_copiare)} record da copiare.")
    #exit()
    # riapro il cursore
    curr_c = conn_c.cursor()
    upsert=''' INSERT INTO raccolta.effettuati_amiu (
        id, 
        id_tappa, data_inserimento, 
        datalav, codice, 
        id_causale, fatto,
        nota) VALUES (
        %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s
        )
        ON CONFLICT (id) /* or you may use [DO NOTHING;] */ DO UPDATE  
        SET id_tappa=EXCLUDED.id_tappa, data_inserimento=EXCLUDED.data_inserimento,
        datalav=EXCLUDED.datalav, codice=EXCLUDED.codice,
        id_causale=EXCLUDED.id_causale, fatto=EXCLUDED.fatto,
        nota=EXCLUDED.nota'''
    
    # faccio upsert
    for row in elenco_dati_copiare:
        
    
        try:
            curr_c.execute(upsert, row)
        except Exception as e:
            logger.error(upsert)
            logger.error(f"Errore su ID {row[0]}: {e}")
    
    
    # faccio commit
    conn_c.commit()
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    #currc1.close()
    conn_c.close()
    
    curr_h.close()
    conn_h.close()




if __name__ == "__main__":
    main()