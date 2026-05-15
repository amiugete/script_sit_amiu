#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script fa la sincronizzazione delle strade (DB Strade di oracle)
con una app pubblica realizzata con AppSheet di Google 

I dati vengono esportati quotidianamente dal DB strade sovrascrivendo un google sheet

Per sovrascrivere è stato implementato un javascript su google (funziona solo da chrome)

Per accedere al codice javascript dal google sheet --> Estensioni → Apps Script

La app la si trova a questo indirizzo
https://www.appsheet.com/start/84bd73f1-4e3a-4581-8390-5d625573ed58


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

from collections import defaultdict

import locale

import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *




#import requests

import logging


filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)










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

    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    # facendo join fra SCHEDE_ESEGUITE_EKOVISION e CONSUNT_EKOVISION_RACCOLTA + CONSUNT_EKOVISION_SPAZZAMENTO 
    # prendo tutte le schede che sono su DB oracle, indipendentemente se sono raccolta o spazzamento, 
    # in modo da fare un confronto più ampio con le schede eseguite su Ekovision. 
    # In questo modo riesco a capire se le schede eseguite su Ekovision arrivano su DB oracle, 
    query_strade_su_db_oracle='''SELECT 
        codice_via, 
        s.CODICE_VIA_PRIMARIO,
        nome2 AS nome, 
        descrizione, 
        --s.COMUNE, 
        c.DESCR_COMUNE,
        --c.ID_AMBITO,
        --s.CIRCOSCRIZIONE, 
        q.DESCR_QUART AS quartiere, 
        cc.DESCR_CIRC AS MUNICIPIO,
        CASE
            WHEN C.ID_COMUNE = 1 THEN au.DESC_UO
            ELSE (SELECT au1.DESC_UO 
        FROM UNIOPE.COMUNI_UT cu 
        JOIN UNIOPE.ANAGR_UO au1 ON au1.ID_UO = cu.ID_UO  
        WHERE id_comune = s.COMUNE)
        END AS UT,
        s.CAP
        FROM STRADE.STRADE s 
        JOIN STRADE.COMUNI c ON c.ID_COMUNE = s.COMUNE 
        LEFT JOIN STRADE.CIRCOSCRIZIONI cc ON cc.id_CIRC = s.CIRCOSCRIZIONE 
        LEFT JOIN STRADE.QUARTIERI q ON q.ID_QUART = s.QUARTIERE
        LEFT JOIN UNIOPE.ANAGR_UO au ON au.ID_UO = s.ID_UO 
        WHERE c.ID_AMBITO > 0
    '''
    try:
        cur.execute(query_strade_su_db_oracle)
        columns = [desc[0] for desc in cur.description]
        strade_su_db=cur.fetchall()
    except Exception as e:
        logger.error(query_strade_su_db_oracle)
        logger.error(e)
    
    
    
    
    #da qua leggo il csv
    # leggi CSV
    """with open("file.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        data = list(reader)
    """
    data = [columns] + [list(row) for row in strade_su_db]
    
    
    
    payload = {
        "values": data
    }

    # devo passare la "URL_DEL_WEBAPP_APPS_SCRIPT"

    response = requests.post(url_google_ricerca_vie, json=payload)

    logger.debug(response.text)
         
    #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()
    
    


if __name__ == "__main__":
    main()      