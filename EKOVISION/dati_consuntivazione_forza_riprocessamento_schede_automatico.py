#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
INPUT 
- una query specifica che restituisce un elenco di ID_SCHEDE 
- elenco ID_SCHEDE


forzo il salvataggio della scheda Ekovision per fare in modo che i dati vengano riprocessati da AMIU


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

import uuid

    
     

def main():
      



    
    #exit()
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    


    # raccolta
    # vedo quelle schede per cui ho letto i dati grezzi ma non cìè nulla in consunt_macro_tappa o consunt_spazzamento
    select_schede="""/*RACCOLTA*/
        WITH tmp_schede_percorsi AS 
        (
        SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
        FROM consunt_macro_tappa
        WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD')
        /*UNION
        SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
        FROM consunt_spazzamento
        WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD') */
        ) SELECT DISTINCT see.ID_SCHEDA
        FROM CONSUNT_EKOVISION_RACCOLTA see
        WHERE see.DATA_ESECUZIONE_PREVISTA >= '202501'
        AND see.record_valido='S'
        AND NOT EXISTS (
        SELECT 1
            FROM tmp_schede_percorsi a
            WHERE a.ID_DATA = see.CODICE_SERV_PRED || '_' || see.DATA_ESECUZIONE_PREVISTA 
        )
        AND EXISTS 
        (SELECT 1 FROM anagr_ser_per_uo aspu 
        WHERE aspu.DTA_DISATTIVAZIONE >= TO_DATE('20250101', 'YYYYMMDD')
        AND aspu.ID_PERCORSO = see.CODICE_SERV_PRED  
        AND to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE)
        """    
    
  
    
    try:
        cur.execute(select_schede)
        check_schede_raccolta=cur.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
    
    
    
    # Spazzamento
    # vedo quelle schede per cui ho letto i dati grezzi ma non cìè nulla in consunt_macro_tappa o consunt_spazzamento
    select_schede="""/*SPAZZAMENTO*/
        WITH tmp_schede_percorsi AS 
(
SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
FROM consunt_spazzamento
WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD')
/*UNION
SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
FROM consunt_spazzamento
WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD') */
) SELECT DISTINCT see.ID_SCHEDA
FROM CONSUNT_EKOVISION_SPAZZAMENTO see
WHERE see.DATA_ESECUZIONE_PREVISTA >= '202501'
AND see.record_valido='S'
  AND NOT EXISTS (
   SELECT 1
    FROM tmp_schede_percorsi a
    WHERE a.ID_DATA = see.CODICE_SERV_PRED || '_' || see.DATA_ESECUZIONE_PREVISTA 
  )
  AND EXISTS 
  (SELECT 1 FROM anagr_ser_per_uo aspu 
  WHERE aspu.DTA_DISATTIVAZIONE >= TO_DATE('20250101', 'YYYYMMDD')
  AND aspu.ID_PERCORSO = see.CODICE_SERV_PRED  
  AND to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE)  
        """    
    
  
    
    try:
        cur.execute(select_schede)
        check_schede_spazzamento=cur.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
    
    check_schede = check_schede_raccolta + check_schede_spazzamento
    
    
    id_schede_problemi=[]
    for id_scheda in check_schede:
    
    
    
    
    
   
        logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda[0]))
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(id_scheda[0]),
                'flg_esponi_consunt' : 1
                }
        
        response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
        #letture2 = response2.json()
        #try: 
        letture2 = response2.json()
        #logger.info(letture2)
        #exit()
        # key to remove
        #key_to_remove = "status"
        del letture2["status"]  
        del letture2['schede_lavoro'][0]['trips']  
        del letture2['schede_lavoro'][0]['risorse_tecniche']
        del letture2['schede_lavoro'][0]['risorse_umane']
        del letture2['schede_lavoro'][0]['serv_conferimenti']
        del letture2['schede_lavoro'][0]['filtri_rfid']        
        #logger.info(letture2)
        #exit()
        #logger.info(json.dumps(letture2).encode("utf-8"))
        
        
        
        
        
        
        
        
        logger.info('Provo a salvare nuovamente la scheda {}'.format(id_scheda[0]))
        
        
        guid = uuid.uuid4()
        params2={'obj':'schede_lavoro',
                'act' : 'w',
                'ruid': '{}'.format(str(guid)),
                'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                }
        #exit()
        response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
        try:
            result2 = response2.json()
            if result2['status']=='error':
                logger.error('Id_scheda = {}'.format(id_scheda))
                logger.error(result2)
        except Exception as e:
            logger.error(e)
            warning_message_mail('Problema scheda {}'.format(id_scheda[0]), 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        
        
        #logger.info('Fatto')
    #else :
    #    logger.info(result2['status'])
    
    '''try: 
        id_scheda=letture['crea_schede_lavoro'][0]['id']
    except Exception as e:
        logger.error(e)
    '''




if __name__ == "__main__":
    main()      