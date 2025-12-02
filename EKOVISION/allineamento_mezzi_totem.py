#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa di scaricare elenco personale da EKOVISION e di scriverlo sul DB del totem

Ci serve per sapere ID ekovision e targa del mezzo associato


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



def empty_to_none(obj):
    '''
    Questa funzione scende in qualsiasi struttura (dict, list, tuple) e sostituisce le stringhe vuote con None.
    
    '''

    if isinstance(obj, dict):
        return {k: empty_to_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [empty_to_none(v) for v in obj]
    elif isinstance(obj, tuple):
        return tuple(empty_to_none(v) for v in obj)
    else:
        return None if obj == "" else obj
     

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
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
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
    
    
    

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    logger.info('Provo a leggere i dati del mezzi')
    
    
    params2={'obj':'risorse_tecniche',
            'act' : 'r',
            'data': '{}'.format(oggi.strftime('%Y%m%d')),
            'tipo_record': 'V'
            }
    
    response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
    letture2 = response2.json()
    letture2 = empty_to_none(letture2)
    logger.info(letture2)
    logger.info('Letti {} record dei mezzi'.format(len(letture2['data'][0]['risorse_tecniche'])))
    
    # scrivo i dati sul DB del totem 
        
    k=0
    while k <len(letture2['data'][0]['risorse_tecniche']):
        #logger.debug(k)
        #logger.debug(letture2['data'][0]['risorse_tecniche'][k]['id'])
        query_upsert_personale= '''
        INSERT INTO totem.mezzi_ekovision (
            id_ekovision, 
            targa, 
            descrizione, 
            famiglia, 
            tipologia, 
            sogg_propr,
            update_data
            ) 
            VALUES (%s, %s, %s,
                %s, %s, %s,
                now())
            ON CONFLICT (id_ekovision) /* or you may use [DO NOTHING;] */ 
            DO UPDATE  SET targa=EXCLUDED.targa, 
            descrizione=EXCLUDED.descrizione, 
            famiglia=EXCLUDED.famiglia, tipologia=EXCLUDED.tipologia, sogg_propr=EXCLUDED.sogg_propr, 
            update_data=now();
        ''' 
        try:
            curr_c.execute(query_upsert_personale, 
                           (
                           letture2['data'][0]['risorse_tecniche'][k]['id'],   
                           letture2['data'][0]['risorse_tecniche'][k]['targa'],
                           letture2['data'][0]['risorse_tecniche'][k]['descrizione'],
                           letture2['data'][0]['risorse_tecniche'][k]['famiglia'],
                           letture2['data'][0]['risorse_tecniche'][k]['tipologia'],
                           letture2['data'][0]['risorse_tecniche'][k]['sogg_propr'],
                           ))
        except Exception as e:
            logger.error(query_upsert_personale)
            logger.error(e)
        k+=1     
    
    conn_c.commit()
    logger.info('Aggiornati/scritti {} record dei mezzi'.format(k)) 
    
    
    #exit()
    
    
     # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
 
    curr_c = conn_c.cursor()
    
    
    # ora prendo i dati anche da INFOPM / UNIOPE
    
    query_oracle ='''SELECT ID, TIPO_RISORSA, 
TIPO_VEICOLO, trim(REPLACE(TARGA, ' ', '')) AS targa, DESCRIZIONE, 
trim(CODICE_TIPOLOGIA_MEZZO) AS CODICE_TIPOLOGIA_MEZZO,
trim(DESCRIZIONE_TIPOLOGIA_MEZZO) AS DESCRIZIONE_TIPOLOGIA_MEZZO, 
CASE 
when TIPO_CARBURANTE ='--' then null
else TIPO_CARBURANTE
END TIPO_CARBURANTE, PORTATA, 
trim(ID_SOGGETTO_PROP) AS ID_SOGGETTO_PROP, 
--SEDE_PRESA_SERV,
au.COD_SEDE AS ID_SEDE_PRESA_SERV, --
--au.DESC_UO, 
STATO_MEZZO as STATO_MEZZO_INFO, 
-- modifico mettendo indisponibile quanto alienato da più di 1 giorno
CASE 
	WHEN DESTAT = 'ALIENATO' AND DTMANU < trunc(current_date - interval '1' day) THEN 'I'
	ELSE 'D'
END AS STATO_MEZZO,
--'D' as STATO_MEZZO,
trim(SPORTELLO) as sportello, 
sysdate as UPDATE_DATA
FROM v_auto_ekovision@info a
LEFT JOIN EKOVISION_MAPPING_UO b ON a.ID_SEDE_PRESA_SERV_INFO = b.ID_UO_INFOPM
LEFT JOIN ANAGR_UO au ON au.ID_UO = b.ID_UO_GEST
WHERE --b.ID_UO_GEST IS NULL AND 
id_soggetto_prop = 'AMIU' AND a.ID_SEDE_PRESA_SERV_INFO != '--' '''
    
    
    
    query_upsert=''' INSERT INTO totem.mezzi_infopm (
        id, tipo_risorsa, tipo_veicolo, 
        targa, descrizione, codice_tipologia_mezzo, descrizione_tipologia_mezzo, 
        tipo_carburante, portata, id_soggetto_prop, 
        id_sede_presa_serv, stato_mezzo_info, stato_mezzo,
        sportello, update_data) 
        VALUES
        (%s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s)
        ON CONFLICT (id) /* or you may use [DO NOTHING;] */ 
        DO UPDATE  
        SET tipo_risorsa=EXCLUDED.tipo_risorsa, tipo_veicolo=EXCLUDED.tipo_veicolo, 
        targa=EXCLUDED.targa, codice_tipologia_mezzo=EXCLUDED.codice_tipologia_mezzo, descrizione_tipologia_mezzo=EXCLUDED.descrizione_tipologia_mezzo,
        tipo_carburante=EXCLUDED.tipo_carburante, portata=EXCLUDED.portata, id_soggetto_prop=EXCLUDED.id_soggetto_prop, 
        id_sede_presa_serv=EXCLUDED.id_sede_presa_serv, stato_mezzo_info=EXCLUDED.stato_mezzo_info, stato_mezzo=EXCLUDED.stato_mezzo,
        sportello=EXCLUDED.sportello, update_data=EXCLUDED.update_data;
    
    '''
    
    logger.info('Eseguo la query sui mezzi su INFOPM / UNIOPE')
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYYMMDD'")
    cur.execute("ALTER SESSION SET NLS_LANGUAGE = 'ITALIAN'")
    cur.execute("ALTER SESSION SET NLS_TERRITORY = 'ITALY'")
    
    
    try:
        cur.execute(query_oracle)
        elenco_mezzi_infopm = cur.fetchall()
    except Exception as e:
        logger.error('Errore esecuzione query sui mezzi su INFOPM / UNIOPE')
        logger.error(e)
    
    
    
    #scrivo i dati sul DB del totem
    logger.info('Scrivo i dati sul DB del totem')
    for riga in elenco_mezzi_infopm:
        try:
            curr_c.execute(query_upsert, riga)
        except Exception as e:
            logger.error('Errore esecuzione upsert sui mezzi su INFOPM / UNIOPE')
            logger.error(e)
            logger.error(query_upsert)
            logger.error(riga) 
            #exit()    
    
    conn_c.commit()
    logger.info('Aggiornati/scritti {} record dei mezzi da INFOPM / UNIOPE'.format(len(elenco_mezzi_infopm)))
    
    cur.close()    
    con.close()
    
    
    

    #currc1.close()
    conn_c.close()
    





if __name__ == "__main__":
    main()      