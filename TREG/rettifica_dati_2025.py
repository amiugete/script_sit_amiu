#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Script creato per rettificare i dati di raccolta e spazzamento caricati su TREG per l'anno 2025,
cancellando quanto non previsto 


E' anche necessario rettificare i dati di tutti i percorsi con gc = -1 
(compreso il percorso 0101393103 che ora ha giorno competenza 0 ma prima aveva erroneamente -1 )
Questo non viene fatto da questo script ma con una semplice query che aggiorni i valori della colona dta_last_update 
in treg_eko.consunt_ekovision
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

import locale

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


from tappa_prevista import *

from crea_dizionario_da_query import *





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

    
tipo = 'PERCORSO' #  'SPAZZAMENTO' # o 'RACCOLTA'
     

def main():



    
    filename = inspect.getframeinfo(inspect.currentframe()).filename
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
    
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    ###################################
    # Recupero token per autenticazione
    ###################################

    logger.info("START READ WS")
    api_url='{}atrif/api/v1/tobin/auth/login'.format(url_ws_treg)
    payload_treg = {"username": user_ws_treg, "password": pwd_ws_treg, }
    logger.debug(payload_treg)
    response = requests.post(api_url, json=payload_treg)
    logger.debug(response)
    #response.json()
    logger.info("Status code: {0}".format(response.status_code))
    try:      
        response.raise_for_status()
        # access JSOn content
        #jsonResponse = response.json()
        #print("Entire JSON response")
        #print(jsonResponse)
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        check=500
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        logger.error(response.json())
        check=500
    token=response.text
    logger.debug(token)



    

    ######################################################
    # Eliminazione dati caricati su TREG per anno e comune
    ######################################################

    guid = uuid.uuid4()
    logger.debug(str(guid))


    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()

    #da mettere a 0 per spazzamento, per raccolta partiamo dal numero di righe eleiminate manualmente al primo giro di script
    deleted_count = 0
    check = 0

    while check == 0:

        if tipo == 'RACCOLTA':
            query_np ='''select distinct trac_code
                from consunt.report_raccolta rr
                where rr.non_previsto = true and rr.eliminato is null
                order by trac_code
                limit 10000
            '''
            tipo_dati='wastecollections'
            tipo_id = 'wasteCollectionIds'

            query_update ='''update consunt.report_raccolta
                    set eliminato = true
                    where trac_code = ANY (%s)
                '''
        elif tipo == 'SPAZZAMENTO':
            query_np ='''select distinct trac_code
                from consunt.report_spazz rr
                where rr.non_previsto = true and rr.eliminato is null
                order by trac_code
                limit 10000
            '''
            tipo_dati='sweepings'
            tipo_id = 'sweepingIds'

            query_update ='''update consunt.report_spazz
                    set eliminato = true
                    where trac_code = ANY (%s)
                '''
        elif tipo == 'PERCORSO':
            
            # vado dentro con un codice percorso alla volta 
            
            cp= '0500115501'
            
            query_np = f''' select  
/*ce.codice_servizio_pred, ce.data_pianif_iniziale, codice, trac_code, 
ep.id_turno,*/
ce.codice || '_'|| ce.data_pianif_iniziale || '_'||ep.id_turno as trac_code_calc/*, 
count(codice_servizio_pred)*/
from treg_eko.consunt_ekovision ce 
join anagrafe_percorsi.elenco_percorsi ep 
            on ep.cod_percorso = ce.codice_servizio_pred 
            and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') 
            between data_inizio_validita and (data_fine_validita - interval '1' day) 
where ce.codice_servizio_pred = '{cp}'
group by ce.codice || '_'|| ce.data_pianif_iniziale || '_'||ep.id_turno
having count(distinct ce.codice_servizio_pred) = 1
order by 1'''


            # questa è la raccolta che va in errore (>= 20260201)
            query_np = '''
            select distinct ce.trac_code_calc  from treg_eko.consunt_ekovision ce 
where ce.tipo_servizio != 'SPAZZ'
and ce.data_pianif_iniziale >= '20260201' and ce.data_pianif_iniziale < '20260228'
and ce.trac_code_calc is not null
and not exists  
(select 1 from consunt.report_raccolta rr where rr.trac_code = ce.trac_code_calc )
            '''
            
            
            
            query_np = '''select distinct ce.trac_code_calc  from treg_eko.consunt_ekovision ce 
where ce.tipo_servizio = 'SPAZZ' 
and ce.trac_code_calc is not null
and not exists  
(select 1 from consunt.report_spazz rr where rr.trac_code = ce.trac_code_calc )'''
            
            
            # questa bisogna capire se raccolta o spazzamento in base al cp
            
            # nel caso cambiare report_raccolta
            tipo_dati='wastecollections'
            tipo_id = 'wasteCollectionIds'
            
            # nel caso cambiare report_spazz
            tipo_dati='sweepings'
            tipo_id = 'sweepingIds'
            
            
            query_update ='''update consunt.report_raccolta
                    set eliminato = true
                    where trac_code = ANY (%s)'''
        
        
        
        try:
            curr.execute(query_np)
            trac_codes=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_np)
            logger.error(e)
        
        
        
        #logger.debug(trac_codes)    
        #exit()
        if len(trac_codes) > 0:
            body_upload={
                'id': str(guid),
                tipo_id: [tc[0] for tc in trac_codes]
            }
            
            #logger.info(f'body_upload:{body_upload}')      
            # costruisco l'url per la cancellazione dei dati con tipo_dati definito sopra
            api_url_reset='{}atrif/api/v1/tobin/b2b/process/rifqt-{}/delete/av1'.format(url_ws_treg, tipo_dati)          
                    
            response_reset = requests.post(api_url_reset, json=body_upload, headers={'accept':'*/*', 
                                                                                    'mde': 'PROD',
                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                    'Content-Type': 'application/json'})
            logger.info(response_reset.status_code)
            logger.info(response_reset.text)
            
            logger.info(f'ws deleteCount a questo giro: {response_reset.json()["deletedCount"]}')
            deleted_count+=response_reset.json()['deletedCount']
            logger.info(f'Parziale record eliminati fino ad ora: {deleted_count}')
            
            if response_reset.status_code == 200:
                logger.info("Dati eliminati correttamente")
                
                try:
                    curr.execute(query_update, ([tc[0] for tc in trac_codes],))
                except Exception as e:
                    logger.error(query_update)
                    logger.error(e)
                
                conn.commit() 
        else:
            check = 1
            logger.info("Non ci sono pèiù righe da eliminare da TREG")

    logger.info(f'Totale record eliminati: {deleted_count}')       
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
   


 
    #response = requests.get(url_bucher, params={'starttime':starttime, 'endtime': endtime}, headers={'Authorization: EIP {}'.format(token)})

if __name__ == "__main__":
    main()      