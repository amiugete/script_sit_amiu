#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Scopo dello script è lavorare giorno per giorno e inviare i dati delle ispezioni dei sovrariempimenti a TREG

1) query che verifica se sono state fatte nuove ispezioni con data < oggi e le carica su TREG e nella tabella treg_sovr.ispezioni_caricate

2) query che verifica se ci sono ispezioni da elimnare e le elimina da TREG e dalla tabella treg_sovr.ispezioni_caricate


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta, timezone

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

import uuid

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

import time

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0
    
def token_treg():
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
    return token

def convert_date_treg(data):

    if not data:
        return None
    else:
        if data.tzinfo is None:
            # i datetime di edok sono già in UTC quindi siamo in questo caso 
            # dove da DB non abbiamo timezone (perchè abbiamo creato colonne come timestamp without tz) 
            # ma nella riga sotto assegniamo tz = UTC perchè di fatto lo è
            data = data.replace(tzinfo=timezone.utc)
        else:
            data = data.astimezone(timezone.utc)
        
        # Restituisco la stringa nel formato richiesto
        return data.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def main():

    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # abbiamo notato che ogni tanto si incarta nel fare l'upload delle liste di wastcollection quindi lo gestiamo con più tentativi
    MAX_RETRIES = 5  # Numero massimo di tentativi
    DELAY_SECONDS = 10  # Tempo di attesa tra i tentativi

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    #oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d')
    
    

    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
      
    # cerco il giono da cui partire
    #######################
    #prendere poi la parte di insert su tabella last_import_treg da calendario spazz/raccolta
    #########################
    query_new= '''SELECT coalesce(max(max_update_dt), to_timestamp('20250101', 'YYYYMMDD')) as max_update
FROM treg_edok.last_import_treg where commit_code=200 and deleted = false; '''

    try:
        curr.execute(query_new)
        max_update_dt=curr.fetchone()[0]
    except Exception as e:
        check_error=1
        logger.error(query_new)
        logger.error(e)
    
    token=token_treg()
    #logger.debug(max_update_dt.replace(tzinfo=None))
    #logger.debug(oggi)
    
    data_start = max_update_dt.replace(tzinfo=None)
    fine_ciclo=oggi
    #logger.debug(data_start)


    logger.info('Carico i dati a partire dal {}'.format(data_start))
    
    ########################
    #recupero import id TREG
    ########################
    guid = uuid.uuid4()
    logger.debug(str(guid))
    #logger.debug(guid.type)
    #json_id={'id': '{}'.format(str(guid))}
    json_id={'id': str(guid)}
    api_url_begin_upload='{}atrif/api/v1/tobin/b2b/process/rifqc-services/begin-upload/av1'.format(url_ws_treg)    

    response = requests.post(api_url_begin_upload, json=json_id, headers={'accept':'*/*', 
                                                                            'mde': 'PROD',
                                                                            'Authorization': 'EIP {}'.format(token),
                                                                            'Content-Type': 'application/json'})
    importId=response.json()['importId']
    #exit()
    
    logger.info('ImportId = {}'.format(importId))
    #exit()
        
    # inizializzo un check 
    # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
    check_error_upload=0
    
    
    ##################################
    # procedo con il recupero dati
    ##################################


    query_pratiche="""select 
        concat('T-', it.cusprotocollo) as traceabilityCode,
        mp.cod_treg as  serviceCode,
        it.cusmetadatidenominazione as userName,
        null as userLastName,
        substring(it.cusmetadaticodcli, 0, 30) as userCode,
        null as contractCode,
        CASE 
        WHEN it.cusmetadaticategoria ilike 'Non %%' THEN 'N'
        ELSE 'D'
        end as contractType,
        it."date" as receptionDate,
        case
            when it.cusmotivazionesosp is not null then cmr.causale_arera
            else 'CSG'
        end as nonComplianceCause,
        it.cusdatacestino as nonFulfillDate,
        case 
            when it.cusdatacestino is not null then 'INF'
            else null
        end as nonFulfillReason,
        'TAR' as serviceManager,
        case 
            when mp.inoltro = true then it.cusdatafine 
            else null
        end as forwardToManagerDate,
        case 
            when it.fromaddresses in ('rimborsitari@comune.genova.it', 'servizitributi@comune.genova.it') then it."date" 
            else null
        end as forwardFromManagerDate,
        it.cusdatafine as replyDate,
        null as creditDate,
        null as nonComplianceCauseCredit,
        null as operationDate,
        null as inspectionDate,
        extract(year from it."date") as year,
        c.cod_istat as istatCode,
        null as url,
        it.updatetime
        from treg_edok.istanze_tari it
        left join treg_edok.mapping_prestazione mp on it.cusmetadatitipopratica = mp.cod_edok 
        left join treg_edok.causali_mancato_risp cmr on it.cusmotivazionesosp = cmr.motivo_edok
        join topo.comuni c on c.descr_comune  = upper(it.filtersd2)
        where it.updatetime > %s and mp.arera = true and lower(it.statusdescription) not in ('scartato', 'acquisito')
        and extract(year from it."date") >=%s 
        order by it.updatetime
"""
    #curr.mogrify(query_pratiche, (data_start,))

    try:
        curr.execute(query_pratiche, (data_start, start_year_treg,))
        pratiche=curr.fetchall()
        #logger.debug(pratiche)
    except Exception as e:
        check_error=1
        logger.error(query_pratiche)
        logger.error(e)
    curr.close()
             
    list_pratiche=[]
    # popolo tratti_sit
    for p in pratiche:
        pratica={
            'traceabilityCode': p[0],
            'serviceCode': p[1],
            'userName': p[2],
            'userLastName': p[3],
            'userCode': p[4],
            'contractCode':p[5],
            'contractType':p[6],
            'receptionDate': convert_date_treg(p[7]),
            'nonComplianceCause':p[8],
            'nonFulfillDate': convert_date_treg(p[9]),
            'nonFulfillReason': p[10],
            'serviceManager':p[11],
            'forwardToManagerDate': convert_date_treg(p[12]),
            'forwardFromManagerDate': convert_date_treg(p[13]),
            'replyDate': convert_date_treg(p[14]),
            'creditDate':convert_date_treg(p[15]),
            'nonComplianceCauseCredit': p[16],
            'operationDate': convert_date_treg(p[17]),
            'inspectionDate': convert_date_treg(p[18]),
            'year':int(p[19]),
            'istatCode': p[20],
            'url': p[21] 
        }
        update_time = p[22]
        try:
            json_pratica = json.dumps(pratica)
        except Exception as e:
            logger.error(pratica)
            logger.error(e)
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
            exit()


        list_pratiche.append(pratica)

    #json_pratiche = json.dumps(list_pratiche)

    logger.debug(f'list pratiche = {json.dumps(list_pratiche)}')
    logger.debug(f'len list pratiche = {len(list_pratiche)}')
    
    ########################################################
    # upload giornaliero delle pratiche
    ########################################################
    #logger.info('Inizio upload dati')
    if len(list_pratiche) > 0:
        api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqc-services/upload/av1'.format(url_ws_treg)
        # questa sarà da passare a TREG, le altre no
        
        body_upload={
            'id': str(guid),
            'importId': str(importId),
            'entities': list_pratiche
        }
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                
                if attempt> 1:
                    logger.warning(f"Tentativo {attempt}")
                
                # 🔁 CODICE CHE PUÒ FALLIRE
                response_upload = requests.post(api_url_upload, json=body_upload, headers={'accept':'*/*', 
                                                                        'mde': 'PROD',
                                                                        'Authorization': 'EIP {}'.format(token),
                                                                        'Content-Type': 'application/json'})
                
                logger.debug(f'response upload = {response_upload.text}')
                #logger.debug(f'json pratiche = {json_pratiche}')
                #logger.debug(f'json body = {body_upload}')
                #logger.debug(f'json body dump= {json.dumps(body_upload)}')
                #logger.debug(response_upload.json())
                #exit()
                
                # controllo che non ci siano errori (nel caso mi stoppo)
            
                if response_upload.json()['errorCount']!=0:
                    logger.error(list_pratiche)   
                    logger.error(list_pratiche.text)
                    
                    
                    # butto il dato su check_error_upload          
                    check_error_upload+=response_upload.json()['errorCount']
                # ✅ Se funziona, esci dal ciclo
                break

            except Exception as e:
                logger.warning(e)

                if attempt == MAX_RETRIES:
                    logger.error("Tutti i tentativi sono falliti. Operazione interrotta.")
                    raise ValueError(e)  # fermo l'esecuzione
                else:
                    time.sleep(DELAY_SECONDS)  # Aspetta prima del prossimo tentativo 


        #exit()
        ####################################
        # commit upload
        ####################################
        logger.info('Inizio il commit degli upload su TREG')
        
        if check_error_upload==0:
            api_url_commit_upload='{}atrif/api/v1/tobin/b2b/process/rifqc-services/commit-upload/av1'.format(url_ws_treg)
            # questa sarà da passare a TREG, le altre no
            
            body_commit_upload={
                'id': str(guid),
                'importId': str(importId)
            }
            
            
            response_commit_upload = requests.post(api_url_commit_upload, json=body_commit_upload, headers={'accept':'*/*', 
                                                                            'mde': 'PROD',
                                                                            'Authorization': 'EIP {}'.format(token),
                                                                            'Content-Type': 'application/json'})
            logger.info('Fine commit - Risposta TREG: {}'.format(response_commit_upload.text))
                
            curr = conn.cursor()
            query_insert='''INSERT INTO treg_edok.last_import_treg
                (max_update_dt, data_insert,
                request_id_amiu, importid_treg, 
                commit_code, commit_message) 
                VALUES(%s, now(), 
                %s, %s, 
                %s, %s);'''
            try:
                curr.execute(query_insert, (update_time,
                                            str(guid), str(importId),
                                            response_commit_upload.status_code, response_commit_upload.text,))
                conn.commit()
            except Exception as e:
                logger.error(query_insert)
                logger.error(e)  
        
        else: 
            logger.warning('Sono presenti errori, non faccio il commit')                
            query_insert='''INSERT INTO treg_edok.last_import_treg 
                (max_update_dt, data_insert,
                request_id_amiu, importid_treg) 
                VALUES(%s, now(), 
                %s, %s);'''
            try:
                curr.execute(query_insert, (update_time,
                                            str(guid), str(importId),))
                conn.commit()
            except Exception as e:
                logger.error(query_insert)
                logger.error(e)    


        
        
        # check se c_handller contiene almeno una riga 
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
        
        
        logger.info("chiudo le connessioni in maniera definitiva")
        curr.close()
        conn.close()
    else:
        logger.info("Nessuna pratica da importare su TREG")

if __name__ == "__main__":
    main()      