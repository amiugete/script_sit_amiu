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

def main():

    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # abbiamo notato che ogni tanto si incarta nel fare l'upload delle liste di wastcollection quindi lo gestiamo con più tentativi
    MAX_RETRIES = 5  # Numero massimo di tentativi
    DELAY_SECONDS = 10  # Tempo di attesa tra i tentativi

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
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
    query_new_ispez= '''select * from sovrariempimenti.ispezioni ii
                        where ii.id not in (
                            select split_part(ic.traciabilitycode, '_', 2)::numeric from treg_sov.ispezioni_caricate ic 
                        ) and ii.data_inserimento::date < CURRENT_DATE'''

    try:
        curr.execute(query_new_ispez)
        new_ispezioni=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_new_ispez)
        logger.error(e)
    
    token=token_treg()
    logger.debug(token)

    #new_ispezioni = []

    if len(new_ispezioni) != 0:
        logger.info('Ci sono {0} ispezioni da caricare'.format(len(new_ispezioni)))
        for ni in new_ispezioni:
            id_ispezione=ni[0]
            data_ins = ni[4]
            logger.debug('Processo ispezione con id {} esguita il {}'.format(id_ispezione, data_ins))
            
            ########################
            #recupero import id TREG
            ########################
            guid = uuid.uuid4()
            logger.debug(str(guid))
            #logger.debug(guid.type)
            #json_id={'id': '{}'.format(str(guid))}
            json_id={'id': str(guid)}
            api_url_begin_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-overfilledbins/begin-upload/av1'.format(url_ws_treg)    
        
            response = requests.post(api_url_begin_upload, json=json_id, headers={'accept':'*/*', 
                                                                                    'mde': 'PROD',
                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                    'Content-Type': 'application/json'})
            importId=response.json()['importId']
            #exit()
            
            logger.info('ImportId = {}'.format(importId))

            
            # inizializzo un check 
            # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
            check_error_upload=0
            
            
            ##################################
            # procedo con il recupero dati
            ##################################

        
            query_ispezioni='''
                select concat(p.id_piazzola,'_',i.id) as traceabilityCode,
                a.id_asta as areaCode,
                --i.ispettore as ispezione_eseguita_da,
                to_char(date_trunc('day', i.data_ora AT TIME ZONE 'Europe/Rome') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as programmingStartDate,
                to_char(date_trunc('day', i.data_ora AT TIME ZONE 'Europe/Rome') AT TIME ZONE 'UTC' + interval '23 hours 59 minutes', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as programmingEndingDate,
                to_char((i.data_ora AT TIME ZONE 'Europe/Rome') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS controlStartDate,
                CASE 
                    WHEN i.data_inserimento - i.data_ora <= interval '10 minutes' THEN to_char((i.data_inserimento AT TIME ZONE 'Europe/Rome')  AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                    ELSE to_char((i.data_ora AT TIME ZONE 'Europe/Rome') AT TIME ZONE 'UTC'  + interval '10 minutes', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                END AS controlEndingDate,
                count(distinct ie.id_elemento) as programmedBins,
                count(distinct ie.id_elemento) as controlledBins,
                count(distinct ie.id_elemento) filter (where ie.sovrariempito) as overfilledBins,
                pe.anno,
                c.cod_istat
                from sovrariempimenti.programmazione_ispezioni pe 
                join sovrariempimenti.ispezioni i on i.id_piazzola = pe.id_piazzola
                inner join sovrariempimenti.ispezione_elementi ie on ie.id_ispezione = i.id 
                join (select id_elemento, id_asta, tipo_elemento from elem.elementi
                    union 
                    select id_elemento, id_asta, tipo_elemento from history.elementi) e on ie.id_elemento = e.id_elemento
                left join elem.piazzole p on p.id_piazzola = i.id_piazzola 
                join elem.aste a on a.id_asta = coalesce(p.id_asta, e.id_asta) 
                join topo.vie v on v.id_via = a.id_via 
                join topo.comuni c on c.id_comune = v.id_comune 
                where i.id = %s and pe.anno = extract(year from i.data_ora)
                group by 
                p.id_piazzola, i.id, pe.anno, c.cod_istat, a.id_asta
            '''

            try:
                curr.execute(query_ispezioni, (id_ispezione,))
                #curr.execute(query_ispezioni, (867,))
                ispezione=curr.fetchall()
            except Exception as e:
                check_error=1
                logger.error(query_ispezioni)
                logger.error(e)
                         
            logger.debug(ispezione)
            #exit()
            list_overfilled=[]
            # popolo tratti_sit
            for isp in ispezione:
                overfilled={
                    'traceabilityCode': str(isp[0]),
                    'areaCode': str(isp[1]),
                    'programmingStartDate': str(isp[2]),
                    'programmingEndingDate': str(isp[3]),
                    'controlStartDate': str(isp[4]),
                    'controlEndingDate':str(isp[5]),
                    'programmedBins':int(isp[6]),
                    'controlledBins':int(isp[7]),
                    'overfilledBins':int(isp[8]),
                    'year':int(isp[9]),
                    'istatCode': str(isp[10]) 
                }
                list_overfilled.append(overfilled)
                    
                logger.debug(list_overfilled)
                
                ########################################################
                # upload di list_wasteCollection di un singolo percorso
                ########################################################
                logger.info('Inizio upload dati')
                api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-overfilledbins/upload/av1'.format(url_ws_treg)
                # questa sarà da passare a TREG, le altre no
                
                body_upload={
                    'id': str(guid),
                    'importId': str(importId),
                    'entities': list_overfilled
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
                        
                        logger.debug(response_upload.text)
                        #logger.debug(response_upload.json()['errorCount'])
                        #exit()
                        
                        # controllo che non ci siano errori (nel caso mi stoppo)
                    
                        if response_upload.json()['errorCount']!=0:
                            logger.error(list_overfilled)   
                            logger.error(list_overfilled.text)
                            
                            
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
                
                query_insert='''INSERT INTO treg_sov.ispezioni_caricate
                                (traciabilitycode, areacode, 
                                programmingstartdate, programmingendingdate, 
                                controlstartdate, controlendingdate, 
                                programmedbins, controlledbins, 
                                overfilledbins, "year", 
                                istatcode, data_caricamento)
                                VALUES(%s, %s,
                                to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                                to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), to_timestamp(%s, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), 
                                %s, %s, 
                                %s, %s, 
                                %s, now()); 
                            '''
                try:
                    curr.execute(query_insert, (isp[0], isp[1],
                                                isp[2], isp[3],
                                                isp[4], isp[5],
                                                isp[6], isp[7],
                                                isp[8], isp[9],
                                                isp[10],))
                    
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)  

            #exit()
            ####################################
            # commit upload
            ####################################
            logger.info('Inizio il commit degli upload su TREG')
            
            if check_error_upload==0:
                api_url_commit_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-overfilledbins/commit-upload/av1'.format(url_ws_treg)
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
        
            else: 
                logger.warning('Sono presenti errori, non faccio il commit')                

    else:
        logger.info('NON ci sono ispezioni da caricare')

    query_del_ispez='''select concat(ie.id_piazzola,'_',ie.id) from sovrariempimenti.ispezioni_eliminate ie 
                        where ie.id in (
                            select split_part(ic.traciabilitycode, '_', 2)::numeric from treg_sov.ispezioni_caricate ic 
                        )
                    '''
 
    try:
        curr.execute(query_del_ispez)
        del_ispezioni=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_del_ispez)
        logger.error(e)

    traciabilityCode_del = []
       

    if len(del_ispezioni) != 0:
        logger.info('ci sono {0} ispezioni da eliminare'.format(len(del_ispezioni)))

        for di in del_ispezioni:
            traciabilityCode_del.append(di[0])

        guid2 = uuid.uuid4()
        body_upload={
            'id': str(guid2),
            'OverfilledBinsIds': traciabilityCode_del
        }
        api_url_delete='{}atrif/api/v1/tobin/b2b/process/rifqt-overfilledbins/delete/av1'.format(url_ws_treg)          
        response_delete = requests.post(api_url_delete, json=body_upload, headers={'accept':'*/*', 
                                                                                    'mde': 'PROD',
                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                    'Content-Type': 'application/json'})
        logger.debug(response_delete.status_code)
        logger.debug(response_delete.text)

        stringa_query = ", ".join(f"'{tc}'" for tc in traciabilityCode_del)

        query_delete='''DELETE from treg_sov.ispezioni_caricate
                        where traciabilitycode in (%s); 
                            '''
        try:
            curr.execute(query_delete, (stringa_query,))
            conn.commit()
        except Exception as e:
            logger.error(query_delete)
            logger.error(e)


    else:
        logger.info('NON ci sono ispezioni da eliminare')

    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      