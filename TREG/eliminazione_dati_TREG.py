#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Script per effettuare il reset dei dati di un anno e comune specifico (o di un anno intero) su TREG, in modo da poter poi ricaricare i dati corretti.

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

import time

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

    
     

def main():

    try:
        if sys.argv[1]== 'prod':
            test=0
            URL = url_ws_treg
        elif sys.argv[1]== 'test':
            test=1
            URL = url_ws_treg_test
        else: 
            print('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        #test=1
        print('Non è stato passato alcun parametro. DEVO specificare se test o prod')
        exit()
    
    
    
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path=os.path.dirname(sys.argv[0]) 
    path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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
    f_handler.setLevel(logging.DEBUG)


    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)


    cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    c_handler.setFormatter(cc_format)
    f_handler.setFormatter(cc_format)
    
    if test==1:
        logger.info('Ambiente di TEST')
        logger.info(f'URL WS TREG: {URL}')
    
    #exit()  
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    ###################################
    # Recupero token per autenticazione
    ###################################

    logger.info("START READ WS")
    api_url='{}atrif/api/v1/tobin/auth/login'.format(URL)
    payload_treg = {"username": user_ws_treg, "password": pwd_ws_treg, }
    logger.debug(payload_treg)
    response = requests.post(api_url, json=payload_treg, verify=True)
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



    ##################################################################################################################################################################
    # check_anno_comune = 0 cancello i dati dei sovrariempimenti di un anno dato
     # check_anno_comune = 1 cancello i dati di un anno dato comune per comune (va fatto per raccolta e spazzamento)
    
    check_anno_comune = 1
    tipo_dati =  'sweepings' # 'wastecollections' #'sweepings' #'wastecollections'  # 'overfilledbins'
    anno_input=2026
    asincrono=1 # se 0 uso metodo sincrono
    ##################################################################################################################################################################

    # costruisco l'url per la cancellazione dei dati con tipo_dati definito sopra
    # reset-data metodo standard
    # reset-data-batch metodo asincronono, subito restituisce pending, poi bisogna interregare altro url per vedere lo stato
    # il metodo asincrono c'è solo per raccolta / spazzameno
    
    """
    METODO DI VERIFICA STATO: /rifqt-wastecollections/reset-data-status/av1
        Request:
        {
        "requestId": "d19d70f5-7420-4540-97c6-706483cbd4a4"
        }

        Response:
        {
        "status": "Completed",
        "deletedCount": 10,
        "errorMessage": "",
        "requestedAt": "2026-06-22T09:58:31.037Z",
        "completedAt": "2026-06-22T09:58:31.037Z"
        }
    
    I possibili stati della richiesta di cancellazione sono: “Pending", "Processing", "Completed", "Failed".
    """
    
    if asincrono == 1:
        api_url_reset='{}atrif/api/v1/tobin/b2b/process/rifqt-{}/reset-data-batch/av1'.format(URL, tipo_dati)
        api_url_reset_status='{}atrif/api/v1/tobin/b2b/process/rifqt-{}/reset-data-status/av1'.format(URL, tipo_dati)
    else:
        api_url_reset='{}atrif/api/v1/tobin/b2b/process/rifqt-{}/reset-data/av1'.format(URL, tipo_dati)
    
    ######################################################
    # Eliminazione dati caricati su TREG per anno e comune
    ######################################################

    guid = uuid.uuid4()
    logger.debug(str(guid))


    if check_anno_comune == 1:
        # connessione a SIT
        nome_db=db
        logger.info('Connessione al db {}'.format(nome_db))
        conn = psycopg2.connect(dbname=nome_db,
                            port=port,
                            user=user,
                            password=pwd,
                            host=host)


        curr = conn.cursor()

        query_code_istat='''SELECT cod_istat from topo.comuni 
        where id_comune <> 3 /* tolgo Rapallo*/
        '''

        try:
            curr.execute(query_code_istat)
            codici_istat=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_code_istat)
            logger.error(e)
        
    
        for ci in codici_istat:
            code_istat=ci[0]
            logger.debug('Elimino i dati per il comune con codice istat {}'.format(code_istat))

            body_upload={
                'id': str(guid),
                'year': anno_input,
                'istatCode': code_istat
            }
            
                      
            
            response_reset = requests.post(api_url_reset, json=body_upload, verify=True, headers={'accept':'*/*', 
                                                                                    'mde': '{}'.format('PROD' if test==0 else 'TEST'),
                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                    'Content-Type': 'application/json'})
            
            
            logger.debug(response_reset.status_code)
            requestId=response_reset.json()['batchRequestId']
            logger.info(f'Interrogo lo stato della richiesta {requestId}') #exit()
            if asincrono==1: 
                compl=0
                sec=0
                while compl<1:
                    if sec ==0:
                        secondi = 15  
                    else: 
                        secondi = 30
                    sec+=1
                    logger.info(f"Attendo {secondi} s")
                    time.sleep(secondi)
                
                    body_upload_status={
                        'requestId': str(requestId)
                    }
                    response_reset_status= requests.post(api_url_reset_status, json=body_upload_status, verify=True, headers={'accept':'*/*', 
                                                                                                    'mde': '{}'.format('PROD' if test==0 else 'TEST'),
                                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                                    'Content-Type': 'application/json'})
                    logger.debug(response_reset_status.status_code)
                    logger.debug(response_reset_status.text)
                    if response_reset_status.json()['status'] == 'Completed':
                        logger.info(f'''Record cancellati:{response_reset_status.json()['deletedCount']}
                                    Richiesta partita il {response_reset_status.json()['requestedAt']}
                                    terminata il {response_reset_status.json()['completedAt']}
                                    ''')
                        try:
                            if int(response_reset_status.json()['deletedCount'])>0:
                                logger.debug('sono qua')
                                if response_reset_status.json()['errorMessage'] !='':
                                    logger.error('Sono presenti messaggi di errore')
                                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)           
                                    exit()
                        except Exception as e:
                            logger.warning('Ci sono deletedCount ma non trovo messaggi di errore')
                        compl = 1            
                    elif response_reset_status.json()['status'] == 'NotFound':
                        logger.error('Cosa vuol dire NotFound???')
                        #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)           
                        exit()            
            #exit()    
        logger.info("chiudo le connessioni in maniera definitiva")
        curr.close()
        conn.close()
    else:
        
        body_upload={
            'id': str(guid),
            'year': anno_input
        }
        response_reset = requests.post(api_url_reset, json=body_upload, verify=True, headers={'accept':'*/*', 
                                                                                    'mde':  '{}'.format('PROD' if test==0 else 'TEST'),
                                                                                    'Authorization': 'EIP {}'.format(token),
                                                                                    'Content-Type': 'application/json'})
        logger.debug(response_reset.status_code)





    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    #response = requests.get(url_bucher, params={'starttime':starttime, 'endtime': endtime}, headers={'Authorization: EIP {}'.format(token)})



    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()

if __name__ == "__main__":
    main()      