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
from urllib.parse import urlencode

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import locale

import xlsxwriter

import psycopg2


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging




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



def token_edok():
    api_url='{}/connect/token'.format(url_autenticazione_edok)
    payload_edok = {"username": user_edok, "password": pwd_edok, "grant_type":"password", 
                    "client_id":"95a9b259-7fb1-454f-93ea-bbf43725aebd",
                    "client_secret":"a76ea10b-d759-44db-b45e-f967b957e380"}
    #logger.debug(payload_edok)
    response = requests.post(api_url, data=payload_edok, headers={"Content-Type": "application/x-www-form-urlencoded"})
    #logger.debug(response)
    #response.json()
    #logger.info("Status code: {0}".format(response.status_code))
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
    
    token=response.json()['access_token']
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

    
    
    
    
    token1= token_edok().strip()
    
    #logger.info(f'Token EDOK = {token1}')    
    #logger.debug("TOKEN TYPE:{}".format(type(token1)))
    #logger.debug("TOKEN LEN:{}".format(len(token1)))
    #logger.debug("TOKEN REPR:{}".format(repr(token1)))
    #exit()
    
    
    
    # definisco la query per l'insert che poi richiamerò dentro ai vari cicli 
    # insert
    query_insert= '''INSERT INTO treg_edok.istanze_tari (
        cusprotocollo, filtersd1, 
        filtersd2, cusmetadatitipopratica,
        cusmetadatidenominazione, cusmetadaticodcli,
        contractcode, cusmetadaticategoria,
        "date", 
        cusdataassegnazione, statusid,
        statusdescription, statusvalue,
        updatetime, cusdatafine,
        cusdatariassegnazione, cusdatacestino,
        cusdatasosp, cusrispostainviata,
        cusmotivazionenorisp, cusmotivazionesosp,
        cusmotivazionerif, id_archivio,
        cusassegnante, cusassegnatario,
        fromaddresses
        ) VALUES (
        %s, %s,
        %s, %s,
        trim(%s), %s,
        NULL, %s, /* da approfondire nel json non c'è contract code*/
        %s, 
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s
        ) ON CONFLICT (cusprotocollo) 
        DO UPDATE  
        SET filtersd1=EXCLUDED.filtersd1, filtersd2=EXCLUDED.filtersd2,
        cusmetadatitipopratica=EXCLUDED.cusmetadatitipopratica, cusmetadatidenominazione=EXCLUDED.cusmetadatidenominazione,
        cusmetadaticodcli=EXCLUDED.cusmetadaticodcli, contractcode=EXCLUDED.contractcode,
        cusmetadaticategoria=EXCLUDED.cusmetadaticategoria, "date"=EXCLUDED."date",
        cusdataassegnazione=EXCLUDED.cusdataassegnazione,
        statusid=EXCLUDED.statusid, statusdescription=EXCLUDED.statusdescription,
        statusvalue=EXCLUDED.statusvalue, updatetime=EXCLUDED.updatetime,
        cusdatafine=EXCLUDED.cusdatafine, cusdatariassegnazione=EXCLUDED.cusdatariassegnazione,
        cusdatacestino=EXCLUDED.cusdatacestino, cusdatasosp=EXCLUDED.cusdatasosp,
        cusrispostainviata=EXCLUDED.cusrispostainviata, cusmotivazionenorisp=EXCLUDED.cusmotivazionenorisp,
        cusmotivazionesosp=EXCLUDED.cusmotivazionesosp, cusmotivazionerif=EXCLUDED.cusmotivazionerif, 
        id_archivio=EXCLUDED.id_archivio, cusassegnante=EXCLUDED.cusassegnante,
        cusassegnatario=EXCLUDED.cusassegnatario, fromaddresses=EXCLUDED.fromaddresses
        
        '''
    
    
    
    
    # cerco l'UpdateTime da cui partire
    query_new_mail= '''select to_char(coalesce(max(updatetime),to_timestamp('20250101', 'YYYYMMDD')), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
from treg_edok.istanze_tari where id_archivio = %s'''

    
    
    
    # per ora iniziamo a cercare tra le mail in entrata e i ticket. Le mail in uscita hanno campi diversi da capire se importarle
    
    ids=[53, 140]
    payload = {}
    
    
    session = requests.Session()
    
       
    session.headers.update({
    'Authorization': 'Bearer {}'.format(token1.strip()),
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    })
    s=0 # sorgente 
    
    while s< len(ids):
        
        curr = conn.cursor()
        try:
            curr.execute(query_new_mail, (ids[s],))
            new_update_time=curr.fetchone()[0]
        except Exception as e:
            check_error=1
            logger.error(query_new_mail)
            logger.error(e)
        
        
        logger.debug(new_update_time)
        curr.close()
        # prima chiamata per vedere  quanti sono con top 1 per prendere il count 
        api_url_arch=f'{url_edok}/api/archives/{ids[s]}/view/1/entries'   
             
        
        params2 = {
            '$select': 'id',
            "$filter": f"(updateTime ge {new_update_time}) and (cusProtocollo ne null)",
            "$skip": 0,
            "$orderby": "updateTime",
            "$top": 1
        }
        
        query = urlencode(params2, safe='$() ')
        query = query.replace("+", "%20")
        api_url_arch = f"{url_edok}/api/archives/{ids[s]}/view/1/entries?{query}"

        logger.debug("URL finale:{0}".format(api_url_arch))

        #response2 = requests.request("GET", api_url_arch, headers=headers2)
        response2 = session.get(api_url_arch, allow_redirects=False)

        try:      
            response2.raise_for_status()
            # access JSOn content
            #jsonResponse = response.json()
            #print("Entire JSON response")
            #print(jsonResponse)
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            check=500
        except Exception as err:
            logger.error(f'Other error occurred: {err}')

        logger.debug("HEADERS:".format(response2.request.headers))
        logger.debug("BODY:".format(response2.request.body))
        logger.info("URL chiamata:{0}".format(response2.url))
        logger.info("Status code: {0}".format(response2.status_code))
        
        # non mi restituisce un json
        
        
        
        num_mail=int(response2.json()['@odata.count'])
        logger.info(f'Numero mail da leggere {num_mail}')
        
        logger.debug(response2.json()['value'][0]['date'])
        step=1000
        count=0
        #exit()
        while count < num_mail:

            params2 = {
            '$select': 'id',
            "$filter": f"(updateTime ge {new_update_time}) and (cusProtocollo ne null)",
            "$skip": count,
            "$orderby": "updateTime",
            "$top": step
                }
        
            query = urlencode(params2, safe='$() ')
            query = query.replace("+", "%20")
            api_url_arch = f"{url_edok}/api/archives/{ids[s]}/view/1/entries?{query}"

            logger.debug("URL finale:{0}".format(api_url_arch))

            #response2 = requests.request("GET", api_url_arch, headers=headers2)
            response2 = session.get(api_url_arch, allow_redirects=False)

            try:      
                response2.raise_for_status()
                # access JSOn content
                #jsonResponse = response.json()
                #print("Entire JSON response")
                #print(jsonResponse)
            except HTTPError as http_err:
                logger.error(f'HTTP error occurred: {http_err}')
                check=500
            except Exception as err:
                logger.error(f'Other error occurred: {err}')

            
            curr = conn.cursor()
            for i in response2.json()['value']:
                try: 
                    risposta_inviata=i['cusRispostaInviata']
                except:
                    risposta_inviata=None
                try:
                    curr.execute(query_insert, 
                                (i['cusProtocollo'],i['filterSD1'],
                                 i['filterSD2'],i['cusMetadatiTipoPratica'],
                                 i['cusMetadatiDenominazione'],i['cusMetadatiCodCli'],
                                 i['cusMetadatiCategoria'],
                                 i['date'],
                                 i['cusDataAssegnazione'],i['statusId'],
                                 i['statusDescription'],i['statusValue'],
                                 i['updateTime'],i['cusDataFine'],
                                 i['cusDataRiassegnazione'],i['cusDataCestino'],
                                 i['cusDataSosp'],risposta_inviata,
                                 i['cusMotivazioneNoRisp'],i['cusMotivazioneSosp'],
                                 i['cusMotivazioneRif'], ids[s],
                                 i['cusAssegnanteUserName'], i['cusAssegnatarioUserFullName'],
                                 i['fromAddresses'])
                                )
                except Exception as e:
                    check_error=1
                    logger.error(query_insert)
                    logger.error(i['cusProtocollo'],i['filterSD1'],
                                 i['filterSD2'],i['cusMetadatiTipoPratica'],
                                 i['cusMetadatiDenominazione'],i['cusMetadatiCodCli'],
                                 i['cusMetadatiCategoria'],
                                 i['date'],
                                 i['cusDataAssegnazione'],i['statusId'],
                                 i['statusDescription'],i['statusValue'],
                                 i['updateTime'],i['cusDataFine'],
                                 i['cusDataRiassegnazione'],i['cusDataCestino'],
                                 i['cusDataSosp'],risposta_inviata,
                                 i['cusMotivazioneNoRisp'],i['cusMotivazioneSosp'],
                                 i['cusMotivazioneRif'], ids[s],
                                 i['cusAssegnanteUserName'], i['cusAssegnatarioUserFullName'],
                                 i['fromAddresses'])
                    logger.error(e)
                    exit()
            
                
            conn.commit()
            curr.close()
            count+=step
        
        
        s+=1
    exit()
    

    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      