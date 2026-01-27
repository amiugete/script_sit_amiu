#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica se i tipi pratica salvati su DB (treg_edok.istanze_tari) sono coerenti con quelli su EDOK.
Se trova delle incongruenze modfica il tipo pratica della prestazione su DB e aggiorna la data si ultimo aggiornamento.

Lo script nasce dal fatto che EDOK, se viene modificato il tipo pratica, non aggiorna la data di last update e uqindi noi ci
perdiamo la modifica.
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import csv

import psycopg2

import requests
from requests.exceptions import HTTPError
from urllib.parse import urlencode

import cx_Oracle

from datetime import date, datetime, timedelta

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
f_handler.setLevel(logging.INFO)


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


    oggi = datetime.now()
    mese = oggi.month
    anno = oggi.year
    ieri = date.today() - timedelta(days=1)

    logger.info (ieri.strftime("%d/%m/%Y"))

    # Se dicembre, passa a gennaio dell'anno successivo
    if mese == 12:
        next_mese = 1
        anno += 1
    else:
        next_mese = mese + 1


    logging.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
            port=port,
            user=user,
            password=pwd,
            host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    # richiamo token, creo sessione per EDOK e header chiamata API
    token1= token_edok().strip()
    session = requests.Session()
    session.headers.update({
    'Authorization': 'Bearer {}'.format(token1.strip()),
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    })

    # recupero tutti i protocolli delle pratiche TARI su DB
    query_ist = '''
        select it.cusprotocollo, it.id_archivio, it.cusmetadatitipopratica, it.statusdescription from treg_edok.istanze_tari it
        where it.cusmetadatitipopratica is not null 
        and lower(it.statusdescription) not in ('scartato', 'acquisito', 'gestione terminata')
        union 
        select it.cusprotocollo, it.id_archivio, it.cusmetadatitipopratica, it.statusdescription from treg_edok.istanze_tari it
        where it.cusmetadatitipopratica is not null
        and (lower(it.statusdescription)  = 'gestione terminata'
        and it.cusdatafine > now() - interval '30' day)
    '''

    update_tipo_pratica ='''
        update treg_edok.istanze_tari
        set cusmetadatitipopratica = %s,
        updatetime = CURRENT_TIMESTAMP
        where cusprotocollo = %s;
    '''


    try:
        curr.execute(query_ist)
        lista_protocolli=curr.fetchall()
        #logger.debug(lista_protocolli)
    except Exception as e:
        logger.error(lista_protocolli)
        logger.error(e)

    i=0
    for p in lista_protocolli:
        i+=1
        if i % 1000 == 0:
            logger.info(f'Elaborate {i} pratiche su {len(lista_protocolli)}')
        # recupero il tipo pratica da EDOK
        params2 = {
            '$select': 'id',
            "$filter": f"cusProtocollo eq '{p[0]}'"
                }
        
        query = urlencode(params2, safe='$() ')
        query = query.replace("+", "%20")
        api_url_arch = f"{url_edok}/api/archives/{p[1]}/view/1/entries?{query}"

        #logger.debug("URL finale:{0}".format(api_url_arch))

        #response2 = requests.request("GET", api_url_arch, headers=headers2)
        response2 = session.get(api_url_arch, allow_redirects=False)

        try:      
            response2.raise_for_status()
            # access JSOn content
            jsonResponse = response2.json()["value"]
            #logger.debug(jsonResponse)
            #print("Entire JSON response")
            #print(jsonResponse)
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            check=500
        except Exception as err:
            logger.error(f'Other error occurred: {err}')

        for item in jsonResponse:
            tipo_pratica_edok=item['cusMetadatiTipoPratica']
            if tipo_pratica_edok.strip() != p[2].strip():
                logger.info(f'Tipo pratica EDOK: {tipo_pratica_edok}, tipo pratica DB: {p[2]} per protocollo {p[0]}')
                # aggiorno il tipo pratica su DB se diverso
                try:
                    curr.execute(update_tipo_pratica, (tipo_pratica_edok, p[0]))
                    conn.commit()
                    logger.info(f'Aggiornato tipo pratica per protocollo {p[0]} a {tipo_pratica_edok}')
                except Exception as e:
                    logger.error(f'Errore aggiornamento tipo pratica per protocollo {p[0]} a {tipo_pratica_edok}')
                    logger.error(e)

    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()

    

if __name__ == "__main__":
    main()