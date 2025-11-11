#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi Roberta Fagandini

'''
Lo script corregge eventuali errori sui percorsi stagionali per cui non è stata creata la versione sulla UO chiamando
la procedura UNIOPE.ATTIVA_PERCORSI_STAGIONALI

modificare i dati dell'array lista_stagionali prima di lanciare

ELIMINARE LO SCRIPT UNA VOLTA SISTEMATE TUTTE LE ANOMALIE

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
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


import requests
from requests.exceptions import HTTPError

import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION
import pysftp

import csv



filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


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


import fnmatch



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
        
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d')

    logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logger.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_ekovision='20231120'
    
    
    

    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    #lista_stagionali = [('0201257801', '15/09/2025', '16/05/2026'), ('0201258001', '15/09/2025', '16/05/2026'), ('0201258101', '15/09/2025', '16/05/2026'), ('0201257501', '15/09/2025', '16/05/2026'), ('0201258609', '15/09/2025', '21/04/2026')]

    lista_stagionali = [('0201254901', '01/09/2025', '01/06/2026')]


    # invernali da correggere
    query_sit='''select /*p.id_percorso,*/ p.cod_percorso,/* p.descrizione, p.stagionalita,
    p.ddmm_switch_on, p.ddmm_switch_off, u.descrizione as ut, 
    s.descrizione as servizio,*/ to_char(data_attivazione, 'DD/MM/YYYY') as data_attivazione, 
    case 
    when data_dismissione is null then '01/12/2099'
    else to_char(data_dismissione, 'DD/MM/YYYY')
    end data_dismissione/*, 
    3 as attivo, 
    id_categoria_uso, 
    ep.cod_percorso*/
    from elem.percorsi p
    left join elem.percorsi_ut pu on pu.cod_percorso = p.cod_percorso 
    left join topo.ut u on u.id_ut = pu.id_ut and pu.responsabile = 'S'
    left join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso and ep.data_inizio_validita = p.data_attivazione
    join elem.servizi s on s.id_servizio= p.id_servizio
    where p.stagionalita is not null 
    AND p.cod_percorso IN (
    '0201255801',
'0506005702',
'0201254701',
'0506005601',
'0507117101',
'0500115801',
'0201255201',
'0507125301',
'0203010903',
'0203010803',
'0507123301',
'0500116001',
'0500123201',
'0508049901',
'0508049701',
'0501004301',
'0506006802',
'0101366002',
'0500115601',
'0500121301',
'0507117201',
'0508057101',
'0203010703',
'0506007002',
'0501016001',
'0507117001',
'0101377401',
'0508060202',
'0203011003',
'0508049801',
'0203010603')
    and data_attivazione >= now()::date
    --and ep.cod_percorso is null
    and id_categoria_uso in (3,6)
    order by p.data_attivazione'''
    
    
    
    
    # estivi da correggere
    query_sit='''select /*p.id_percorso,*/ p.cod_percorso,/* p.descrizione, p.stagionalita,
    p.ddmm_switch_on, p.ddmm_switch_off, u.descrizione as ut, 
    s.descrizione as servizio,*/ to_char(data_attivazione, 'DD/MM/YYYY') as data_attivazione, 
    case 
    when data_dismissione is null then '01/12/2099'
    else to_char(data_dismissione, 'DD/MM/YYYY')
    end data_dismissione/*, 
    3 as attivo, 
    id_categoria_uso, 
    ep.cod_percorso*/
    from elem.percorsi p
    left join elem.percorsi_ut pu on pu.cod_percorso = p.cod_percorso 
    left join topo.ut u on u.id_ut = pu.id_ut and pu.responsabile = 'S'
    left join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso and ep.data_inizio_validita = p.data_attivazione
    join elem.servizi s on s.id_servizio= p.id_servizio
    where p.stagionalita is not null 
    AND p.cod_percorso IN ('0101392801',
'0201036501',
'0201255101',
'0201255501',
'0201257601',
'0201258201',
'0201258301',
'0201259001',
'0203009701',
'0208000601',
'0500133101',
'0500133201',
'0500135701',
'0500136101',
'0500136201',
'0500136301',
'0500136401',
'0500136501',
'0500136601',
'0500136701',
'0500136801',
'0501016101',
'0501016201',
'0501021801',
'0502044302',
'0502044402',
'0506005401',
'0506005502',
'0506008502',
'0507118502',
'0507120802',
'0507123802',
'0507138101',
'0507138401',
'0507138703',
'0507138802',
'0508057302',
'0508075202',
'0508077601')
    and data_attivazione >= now()::date
    --and ep.cod_percorso is null
    and id_categoria_uso in (3,6)
    order by p.data_attivazione'''
    
    
    
    try:
        curr.execute(query_sit)
        lista_stagionali=curr.fetchall()
    except Exception as e:
        logger.error(query_sit)
        logger.error(e)
    
    
    for ls in lista_stagionali:
        # id_percorso  ls[0]
        # cod_percorso ls[1]
        # data attivazione ls[8]
        # data disattivazione ls[9]
        logger.debug(ls[0])
        logger.debug(ls[1])
        logger.debug(ls[2])
        #exit()
        
        # lanciare procedura o funzione della UO 
    
        try:
            #logger.debug(ls[8])
            #exit()
            #strptime
            ret=cur.callproc('UNIOPE.ATTIVA_PERCORSI_STAGIONALI',
                    [ls[0],datetime.strptime(ls[1], '%d/%m/%Y'), datetime.strptime(ls[2], '%d/%m/%Y')])
            logger.debug(ret)
        except Exception as e:
            logger.error(e) 
    
    
    
        con.commit()
        #exit()
        
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      
    