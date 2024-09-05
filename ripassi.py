#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica anomalie sui ripassi segnati sulla tabella elem.elementi_aste_percorso

Se trova delle anomalie: 
- corregge le anomalie
- segna che il percorso è stato variato affinchè venga correttamente importato sulla UO. La variazione viene associata all'utente procedure
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail


#import requests

import logging
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


from crea_dizionario_da_query import *


import csv


# per mandare file a EKOVISION
import pysftp

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

'''#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    filename=logfile,
    level=logging.DEBUG)
'''


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/ripassi.log'.format(path)
errorfile='{}/log/error_ripassi.log'.format(path)
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




def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    
    query_procedure='''select id_user from util.sys_users su where name ilike 'procedure' '''

    try:
        curr.execute(query_procedure)
        user_proceudure=curr.fetchall()
    except Exception as e:
        logger.error(e)


    

           
    for up in user_proceudure:
        id_user_procedure=up[0]
    
    logger.info('''Id utente procedure: {0}'''.format(id_user_procedure))
    
    curr.close()
    curr = conn.cursor()
    
    #inizializzo gli array
    cod_percorso=[]
    descrizione=[]
    servizio=[]
    ut=[]
    stato_importazione=[]
    
    
    query_anomalie='''
    select distinct 
    p.id_percorso, 
    p.cod_percorso, 
    p.versione, 
    e.id_piazzola,
    eap.id_elemento,
    max(eap.id_asta_percorso) as id_asta_percorso_max,
    string_agg(eap.id_asta_percorso::text, ',' order by ap2.num_seq) as aste_percorso, 
    count(distinct ripasso) as ripassi_regisrati_sit,
    count(eap.id_asta_percorso) as num_aste_percorso,
    p.descrizione,
    coalesce((select su.email from util.sys_history sh 
    join util.sys_users su on su.id_user= sh.id_user where 
    (sh.id_percorso= p.id_percorso or e.id_piazzola = sh.id_piazzola)
    and sh.id_user not in (0,179) 
    order by datetime desc limit 1), 'assterritorio@amiu.genova.it')
    from elem.elementi_aste_percorso eap
    join elem.elementi e on e.id_elemento = eap.id_elemento 
    join elem.aste_percorso ap2 on ap2.id_asta_percorso = eap.id_asta_percorso 
    join elem.percorsi p on p.id_percorso=ap2.id_percorso 
    where
    eap.id_asta_percorso in (
        select id_asta_percorso  
        from elem.aste_percorso ap 
        where id_percorso in (select id_percorso from elem.percorsi where id_categoria_uso in (3,6))
    )
    group by eap.id_elemento, p.id_percorso, p.cod_percorso, p.versione, e.id_piazzola, p.descrizione
    having count(distinct ripasso) != count(eap.id_asta_percorso);
    '''
    
    
    try:
        curr.execute(query_anomalie)
        anomalie_ripassi=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(anomalie_ripassi)


    
    curr1 = conn.cursor()
    curr2 = conn.cursor()
           
    for ap in anomalie_ripassi:
        #logger.debug(ap[6].split(','))
        k=0 
        while k < len(ap[6].split(',')):
            update_query='''update elem.elementi_aste_percorso set ripasso = %s
            where id_elemento = %s and id_asta_percorso=%s'''
            #logger.debug(int(ap[6].split(',')[k]))
            try:
                curr1.execute(update_query, (k, ap[4],int(ap[6].split(',')[k])))
            except Exception as e:
                logger.error(e)
                logger.error(update_query)
            k+=1
        
        descrizione = 'Sistemato ripasso su piazzola {0} - Percorso {1} v.{2}'.format(ap[3], ap[1], ap[2])
        insert_query='''INSERT INTO util.sys_history
                ("type", "action", description, datetime, id_user, id_piazzola, id_percorso, id_elemento)
                VALUES
                ('PERCORSO', 'UPDATE_ELEM', %s, now(), %s, %s, %s, %s)'''
        try:
            curr2.execute(insert_query, (descrizione, id_user_procedure, ap[3], ap[0], ap[4]))
        except Exception as e:
            logger.error(e)
            logger.error(insert_query)
        conn.commit()
    
        body_mail='''ATTENZIONE<br>Poco fa, sul percorso <ul> <li><b>Codice</b>: {}</li> <li><b>Descrizione</b>: {}</li></ul>
        L'utente <i><b>{}</b></i> dovrebbe aver inserito 2 volte la piazzola <b>{}</b> (potrebbe essere la chiusura di un intervento aperto in precedenza). 
        Il secondo inserimento è automaticamente considerato un ripasso. 
        
        Se così non fosse è sufficiente rimuovere il secondo inserimento su
        <a href="https://amiupostgres.amiu.genova.it/SIT/#!/percorsi/percorso-details/?idPercorso={}"> SIT </a>
        '''.format(ap[1], ap[9], ap[10].split('@')[0], ap[3],ap[0])
        ripasso_mail(body_mail, ap[10], os.path.basename(__file__), logger)
        #ap[9]  #descrizione percorso
        #ap[10] #mail
    
    
    
    
    
    curr.close()
    curr1.close()
    curr2.close()
    
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    
if __name__ == "__main__":
    main()
    