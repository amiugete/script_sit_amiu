#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
SCRIPT da usare per ripristinare le frequenze delle singole aste su SIT 
qualora venga erroneamente modificata la frequenza della testata su SIT (Staltari docet!)



Dato 
 - codice percorso 
 - data importazione su UO da recuperare 
 
 prende la frequenza delle aste e la riporta su SIT
 
 
 Per recuperare le date di importazione sulla UO usare la seguente query: 
 
SELECT DISTINCT to_char(data_prevista, 'YYYYMMDD)
from CONS_PERCORSI_VIE_TAPPE cpvt 
WHERE id_percorso = '0203005001'
ORDER BY 1 desc


PER ORA SOLO SPAZZAMENTO!!
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


from credenziali import *

import report_settimanali_percorsi_ok 


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








def main():
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    cur= con.cursor()
    
    cod_percorso = '0203005001'
    data_import= 20250328 # YYYYMMDD
    
    
    # seleziono aste su UO
    
    query_uo= '''SELECT cronologia, id_tappa, id_asta,
    nvl(trim(cmt.NOTA_VIA),'nndd') AS nota_via,
    cmt.FREQUENZA AS frequenza_UO, 
    fds.COD_FREQUENZA AS frequenza_sit 
    FROM CONS_PERCORSI_VIE_TAPPE cpvt 
    JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA 
    LEFT JOIN FREQUENZE_DA_SIT fds ON fds.FREQ_BINARIA = cmt.FREQUENZA 
    WHERE id_percorso = :d1 
    AND cpvt.DATA_PREVISTA = to_date(:d2, 'YYYYMMDD')
    ORDER BY cronologia ASC
    '''
    
    
    try:
        cur.execute(query_uo, (cod_percorso, data_import,))
        #cur1.rowfactory = makeDictFactory(cur1)
        frequenze=cur.fetchall()
    except Exception as e:
        logger.error(query_uo)
        logger.error(e)
        
        
    for ff in frequenze:
        
        query_update='''update elem.aste_percorso ap
        set frequenza = %s
        where id_asta = %s 
        and num_seq = %s 
        and coalesce(nullif(trim(nota),''),'nndd')=%s
        and id_percorso in (SELECT p.id_percorso FROM elem.percorsi p WHERE cod_percorso = %s
	        and versione = (
                        select max(versione) from elem.percorsi p2 where p2.cod_percorso = p.cod_percorso
                    )
            )
        RETURNING  ap.*
        ''' 
        
        try:
            curr.execute(query_update, (ff[5], ff[2], ff[0], ff[3],
                                        cod_percorso))
            logger.debug(f'{ff[5]}, {ff[2]}, {ff[0]}, {ff[3]}, {cod_percorso}')
            riga_aggiornata=curr.fetchall()
            logger.debug(riga_aggiornata)
        except Exception as e:
            logger.error(query_update)
            logger.error(e)
        
        
        conn.commit()
        
        
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)




if __name__ == "__main__":
    main()