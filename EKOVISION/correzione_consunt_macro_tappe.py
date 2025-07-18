#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script monitora se il problema su la modifica di tipo_elemento fosse risolto con l'intervento del 10/06/2025 sullo script dati_consuntivazione_su_uo.py


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


import fnmatch



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
        
    

    
    

    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    query_errori_consuntivazione= '''SELECT cmt.ID_PERCORSO, 
            to_char(cmt.DATA_CONS, 'YYYYMMDD') as DATA_CONS,
            cmt.TIPO_ELEMENTO as TIPO_ELEMENTO_KO,
            cmt.ID_MACRO_TAPPA, 
            ce.TIPO_ELEMENTO as TIPO_ELEMENTO_OK,
            cmt1.ID_PIAZZOLA,
            count(DISTINCT ce.ID_ELEMENTO) AS num_elementi 
            FROM CONSUNT_MACRO_TAPPA cmt
            JOIN CONS_MACRO_TAPPA cmt1 ON cmt1.ID_MACRO_TAPPA = cmt.ID_MACRO_TAPPA
            JOIN CONS_MICRO_TAPPA cmt2 ON cmt.ID_MACRO_TAPPA = cmt2.ID_MACRO_TAPPA
            JOIN CONS_ELEMENTI ce ON ce.ID_ELEMENTO = cmt2.ID_ELEMENTO
            JOIN (SELECT id_macro_tappa, count(DISTINCT ce.TIPO_ELEMENTO) FROM CONS_MICRO_TAPPA cmt 
                JOIN CONS_ELEMENTI ce ON ce.ID_ELEMENTO = cmt.ID_ELEMENTO
                GROUP BY id_macro_tappa
                HAVING count(DISTINCT ce.TIPO_ELEMENTO)=1) tde ON tde.ID_MACRO_TAPPA = cmt.ID_MACRO_TAPPA 
            WHERE cmt.TIPO_ELEMENTO != ce.TIPO_ELEMENTO
            /*AND data_cons between to_date('20250101', 'YYYYMMDD') and to_date('20250609', 'YYYYMMDD')*/
            and INS_DATE  > to_date('20250610 11', 'YYYYMMDD HH24') /* data in cui è stato corretto lo script dati_consuntivazione_su_uo.py*/
            GROUP BY ce.tipo_elemento, cmt.ID_PERCORSO, cmt.DATA_CONS, cmt.TIPO_ELEMENTO, cmt.ID_MACRO_TAPPA, cmt1.ID_PIAZZOLA 
            ORDER BY 2,1'''
    
    
    try:
        cur.execute(query_errori_consuntivazione)
        pdc=cur.fetchall()
    except Exception as e:
        logger.error(query_errori_consuntivazione)
        logger.error(e)
    
    #monitoraggio se il problema fosse risolto con l'intervento del 10/06/2025 sullo script dati_consuntivazione_su_uo.py
     
    if len(pdc)>0:
        messaggio = 'ATTENZIONE!!!!!! Ci sono dei tipi elementi sbagliati in CONSUNT_MACRO_TAPPA:<br><br>'
        for pp in pdc:
            messaggio += '<li>codice percorso: {0} - id piazzola: {1} - tipo KO: {2} - tipo OK: {3} - id tappa UO: {4} - n° elementi: {5}</li>'.format(pp[0], pp[5], pp[2], pp[4], pp[3], pp[6] )
        messaggio += '<br><br>Gli errori sono stati automaticamente corretti dallo script correzione_consunt_macro_tappe.py <br>'
        logger.warning(messaggio)
        warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
        #exit()
        for p in pdc: 
            # cerco se con inserimenti successivi ho inserito il tipo_elemento corretto p[4]
            query_correzione_tappa= '''SELECT * FROM CONSUNT_MACRO_TAPPA cmt 
            WHERE id_percorso = :d1 
            AND cmt.DATA_CONS = to_date(:d2, 'YYYYMMDD')
            AND id_macro_tappa = :d3
            and tipo_elemento = :d4'''
            
            
            try:
                cur1.execute(query_correzione_tappa, (p[0], p[1], p[3], p[4]))
                check_correzione=cur1.fetchall()
            except Exception as e:
                logger.error(query_correzione_tappa)
                logger.error(e)
                exit()

            # in caso positivo devo solo fare delete del dato sbagliato 
            
            if len(check_correzione)>0:
                logger.debug('Trovato dato corretto - DELETE')
                query_delete_tappa= '''DELETE FROM CONSUNT_MACRO_TAPPA cmt 
                WHERE id_percorso = :d1 
                AND cmt.DATA_CONS = to_date(:d2, 'YYYYMMDD')
                AND id_macro_tappa = :d3
                and tipo_elemento = :d4'''
            
                try:
                    cur2.execute(query_delete_tappa, (p[0], p[1], p[3], p[2]))
                except Exception as e:
                    logger.error(query_delete_tappa)
                    logger.error(e)
                    exit()
            
            # in caso negativo devo fare update del dato sbagliato correggendo la tipologia elemento 
            else:
                logger.debug('solo UPDATE')
                query_update_tappa= '''UPDATE CONSUNT_MACRO_TAPPA cmt 
                SET tipo_elemento = :d1,
                nota='correzione_consunt_macro_tappe.py'
                WHERE id_percorso = :d2 
                AND cmt.DATA_CONS = to_date(:d3, 'YYYYMMDD')
                AND id_macro_tappa = :d4
                and tipo_elemento = :d5'''
            
                try:
                    cur2.execute(query_update_tappa, (p[4], p[0], p[1], p[3], p[2]))
                except Exception as e:
                    logger.error(query_update_tappa)
                    logger.error(e)
                    exit()
        #commit
        con.commit()
    else: 
        logger.info('Tutto ok')
    #exit()
     
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    cur.close()
    cur1.close()
    cur2.close()
    con.close()
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()