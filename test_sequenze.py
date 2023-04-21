#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica le variazioni e manda CSV a assterritorio@amiu.genova.it giornalmemte con la sintesi delle stesse 
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
logfile='{}/log/update_sequenza.log'.format(path)
errorfile='{}/log/error_update_sequenza.log'.format(path)
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
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    

    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MACRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MACRO_TAPPA) FROM CONS_MACRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_macro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_macro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MACRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_macro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} macro={}'.format(max,seq_macro[0][0]))
        if seq_macro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()      
    
    
    
    
    ## MICRO TAPPE
    
    cur0 = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    sel_uo0='''SELECT last_number
    FROM user_sequences
    WHERE sequence_name ='SEQ_ID_MICRO_TAPPA' ''' 
    try:
        cur0.execute(sel_uo0)
        #cur1.rowfactory = makeDictFactory(cur1)
        current_seq=cur0.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
        
    cur0.close() 
    
    
    sel_uo='''SELECT max(ID_MICRO_TAPPA) FROM CONS_MICRO_TAPPA cmt ''' 
    try:
        cur1.execute(sel_uo)
        #cur1.rowfactory = makeDictFactory(cur1)
        max_micro=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    
    max=max_micro[0][0]
    
    cur1.close()      

    if (current_seq[0][0]<max):
        check=0
        logger.debug('La sequenza è da correggere')
    else:
        logger.debug('La sequenza è OK, non devo fare nulla')
        check=2
    
    if check ==0:
        logger.info(' Faccio un ciclo per portare la sequenza fino al valore massimo di {}'.format(max))
    
    
    while check==0:
        sel_uo2='''select SEQ_ID_MICRO_TAPPA.NEXTVAL from dual'''
        try:
            cur2.execute(sel_uo2)
            #cur1.rowfactory = makeDictFactory(cur1)
            seq_micro=cur2.fetchall()
        except Exception as e:
            logger.error(sel_uo2)
            logger.error(e)
        logger.debug('max={} micro={}'.format(max,seq_micro[0][0]))
        if seq_micro[0][0]== max:
            check=1
        #exit()
    

    cur2.close()
    
    
    # testo una funzione
    
    
    cur0 = con.cursor()
    try:
        ret_func=cur0.callfunc('REP_CREADATEPERCORSI', int, [None])
        #cur1.rowfactory = makeDictFactory(cur1)
    except Exception as e:
        logger.error(sel_uo)
        logger.error(e)
    logger.info('Risposta REP_CREADATEPERCORSI={}'.format(ret_func))

    cur0.close()
    
    
    # CHIUDO LE CONNESSIONI 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()


if __name__ == "__main__":
    main()