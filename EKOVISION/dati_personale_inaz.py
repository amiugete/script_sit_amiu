#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) scarico dati da SFTP del Personale da INAZ

2) aggio


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
    
    
    
    cartella_inaz_sftp='HR/OUTPUT/'    
    logger.info('Leggo e scarico file SFTP da cartella {}'.format(cartella_inaz_sftp))
    


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
    
    
    
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_inaz_sftp, username=user_inaz_sftp,
    password=pwd_inaz_sftp, port= port_inaz_sftp,  cnopts=cnopts,
    log="/tmp/pysftp_inaz.log")

        file_presente=0
        with srv.cd(cartella_inaz_sftp): #chdir to public
            #print(srv.listdir('./'))
            for filename in srv.listdir('./'):
                #logger.debug(filename)
                if fnmatch.fnmatch(filename, "ekovision1_{}.csv".format(giorno_file)):
                    srv.get(filename, path + "/inaz_output/" + filename)
                    logger.info('Scaricato file {}'.format(filename))
                    file_presente=1
                    
                    
                    if file_presente==1:
                        logger.info ('Inizio processo file'.format(filename))   
                        with open(path + "/inaz_output/" + filename, newline='') as csvfile:
                            spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
                            
 
                            # converting the file to dictionary
                            # by first converting to list
                            # and then converting the list to dict
                            #dict_from_csv = dict(list(spamreader)[0])
                        
                            # making a list from the keys of the dict
                            #list_of_column_names = list(dict_from_csv.keys())
                            
                            
                            i=0
                            for row in spamreader:
                                #columns=[]
                                if i>0:
                                    #logger.debug(row)
                                    query_select='''SELECT * FROM UNIOPE.EKOVISION_PERSONALE_INAZ 
                                    where CODICE_AZIENDA = :p1 and CODICE_DIPENDENTE = :p2 '''
                            
                                    try:
                                        cur.execute(query_select, (int(row[0]), int(row[1])))
                                        pp=cur.fetchall()
                                    except Exception as e:
                                        logger.error(query_select)
                                        logger.error(e)
                                    
                                    if row[3] is None or row[3].strip()=='':
                                        tipo_risorsa = None
                                    else:
                                        tipo_risorsa = int(row[3])
                                    if len(pp)==1:
                                        #update
                                        logger.debug('Update dati {} {}'.format(row[6], row[7]))
                                        if row[5].strip()=='GRBCRL61T15D969S':
                                            logger.debug('*************************************************************')
                                            logger.debug('data_licenziamento = {}'.format(row[21]))
                                        query_update='''UPDATE UNIOPE.EKOVISION_PERSONALE_INAZ 
                                        SET TIPO_RISORSA=:c1, CATEGORIA=:c2, CATEGORIA_AMIU=:c3,
                                        CODICE_FISCALE=:c4, COGNOME=:c5, NOME=:c6,
                                        DATA_NASCITA=:c7, ID_SEDE_TRASPORTO=:c8, DES_SEDE=:c9,
                                        CODICE_CDC=:c10, DES_CDC=:c11, COD_UNITAORG=:c12,
                                        DES_UNITAORG=:c13, SESSO=:c14, INDIRIZZO=:c15,
                                        CAP=:c16, CITTA=:c17, LIVELLO=:c18,
                                        DATA_ASSUNZIONE=:c19, DATA_LICENZIAMENTO=:c20 
                                        WHERE CODICE_AZIENDA=:c21 AND CODICE_DIPENDENTE=:c22 '''
                                        try:
                                            cur.execute(query_update, (row[2], tipo_risorsa , row[4],
                                                                    row[5], row[6], row[7],
                                                                    row[8], row[9], row[10],
                                                                    row[11], row[12], row[13],
                                                                    row[14], row[15], row[16],
                                                                    row[17], row[18], row[19],
                                                                    row[20], row[21], 
                                                                    int(row[0]), int(row[1])))
                                        except Exception as e:
                                            logger.error(query_update)
                                            logger.error(e)
                                    else:
                                        #insert
                                        query_insert='''INSERT INTO UNIOPE.EKOVISION_PERSONALE_INAZ (
                                            CODICE_AZIENDA, CODICE_DIPENDENTE,
                                            TIPO_RISORSA, CATEGORIA, CATEGORIA_AMIU, 
                                            CODICE_FISCALE, COGNOME, NOME,
                                            DATA_NASCITA, ID_SEDE_TRASPORTO, DES_SEDE,
                                            CODICE_CDC, DES_CDC, COD_UNITAORG,
                                            DES_UNITAORG, SESSO, INDIRIZZO,
                                            CAP, CITTA, LIVELLO,
                                            DATA_ASSUNZIONE, DATA_LICENZIAMENTO) VALUES ( 
                                            :c1, :c2,
                                            :c3, :c4, :c5,
                                            :c6, :c7, :c8,
                                            :c9, :c10, :c11,
                                            :c12, :c13, :c14,
                                            :c15, :c16, :c17,
                                            :c18, :c19, :c20,
                                            :c21, :c22
                                            ) '''
                                        try:
                                            cur.execute(query_insert, (int(row[0]), int(row[1]),
                                                                    row[2], tipo_risorsa, row[4],
                                                                    row[5], row[6], row[7],
                                                                    row[8], row[9], row[10],
                                                                    row[11], row[12], row[13],
                                                                    row[14], row[15], row[16],
                                                                    row[17], row[18], row[19],
                                                                    row[20], row[21]
                                                                    ))
                                        except Exception as e:
                                            logger.error(query_insert)
                                            logger.error(
                                                '''1: {}
                                                    2: {}
                                                    3: {}
                                                    4: {}
                                                    5: {}
                                                    6: {}
                                                    7: {}
                                                    8: {}
                                                    9: {}
                                                    10: {}
                                                    11: {}
                                                    12: {}
                                                    13: {}
                                                    14: {}
                                                    15 :{}
                                                    16: {}
                                                    17: {}
                                                    18: {}
                                                    19: {}
                                                    20: {}
                                                    21: {}
                                                    22: {}'''. format(
                                                row[0], row[1],
                                                                    row[2], tipo_risorsa, row[4],
                                                                    row[5], row[6], row[7],
                                                                    row[8], row[9], row[10],
                                                                    row[11], row[12], row[13],
                                                                    row[14], row[15], row[16],
                                                                    row[17], row[18], row[19],
                                                                    row[20], row[21]
                                                                    ))
                                            logger.error(e)
                                else:
                                    i+=1
                                    
                                        
                    
        con.commit()                      
        
        # Closes the connection
        srv.close()
        logger.info('Connessione chiusa')
    except Exception as e:
        logger.error('Problema scarico file personale per Ekovision da spazio SFTP di INAZ')
        logger.error(e)
        check_ekovision=103 # problema scarico SFTP  
    
    
    if file_presente==0:
        messaggio = 'Su spazio SFTP di INAZ non è presente file personale per Ekovision con la data di oggi'
        logger.warning(messaggio)
        warning_message_mail(messaggio, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    
    
    
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
    