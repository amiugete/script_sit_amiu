#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi

'''
script una tantum che forse possiamo anche cancellare


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
        
    

    
    

    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    cur2 = con.cursor()
    cur3 = con.cursor()
    
                                            
    query_select='''SELECT pers, min(dta_servizio), max(dta_servizio)
  FROM (
  SELECT  distinct
            coalesce(hs.COD_DIPENDENTE, CAST(hs.ID_PERSONA AS varchar(20))) AS pers,
                per.cod_postoorg AS MANSIONE, 
                COALESCE (asc2.ID_SERVIZIO_COGE,'SRV0000') AS ID_SERVIZIO_COGE,
                COALESCE (asc2.DESCR_SERVIZIO_COGE,'Ore non assegnate') AS DESCR_SERVIZIO_COGE,
                hs.DURATA, 
                aspu.ID_PERCORSO, 
                hs.ID_SER_PER_UO,
                hs.DTA_SERVIZIO,
                to_char(hs.DTA_SERVIZIO, 'YYYY/MM') AS mese,
                id_comune,  
                comune,
                id_municipio,
                municipio,
                au1.ID_UO, 
                au1.DESC_UO, -- da  DESC_UO_SERVIZIO
                au2.id_uo AS id_uo_lavoro, 
                au2.DESC_UO AS desc_uo_lavoro,-- da chiamare DESC_UO_UOMO
                perc
            FROM HIST_SERVIZI hs
                JOIN ANAGR_SER_PER_UO aspu 
                    ON aspu.ID_SER_PER_UO = hs.ID_SER_PER_UO
                JOIN anagr_servizi as2 ON aspu.id_servizio = as2. id_servizio
                LEFT JOIN ANAGR_SERVIZI_COGE asc2 
                    ON asc2.id_servizio_COGE = as2.id_servizio_coge		
                LEFT JOIN PERCORSI_X_COMUNE_UO_GIORNO pxcuo 
                    ON pxcuo.id_percorso = aspu.ID_PERCORSO 
                    AND pxcuo.giorno = hs.dta_servizio 
                    AND pxcuo.giorno BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE
                JOIN anagr_uo au1 ON aspu.ID_UO = au1.ID_UO 
                LEFT JOIN T_ANAGR_PERS_EKOVISION per
                ON     (/*per.id_persona = hs.id_persona OR*/ hs.COD_DIPENDENTE = concat(concat(lpad(cod_matlibromat, 5,'0'), '_'),per.id_azienda))
                    AND hs.dta_servizio BETWEEN per.dta_inizio AND per.dta_fine
                    AND per.dta_fine > TO_DATE('01/01/2024', 'DD/MM/YYYY')
                /*JOIN HCMDB9.hrhistory@cezanne8 h 
                    ON h.ID_PERSONA = hs.ID_PERSONA 
                    AND hs.dta_servizio BETWEEN h.DTA_INIZIO AND h.DTA_FINE*/
                LEFT JOIN UNIOPE.V_AFFERENZE_PERSONALE vap 
                    ON per.COD_SEDE=vap.ID_SEDE_TRASPORTO AND per.COD_CDC = vap.CODICE_CDC
                    AND per.COD_UNITAORG = vap.COD_UNITAORG
                LEFT JOIN ANAGR_UO au2 ON vap.ID_UO_GEST = au2.ID_UO
                --JOIN anagr_uo au2 ON hs.ID_UO_LAVORO = au2.ID_UO
            WHERE  /*aspu.ID_PERCORSO = '0101032603' AND 
            hs.DTA_SERVIZIO BETWEEN TO_DATE('10/01/2024', 'DD/MM/YYYY') AND to_date('10/01/2024', 'DD/MM/YYYY')*/
            hs.DTA_SERVIZIO BETWEEN TO_DATE('01/01/2024', 'DD/MM/YYYY') AND to_date('31/12/2024', 'DD/MM/YYYY')
            AND hs.durata > 0 AND coalesce(perc,1) > 0
            AND au2.id_uo IS NULL
  ) GROUP BY pers
    '''

    
    
                                
    try:
        cur.execute(query_select)
        pp=cur.fetchall()
    except Exception as e:
        logger.error(query_select)
        logger.error(e)
     
    
    
    for p in pp:
        
        logger.debug('Provo a correggere hist servizi di cod_dipendente {}'.format(p[0]))       
        
        query2='''SELECT lpad(cod_matlibromat, 5,'0') || '_'||id_azienda, TO_CHAR(DTA_INIZIO, 'YYYYMMDD'), TO_CHAR(DTA_FINE,'YYYYMMDD') 
  FROM T_ANAGR_PERS_EKOVISION WHERE cod_fisc_ris IN (
  SELECT cod_fisc_ris FROM T_ANAGR_PERS_EKOVISION 
  WHERE lpad(cod_matlibromat, 5,'0') || '_'||id_azienda = :p1)
  AND dta_fine >= to_date('20240101', 'YYYYMMDD')
ORDER BY DTA_INIZIO'''
                                    
        try:
            cur2.execute(query2, (p[0],))
            ppdd = cur2.fetchall()
        except Exception as e:
            logger.error(query2)
            logger.error(e)                               
                                            
        for pd in ppdd:
            logger.debug('cod_dipendente_ok : {}, data_inizio : {}, data_fine : {}'.format(pd[0], pd[1], pd[2]))   
            query_update='''UPDATE hist_servizi SET cod_dipendente = :pd1
            WHERE cod_dipendente =  :pd2
            AND dta_servizio BETWEEN to_date(:pd3, 'YYYYMMDD') AND  to_date(:pd4, 'YYYYMMDD')'''
            
            try:
                cur3.execute(query_update, (pd[0], p[0], pd[1], pd[2]))
            except Exception as e:
                logger.error(query_update)
                logger.error(e)                   

    con.commit()                      
            
            
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")

    
    cur.close()
    cur2.close()
    cur3.close()
    con.close()




if __name__ == "__main__":
    main()      
    