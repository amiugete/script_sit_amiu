#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script usa i trigger presenti sul DB ORACLE UNIOPE

- update descrizione


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

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


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/update_descrizioni_UO.log'.format(path)
errorfile='{}/log/error_update_descrizioni_UO.log'.format(path)
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





def main():
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Collegato a DB Oracle. Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    
    


    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

    


    # query descrizioni su più UT 
    query_new_descrizioni = '''SELECT a.id_percorso, trim(l.DESCRIZIONE), DTA_UPDATE
        FROM 
        (
        SELECT id_percorso, count(DISTINCT DESCRIZIONE) FROM ANAGR_SER_PER_UO aspu 
        WHERE DTA_DISATTIVAZIONE > SYSDATE
        GROUP BY id_percorso 
        HAVING count(DISTINCT DESCRIZIONE) > 1
        ) a
        JOIN LOG_UPDATE_ANAGR_SER_PER_UO l ON l.id_percorso = a.ID_PERCORSO 
        AND l.descrizione IS NOT NULL 
        AND "CHECK" = 'N'
        WHERE DTA_UPDATE = (SELECT max(DTA_UPDATE) FROM LOG_UPDATE_ANAGR_SER_PER_UO WHERE id_percorso = l.ID_PERCORSO AND descrizione IS NOT NULL)
        ORDER BY a.ID_PERCORSO, DTA_UPDATE'''


    try:
        cur.execute(query_new_descrizioni)
        new_descrizioni = cur.fetchall()
    except Exception as e:
        logger.error(query_new_descrizioni)
        logger.error(e)



    for dd in new_descrizioni:
        # faccio UPDATE di ANAGR_SER_PER_UO
        logger.debug('cod_percorso = {}, descrizione = {}'.format(dd[0],dd[1]))
        
        update1='''UPDATE ANAGR_SER_PER_UO 
        SET DESCRIZIONE = :t1 
        WHERE DTA_DISATTIVAZIONE > SYSDATE 
        and ID_PERCORSO = :t2'''
        
        try:
            cur.execute(update1, (dd[1], dd[0],))
            #macro_tappe.append(tappa[0])
        except Exception as e:
            logger.error(dd)
            logger.error(update1)
            logger.error(e) 
        
        con.commit()
        
        # faccio UPDATE dei log generati dal trigger (compresi gli ultimi)
        update2='''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO 
        SET "CHECK" = 'S' 
        WHERE DESCRIZIONE IS NOT NULL  
        and ID_PERCORSO = :t1'''
        
        try:
            cur.execute(update2, (dd[0],))
            #macro_tappe.append(tappa[0])
        except Exception as e:
            logger.error(dd)
            logger.error(update2)
            logger.error(e) 
        
        con.commit()
               
                
        # Faccio update descrizione anche sul SIT
        
        update_sit= '''UPDATE elem.percorsi 
        SET descrizione = %s 
        WHERE cod_percorso = %s 
        AND id_categoria_uso = 3'''

        try:
            curr.execute(update_sit, (dd[1],dd[0]))
            #macro_tappe.append(tappa[0])
        except Exception as e:
            logger.error(dd)
            logger.error(update_sit)
            logger.error(e) 
        
        conn.commit()
         
     


    
    
    
    ##################################################################################
    #query descrizioni su singole UT
    
    cur.close()
    cur = con.cursor()
    
    query_new_descrizioni2 = '''SELECT ID_PERCORSO, DESCRIZIONE, DTA_UPDATE FROM LOG_UPDATE_ANAGR_SER_PER_UO l
        WHERE l."CHECK" = 'N' 
        AND DESCRIZIONE IS NOT NULL
        AND ID_PERCORSO IN (
            SELECT ID_PERCORSO --, count(DISTINCT id_uo)
            FROM ANAGR_SER_PER_UO aspu 
            WHERE DTA_DISATTIVAZIONE > SYSDATE 
            GROUP BY ID_PERCORSO 
            HAVING count(DISTINCT id_uo) = 1
        )
        ORDER BY DTA_UPDATE'''
    
    try:
        cur.execute(query_new_descrizioni2)
        new_descrizioni = cur.fetchall()
    except Exception as e:
        logger.error(query_new_descrizioni2)
        logger.error(e)


    for dd in new_descrizioni:
        # faccio UPDATE di ANAGR_SER_PER_UO
        logger.debug('cod_percorso = {}, descrizione = {}'.format(dd[0],dd[1]))
        
        
        # faccio UPDATE dei log generati dal trigger (compresi gli ultimi)
        update2='''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO 
        SET "CHECK" = 'S' 
        WHERE DESCRIZIONE IS NOT NULL  
        and ID_PERCORSO = :t1'''
        
        try:
            cur.execute(update2, (dd[0],))
            #macro_tappe.append(tappa[0])
        except Exception as e:
            logger.error(dd)
            logger.error(update2)
            logger.error(e) 
        
        con.commit()
               
                
        # Faccio update descrizione anche sul SIT
        
        update_sit= '''UPDATE elem.percorsi 
        SET descrizione = %s 
        WHERE cod_percorso = %s 
        AND id_categoria_uso = 3'''

        try:
            curr.execute(update_sit, (dd[1],dd[0]))
            #macro_tappe.append(tappa[0])
        except Exception as e:
            logger.error(dd)
            logger.error(update_sit)
            logger.error(e) 
        
        conn.commit()
        
    
    
    
    
    ##################################################################################
    #query cambio turni
    
    cur.close()
    cur = con.cursor()
    
    logger.info("Sistemo quei casi di cambio turno perchè vengono gestiti dal altro script")
    
    update_cambio_turno = '''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO luaspu SET "CHECK" = 'S'
    WHERE "CHECK" = 'N'
    AND ID_TURNO IS NOT NULL 
    AND NEW_DISATTIVAZIONE IS NULL 
    AND DURATA IS NULL 
    AND FAM_MEZZO IS NULL 
    AND Descrizione IS NULL'''
    
    
    try:
        cur.execute(update_cambio_turno)
        #macro_tappe.append(tappa[0])
    except Exception as e:
        logger.error(update_cambio_turno)
        logger.error(e)
    
    
    con.commit()
    
    
    ##################################################################################
    # altri cambi senza impatti
    
    cur.close()
    cur = con.cursor()
    
    logger.info("Sistemo quei casi di altri cambi senza impatti - cambi turno, durata e/o fam mezzo")
    
    update_altri_cambi = '''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO luaspu SET "CHECK" = 'S' 
    WHERE "CHECK" = 'N'
    AND 
    (ID_TURNO IS NOT NULL  
    OR DURATA IS NOT NULL 
    OR FAM_MEZZO IS NOT NULL 
    )'''

    
    try:
        cur.execute(update_altri_cambi)
        #macro_tappe.append(tappa[0])
    except Exception as e:
        logger.error(update_altri_cambi)
        logger.error(e)


    con.commit()    
    
    
    
    ##################################################################################
    # altri cambi senza impatti 2
    
    cur.close()
    cur = con.cursor()
    
    logger.info("Sistemo quei casi dove c'è solo una testata su UO e non su SIT")
    
    update_altri_cambi2 = '''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO luaspu SET "CHECK" = 'S' 
    WHERE "CHECK" = 'N'
    AND ID_PERCORSO IN (
        SELECT ID_PERCORSO --, count(DISTINCT id_uo)
        FROM ANAGR_SER_PER_UO aspu 
        WHERE DTA_DISATTIVAZIONE > SYSDATE 
        GROUP BY ID_PERCORSO 
        HAVING count(DISTINCT id_uo) = 1
    ) AND ID_PERCORSO NOT IN (
        SELECT DISTINCT COD_PERCORSO  FROM PERCORSI_DA_SIT pds WHERE ID_CATEGORIA_USO  IN (3,6)
    )'''

    
    try:
        cur.execute(update_altri_cambi2)
        #macro_tappe.append(tappa[0])
    except Exception as e:
        logger.error(update_altri_cambi2)
        logger.error(e)


    con.commit() 
    
    
    ##################################################################################   
    # percorsi disattivi sia su UO che su SIT

    cur.close()
    cur = con.cursor()
    
    logger.info("Sistemo percorsi disattivi sia su UO che su SIT")
    
    update_altri_cambi2 = '''UPDATE LOG_UPDATE_ANAGR_SER_PER_UO luaspu SET "CHECK" = 'S' 
    WHERE "CHECK" = 'N'
    AND ID_PERCORSO NOT IN (
        SELECT DISTINCT ID_PERCORSO FROM ANAGR_SER_PER_UO aspu WHERE DTA_DISATTIVAZIONE > sysdate
    )
    AND ID_PERCORSO NOT IN (
        SELECT DISTINCT COD_PERCORSO  FROM PERCORSI_DA_SIT pds WHERE ID_CATEGORIA_USO  IN (3,6)
    )'''

    
    try:
        cur.execute(update_altri_cambi2)
        #macro_tappe.append(tappa[0])
    except Exception as e:
        logger.error(update_altri_cambi2)
        logger.error(e)


    con.commit()  
    
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      