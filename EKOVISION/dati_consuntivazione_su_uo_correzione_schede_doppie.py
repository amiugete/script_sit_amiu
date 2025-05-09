#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Le tabelle per la raccolta sono 2: 

- EKOVISION_CONSUNT_DOPP_PERC_R : 2 schede nello stesso giorno che producono doppia consuntivazione 
        --> in questo caso ai fini della consuntivazione a città metropolitana e ARERA tengo la migliore delle 2  
- EKOVISION_CONSUNT_DOPP_TAPP_R : questi invece sono frutto degli errori degli itinerari che ci sono nel flusso tra il SIT / UO / Ekovision 
        --> in questi casi devo tenere il non fatto mnetre spesso la tappa doppia risulta 
        


1) aggiorno la tabella EKOVISION_CONSUNT_DOPPIE_XXXX

2) processo le righe non ancora corrette (corretto = 0)

3) cerco la tappa e correggo la CONSUNT_MACRO_TAPPA

4) faccio update a corretto = 1


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

import json



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
    logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logger.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_ekovision='20231120'
    

    



    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    
    
    
    # aggiornamento tabella EKOVISION_CONSUNT_DOPP_PERC_R
    select_query='''SELECT  cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_COMPONENTE, cer.POSIZIONE,
LISTAGG(causale, ',') WITHIN GROUP (ORDER BY cer.ID_SCHEDA) AS CAUSALI,
CASE -- al posto del null dovrei vedere la scheda che è eseguita  
	WHEN LISTAGG(causale, ',') WITHIN GROUP (ORDER BY causale) LIKE '%100%' THEN 100
	WHEN LISTAGG(causale, ',') WITHIN GROUP (ORDER BY causale) LIKE '%110%' THEN 110
	WHEN trim(REPLACE(LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY causale), '102', '')) LIKE '% %'  THEN NULL
	ELSE CAST(trim(REPLACE(LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY causale),'102', '')) AS INTEGER) 
END AS causale_OK, 
max(see.NOMEFILE) AS LAST_FILE_RECEIVED, 
0 AS corretto
FROM
/*(
	SELECT DISTINCT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE, CAUSALE
	 FROM  CONSUNT_EKOVISION_RACCOLTA cer
	 WHERE RECORD_VALIDO = 'S'
) */
CONSUNT_EKOVISION_RACCOLTA cer
JOIN SCHEDE_ESEGUITE_EKOVISION see ON see.iD_SCHEDA=cer.iD_SCHEDA AND see.RECORD_VALIDO = 'S'
WHERE cer.RECORD_VALIDO = 'S'
AND concat(cer.CODICE_SERV_PRED,cer.DATA_ESECUZIONE_PREVISTA)  IN (
	SELECT concat(CODICE_SERV_PRED,DATA_ESECUZIONE_PREVISTA)  FROM SCHEDE_ESEGUITE_EKOVISION see1 
	WHERE see1.NOMEFILE > 
		(SELECT max(LAST_FILE_RECEIVED) FROM EKOVISION_CONSUNT_DOPP_PERC_R ecdp)
	)
GROUP BY cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_COMPONENTE, cer.POSIZIONE
HAVING COUNT(DISTINCT causale)>1  AND count(DISTINCT cer.id_scheda)>1--AND 100 IN
ORDER BY  CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE'''
    
    
    try:                                                
        cur.execute(select_query, ())
        percorsi_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_query)
        logger.error(e)
        
    if len(percorsi_correggere)> 1:
        logger.info('Aggiono la tabella EKOVISION_CONSUNT_DOPP_PERC_R')
        for pc in percorsi_correggere:
            
            try:
                causale_ok=int(pc[5])
            except:
                causale_ok=None
                
            select_query1='''SELECT * FROM EKOVISION_CONSUNT_DOPP_PERC_R
            WHERE CODICE_SERV_PRED = :c1 AND 
            DATA_ESECUZIONE_PREVISTA = :c2 AND 
            COD_COMPONENTE = :c3 AND 
            POSIZIONE = :c4 '''
            cur1 = con.cursor()
            try:                                                
                cur1.execute(select_query1, (pc[0], pc[1], pc[2], pc[3]))
                check_perc=cur1.fetchall()
            except Exception as e:
                logger.error(select_query1)
                logger.error(e)

            cur1.close()
            
            cur1 = con.cursor()
            if len(check_perc) > 0:
                # update
                query_update='''
                UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_R 
                SET CAUSALI= :c1, 
                CAUSALE_OK= :c2, 
                LAST_FILE_RECEIVED= :c3, 
                CORRETTO=0
                WHERE CODICE_SERV_PRED = :c4 AND 
                DATA_ESECUZIONE_PREVISTA = :c5 AND 
                COD_COMPONENTE = :c6 AND 
                POSIZIONE = :c7
                '''
                try:                                                
                    cur1.execute(query_update, (pc[4], causale_ok, pc[6], pc[0], pc[1], int(pc[2]), int(pc[3])))
                except Exception as e:
                    logger.error(query_update)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[4], pc[5], pc[6], pc[0], pc[1], pc[2], pc[3]  
                    ))
                    logger.error(e)
                
                
            else: 
                # insert
                query_insert='''INSERT INTO UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_R 
                (CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE,
                CAUSALI, CAUSALE_OK, LAST_FILE_RECEIVED, CORRETTO) 
                VALUES
                (:c1, :c2, :c3, :c4, :c5, :c6, :c7, 0) '''
                try:                                                
                    cur1.execute(query_insert, (pc[0], pc[1], int(pc[2]), int(pc[3]), pc[4], causale_ok, pc[6]))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[0], pc[1], pc[2], pc[3], pc[4], pc[5], pc[6] 
                    ))
                    logger.error(e)
                
                
                
            cur1.close()
            con.commit()
    else: 
        logger.info('''Non c'è nessun aggiornamento della tabella EKOVISION_CONSUNT_DOPP_PERC_R''')
    
    cur.close()
    cur = con.cursor()
    select_correzioni='''SELECT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA,
        COD_COMPONENTE, POSIZIONE,
        CAUSALI, CAUSALE_OK,
        LAST_FILE_RECEIVED 
        FROM UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_R 
        WHERE CORRETTO = 0'''
        
    # cerco macro tappa da correggere
    
    select_MT_correggere='''SELECT ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA, 
    cmt.id_piazzola,
    cmt2.ID_MACRO_TAPPA, cmt2.QTA_ELEM_NON_VUOTATI,
    cmt2.CAUSALE_ELEM,  
    LISTAGG(
    concat(concat(ec.cod_componente, '_'), ec.posizione), ' '
    ) WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA) AS elementi_corretti,
    LISTAGG(ec.CAUSALE_OK, ' ') WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA)
    causali, 
    CASE 
        WHEN count(DISTINCT causale_ok) = 1 THEN max(causale_ok)
        ELSE NULL
    END AS causale_ok
    FROM UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_R  ec
    JOIN CONS_MICRO_TAPPA cmt 
    ON cmt.ID_ELEMENTO  =  CAST(ec.COD_COMPONENTE AS varchar(6))
    AND cmt.ID_MACRO_TAPPA in
    (
        SELECT ID_TAPPA FROM CONS_PERCORSI_VIE_TAPPE cpvt 
        JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
        WHERE ID_PERCORSO = ec.CODICE_SERV_PRED AND DATA_PREVISTA = 
        (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
        AND DATA_PREVISTA <= to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD')
        )
    )
    JOIN CONSUNT_MACRO_TAPPA cmt2 
    ON cmt2.ID_MACRO_TAPPA = cmt.ID_MACRO_TAPPA 
    AND DATA_CONS = to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD') 
    WHERE CORRETTO = 0 /*AND causale_elem NOT IN (100, 110)*/
    GROUP BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA,
    cmt2.QTA_ELEM_NON_VUOTATI,
    cmt2.CAUSALE_ELEM, cmt2.ID_MACRO_TAPPA, cmt.id_piazzola'''
    
    
    
    try:                                                
        cur.execute(select_MT_correggere, ())
        tappe_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_MT_correggere)
        logger.error(e)
        
    if len(tappe_correggere)> 1:
        logger.info('Inizio correzione tappe')
        update_consunt_macro_tappe='''UPDATE UNIOPE.CONSUNT_MACRO_TAPPA
            set QTA_ELEM_NON_VUOTATI = 0, 
            CAUSALE_ELEM = 100
            WHERE ID_MACRO_TAPPA=:c1
            AND DATA_CONS = to_date(:c2, 'YYYYMMDD')''' 
            
            
        for tc in tappe_correggere:
            if tc[8]==100:
                logger.debug('Correggo percorso {} data {} - piazzola {}'.format(tc[0], tc[1], tc[2]))
                check=0
                try:                                                
                    cur.execute(update_consunt_macro_tappe, (tc[3], tc[1]))
                except Exception as e:
                    logger.error(update_consunt_macro_tappe)
                    logger.error('1: {}, 2: {}'.format(tc[3], tc[1]))
                    logger.error(e)
                    check=1
                if check==0:
                    componenti_corrette=tc[6].split()

                    for cc in componenti_corrette:
                        update_correzioni=''' UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_R
                        SET CORRETTO = 1 
                        WHERE CODICE_SERV_PRED = :c1 AND 
                        DATA_ESECUZIONE_PREVISTA = :c2 AND 
                        COD_COMPONENTE = :c3 AND 
                        POSIZIONE = :c4 
                        '''
                    try:                                                
                        cur.execute(update_correzioni, (tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                    except Exception as e:
                        logger.error(update_correzioni)
                        logger.error('1: {}, 2: {}'.format(tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                        logger.error(e)
                        check=1    
            else: 
                logger.info('''Non c'è nulla da correggere''')
            con.commit()
    
    
    
    
    # GESTIONE ERRORI EKOVISION (tappe doppie non per scheda doppia)
    # aggiornamento tabella  EKOVISION_CONSUNT_DOPP_TAPP_R
    select_query= '''SELECT  cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_COMPONENTE, cer.POSIZIONE,
LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY cer.ID_SCHEDA) AS CAUSALI,
/*CASE   
	WHEN TOTEM = 1  THEN causale
	ELSE NULL
END AS causale_OK, */
max(see.NOMEFILE) AS LAST_FILE_RECEIVED, 
count(DISTINCT totem) AS TIPI_CONSUNTIVAZIONE, 
0 AS corretto
FROM
/*(
	SELECT DISTINCT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE, CAUSALE
	 FROM  CONSUNT_EKOVISION_RACCOLTA cer
	 WHERE RECORD_VALIDO = 'S'
) */
CONSUNT_EKOVISION_RACCOLTA cer
JOIN SCHEDE_ESEGUITE_EKOVISION see ON see.iD_SCHEDA=cer.iD_SCHEDA AND see.RECORD_VALIDO = 'S'
WHERE cer.RECORD_VALIDO = 'S'
AND concat(cer.CODICE_SERV_PRED,cer.DATA_ESECUZIONE_PREVISTA)  IN (
	SELECT concat(CODICE_SERV_PRED,DATA_ESECUZIONE_PREVISTA)  FROM SCHEDE_ESEGUITE_EKOVISION see1 
	WHERE see1.NOMEFILE > 
		(SELECT max(LAST_FILE_RECEIVED) FROM EKOVISION_CONSUNT_DOPP_TAPP_R ecdt)
	)
GROUP BY cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_COMPONENTE, cer.POSIZIONE
HAVING COUNT(DISTINCT causale)>1 AND count(DISTINCT cer.id_scheda)=1
ORDER BY  CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE'''


    try:                                                
        cur.execute(select_query, ())
        percorsi_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_query)
        logger.error(e)
        
    if len(percorsi_correggere)> 1:
        logger.info('Aggiono la tabella EKOVISION_CONSUNT_DOPP_TAPP_R')
        for pc in percorsi_correggere:
            select_query1='''SELECT * FROM EKOVISION_CONSUNT_DOPP_TAPP_R
            WHERE CODICE_SERV_PRED = :c1 AND 
            DATA_ESECUZIONE_PREVISTA = :c2 AND 
            COD_COMPONENTE = :c3 AND 
            POSIZIONE = :c4 '''
            cur1 = con.cursor()
            try:                                                
                cur1.execute(select_query1, (pc[0], pc[1], int(pc[2]), int(pc[3])))
                check_perc=cur1.fetchall()
            except Exception as e:
                logger.error(select_query1)
                logger.error(e)

            cur1.close()
            
            cur1 = con.cursor()
            if len(check_perc) > 0:
                # update
                query_update='''
                UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_R 
                SET CAUSALI= :c1, 
                LAST_FILE_RECEIVED= :c2, 
                CORRETTO=0
                WHERE CODICE_SERV_PRED = :c3 AND 
                DATA_ESECUZIONE_PREVISTA = :c4 AND 
                COD_COMPONENTE = :c5 AND 
                POSIZIONE = :c6
                '''
                try:                                                
                    cur1.execute(query_update, (pc[4], pc[5], pc[0], pc[1], int(pc[2]), int(pc[3])))
                except Exception as e:
                    logger.error(query_update)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[4], pc[5], pc[6], pc[0], pc[1], pc[2], pc[3]  
                    ))
                    logger.error(e)
                
                
            else: 
                # insert
                query_insert='''INSERT INTO UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_R 
                (CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_COMPONENTE, POSIZIONE,
                CAUSALI,  LAST_FILE_RECEIVED, CORRETTO) 
                VALUES
                (:c1, :c2, :c3, :c4, :c5, :c6,  0) '''
                try:                                                
                    cur1.execute(query_insert, (pc[0], pc[1], int(pc[2]), int(pc[3]), pc[4], pc[5]))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[0], pc[1], pc[2], pc[3], pc[4], pc[5], pc[6] 
                    ))
                    logger.error(e)
                
                
                
            cur1.close()
            con.commit()
    else: 
        logger.info('''Non c'è nessun aggiornamento della tabella EKOVISION_CONSUNT_DOPP_TAPP_R''')
    
    cur.close()
    cur = con.cursor()

    
    select_MT_correggere='''SELECT ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA, 
cmt.id_piazzola,
cmt2.ID_MACRO_TAPPA, 
cmt2.QTA_ELEM_NON_VUOTATI,
cmt2.CAUSALE_ELEM,  
ec.causali,
trim(REPLACE(REPLACE(ec.causali, '100', ''), '110', '')) AS causale_ok,
sum(
CASE 
	WHEN trim(REPLACE(REPLACE(ec.causali, '100', ''), '110', '')) IS NOT NULL THEN 1
	ELSE 0
END
) AS QTA_ELEM_NON_VUOTATI_OK,
LISTAGG(
concat(concat(ec.cod_componente, '_'), ec.posizione), ' '
) WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA) AS elementi_corretti
/*,*/
/*LISTAGG(ec.CAUSALE_OK, ' ') WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA)
causali, */
/*CASE 
	WHEN count(DISTINCT causale_ok) = 1 THEN max(causale_ok)
	ELSE NULL
END AS causale_ok*/
FROM UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_R  ec
JOIN CONS_MICRO_TAPPA cmt 
ON cmt.ID_ELEMENTO  =  CAST(ec.COD_COMPONENTE AS varchar(6))
AND cmt.ID_MACRO_TAPPA in
(
	SELECT ID_TAPPA FROM CONS_PERCORSI_VIE_TAPPE cpvt 
	JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
	WHERE ID_PERCORSO = ec.CODICE_SERV_PRED AND DATA_PREVISTA = 
	(SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
	AND DATA_PREVISTA <= to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD')
	)
)
JOIN CONSUNT_MACRO_TAPPA cmt2 
ON cmt2.ID_MACRO_TAPPA = cmt.ID_MACRO_TAPPA 
AND DATA_CONS = to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD') 
WHERE CORRETTO = 0 /*AND causale_elem  IN (100, 110)*/
GROUP BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA,
cmt2.QTA_ELEM_NON_VUOTATI,
cmt2.CAUSALE_ELEM, cmt2.ID_MACRO_TAPPA, cmt.id_piazzola, ec.causali
/*HAVING(count causale_ok) > 1*/ /*CONTROLLO*/'''
    
    
    
    try:                                                
        cur.execute(select_MT_correggere, ())
        tappe_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_MT_correggere)
        logger.error(e)
        
    if len(tappe_correggere)> 1:
        logger.info('Inizio correzione tappe')
        update_consunt_macro_tappe='''UPDATE UNIOPE.CONSUNT_MACRO_TAPPA
            set QTA_ELEM_NON_VUOTATI = :c1, 
            CAUSALE_ELEM = :c2
            WHERE ID_MACRO_TAPPA=:c3
            AND DATA_CONS = to_date(:c4, 'YYYYMMDD')''' 
            
            
        for tc in tappe_correggere:
            if tc[7] is None:
                logger.info('''Non c'è nulla da correggere''')
            else:
                logger.debug('Correggo percorso {} data {} - piazzola {}'.format(tc[0], tc[1], tc[2]))
                check=0
                try:                                                
                    cur.execute(update_consunt_macro_tappe, (int(tc[8]), int(tc[7]), tc[3], tc[1]))
                except Exception as e:
                    logger.error(update_consunt_macro_tappe)
                    logger.error('1: {}, 2: {}, 3:{}, 4:{}'.format(tc[8], tc[7], tc[3], tc[1]))
                    logger.error(e)
                    check=1
                if check==0:
                    componenti_corrette=tc[9].split()

                    for cc in componenti_corrette:
                        update_correzioni=''' UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_R
                        SET CORRETTO = 1 
                        WHERE CODICE_SERV_PRED = :c1 AND 
                        DATA_ESECUZIONE_PREVISTA = :c2 AND 
                        COD_COMPONENTE = :c3 AND 
                        POSIZIONE = :c4 
                        '''
                    try:                                                
                        cur.execute(update_correzioni, (tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                    except Exception as e:
                        logger.error(update_correzioni)
                        logger.error('1: {}, 2: {}'.format(tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                        logger.error(e)
                        check=1    
                    
            con.commit()
    
    cur.close()
    cur = con.cursor()
    
    
    # bisogna fare la stessa cosa anche per lo spazzamento 
    
    
    # aggiornamento tabella EKOVISION_CONSUNT_DOPP_PERC_SP
    select_query='''SELECT  cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_TRATTO, cer.POSIZIONE,
LISTAGG(causale, ',') WITHIN GROUP (ORDER BY cer.ID_SCHEDA) AS CAUSALI,
CASE -- al posto del null dovrei vedere la scheda che è eseguita  
	WHEN LISTAGG(causale, ',') WITHIN GROUP (ORDER BY causale) LIKE '%100%' THEN 100
	WHEN LISTAGG(causale, ',') WITHIN GROUP (ORDER BY causale) LIKE '%110%' THEN 110
	WHEN trim(REPLACE(LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY causale), '102', '')) LIKE '% %'  THEN NULL
	ELSE CAST(trim(REPLACE(LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY causale),'102', '')) AS INTEGER) 
END AS causale_OK, 
max(see.NOMEFILE) AS LAST_FILE_RECEIVED, 
0 AS corretto
FROM
/*(
	SELECT DISTINCT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE, CAUSALE
	 FROM  CONSUNT_EKOVISION_SPAZZAMENTO cer
	 WHERE RECORD_VALIDO = 'S'
) */
CONSUNT_EKOVISION_SPAZZAMENTO cer
JOIN SCHEDE_ESEGUITE_EKOVISION see ON see.iD_SCHEDA=cer.iD_SCHEDA AND see.RECORD_VALIDO = 'S'
WHERE cer.RECORD_VALIDO = 'S'
AND concat(cer.CODICE_SERV_PRED,cer.DATA_ESECUZIONE_PREVISTA)  IN (
	SELECT concat(CODICE_SERV_PRED,DATA_ESECUZIONE_PREVISTA)  FROM SCHEDE_ESEGUITE_EKOVISION see1 
	WHERE see1.NOMEFILE > 
		(SELECT max(LAST_FILE_RECEIVED) FROM EKOVISION_CONSUNT_DOPP_PERC_SP ecdp)
	)
GROUP BY cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_TRATTO, cer.POSIZIONE
HAVING COUNT(DISTINCT causale)>1  AND count(DISTINCT cer.id_scheda)>1--AND 100 IN
ORDER BY  CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE'''
    
    
    try:                                                
        cur.execute(select_query, ())
        percorsi_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_query)
        logger.error(e)
        
    if len(percorsi_correggere)> 1:
        logger.info('Aggiono la tabella EKOVISION_CONSUNT_DOPP_PERC_SP')
        for pc in percorsi_correggere:
            
            try:
                causale_ok=int(pc[5])
            except:
                causale_ok=None
                
            select_query1='''SELECT * FROM EKOVISION_CONSUNT_DOPP_PERC_SP
            WHERE CODICE_SERV_PRED = :c1 AND 
            DATA_ESECUZIONE_PREVISTA = :c2 AND 
            COD_TRATTO = :c3 AND 
            POSIZIONE = :c4 '''
            cur1 = con.cursor()
            try:                                                
                cur1.execute(select_query1, (pc[0], pc[1], pc[2], pc[3]))
                check_perc=cur1.fetchall()
            except Exception as e:
                logger.error(select_query1)
                logger.error(e)

            cur1.close()
            
            cur1 = con.cursor()
            if len(check_perc) > 0:
                # update
                query_update='''
                UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_SP 
                SET CAUSALI= :c1, 
                CAUSALE_OK= :c2, 
                LAST_FILE_RECEIVED= :c3, 
                CORRETTO=0
                WHERE CODICE_SERV_PRED = :c4 AND 
                DATA_ESECUZIONE_PREVISTA = :c5 AND 
                COD_TRATTO = :c6 AND 
                POSIZIONE = :c7
                '''
                try:                                                
                    cur1.execute(query_update, (pc[4], causale_ok, pc[6], pc[0], pc[1], int(pc[2]), int(pc[3])))
                except Exception as e:
                    logger.error(query_update)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[4], pc[5], pc[6], pc[0], pc[1], pc[2], pc[3]  
                    ))
                    logger.error(e)
                
                
            else: 
                # insert
                query_insert='''INSERT INTO UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_SP 
                (CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE,
                CAUSALI, CAUSALE_OK, LAST_FILE_RECEIVED, CORRETTO) 
                VALUES
                (:c1, :c2, :c3, :c4, :c5, :c6, :c7, 0) '''
                try:                                                
                    cur1.execute(query_insert, (pc[0], pc[1], int(pc[2]), int(pc[3]), pc[4], causale_ok, pc[6]))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[0], pc[1], pc[2], pc[3], pc[4], pc[5], pc[6] 
                    ))
                    logger.error(e)
                
                
                
            cur1.close()
            #con.commit()
    else: 
        logger.info('''Non c'è nessun aggiornamento della tabella EKOVISION_CONSUNT_DOPP_PERC_SP''')
    
    cur.close()
    cur = con.cursor()
    select_correzioni='''SELECT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA,
        COD_TRATTO, POSIZIONE,
        CAUSALI, CAUSALE_OK,
        LAST_FILE_RECEIVED 
        FROM UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_SP 
        WHERE CORRETTO = 0'''
        
    # cerco macro tappa da correggere
    
    select_MT_correggere='''SELECT ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA, 
    cmt.ID_ASTA,
    cmt2.ID_TAPPA, cmt2.QTA_SPAZZATA,
    cmt2.CAUSALE_SPAZZ,  
    LISTAGG(
    concat(concat(ec.COD_TRATTO, '_'), ec.posizione), ' '
    ) WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA) AS tratti_corretti,
    LISTAGG(ec.CAUSALE_OK, ' ') WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA)
    causali, 
    CASE 
        WHEN count(DISTINCT causale_ok) = 1 THEN max(causale_ok)
        ELSE NULL
    END AS causale_ok
    FROM UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_SP  ec
    JOIN CONS_MACRO_TAPPA cmt 
    ON cmt.ID_ASTA  =  ec.COD_TRATTO--CAST(ec.COD_TRATTO AS varchar(6))
    AND cmt.ID_MACRO_TAPPA in
    (
        SELECT ID_TAPPA FROM CONS_PERCORSI_VIE_TAPPE cpvt 
        JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
        WHERE ID_PERCORSO = ec.CODICE_SERV_PRED AND DATA_PREVISTA = 
        (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
        AND DATA_PREVISTA <= to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD')
        )
    )
    JOIN CONSUNT_SPAZZAMENTO cmt2 
    ON cmt2.ID_TAPPA = cmt.ID_MACRO_TAPPA 
    AND DATA_CONS = to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD') 
    WHERE CORRETTO = 0 /*AND causale_elem NOT IN (100, 110)*/
    GROUP BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA,
    cmt2.QTA_SPAZZATA,
    cmt2.CAUSALE_SPAZZ, cmt2.ID_TAPPA, cmt.id_asta'''
    
    
    
    try:                                                
        cur.execute(select_MT_correggere, ())
        tappe_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_MT_correggere)
        logger.error(e)
        
    if len(tappe_correggere)> 1:
        logger.info('Inizio correzione tappe')
        update_consunt_spazzamento='''UPDATE UNIOPE.CONSUNT_SPAZZAMENTO
            set QTA_SPAZZATA = :c1, 
            CAUSALE_SPAZZ = 100
            WHERE ID_TAPPA=:c2
            AND DATA_CONS = to_date(:c3, 'YYYYMMDD')''' 
            
            
        for tc in tappe_correggere:
            if tc[8]==100:
                logger.debug('Correggo percorso {} data {} - piazzola {}'.format(tc[0], tc[1], tc[2]))
                check=0
                try:                                                
                    cur.execute(update_consunt_spazzamento, (tc[4], tc[3], tc[1]))
                except Exception as e:
                    logger.error(update_consunt_spazzamento)
                    logger.error('1: {}, 2: {}'.format(tc[3], tc[1]))
                    logger.error(e)
                    check=1
                if check==0:
                    componenti_corrette=tc[6].split()

                    for cc in componenti_corrette:
                        update_correzioni=''' UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_PERC_SP
                        SET CORRETTO = 1 
                        WHERE CODICE_SERV_PRED = :c1 AND 
                        DATA_ESECUZIONE_PREVISTA = :c2 AND 
                        COD_TRATTO = :c3 AND 
                        POSIZIONE = :c4 
                        '''
                    try:                                                
                        cur.execute(update_correzioni, (tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                    except Exception as e:
                        logger.error(update_correzioni)
                        logger.error('1: {}, 2: {}'.format(tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                        logger.error(e)
                        check=1    
            else: 
                logger.info('''Non c'è nulla da correggere''')
            con.commit()
    
    
    
    
    
    cur.close()
    cur = con.cursor()
    cur1 = con.cursor()
    cur2 = con.cursor()
    
    
    # aggiornamento tabella EKOVISION_CONSUNT_DOPP_PERC_SP
    
    
    
    # GESTIONE ERRORI EKOVISION (tappe doppie non per scheda doppia)
    # aggiornamento tabella  EKOVISION_CONSUNT_DOPP_TAPP_SP
    select_query= '''SELECT  cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_TRATTO, cer.POSIZIONE,
LISTAGG(causale, ' ') WITHIN GROUP (ORDER BY cer.ID_SCHEDA) AS CAUSALI,
/*CASE   
	WHEN TOTEM = 1  THEN causale
	ELSE NULL
END AS causale_OK, */
max(see.NOMEFILE) AS LAST_FILE_RECEIVED, 
count(DISTINCT totem) AS TIPI_CONSUNTIVAZIONE, 
0 AS corretto
FROM
/*(
	SELECT DISTINCT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE, CAUSALE
	 FROM  CONSUNT_EKOVISION_RACCOLTA cer
	 WHERE RECORD_VALIDO = 'S'
) */
CONSUNT_EKOVISION_SPAZZAMENTO cer
JOIN SCHEDE_ESEGUITE_EKOVISION see ON see.iD_SCHEDA=cer.iD_SCHEDA AND see.RECORD_VALIDO = 'S'
WHERE cer.RECORD_VALIDO = 'S'
AND concat(cer.CODICE_SERV_PRED,cer.DATA_ESECUZIONE_PREVISTA)  IN (
	SELECT concat(CODICE_SERV_PRED,DATA_ESECUZIONE_PREVISTA)  FROM SCHEDE_ESEGUITE_EKOVISION see1 
	WHERE see1.NOMEFILE > 
		(SELECT max(LAST_FILE_RECEIVED) FROM EKOVISION_CONSUNT_DOPP_TAPP_SP ecdt)
	)
GROUP BY cer.CODICE_SERV_PRED, cer.DATA_ESECUZIONE_PREVISTA, cer.COD_TRATTO, cer.POSIZIONE
HAVING COUNT(DISTINCT causale)>1 AND count(DISTINCT cer.id_scheda)=1
ORDER BY  CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE'''

    #logger.info(select_query)
    try:                                                
        cur.execute(select_query)
        percorsi_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_query)
        logger.error(e)
        
    if len(percorsi_correggere)> 1:
        logger.info('Aggiono la tabella EKOVISION_CONSUNT_DOPP_TAPP_SP')
        for pc in percorsi_correggere:
            select_query1='''SELECT * FROM EKOVISION_CONSUNT_DOPP_TAPP_SP
            WHERE CODICE_SERV_PRED = :c1 AND 
            DATA_ESECUZIONE_PREVISTA = :c2 AND 
            COD_TRATTO = :c3 AND 
            POSIZIONE = :c4 '''
            cur1 = con.cursor()
            try:                                                
                cur1.execute(select_query1, (pc[0], pc[1], int(pc[2]), int(pc[3])))
                check_perc=cur1.fetchall()
            except Exception as e:
                logger.error(select_query1)
                logger.error(e)

            cur1.close()
            
            cur1 = con.cursor()
            if len(check_perc) > 0:
                # update
                query_update='''
                UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_SP 
                SET CAUSALI= :c1, 
                LAST_FILE_RECEIVED= :c2, 
                CORRETTO=0
                WHERE CODICE_SERV_PRED = :c3 AND 
                DATA_ESECUZIONE_PREVISTA = :c4 AND 
                COD_TRATTO = :c5 AND 
                POSIZIONE = :c6
                '''
                try:                                                
                    cur1.execute(query_update, (pc[4], pc[5], pc[0], pc[1], int(pc[2]), int(pc[3])))
                except Exception as e:
                    logger.error(query_update)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[4], pc[5], pc[6], pc[0], pc[1], pc[2], pc[3]  
                    ))
                    logger.error(e)
                
                
            else: 
                # insert
                query_insert='''INSERT INTO UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_SP 
                (CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, COD_TRATTO, POSIZIONE,
                CAUSALI,  LAST_FILE_RECEIVED, CORRETTO) 
                VALUES
                (:c1, :c2, :c3, :c4, :c5, :c6,  0) '''
                try:                                                
                    cur1.execute(query_insert, (pc[0], pc[1], int(pc[2]), int(pc[3]), pc[4], pc[5]))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'. format(
                        pc[0], pc[1], pc[2], pc[3], pc[4], pc[5], pc[6] 
                    ))
                    logger.error(e)
                
                
                
            cur1.close()
            con.commit()
    else: 
        logger.info('''Non c'è nessun aggiornamento della tabella EKOVISION_CONSUNT_DOPP_TAPP_SP''')
    
    cur.close()
    cur = con.cursor()

    
    


        
    # cerco macro tappa da correggere
    
    select_MT_correggere='''SELECT ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA, 
cmt.ID_ASTA,
cmt2.ID_TAPPA, 
cmt2.QTA_SPAZZATA,
cmt2.CAUSALE_SPAZZ,  
ec.causali,
trim(REPLACE(REPLACE(ec.causali, '100', ''), '110', '')) AS causale_ok,
sum(
CASE 
	WHEN trim(REPLACE(REPLACE(ec.causali, '100', ''), '110', '')) IS NOT NULL THEN 0
	ELSE 100
END
) AS QTA_SPAZZ_OK,
LISTAGG(
concat(concat(ec.cod_tratto, '_'), ec.posizione), ' '
) WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA) AS TRATTI_corretti
/*,*/
/*LISTAGG(ec.CAUSALE_OK, ' ') WITHIN GROUP (ORDER BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA)
causali, */
/*CASE 
	WHEN count(DISTINCT causale_ok) = 1 THEN max(causale_ok)
	ELSE NULL
END AS causale_ok*/
FROM UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_SP  ec
JOIN CONS_MACRO_TAPPA cmt 
ON cmt.ID_ASTA  =  ec.COD_TRATTO
AND cmt.ID_MACRO_TAPPA in
(
	SELECT ID_TAPPA FROM CONS_PERCORSI_VIE_TAPPE cpvt 
	JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
	WHERE ID_PERCORSO = ec.CODICE_SERV_PRED AND DATA_PREVISTA = 
	(SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
	AND DATA_PREVISTA <= to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD')
	)
)
JOIN CONSUNT_SPAZZAMENTO cmt2 
ON cmt2.ID_TAPPA = cmt.ID_MACRO_TAPPA 
AND DATA_CONS = to_date(ec.DATA_ESECUZIONE_PREVISTA , 'YYYYMMDD') 
WHERE CORRETTO = 0 /*AND causale_elem  IN (100, 110)*/
GROUP BY ec.CODICE_SERV_PRED, ec.DATA_ESECUZIONE_PREVISTA, ec.CAUSALI,
cmt2.QTA_SPAZZATA, cmt2.CAUSALE_SPAZZ, cmt2.ID_TAPPA, cmt.ID_ASTA'''
    
    
    
    
    
    
    
    try:                                                
        cur.execute(select_MT_correggere, ())
        tappe_correggere=cur.fetchall()
    except Exception as e:
        logger.error(select_MT_correggere)
        logger.error(e)
        
    if len(tappe_correggere)> 1:
        logger.info('Inizio correzione tappe')
        update_consunt_macro_tappe='''UPDATE UNIOPE.CONSUNT_SPAZZAMENTO
            set QTA_SPAZZATA = :c1, 
            CAUSALE_SPAZZ = :c2
            WHERE ID_TAPPA=:c3
            AND DATA_CONS = to_date(:c4, 'YYYYMMDD')''' 
            
            
        for tc in tappe_correggere:
            if tc[7] is None:
                logger.info('''Non c'è nulla da correggere''')
            else:
                logger.debug('Correggo percorso {} data {} - tratto {}'.format(tc[0], tc[1], tc[2]))
                check=0
                try:                                                
                    cur1.execute(update_consunt_macro_tappe, (int(tc[8]), int(tc[7]), tc[3], tc[1]))
                except Exception as e:
                    logger.error(update_consunt_macro_tappe)
                    logger.error('1: {}, 2: {}, 3:{}, 4:{}'.format(tc[8], tc[7], tc[3], tc[1]))
                    logger.error(e)
                    check=1
                if check==0:
                    componenti_corrette=tc[9].split()

                    for cc in componenti_corrette:
                        update_correzioni=''' UPDATE UNIOPE.EKOVISION_CONSUNT_DOPP_TAPP_SP
                        SET CORRETTO = 1 
                        WHERE CODICE_SERV_PRED = :c1 AND 
                        DATA_ESECUZIONE_PREVISTA = :c2 AND 
                        COD_TRATTO = :c3 AND 
                        POSIZIONE = :c4 
                        '''
                    try:                                                
                        cur2.execute(update_correzioni, (tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                    except Exception as e:
                        logger.error(update_correzioni)
                        logger.error('1: {}, 2: {}'.format(tc[0], tc[1], cc.split('_')[0], cc.split('_')[1]))
                        logger.error(e)
                        check=1 
            con.commit()
    
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    
    cur.close()
    cur1.close()
    cur2.close()
    con.close()




if __name__ == "__main__":
    main()      