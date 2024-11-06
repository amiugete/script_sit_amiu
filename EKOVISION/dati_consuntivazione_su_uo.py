#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) scarico dati da SFTP Ekovision

2) processo il file json

3) se processo OK lo copio in spazio archiviazione


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
    
    
    
    cartella_sftp_eko='sch_lav_cons/out/'    
    logger.info('Leggo e scarico file SFTP da cartella {}'.format(cartella_sftp_eko))
    


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
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd(cartella_sftp_eko): #chdir to public
            #print(srv.listdir('./'))
            for filename in srv.listdir('./'):
                #logger.debug(filename)
                if fnmatch.fnmatch(filename, "sch_lav_consuntivi*"):
                    srv.get(filename, path + "/eko_output/" + filename)
                    logger.info('Scaricato file {}'.format(filename))
                    
                    
                    
                    logger.info ('Inizio processo file'.format(filename))   
                    
                    # imposto a 0 un controllo sulla lettura del file
                    check_lettura=0
                    
                    # Opening JSON file
                    f = open(path + "/eko_output/" + filename)
                    
                    # returns JSON object as 
                    # a dictionary
                    try:
                        data = json.load(f)
                     
                        
                        
                        i=0
                        while i<len(data):
                            try:
                                logger.info('{} - Leggo dati della scheda di lavoro {}'.format(i, data[i]['id_scheda']))
                                                    
                                
                                if data[i]['data_esecuzione_prevista']>=data_start_ekovision:
                                    ''' devo leggere quello che c'è in
                                    -   cons_conferimenti 
                                            --> pesi percorsi
                                    -   cons_ris_tecniche
                                    -   cons_ris_umane
                                            --> hist_servizi
                                    -   cons_works
                                            tipo_rec - TRATTI STRADALI   
                                            --> 
                                    '''
                                    
                                    # leggo la "testata" e salvo i dati della tabella 
                                    
                                    select_scheda='''SELECT ID_SCHEDA, NOMEFILE FROM SCHEDE_ESEGUITE_EKOVISION see 
                                        WHERE ID_SCHEDA =:i1'''
                                        
                                    
                                    try:
                                        cur.execute(select_scheda, (data[i]['id_scheda'],))
                                        schede_eseguite=cur.fetchall()
                                    except Exception as e:
                                        logger.error(select_scheda)
                                        logger.error(e)
                                    
                                    if data[i]['cod_caus_srv_non_eseg_ext']=='':
                                        causale_non_es=None
                                    else:
                                        try:
                                            causale_non_es=int(data[i]['cod_caus_srv_non_eseg_ext'])
                                        except Exception as e:
                                            logger.error('PROBLEMA INSERIMENTO CAUSALE NON ESEGUITO')
                                            logger.error(e)
                                    
                                    
                                            
                                    check_update=0       
                                    check_scheda=0
                                                                        
                                    if len(schede_eseguite)>=1:
                                        check_scheda=1
                                        #update 
                                        """update_schede='''UPDATE UNIOPE.SCHEDE_ESEGUITE_EKOVISION 
                                        SET DATA_ESECUZIONE_PREVISTA=:s1, CODICE_SERV_PRED=:s2,
                                        COD_CAUS_SRV_NON_ESEG_EXT=:s3, COD_CAUS_SRV_NON_COMPL_EXT=:s4, 
                                        FLG_SEGN_SRV_NON_EFFETT=:s5, FLG_SEGN_SRV_NON_COMPL=:s6
                                        WHERE ID_SCHEDA=:s7'''
                                        try:
                                            cur.execute(update_schede, (
                                                data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                causale_non_es, data[i]['cod_caus_srv_non_eseg_ext'],  
                                                int(data[i]['flg_segn_srv_non_effett']), int(data[i]['flg_segn_srv_non_compl']), 
                                                data[i]['id_scheda']
                                                ))
                                        except Exception as e:
                                            logger.error(update_schede)
                                            logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}'.format(
                                                data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                causale_non_es, data[i]['cod_caus_srv_non_compl_ext'], 
                                                int(data[i]['flg_segn_srv_non_effett']), int(data[i]['flg_segn_srv_non_compl'])
                                                ,data[i]['id_scheda']
                                            ))
                                            logger.error(e)
                                            check_lettura+=1"""
                                        
                                        
                                        for ff in schede_eseguite:
                                            if ff[1]<filename:
                                                check_update=1
                                        
                                        if check_update == 1:    
                                            update_schede='''UPDATE UNIOPE.SCHEDE_ESEGUITE_EKOVISION 
                                            SET RECORD_VALIDO='N'
                                            WHERE ID_SCHEDA=:s1'''
                                            try:
                                                cur.execute(update_schede, (
                                                        data[i]['id_scheda'],
                                                    ))
                                            except Exception as e:
                                                logger.error(update_schede)
                                                logger.error('1:{}'.format(
                                                data[i]['id_scheda']
                                                ))
                                                logger.error(e)
                                                check_lettura+=1
                                    #else:
                                    # in qualunque caso faccio gli insert
                                    
                                    insert_schede='''INSERT INTO UNIOPE.SCHEDE_ESEGUITE_EKOVISION 
                                        (ID_SCHEDA, DATA_PIANIF_INIZIALE, DATA_ESECUZIONE_PREVISTA, CODICE_SERV_PRED,
                                        COD_CAUS_SRV_NON_ESEG_EXT, COD_CAUS_SRV_NON_COMPL_EXT, 
                                        FLG_SEGN_SRV_NON_EFFETT, FLG_SEGN_SRV_NON_COMPL,
                                        NOMEFILE, NR_RIGA, RECORD_VALIDO
                                        ) 
                                        VALUES
                                        (:s1, :s2, :s3, :s4, :s5, :s6, :s7, :s8, :s9,
                                        (select (max(NR_RIGA)+1) from UNIOPE.SCHEDE_ESEGUITE_EKOVISION),
                                        :s10)'''
                                    if check_update == 0 and check_scheda == 1: 
                                        rvalido='N'
                                    else:
                                        rvalido='S'
                    
                                    try:
                                        cur.execute(insert_schede, (
                                            data[i]['id_scheda'], data[i]['data_pianif_iniziale'],
                                            data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                            causale_non_es, data[i]['cod_caus_srv_non_compl_ext'], 
                                            int(data[i]['flg_segn_srv_non_effett']), int(data[i]['flg_segn_srv_non_compl']),
                                            filename, rvalido
                                            ))
                                    except Exception as e:
                                        logger.error(insert_schede)
                                        logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}, 8:{}, 9:{}, 10:{}'.format(
                                            data[i]['id_scheda'], data[i]['data_pianif_iniziale'],
                                            data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                            causale_non_es, data[i]['cod_caus_srv_non_compl_ext'], 
                                            int(data[i]['flg_segn_srv_non_effett']), int(data[i]['flg_segn_srv_non_compl']), 
                                            filename, rvalido
                                        ))
                                        logger.error(e)
                                        check_lettura+=1
                                    
                                    
                                    # popolamento hist_servizi
                                    
                                    # STEP 0 mi prendo id_ser_per_uo
                                    query0='''SELECT ID_SER_PER_UO , ID_TURNO, ID_UO, ID_SERVIZIO 
                                    FROM ANAGR_SER_PER_UO aspu WHERE ID_PERCORSO LIKE :c1
                                    AND to_date(:c2, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE '''
                                    
                                    
                                    
                                    try:
                                        cur.execute(query0, (data[i]['codice_serv_pred'], data[i]['data_esecuzione_prevista']))
                                        ii_ss=cur.fetchall()
                                    except Exception as e:
                                        logger.error(query0)
                                        logger.error(e)
                                        check_lettura+=1                                            

                                    id_rimessa=''
                                    id_ut=''
                                    for ispu in ii_ss:
                                        id_ser_per_uo=ispu[0]
                                        id_turno=ispu[1]
                                        id_servizio=ispu[3]
                                        if int(ispu[2])==16 or int(ispu[2])==17:
                                            id_rimessa=ispu[2]
                                        else:
                                            id_ut=ispu[2]
                                    
                                    
                                    cur.close()
                                    cur = con.cursor()
                                    
                                    # pulisco la HIST_SERVIZI se ci fosse già qualcosa
                                    query_select='''SELECT * FROM HIST_SERVIZI hs 
                                            WHERE DTA_SERVIZIO = to_date(:h1,'YYYYMMDD')
                                            AND ID_SER_PER_UO = :h2'''
                                            
                                            
                                            
                                    try:
                                        #cur.execute(query_select, (data[i]['data_esecuzione_prevista'], idpersona, id_ser_per_uo)
                                        cur.execute(query_select, (data[i]['data_esecuzione_prevista'], id_ser_per_uo))
                                        #cur1.rowfactory = makeDictFactory(cur1)
                                        persone_su_uo=cur.fetchall()
                                    except Exception as e:
                                        logger.error(query_select)
                                        logger.error(e)
                                    
                                    
                                    if (len(persone_su_uo)>0):
                                        """query_update='''UPDATE UNIOPE.HIST_SERVIZI 
                                        SET ID_UO_LAVORO=:h1, DURATA=:h2, ID_TURNO=:h3,
                                        SPORTELLO=:h4
                                        WHERE DTA_SERVIZIO=to_date(:h5,'YYYYMMDD') AND 
                                        ID_SER_PER_UO=:h6 AND 
                                        ID_PERSONA=:h7'''
                                        """
                                        query_delete='''DELETE FROM UNIOPE.HIST_SERVIZI 
                                        WHERE DTA_SERVIZIO=to_date(:h1,'YYYYMMDD') AND 
                                        ID_SER_PER_UO=:h2 
                                        '''
                                        try:
                                            """cur.execute(query_update, (id_ut_ok, durata, id_turno, sportello,
                                                                    data[i]['data_esecuzione_prevista'], 
                                                                    id_ser_per_uo, idpersona)
                                                        )"""
                                            cur.execute(query_delete, (data[i]['data_esecuzione_prevista'], id_ser_per_uo))
                                        except Exception as e:
                                            """logger.error(query_update)
                                            logger.error('1:{}, 2:{}, 3{}, 4:{}, 5:{}, 6:{}, 7:{}'.format(id_ut_ok, durata, id_turno, sportello,
                                                                    data[i]['data_esecuzione_prevista'], 
                                                                    id_ser_per_uo, idpersona))
                                            """
                                            logger.error(query_delete)
                                            logger.error('1:{}, 2:{}'.format(data[i]['data_esecuzione_prevista'], id_ser_per_uo))
                                            logger.error(e)
                                    
                                    
                                    
                                    cur.close()
                                    cur = con.cursor()
                                    
                                    if data[i]['cod_caus_srv_non_eseg_ext']!='' and len(data[i]['cons_works'])>0:
                                
                                        # cerco se raccolta o spazzamento o altro e salvo il risultato nella variabile tipo_percorso
                                        
                                        
                                        query_tipo= ''' SELECT GETTIPOPERCORSO(:cod_perc, TO_DATE (:data1, 'YYYYMMDD')) FROM DUAL'''
                                        try:
                                            cur.execute(query_tipo, (data[i]['codice_serv_pred'], data[i]['data_esecuzione_prevista']))
                                            tt_pp=cur.fetchall()
                                        except Exception as e:
                                            logger.error(query_tipo)
                                            logger.error(e)                                            


                                        
                                        for t_p in tt_pp:
                                            tipo_percorso=t_p[0]
                                        
                                        cur.close() 
                                        cur = con.cursor()
                                        
                                        
                                        if tipo_percorso=='R':
                                            # verifico che non ci sia già qualche altra consuntivazione
                                            query_select='''SELECT DISTINCT cmt.ID_MACRO_TAPPA,
                                                QTA_ELEM_NON_VUOTATI,
                                                CAUSALE_ELEM,
                                                NOTA, 
                                                DATA_CONS
                                                FROM CONSUNT_MACRO_TAPPA cmt 
                                                WHERE cmt.ID_MACRO_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                    :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione*/
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD')
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0*/
                                                )
                                                AND DATA_CONS = to_date(:dataperc, 'YYYYMMDD')
                                                )'''
                                                                                    
                                            try:
                                                cur.execute(query_select, (data[i]['codice_serv_pred'],
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        data[i]['data_esecuzione_prevista']
                                                                        ))
                                                cp=cur.fetchall()
                                            except Exception as e:
                                                logger.error(query_select)
                                                logger.error(e) 
                                            if len(cp)==0:
                                                query_insert='''INSERT INTO UNIOPE.CONSUNT_MACRO_TAPPA
                                                (ID_MACRO_TAPPA, QTA_ELEM_NON_VUOTATI, CAUSALE_ELEM, NOTA, DATA_CONS, ID_PERCORSO, ID_VIA, TIPO_ELEMENTO, ID_SERVIZIO, INS_DATE, MOD_DATE, ORIGINE_DATO)
                                                /* costruisco la data entry con causale che voglio
                                                --102 PERCORSO NON PREVISTO
                                                -- 83 PERCORSO ESEGUITO IN ALTRA DATA */
                                                SELECT DISTINCT cmt.ID_MACRO_TAPPA,
                                                /*----------------------------------------------------------------
                                                -- gli elementi non vuotati dovrebbero essere > 0 e allora bisognerevve fare un count*/
                                                0 AS QTA_ELEM_NON_VUOTATI,
                                                /*----------------------------------------------------------------*/
                                                :causale AS CAUSALE_ELEM,
                                                NULL AS NOTA, 
                                                /*----------------------------------------------------------------
                                                --data consuntivazione */
                                                to_date(:dataperc, 'YYYYMMDD') AS DATA_CONS,
                                                /*----------------------------------------------------------------*/
                                                (SELECT ID_PERCORSO  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cmt.ID_MACRO_TAPPA) AS ID_PERCORSO,
                                                (SELECT ID_VIA  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cmt.ID_MACRO_TAPPA) AS ID_VIA, 
                                                ce.tipo_elemento, 
                                                (SELECT DISTINCT ID_SERVIZIO FROM ANAGR_SER_PER_UO 
                                                WHERE ID_PERCORSO = (SELECT ID_PERCORSO  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cmt.ID_MACRO_TAPPA)
                                                AND DTA_ATTIVAZIONE <= (SELECT DATA_PREVISTA FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cmt.ID_MACRO_TAPPA)
                                                AND DTA_DISATTIVAZIONE > (SELECT DATA_PREVISTA FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cmt.ID_MACRO_TAPPA)
                                                AND id_servizio NOT IN (9)) 
                                                AS ID_SERVIZIO,
                                                /*--1 AS ID_SERVIZIO, */
                                                SYSDATE AS INS_DATE, 
                                                NULL AS MOD_DATE,
                                                'Ekovision non eseguito'
                                                FROM CONS_MACRO_TAPPA cmt 
                                                JOIN CONS_MICRO_TAPPA cmt2 ON cmt2.ID_MACRO_TAPPA = cmt.ID_MACRO_TAPPA
                                                /*--JOIN ASTE_INFO_DA_SIT aids ON aids.ID_ASTA = cmt.ID_ASTA*/
                                                JOIN CONS_ELEMENTI ce ON ce.ID_ELEMENTO=cmt2.ID_ELEMENTO
                                                WHERE cmt.ID_MACRO_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione */
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD'))
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0 */
                                                )
                                                GROUP BY cmt.ID_MACRO_TAPPA, 
                                                ce.tipo_elemento'''
                                                try:
                                                    '''cur.execute(query_insert, (int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred'], 
                                                                            data[i]['data_esecuzione_prevista'],
                                                                            data[i]['data_esecuzione_prevista']))'''
                                                    cur.execute(query_insert, (int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred'], 
                                                                            data[i]['data_esecuzione_prevista']))
                                                    
                                                except Exception as e:
                                                    logger.error(query_insert)
                                                    logger.error('causale:{} data:{} percorso:{}'.format(int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred']))
                                                    logger.error(e)                                          
                                            
                                            else:
                                                update_query='''UPDATE CONSUNT_MACRO_TAPPA cmt 
                                                SET 
                                                QTA_ELEM_NON_VUOTATI = 0,
                                                CAUSALE_ELEM= :causale
                                                WHERE cmt.ID_MACRO_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                    :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione*/
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD')
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0*/
                                                )
                                                AND DATA_CONS = to_date(:dataperc, 'YYYYMMDD')
                                                )'''
                                                                                    
                                                try:
                                                    cur.execute(update_query, (int(data[i]['cod_caus_srv_non_eseg_ext']), 
                                                                            data[i]['codice_serv_pred'],
                                                                            data[i]['data_esecuzione_prevista'],
                                                                            data[i]['data_esecuzione_prevista']
                                                                            ))
                                                except Exception as e:
                                                    logger.error(update_query)
                                                    logger.error('causale:{} data:{} percorso:{}'.format(
                                                                            int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred']))
                                                    logger.error(e)
                                            
                                            
                                            
                                        elif tipo_percorso=='S':
                                            # verifico che non ci sia già qualche altra consuntivazione
                                            query_select='''SELECT *
                                                FROM CONSUNT_SPAZZAMENTO cs  
                                                WHERE cs.ID_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                    :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione*/
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD')
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0*/
                                                )
                                                AND DATA_CONS = to_date(:dataperc, 'YYYYMMDD')
                                                )'''
                                                                                    
                                            try:
                                                cur.execute(query_select, (data[i]['codice_serv_pred'],
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        data[i]['data_esecuzione_prevista']  ))
                                                cp=cur.fetchall()
                                            except Exception as e:
                                                logger.error(query_select)
                                                logger.error(e) 
                                            if len(cp)==0:
                                                query_insert='''INSERT INTO UNIOPE.CONSUNT_SPAZZAMENTO 
                                                (ID_TAPPA, QTA_SPAZZATA, CAUSALE_SPAZZ, NOTA, DATA_CONS, ID_PERCORSO, ID_VIA, ID_SERVIZIO, DATA_INS, ORIGINE_DATO)
                                                /* inserisco i dati  */
                                                SELECT DISTINCT cs.ID_TAPPA,
                                                0 AS QTA_SPAZZATA,
                                                :causale AS CAUSALE_SPAZZ,
                                                NULL AS NOTA, 
                                                to_date(:dataperc, 'YYYYMMDD') AS DATA_CONS,
                                                /*----------------------------------------------------------------*/
                                                (SELECT ID_PERCORSO  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cs.ID_TAPPA) AS ID_PERCORSO,
                                                (SELECT ID_VIA  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cs.ID_TAPPA) AS ID_VIA, 
                                                (SELECT DISTINCT ID_SERVIZIO FROM ANAGR_SER_PER_UO 
                                                WHERE ID_PERCORSO = (SELECT ID_PERCORSO  FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cs.ID_TAPPA)
                                                AND DTA_ATTIVAZIONE <= (SELECT DATA_PREVISTA FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cs.ID_TAPPA)
                                                AND DTA_DISATTIVAZIONE > (SELECT DATA_PREVISTA FROM CONS_PERCORSI_VIE_TAPPE cpvt WHERE ID_TAPPA = cs.ID_TAPPA)
                                                AND id_servizio NOT IN (9)) 
                                                AS ID_SERVIZIO,
                                                SYSDATE AS INS_DATE, 
                                                'Ekovision'
                                                FROM CONSUNT_SPAZZAMENTO cs 
                                                WHERE cs.ID_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione*/
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD'))
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0*/
                                                )
                                                GROUP BY cs.ID_TAPPA'''
                                                try:
                                                    cur.execute(query_insert, (int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred'], 
                                                                            data[i]['data_esecuzione_prevista']))
                                                except Exception as e:
                                                    logger.error(query_insert)
                                                    logger.error('1:{}, 2:{}, 3{}, 4:{}, 5:{}'.format(int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred'], 
                                                                            data[i]['data_esecuzione_prevista'],
                                                                            data[i]['data_esecuzione_prevista'])) 
                                                    logger.error(e)
                                            
                                            else:
                                                update_query='''UPDATE UNIOPE.CONSUNT_SPAZZAMENTO cs  
                                                SET QTA_SPAZZATA=0,
                                                CAUSALE_SPAZZ =:causale
                                                WHERE cs.ID_TAPPA IN 
                                                (
                                                SELECT ID_TAPPA  FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA=cpvt.ID_TAPPA
                                                WHERE ID_PERCORSO IN (
                                                    :cod_percorso
                                                ) AND DATA_PREVISTA = 
                                                (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE cpvt1 WHERE  cpvt1.id_percorso =cpvt.ID_PERCORSO  
                                                /*----------------------------------------------------------------
                                                --data consuntivazione*/
                                                AND DATA_PREVISTA <= to_date(:dataperc, 'YYYYMMDD')
                                                /*AND (SELECT UNIOPE.ISDATEINFREQ(to_date(:dataperc, 'YYYYMMDD'), cmt.FREQUENZA) FROM dual)>0*/
                                                )
                                                AND DATA_CONS = to_date(:dataperc, 'YYYYMMDD')
                                                )'''
                                                                                    
                                                try:
                                                    cur.execute(update_query, (int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['codice_serv_pred'],
                                                                            data[i]['data_esecuzione_prevista'],
                                                                            data[i]['data_esecuzione_prevista']  ))
                                                except Exception as e:
                                                    logger.error(update_query)
                                                    logger.error('causale:{} data:{} percorso:{}'.format(int(data[i]['cod_caus_srv_non_eseg_ext']),
                                                                            data[i]['data_esecuzione_prevista'], 
                                                                            data[i]['codice_serv_pred']))
                                                    logger.error(e) 
                                                
                                                    
                                        else:
                                            logger.info('Tipo percorso senza tappe')
                                            logger.info('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                            logger.info('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))                                    
                                            logger.info('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                            #exit()
                                    
                                        
                                        
                                        con.commit()
                                        cur.close() 
                                        cur = con.cursor()
                                    
                                    
                                    # STEP 1 cerco lo sportello o gli sportelli
                                    
                                    sportello=''
                                    s=0
                                    while s<len(data[i]['cons_ris_tecniche']):
                                        if data[i]['cons_ris_tecniche'][s]['id_giustificativo']=='0':
                                            # con la funzione strip e usando lo spazio come separatore fra sportelli 
                                            # non dovrebbero servire condizioni che distinguano il primo sportello dagli altri
                                            sportello='{} {}'.format(sportello, (data[i]['cons_ris_tecniche'][s]['cod_matricola_ristec']).lstrip('0')).strip() 
                                            #logger.debug(sportello)
                                        s+=1
                                    
                                    
                                    
                                    # ciclo sulle persone 
                                    
                                    p=0
                                    while p<len(data[i]['cons_ris_umane']):
                                        if data[i]['cons_ris_umane'][p]['cod_dipendente'].strip()!='' and (id_rimessa!='' or id_ut != ''):
                                            # STEP 2 mi ricavo la persona, la durata e il turno (se disponibile)
                                            if id_rimessa!='' and data[i]['cons_ris_umane'][p]['id_mansione']==33:
                                                id_ut_ok=id_rimessa
                                            elif id_ut != '' and data[i]['cons_ris_umane'][p]['id_mansione']!=33 :
                                                id_ut_ok=id_ut
                                            elif id_ut=='' and id_rimessa!='':
                                                id_ut_ok=id_rimessa
                                            elif id_ut!='' and id_rimessa=='':
                                                id_ut_ok=id_ut
                                            else:
                                                logger.error('Problema con attribuzione UT')
                                                logger.error('Dipendente {}'.format(data[i]['cons_ris_umane'][p]['cod_dipendente']))
                                                logger.error('Mansione (id ekovision) {}'.format(data[i]['cons_ris_umane'][p]['id_mansione']))
                                                logger.error('Id ut {}'.format(id_ut))
                                                logger.error('Id rimessa {}'.format(id_rimessa))
                                                logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))                                    
                                                logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                                
                                            idpersona=data[i]['cons_ris_umane'][p]['cod_dipendente']
                                            durata = 0
                                            o=0
                                            while o<len(data[i]['cons_ris_umane'][p]['cons_risum_orari']):
                                                
                                                data_ora_start='{} {}'.format(
                                                    data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_ini'],
                                                    data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_ini']
                                                    )
                                                data_ora_fine='{} {}'.format(
                                                    data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_fine'],
                                                    data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_fine']
                                                    )
                                                
                                                fmt='%Y%m%d %H%M%S'
                                                data_ora_start_ok = datetime.strptime(data_ora_start, fmt)
                                                data_ora_fine_ok = datetime.strptime(data_ora_fine, fmt)
                                                # calcolo differenza in minuti ()
                                                durata+=(data_ora_fine_ok - data_ora_start_ok).total_seconds() / 60.0
                                                
                                                o+=1
                                        
                                        
                                            #logger.debug('{}, {}, {}, {}, {}'.format(id_ser_per_uo, data[i]['data_esecuzione_prevista'], sportello, idpersona, durata))
                                            
                                            #################################################
                                            
                                            # devo fare insert o update se trovo terna di id_ser_per_uo / data / idpersona
                                            
                                            #################################################
                                            
                                            """query_select='''SELECT * FROM HIST_SERVIZI hs 
                                            WHERE DTA_SERVIZIO = to_date(:h1,'YYYYMMDD')
                                            AND ID_PERSONA = :h2
                                            AND ID_SER_PER_UO = :h3'''
                                            """
                                            
                                            
                                             
                                                    
                                            #elif (len(persone_su_uo)==0):
                                            query_insert='''INSERT INTO UNIOPE.HIST_SERVIZI 
                                            (DTA_SERVIZIO, ID_SER_PER_UO, ID_PERSONA,
                                            PROG_SERVIZIO, ID_UO_LAVORO, DURATA,
                                            ID_TURNO, SPORTELLO) 
                                            VALUES(to_date(:h1,'YYYYMMDD'), :h2, :h3,
                                            1 , :h4, :h5,
                                            :h6, :h7)'''
                                            try:
                                                cur.execute(query_insert, (data[i]['data_esecuzione_prevista'], 
                                                                        id_ser_per_uo, idpersona,
                                                                        id_ut_ok, durata, 
                                                                        id_turno, sportello)
                                                            )
                                            except Exception as e:
                                                logger.error(query_insert)
                                                logger.error('1:{}, 2:{}, 3{}, 4:{}, 5:{}, 6:{}, 7:{}'.format(data[i]['data_esecuzione_prevista'], 
                                                                        id_ser_per_uo, idpersona,
                                                                        id_ut_ok, durata, 
                                                                        id_turno, sportello))
                                                logger.error(e)
                                                
                                            """else:
                                                logger.error('In HIST_SERVIZI ci sono troppe righe')
                                                logger.error('Dipendente {}'.format(data[i]['cons_ris_umane'][p]['cod_dipendente']))
                                                logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                            """    
                                        else: 
                                            logger.info("Nessuna persona caricata. Non processo il record")
                                        
                                        
                                        
                                        p+=1
                                    con.commit()
                                    cur.close()
                                    cur = con.cursor()
                                    
                                    # popolamento pesi
                                    c=0 # conferimenti
                                    while c<len(data[i]['cons_conferimenti']):
                                        # con la funzione strip e usando lo spazio come separatore fra sportelli 
                                        # non dovrebbero servire condizioni che distinguano il primo sportello dagli altri
                                        logger.info('Ci sono dei conferimenti')
                                        data_percorso=data[i]['data_pianif_iniziale']
                                        if data[i]['cons_conferimenti'][c]['data_rilevazione'].strip('0')!='':
                                            data_conferimento=data[i]['cons_conferimenti'][c]['data_rilevazione']
                                        else:
                                            data_conferimento=data[i]['data_esecuzione_prevista']
                                        oc= data[i]['cons_conferimenti'][c]['ora_rilevazione']
                                        ora_conferimento=oc[:2] + ':'+ oc[2:2]+oc[4:]
                                        peso_netto=float(data[i]['cons_conferimenti'][c]['peso_netto'])
                                        peso_lordo=float(data[i]['cons_conferimenti'][c]['peso_lordo'])
                                        impianto=data[i]['cons_conferimenti'][c]['cod_sede_dest_ext'].split('_')
                                        imp_cod_ecos=impianto[0]
                                        uni_cod_ecos=impianto[1]
                                        #logger.debug('Conferimento {} -  {}, {}, {}, {}, {}, {}'.format(c,data_percorso, data_conferimento, ora_conferimento, imp_cod_ecos, uni_cod_ecos, peso_netto))  
                                        
                                        #exit()
                                        # devo vedere che non ci sia già un conferimento (registrato come PROVENIENZA = 'ECOS' e COD_PROTOCOLLO = 838) in tal caso non faccio niente 
                                        
                                        
                                        
                                        #altrimenti
                                        
                                        # ID_UO_TITOLARE, COD_CER, DESCR_RIFIUTO vanno in qualche modo recuperati
                                        
                                        # se il peso lordo è 0 vuol dire che il peso proviene da ECOS quindi non serve nemmeno provare a re-inserirlo (solo perdita di tempo)
                                        #logger.debug(peso_lordo)
                                        if peso_lordo > 0:
                                            
                                            select_query='''SELECT * FROM TB_PESI_PERCORSI tpp 
                                            WHERE PROVENIENZA = 'RIMESSA'
                                            AND DATA_PERCORSO = to_date(:c1, 'YYYYMMDD') 
                                            AND ID_SER_PER_UO = :c2
                                            AND NOTE = :c3'''
                                            
                                            
                                            try:
                                                cur.execute(select_query, (data[i]['data_esecuzione_prevista'], id_ser_per_uo,
                                                                    data[i]['cons_conferimenti'][c]['id'])
                                                        )
                                                #cur1.rowfactory = makeDictFactory(cur1)
                                                conferimenti_su_uo=cur.fetchall()
                                            except Exception as e:
                                                logger.error(select_query)
                                                logger.error('1:{}, 2:{}, 3{}'.format(data[i]['data_esecuzione_prevista'], id_ser_per_uo,
                                                                    data[i]['cons_conferimenti'][c]['id']))
                                                logger.error(e)
                                            
                                            
                                            if len(conferimenti_su_uo)==0:
                                                # nelle note ci metto l'ID
                                                """insert_query='''INSERT INTO UNIOPE.TB_PESI_PERCORSI (
                                                ID_SER_PER_UO, DATA_PERCORSO, 
                                                DATA_CONFERIMENTO, ORA_CONFERIMENTO,
                                                PESO, DESTINAZIONE, PROVENIENZA, INS_DATE, 
                                                ID_UO_TITOLARE, 
                                                COD_CER, DESCR_RIFIUTO, NOTE) 
                                                VALUES
                                                (:c1, to_date(:c2, 'YYYYMMDD'), 
                                                to_date(:c3, 'YYYYMMDD'), 
                                                :c4,
                                                :c5, 
                                                (SELECT ID_DESTINAZIONE 
                                                FROM ANAGR_DESTINAZIONI ad 
                                                WHERE IMP_COD_ECOS =:c6 
                                                AND UNI_COD_ECOS =:c7),'RIMESSA', sysdate,
                                                (SELECT ID_UT_TITOLARE FROM PERCORSI_UT_TITOLARE put
                                                WHERE ID_PERCORSO = :c8 AND 
                                                to_date(:c9, 'YYYYMMDD') BETWEEN DATA_INIZIO AND DATA_FINE),
                                                (SELECT as2.CER  
                                                FROM ANAGR_SERVIZI as2 
                                                JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                                WHERE ID_SERVIZIO =
                                                    (SELECT ID_SERVIZIO 
                                                    FROM ANAGR_SER_PER_UO 
                                                    WHERE ID_SER_PER_UO=:c10
                                                    )),
                                                (SELECT ac.DESCR_SEMPL  
                                                FROM ANAGR_SERVIZI as2 
                                                JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                                WHERE ID_SERVIZIO =
                                                    (SELECT ID_SERVIZIO 
                                                    FROM ANAGR_SER_PER_UO 
                                                    WHERE ID_SER_PER_UO=:c11
                                                    )),
                                                :c12)'''
                                                """
                                                
                                                # ho corretto il caso di UT TITOLARE DITTE TERZE
                                                insert_query='''INSERT INTO UNIOPE.TB_PESI_PERCORSI (
                                                ID_SER_PER_UO, DATA_PERCORSO, 
                                                DATA_CONFERIMENTO, ORA_CONFERIMENTO,
                                                PESO, DESTINAZIONE, PROVENIENZA, INS_DATE, 
                                                ID_UO_TITOLARE, 
                                                COD_CER, DESCR_RIFIUTO, NOTE) 
                                                VALUES
                                                (:c1, to_date(:c2, 'YYYYMMDD'), 
                                                to_date(:c3, 'YYYYMMDD'), 
                                                :c4,
                                                :c5, 
                                                (SELECT ID_DESTINAZIONE 
                                                FROM ANAGR_DESTINAZIONI ad 
                                                WHERE IMP_COD_ECOS =:c6 
                                                AND UNI_COD_ECOS =:c7),'RIMESSA', sysdate,
                                                (CASE 
                                                    WHEN 
                                                        (SELECT au.ID_ZONATERRITORIALE FROM ANAGR_SER_PER_UO aspu1 
                                                        JOIN anagr_uo au ON au.ID_UO =aspu1.id_Uo
                                                        WHERE ID_SER_PER_UO =  :c8 ) = 7
                                                    THEN 
                                                        (SELECT id_uo FROM anagr_ser_per_uo WHERE ID_SER_PER_UO = :c9 ) 
                                                ELSE 
                                                    (SELECT ID_UT_TITOLARE FROM PERCORSI_UT_TITOLARE put
                                                    WHERE ID_PERCORSO = :c10 AND 
                                                    to_date(:c11, 'YYYYMMDD') BETWEEN DATA_INIZIO AND DATA_FINE)
                                                END),
                                                (SELECT as2.CER  
                                                FROM ANAGR_SERVIZI as2 
                                                JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                                WHERE ID_SERVIZIO =
                                                    (SELECT ID_SERVIZIO 
                                                    FROM ANAGR_SER_PER_UO 
                                                    WHERE ID_SER_PER_UO=:c12
                                                    )),
                                                (SELECT ac.DESCR_SEMPL  
                                                FROM ANAGR_SERVIZI as2 
                                                JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                                WHERE ID_SERVIZIO =
                                                    (SELECT ID_SERVIZIO 
                                                    FROM ANAGR_SER_PER_UO 
                                                    WHERE ID_SER_PER_UO=:c13
                                                    )),
                                                :c14)'''
                                                
                                                
                                                
                                                try:
                                                    cur.execute(insert_query, (
                                                                        id_ser_per_uo,
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        data_conferimento,
                                                                        ora_conferimento,
                                                                        peso_netto,
                                                                        imp_cod_ecos,
                                                                        uni_cod_ecos,
                                                                        id_ser_per_uo,
                                                                        id_ser_per_uo,
                                                                        data[i]['codice_serv_pred'],
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        id_ser_per_uo,
                                                                        id_ser_per_uo,
                                                                        int(data[i]['cons_conferimenti'][c]['id']
                                                                        ))
                                                            )
                                                except Exception as e:
                                                    logger.error(insert_query)
                                                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}, 8:{}, 9:{}, 10{}, 11:{}, 12:{}, 13:{}, 14:{}'.format(
                                                                        id_ser_per_uo,
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        data_conferimento,
                                                                        ora_conferimento,
                                                                        peso_netto,
                                                                        imp_cod_ecos,
                                                                        uni_cod_ecos,
                                                                        id_ser_per_uo,
                                                                        id_ser_per_uo,
                                                                        data[i]['codice_serv_pred'],
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        id_ser_per_uo,
                                                                        id_ser_per_uo,
                                                                        data[i]['cons_conferimenti'][c]['id']))
                                                    logger.error(e)
                                                    exit()
                                                
                                            elif len(conferimenti_su_uo)==1:
                                                # da fare UPDATE
                                                update_query='''UPDATE UNIOPE.TB_PESI_PERCORSI 
                                                SET PESO=:c1, 
                                                DESTINAZIONE=(SELECT ID_DESTINAZIONE 
                                                FROM ANAGR_DESTINAZIONI ad 
                                                WHERE IMP_COD_ECOS =:c2 
                                                AND UNI_COD_ECOS =:c3),
                                                INS_DATE=sysdate
                                                WHERE PROVENIENZA = 'RIMESSA'
                                                AND DATA_PERCORSO = to_date(:c4, 'YYYYMMDD') 
                                                AND ID_SER_PER_UO = :c5
                                                AND NOTE = :c6'''
                                                try:
                                                    cur.execute(update_query, (
                                                                        peso_netto,
                                                                        imp_cod_ecos,
                                                                        uni_cod_ecos,
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        id_ser_per_uo,
                                                                        data[i]['cons_conferimenti'][c]['id'])
                                                            )
                                                except Exception as e:
                                                    logger.error(update_query)
                                                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}'.format(peso_netto,
                                                                        imp_cod_ecos,
                                                                        uni_cod_ecos,
                                                                        data[i]['data_esecuzione_prevista'],
                                                                        id_ser_per_uo,
                                                                        data[i]['cons_conferimenti'][c]['id']))
                                                    logger.error(e)
                                            else:
                                                logger.error('Ci sono troppi conferimenti con ID {}'.format(data[i]['cons_conferimenti'][c]['id']))
                                                logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                        else:
                                            logger.info('Peso proveniente da ECOS. Non lo processo')
                                        c+=1    
                                        
                                    
                                    
                                    
                                    con.commit()
                                    cur.close()
                                    cur = con.cursor()
                                    
                                    
                                    
                                    if len(data[i]['cons_works'])==0:
                                        logger.warning('Il percorso {0} in data_pianif_iniziale {1} (id_scheda = {2}) non ha nessuna tappa'.format(
                                            data[i]['codice_serv_pred'],
                                            data[i]['data_pianif_iniziale'],
                                            data[i]['id_scheda']))
                                    elif (data[i]['cod_caus_srv_non_eseg_ext']!=''):
                                        logger.info('Dati del percorso {0} in in data_pianif_iniziale {1} (id_scheda = {2}) già inseriti massivamente'.format(
                                            data[i]['codice_serv_pred'],
                                            data[i]['data_pianif_iniziale'],
                                            data[i]['id_scheda']
                                        ))
                                    elif (len(data[i]['cons_works'])>0 and data[i]['cod_caus_srv_non_eseg_ext']==''):
                                        # consuntivazione 
                                        t=0
                                        check_tappe_non_trovate=0
                                        check_tappe_multiple=0
                                        elenco_codici_via=[] # re-inizializzo ogni volta
                                        elenco_elementi=[] # re-inizializzo ogni volta
                                        elenco_piazzole=[]  # da usare per calcolo elementi non vuotati 
                                        elenco_tappe=[] # da usare per calcolo elementi non vuotati
                                        elenco_tipi=[] # da usare per calcolo elementi non vuotati
                                        elenco_num_elementi=[] 
                                        elenco_causali=[]
                                        #logger.debug('Ho inizializzato gli array. La lunghezza è {}'.format(len(elenco_tappe)))
                                        ripasso=0
                                        ripasso_sit=0
                                        while t<len(data[i]['cons_works']):
                                            #if int(data[i]['id_scheda'])==116601:
                                            #    logger.debug(t)      
                                            if data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                #logger.debug('Consuntivazione spazzamento')
                                                # SU SIT cerco info sul tratto
                                                
                                                #if int(data[i]['id_scheda'])==116601:
                                                #    logger.debug(int(data[i]['cons_works'][t]['cod_tratto'].strip()))
                                                #    logger.debug(int(data[i]['cons_works'][t]['pos']))
                                                    
                                                                                            
                                                if int(data[i]['cons_works'][t]['pos'])>0 and int(data[i]['cons_works'][t]['flg_non_previsto'])==0:
                                                    
                                                    if int(data[i]['cons_works'][t]['cod_tratto'].strip()) in elenco_codici_via:
                                                        ripasso_sit=elenco_codici_via.count(int(data[i]['cons_works'][t]['cod_tratto'].strip()))
                                                    else:
                                                        ripasso_sit=0
                                                    
                                                    elenco_codici_via.append(int(data[i]['cons_works'][t]['cod_tratto'].strip()))
                                                    
                                                    
                                                    # cerco i dati di quella tappa
                                                    # ho rimosso l'ordine che non dovrebbe servire anzi essere fuorviante
                                                    """select_sit_per_tappa='''select codice_modello_servizio, ordine,  a.id_via, at.nota, at.ripasso 
                                                from 
                                                    (SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti vpet 
                                                    union 
                                                    SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs vpeto) at
                                                join elem.aste a on a.id_asta = at.codice
                                                where codice_tipo_servizio = %s and codice_modello_servizio =  %s
                                                and codice = %s and ripasso = %s
                                                and (%s between data_inizio and coalesce(data_fine,'20991231'))
                                                and ordine=%s'''
                                                #la query è la stessa i dati sono diversi nei 2 casi
                                                    try:
                                                        curr.execute(select_sit_per_tappa, (data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                            data[i]['codice_serv_pred'],
                                                                                            int(data[i]['cons_works'][t]['cod_tratto']),
                                                                                            ripasso_sit,
                                                                                            data[i]['data_pianif_iniziale'],
                                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                                            ))
                                                    
                                                    """
                                                    
                                                    
                                                    select_sit_per_tappa='''select codice_modello_servizio, 
                                                    min(ordine) as ordine,  
                                                    a.id_via, at.nota, at.ripasso 
                                                from 
                                                    (
                                                    SELECT codice_modello_servizio, ordine, objecy_type, 
                                                codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                    FROM anagrafe_percorsi.v_percorsi_elementi_tratti 
                                                    where data_inizio < coalesce(data_fine, '20991231')
                                                    and codice_modello_servizio =  %s
                                                    union 
                                                    SELECT codice_modello_servizio, ordine, objecy_type, 
                                                codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                    FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs 
                                                    where data_inizio < coalesce(data_fine, '20991231')
                                                    and codice_modello_servizio =  %s
                                                    union 
                                                    SELECT codice_modello_servizio, ordine, objecy_type, 
                                                codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                    FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi 
                                                    where data_inizio < coalesce(data_fine, '20991231')
                                                    and codice_modello_servizio =  %s
                                                ) at
                                                join elem.aste a on a.id_asta = at.codice
                                                where codice_tipo_servizio = %s
                                                and codice = %s and ripasso = %s
                                                and (%s between data_inizio and coalesce((data_fine::int-1)::varchar,'20991231'))
                                                group by codice_modello_servizio, a.id_via, at.nota, at.ripasso'''
                                                #la query è la stessa i dati sono diversi nei 2 casi
                                                    try:
                                                        curr.execute(select_sit_per_tappa, (data[i]['codice_serv_pred'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                            int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                            ripasso_sit,
                                                                                            data[i]['data_pianif_iniziale']
                                                                                            ))
                                                        tappe=curr.fetchall()
                                                    except Exception as e:
                                                        logger.error(select_sit_per_tappa)
                                                        logger.error('{} {} {} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                            int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                            ripasso_sit,
                                                                                            data[i]['data_pianif_iniziale']
                                                                                            ))
                                                        logger.error(e)
                                                    
                                                    ct=0
                                                    for tt in tappe:
                                                        ordine=tt[1]
                                                        id_via=tt[2]
                                                        nota_via=tt[3]
                                                        #logger.debug('Ordine {} - Id_via {} - Nota {}'.format(tt[1],tt[2],tt[3]))
                                                        if ct>=1:
                                                            #check_tappe_multiple = 1
                                                            logger.error('Trovata più di una tappa')
                                                            logger.error(select_sit_per_tappa)
                                                            logger.error('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                            data[i]['codice_serv_pred'],
                                                                                            int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                            data[i]['data_pianif_iniziale'],
                                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                                            ))                                              
                                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                            #exit()                                       
                                                        ct+=1
                                                    
                                                    if ct == 0:
                                                        check_tappe_non_trovate=1
                                                        logger.error('Tappa non trovata su SIT')
                                                        logger.error(select_sit_per_tappa)
                                                        logger.error('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                            data[i]['codice_serv_pred'],
                                                                                            int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                            ripasso_sit,
                                                                                            data[i]['data_pianif_iniziale']
                                                                                            ))
                                                        #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                        #exit()    
                                                    
                                                    if nota_via is None:
                                                        nota_via='ND'
                                                    
                                                    query_id_tappa='''SELECT DISTINCT ID_TAPPA, DTA_IMPORT, DATA_PREVISTA 
                                                    FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                    JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA
                                                    WHERE ID_PERCORSO = :t1
                                                    AND ID_VIA = :t2
                                                    AND ID_ASTA = :t3
                                                    AND (NVL(trim(to_char(NOTA_VIA)),'ND') LIKE :t4) AND (CRONOLOGIA=:t5) 
                                                    and  DATA_PREVISTA = (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE 
                                                    WHERE DATA_PREVISTA <= to_date(:t6, 'YYYYMMDD') AND to_char(DATA_PREVISTA, 'HH24') = '00' AND
                                                    ID_PERCORSO = :t7) 
                                                    ORDER BY 1'''
                                

                                                    
                                                    try:
                                                        cur.execute(query_id_tappa, (data[i]['codice_serv_pred'],
                                                                                    id_via,
                                                                                    int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                    nota_via.strip(),
                                                                                    ordine, 
                                                                                    data[i]['data_pianif_iniziale'], 
                                                                                    data[i]['codice_serv_pred'])
                                                                    )
                                                        #cur1.rowfactory = makeDictFactory(cur1)
                                                        tappe_uo=cur.fetchall()
                                                    except Exception as e:
                                                        logger.error(query_id_tappa)
                                                        logger.error('1:{} 2:{} 3:{} 4:{} 5:{} 6:{} 7:{}'.format(data[i]['codice_serv_pred'],
                                                                                    id_via,
                                                                                    int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                    nota_via.strip(),
                                                                                    ordine, 
                                                                                    data[i]['data_pianif_iniziale'], 
                                                                                    data[i]['codice_serv_pred']
                                                        ))
                                                        logger.error(e)
                                                        exit()
                                                
                                                    ct=0
                                                    for ttu in tappe_uo:
                                                        #logger.debug(ttu[0])
                                                        id_tappa=ttu[0]
                                                        if ct>=1:
                                                            check_tappe_multiple = 1
                                                            logger.error('Trovata più di una tappa')
                                                            logger.error(query_id_tappa)
                                                            logger.error('{} {} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                    id_via,
                                                                                    int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                    nota_via, ordine,
                                                                                    data[i]['data_pianif_iniziale']))
                                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                            #exit()                                       
                                                        ct+=1
                                                        if ct == 0:
                                                            check_tappe_non_trovate=1
                                                            logger.warning('Tappa non trovata su UO')
                                                            logger.warning(query_id_tappa)
                                                            logger.warning('{} {} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                        id_via,
                                                                                        int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                                                        nota_via, ordine,
                                                                                        data[i]['data_pianif_iniziale']))
                                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                            #exit()
                                                        
                                                        else:     
                                                            
                                                            # da fare insert/update
                                                            if int(data[i]['cons_works'][t]['flg_exec'].strip())==1: #and int(data[i]['cons_works'][t]['cod_std_qualita'])==100:
                                                                causale=100
                                                            else:
                                                                if causale_non_es != None:
                                                                    causale=causale_non_es
                                                                else:
                                                                    try:
                                                                        causale=int(data[i]['cons_works'][t]['cod_giustificativo_ext'].strip())
                                                                    except Exception as e:
                                                                        logger.warning(e)
                                                                        logger.warning('Scheda {} - Posizione: {} Manca la causale quindi lo do per fatto'.format(
                                                                            int(data[i]['id_scheda']),
                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                        ))
                                                                        causale=100
                                                                    
                                                                    
                                                            nota_consuntivazione=''
                                                            
                                                            query_select=''' 
                                                            SELECT * 
                                                            FROM CONSUNT_SPAZZAMENTO cs 
                                                            WHERE DATA_CONS = to_date(:c1, 'YYYYMMDD')
                                                            and id_TAPPA= :c2
                                                            '''
                                                            
                                                            
                                                            try:
                                                                cur.execute(query_select, (data[i]['data_esecuzione_prevista'], int(id_tappa)))
                                                                #cur1.rowfactory = makeDictFactory(cur1)
                                                                consuntivazioni_uo=cur.fetchall()
                                                            except Exception as e:
                                                                logger.error(query_select)
                                                                #logger.error()
                                                                logger.error(e)
                                                            
                                                            
                                                            cur.close()
                                                            cur = con.cursor()
                                                            
                                                            if len(consuntivazioni_uo)==0:
                                                                #logger.debug('Insert tappa {}'.format(int(id_tappa)))
                                                                query_insert='''INSERT INTO UNIOPE.CONSUNT_SPAZZAMENTO (
                                                                        ID_PERCORSO, ID_VIA, QTA_SPAZZATA, 
                                                                        CAUSALE_SPAZZ, NOTA, DATA_CONS,
                                                                        ID_TAPPA,
                                                                        ID_SERVIZIO, 
                                                                        DATA_INS,
                                                                        FIRMA, ORIGINE_DATO) VALUES
                                                                        (:c1, :c2, :c3,
                                                                        :c4, :c5, to_date(:c6, 'YYYYMMDD') ,
                                                                        :c7,
                                                                        (SELECT DISTINCT ID_SERVIZIO 
                                                                        FROM ANAGR_SER_PER_UO aspu 
                                                                        WHERE ID_PERCORSO = :c1
                                                                        AND to_date(:c6, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE),
                                                                        sysdate,
                                                                        NULL, 'EKOVISION')'''
                                                                try:
                                                                    cur.execute(query_insert, (data[i]['codice_serv_pred'],
                                                                                                int(id_via),
                                                                                                int(data[i]['cons_works'][t]['cod_std_qualita'].strip()),
                                                                                                causale,
                                                                                                nota_consuntivazione,
                                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                                int(id_tappa)))
                                                                    #cur1.rowfactory = makeDictFactory(cur1)
                                                                except Exception as e:
                                                                    logger.error(query_insert)
                                                                    logger.error('1:{} 2:{} 3:{} 4:{} 5:{} 6:{} 7:{}'.format(data[i]['codice_serv_pred'],
                                                                                                id_via,
                                                                                                int(data[i]['cons_works'][t]['cod_std_qualita'].strip()),
                                                                                                causale,
                                                                                                nota_consuntivazione,
                                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                                int(id_tappa)))
                                                                    logger.error(e)
                                                                    #logger.error('Ci sono troppi conferimenti con ID {}'.format(data[i]['cons_conferimenti'][c]['id']))
                                                                    #logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                                    #logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                                    #logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                                    #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                                    #exit()        
                                                                
                                                            elif len(consuntivazioni_uo)==1:
                                                                query_update='''
                                                                    UPDATE UNIOPE.CONSUNT_SPAZZAMENTO 
                                                                    SET QTA_SPAZZATA=:c1, 
                                                                    CAUSALE_SPAZZ=:c2, 
                                                                    NOTA=:c3, 
                                                                    DATA_INS=sysdate, 
                                                                    ORIGINE_DATO='EKOVISION'
                                                                    WHERE DATA_CONS=to_date(:c4, 'YYYYMMDD') AND ID_TAPPA=:c5
                                                            '''
                                                                try:
                                                                    cur.execute(query_update, (int(data[i]['cons_works'][t]['cod_std_qualita'].strip()),
                                                                                                causale,
                                                                                                nota_consuntivazione,
                                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                                id_tappa))
                                                                except Exception as e:
                                                                    logger.error(query_insert)
                                                                    logger.error('{} {} {} {} {}'.format(int(data[i]['cons_works'][t]['cod_std_qualita'].strip()),
                                                                                                causale,
                                                                                                nota_consuntivazione,
                                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                                id_tappa))
                                                                    logger.error(e) 
                                                            else: 
                                                                logger.error('Problema consuntivazioni doppie su UO')
                                                                logger.error('Id tappa {}'.format(id_tappa))
                                                                logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                                logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                                logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                                exit()
                                                
                                                
                                                
                                            elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC' or data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                #logger.debug('Consuntivazione raccolta')
                                                tipo_servizio='RACC'
                                                #logger.debug(int(data[i]['cons_works'][t]['cod_componente']))
                                                if int(data[i]['cons_works'][t]['pos'])>0 and int(data[i]['cons_works'][t]['flg_non_previsto'].strip())==0:
                                                    if int(data[i]['cons_works'][t]['cod_componente'].strip()) in elenco_elementi:
                                                        ripasso_sit=elenco_elementi.count(int(data[i]['cons_works'][t]['cod_componente'].strip()))
                                                    else:
                                                        ripasso_sit=0
                                                    elenco_elementi.append(int(data[i]['cons_works'][t]['cod_componente'].strip()))
                                                    
                                                    if id_servizio != 114: # se non è botticella
                                                        select_sit_per_tappa='''select codice_modello_servizio, 
                                                        min(ordine)  as ordine, 
                                                        vc.codice_punto_raccolta as id_piazzola , at.nota, at.ripasso, at.codice, 
                                                        min(data_inizio) as data_inizio, 
                                                        case 
                                                            when max(data_fine) = '20991231' then null 
                                                            else max(data_fine)
                                                        end data_fine, 
                                                        vc.tipo_servizio_componente
                                                        from 
                                                            (SELECT codice_modello_servizio, ordine, objecy_type, 
                                                        codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                            FROM anagrafe_percorsi.v_percorsi_elementi_tratti 
                                                            where data_inizio < coalesce(data_fine, '20991231')
                                                            and codice_modello_servizio =  %s
                                                            union 
                                                            SELECT codice_modello_servizio, ordine, objecy_type, 
                                                        codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                            FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs 
                                                            where data_inizio < coalesce(data_fine, '20991231')
                                                            and codice_modello_servizio =  %s
                                                            union 
                                                            SELECT codice_modello_servizio, ordine, objecy_type, 
                                                        codice, quantita, lato_servizio, percent_trattamento,frequenza,
                                                        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                                                        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine
                                                            FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi 
                                                            where data_inizio < coalesce(data_fine, '20991231')
                                                            and codice_modello_servizio =  %s
                                                            ) at
                                                        left join etl.v_componenti vc on vc.cod_componente = at.codice
                                                        where codice_tipo_servizio = %s 
                                                        and codice = %s and ripasso=%s
                                                        and (%s between data_inizio and coalesce((data_fine::int-1)::varchar,'20991231'))
                                                        group by codice_modello_servizio,  
                                                        vc.codice_punto_raccolta, at.nota, at.ripasso, at.codice, /*at.data_inizio, at.data_fine, */
                                                        vc.tipo_servizio_componente
                                                        '''
                                                        try:
                                                            
                                                            
                                                            curr.execute(select_sit_per_tappa, (data[i]['codice_serv_pred'],
                                                                                                data[i]['codice_serv_pred'],
                                                                                                data[i]['codice_serv_pred'],
                                                                                                tipo_servizio, 
                                                                                                int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ripasso_sit,
                                                                                                data[i]['data_pianif_iniziale'])
                                                                                                )
                                                            tappe=curr.fetchall()
                                                        except Exception as e:
                                                            logger.error(select_sit_per_tappa)
                                                            logger.error('1:{} 1:{} 1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['codice_serv_pred'],
                                                                                                data[i]['codice_serv_pred'],
                                                                                                data[i]['codice_serv_pred'],
                                                                                                tipo_servizio, 
                                                                                                int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ripasso_sit,
                                                                                                data[i]['data_pianif_iniziale']))
                                                            logger.error(e)
                                                        
                                                        
                                                        
                                                        
                                                        #counter=1
                                                        ct=0
                                                        for tt in tappe:
                                                            #logger.debug(elenco_elementi.count(int(data[i]['cons_works'][t]['cod_componente'])))
                                                            #if counter==elenco_elementi.count(int(data[i]['cons_works'][t]['cod_componente'])):
                                                            ordine=tt[1]
                                                            id_piazzola=tt[2]
                                                            #elenco_piazzole.append(tt[2])
                                                            ripasso=tt[4]
                                                            tipo_elemento = int(tt[8])
                                                            #logger.debug('Ordine {} - Id_via {} - Ripasso {}'.format(ordine, id_piazzola, ripasso))
                                                            if ct>=1 :
                                                                check_tappe_multiple=1
                                                                logger.error('Trovata più di una tappa')
                                                                logger.error(select_sit_per_tappa)
                                                                logger.error('1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                                data[i]['codice_serv_pred'],
                                                                                                int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ripasso_sit,
                                                                                                data[i]['data_pianif_iniziale']))
                                                                #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                                #exit()  
                                                            ct+=1                                     
                                                            
                                                            #counter+=1
                                                        
                                                            if ct == 0:
                                                                check_tappe_non_trovate=1
                                                                logger.warning('Tappa non trovata su SIT')
                                                                logger.warning(select_sit_per_tappa)
                                                                logger.warning('1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                                data[i]['codice_serv_pred'],
                                                                                                int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ripasso_sit,
                                                                                                data[i]['data_pianif_iniziale']))
                                                                #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                                #exit() 
                                                    
                                                        # cerco la tappa su UO
                                                        query_id_tappa='''SELECT DISTINCT ID_TAPPA, DTA_IMPORT, DATA_PREVISTA, cmt.ID_PIAZZOLA, cmt2.ID_ELEMENTO 
                                                            FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                            JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA
                                                            JOIN CONS_MICRO_TAPPA cmt2 ON cmt2.ID_MACRO_TAPPA=cmt.ID_MACRO_TAPPA
                                                            WHERE ID_PERCORSO = :t1
                                                            /*AND cmt.ID_PIAZZOLA = :t2*/
                                                            AND cmt.RIPASSO = :t3
                                                            AND cmt2.ID_ELEMENTO = :t4
                                                            and  DATA_PREVISTA = (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE 
                                                            WHERE DATA_PREVISTA <= to_date(:t5, 'YYYYMMDD') AND to_char(DATA_PREVISTA, 'HH24') LIKE '00' AND
                                                            ID_PERCORSO = :t6)
                                                            order by 1'''
                                                    
                                                    
                                                        try:
                                                            cur.execute(query_id_tappa, (data[i]['codice_serv_pred'],
                                                                                        ripasso, 
                                                                                        int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                        data[i]['data_pianif_iniziale'], 
                                                                                        data[i]['codice_serv_pred'])
                                                                        )
                                                            #cur1.rowfactory = makeDictFactory(cur1)
                                                            tappe_uo=cur.fetchall()
                                                        except Exception as e:
                                                            logger.error(query_id_tappa)
                                                            logger.error('1:{} 2:{} 3:{} 4:{} 5:{} 6:{}'.format(data[i]['codice_serv_pred'],

                                                                                                    ripasso, 
                                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                    data[i]['data_pianif_iniziale'], 
                                                                                                    data[i]['codice_serv_pred']
                                                                                                    ))
                                                            logger.error(e)
                                                            exit()
                                                    
                                                    
                                                    
                                                    elif id_servizio == 114:
                                                        # botticella 
                                            
                                                        quey_nota='''select coalesce(nota,'') from (
                                                            SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti vpet 
                                                            where codice_modello_servizio=%s and codice = %s
                                                            union 
                                                            SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs vpeto
                                                            where codice_modello_servizio=%s and codice = %s
                                                            union 
                                                            SELECT * FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi vpetd
                                                            where codice_modello_servizio=%s and codice = %s
                                                        )as foo '''

                                                        try:
                                                            curr.execute(quey_nota, (data[i]['codice_serv_pred'],
                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                    data[i]['codice_serv_pred'],
                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                    data[i]['codice_serv_pred'],
                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip())
                                                                                                ))
                                                            note=curr.fetchall()
                                                        
                                                            for nn in note:
                                                                nota_asta=nn[0]
                                                        
                                                        except Exception as e:
                                                            logger.error(quey_nota)
                                                            logger.error('{} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                                int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ))
                                                            logger.error(e)
                                                        

                                                        
                                                        quey_id_asta='''select id_asse_stradale, tipo_servizio_componente 
                                                        from etl.v_componenti vc 
                                                        where cod_componente = %s'''
                                                        
                                                        try:
                                                            curr.execute(quey_id_asta, (int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ))
                                                            id_aste=curr.fetchall()
                                                        
                                                            for ia in id_aste:
                                                                id_asta=ia[0]
                                                                tipo_elemento=ia[1]
                                                        
                                                        except Exception as e:
                                                            logger.error(quey_id_asta)
                                                            logger.error('{} {} {} {} {}'.format(int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                                ))
                                                            logger.error(e)
                                                    
                                                    
                                                        query_id_tappa='''SELECT ID_TAPPA, DTA_IMPORT, DATA_PREVISTA, cmt.ID_PIAZZOLA, cmt2.ID_ELEMENTO 
                                                            FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                                            JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA
                                                            LEFT JOIN CONS_MICRO_TAPPA cmt2 ON cmt2.ID_MACRO_TAPPA=cmt.ID_MACRO_TAPPA
                                                            WHERE ID_PERCORSO = :t1
                                                            AND cmt.ID_ASTA = :t2
                                                            AND trim(COALESCE(cmt.NOTA_VIA, 'ND')) LIKE trim(COALESCE(:t3, 'ND'))
                                                            and  DATA_PREVISTA = (SELECT max(DATA_PREVISTA) FROM CONS_PERCORSI_VIE_TAPPE 
                                                            WHERE DATA_PREVISTA <= to_date(:t4, 'YYYYMMDD') AND to_char(DATA_PREVISTA, 'HH24') LIKE '00' AND
                                                            ID_PERCORSO = :t5)
                                                            order by 1'''
                                                        
                                                        try:
                                                            cur.execute(query_id_tappa, (data[i]['codice_serv_pred'],
                                                                                        id_asta, 
                                                                                        nota_asta,
                                                                                        data[i]['data_pianif_iniziale'], 
                                                                                        data[i]['codice_serv_pred'])
                                                                        )
                                                            #cur1.rowfactory = makeDictFactory(cur1)
                                                            tappe_uo=cur.fetchall()
                                                        except Exception as e:
                                                            logger.error(query_id_tappa)
                                                            logger.error('1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['codice_serv_pred'],
                                                                            id_asta, 
                                                                            nota_asta,
                                                                            data[i]['data_pianif_iniziale'], 
                                                                            data[i]['codice_serv_pred']
                                                                                                    ))
                                                            logger.error(e)
                                                            exit()
                                                        #ct=0
                                                        # qua devo ancora scriverlo..
                                                        
                                                    
                                                    
                                                        
                                                        
                                                        
                                                        
                                                    ct=0
                                                    for ttu in tappe_uo:
                                                        #logger.debug(ttu[0])
                                                        id_tappa=ttu[0]
                                                        #elenco_tappe.append(ttu[0])
                                                        ct+=1
                                                        #logger.debug('Sono qua')                                           
                                                        # verificare se nel caso di tipologie diverse la tappa sia diversa o meno (prendi percorso 0101367901)
                                                        
                                                    if ct>1:
                                                            check_tappe_multiple = 1
                                                            logger.error('Trovata più di una tappa')
                                                            logger.error(query_id_tappa)
                                                            if id_servizio != 114: # se non è botticella
                                                                logger.error('{} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                    id_piazzola,
                                                                                    ripasso,
                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                    data[i]['data_pianif_iniziale']))
                                                            else :
                                                                logger.error('1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['codice_serv_pred'],
                                                                            id_asta, 
                                                                            nota_asta,
                                                                            data[i]['data_pianif_iniziale'], 
                                                                            data[i]['codice_serv_pred']
                                                                                                    ))

                                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                            #exit()                                       
                                                    
                                                    
                                                    # se 
                                                    elif ct == 0:
                                                        check_tappe_non_trovate=1
                                                        logger.warning('Tappa non trovata su UO. La inserisco nella tabella dei soccorsi')
                                                        logger.warning(query_id_tappa)
                                                        if id_servizio != 114: # se non è botticella
                                                            logger.warning('{} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                                    id_piazzola,
                                                                                    ripasso,
                                                                                    int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                    data[i]['data_pianif_iniziale']))                                                                          
                                                        
                                                            # DA VERIFICARE e RIVEDERE con il nuovo tracciato
                                                            
                                                            
                                                            # inserisco la tappa nella tabella apposita 
                                                            if int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                    causale=100
                                                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                    causale=110
                                                            
                                                                query_select= '''SELECT * 
                                                                from UNIOPE.CONSUNT_ELEMENTO_SOCCORSO
                                                                WHERE ID_ELEMENTO=:c1
                                                                AND CAUSALE = :c2
                                                                and DATALAV= to_date(:c3, 'YYYYMMDD')
                                                                and ID_PERCORSO_OSPITANTE= :c4'''
                                                                
                                                                try:
                                                                    cur.execute(query_select, (int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred']))
                                                                    #cur1.rowfactory = makeDictFactory(cur1)
                                                                    consuntivazioni_socc_uo=cur.fetchall()
                                                                except Exception as e:
                                                                    logger.error(query_select)
                                                                    logger.error('1:{}, 2:{}, 3:{}, 4:{}'.format(int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred']))
                                                                    logger.error(e)
                                                                
                                                                
                                                                
                                                                cur.close()
                                                                cur = con.cursor()
                                                                
                                                                #logger.debug(len(consuntivazioni_uo))
                                                                #exit()
                                                                # FASCIA TURNI SAREBBE DA RIVEDERE MEGLIO CON IL TURNO EFFETTIVO
                                                                    
                                                                if len(consuntivazioni_socc_uo)==0:
                                                                    query_insert='''INSERT INTO UNIOPE.CONSUNT_ELEMENTO_SOCCORSO (
                                                                    ID_ELEMENTO, ID_PERCORSO_OSPITANTE, CAUSALE,
                                                                    DATALAV, FASCIA_TURNO, ORIGINE) 
                                                                    VALUES (
                                                                    :c1, :c2, :c3,
                                                                    to_date(:c4, 'YYYYMMDD'), 
                                                                    (SELECT DISTINCT at2.FASCIA_TURNO 
                                                                    FROM ANAGR_SER_PER_UO aspu 
                                                                    JOIN ANAGR_TURNI at2 ON at2.ID_TURNO = aspu.ID_TURNO 
                                                                    WHERE aspu.ID_PERCORSO = :c5
                                                                    AND to_date(:c6, 'YYYYMMDD') 
                                                                    BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE),
                                                                    'Ekovision')'''
                                                                    try:
                                                                        cur.execute(query_insert, (int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            data[i]['codice_serv_pred'],
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista']))
                                                                    except Exception as e:
                                                                        logger.error(query_insert)
                                                                        logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}'.format(
                                                                                            int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            data[i]['codice_serv_pred'],
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista']))
                                                                        logger.error(e)
                                                                else:
                                                                    query_update='''UPDATE UNIOPE.CONSUNT_ELEMENTO_SOCCORSO 
                                                                        SET FASCIA_TURNO=
                                                                        (SELECT DISTINCT at2.FASCIA_TURNO 
                                                                        FROM ANAGR_SER_PER_UO aspu 
                                                                        JOIN ANAGR_TURNI at2 ON at2.ID_TURNO = aspu.ID_TURNO 
                                                                        WHERE aspu.ID_PERCORSO = :c1
                                                                        AND to_date(:c2, 'YYYYMMDD') 
                                                                        BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE), 
                                                                        DATA_ORA_INSER=SYSDATE , 
                                                                        ORIGINE='Ekovision' 
                                                                        WHERE ID_ELEMENTO=:c3 
                                                                        AND CAUSALE=:c4 AND 
                                                                        DATALAV=to_date(:c5, 'YYYYMMDD') 
                                                                        AND ID_PERCORSO_OSPITANTE=:c6'''
                                                                    try:
                                                                        cur.execute(query_update, (
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred']                                                                                          
                                                                                            ))
                                                                    except Exception as e:
                                                                        logger.error(query_update)
                                                                        logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}'.format(
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                                                            causale,
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred']))
                                                                        logger.error(e)
                                                        else : # se fosse botticella non sto a fare tutto il giro sopra
                                                            logger.error('1:{} 2:{} 3:{} 4:{} 5:{}'.format(data[i]['codice_serv_pred'],
                                                                        id_asta, 
                                                                        nota_asta,
                                                                        data[i]['data_pianif_iniziale'], 
                                                                        data[i]['codice_serv_pred']
                                                                                                ))
                                                        #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                        #exit()
                                                    
                                            
                                                    
                                                    #se trovo una tappa
                                                    
                                                    else:
                                                        # conto gli elementi
                                                        #for tt in tappe:  
                                                        if len(elenco_tappe)==0:
                                                            count_elementi=1
                                                            #if id_servizio != 114:
                                                            #    elenco_piazzole.append(int(id_piazzola))
                                                            elenco_tappe.append(int(id_tappa))
                                                            elenco_tipi.append(int(tipo_elemento))
                                                            ##########################################
                                                            # questa parte sarà da cambiare
                                                            nota_consuntivazione=''
                                                            ##########################################
                                                            if int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                                count_fatti=1
                                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                    causale=100
                                                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                    causale=110
                                                            else:
                                                                if causale_non_es != None:
                                                                    causale=causale_non_es
                                                                else:
                                                                    try:
                                                                        causale=int(data[i]['cons_works'][t-1]['cod_giustificativo_ext'].strip())
                                                                        count_fatti=0
                                                                    except Exception as e:
                                                                        logger.warning(e)
                                                                        logger.warning('Scheda {} - Posizione: {} Manca la causale quindi lo do per fatto'.format(
                                                                            int(data[i]['id_scheda']),
                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                        ))
                                                                        if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                            causale=100
                                                                        elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                            causale=110
                                                                        count_fatti=1
                                                        elif id_tappa == elenco_tappe[-1] and tipo_elemento == elenco_tipi[-1]:
                                                            # stessa tappa di prima 
                                                            count_elementi+=1
                                                            if int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                                count_fatti+=1
                                                            else:
                                                                if causale_non_es != None:
                                                                    causale=causale_non_es
                                                                else:
                                                                    try:
                                                                        causale=int(data[i]['cons_works'][t-1]['cod_giustificativo_ext'].strip())
                                                                    except Exception as e:
                                                                        logger.warning(e)
                                                                        logger.warning('Scheda {} - Posizione: {} Manca la causale quindi lo do per fatto'.format(
                                                                            int(data[i]['id_scheda']),
                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                        ))
                                                                        if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                            causale=100
                                                                        elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                            causale=110
                                                                        count_fatti+=1
                                                            if (count_elementi-count_fatti)==0 and (causale==100 or causale==110):
                                                                causale=causale  
                                                            ##########################################
                                                            # questa parte sarà da cambiare
                                                            nota_consuntivazione=''
                                                            ##########################################
                                                        elif id_tappa != elenco_tappe[-1] or tipo_elemento != elenco_tipi[-1]:
                                                            # nuova tappa (o tipo elemento)
                                                            elenco_tappe.append(int(id_tappa))
                                                            elenco_tipi.append(int(tipo_elemento))
                                                            count_elementi=1
                                                            if int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                                count_fatti=1
                                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                    causale=100
                                                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                    casuale=110
                                                            else:
                                                                if causale_non_es != None:
                                                                    causale=causale_non_es
                                                                else:
                                                                    try:
                                                                        causale=int(data[i]['cons_works'][t-1]['cod_giustificativo_ext'].strip())
                                                                        count_fatti=0
                                                                    except Exception as e:
                                                                        logger.warning(e)
                                                                        logger.warning('Scheda {} - Posizione: {} Manca la causale quindi lo do per fatto'.format(
                                                                            int(data[i]['id_scheda']),
                                                                            int(data[i]['cons_works'][t]['pos'])
                                                                        ))
                                                                        if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                                                            causale=100
                                                                        elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                                            causale=110
                                                                        count_fatti=1
                                                                        #causale=int(data[i]['cons_works'][t-1]['cod_giustificativo_ext'].strip())
                                                            ##########################################
                                                            # questa parte sarà da cambiare
                                                            nota_consuntivazione=''
                                                            ##########################################
                                                        else:
                                                            logger.error('Non capisco perchè finisca qua')
                                                                
                                                    
                                                    
                                                
                                                    
                                                        # devo fare gli insert
                                                        query_select=''' 
                                                        SELECT * 
                                                        FROM CONSUNT_MACRO_TAPPA cs 
                                                        WHERE DATA_CONS = to_date(:c1, 'YYYYMMDD')
                                                        and id_MACRO_TAPPA = :c2
                                                        and TIPO_ELEMENTO = :c3
                                                        '''
                                                        
                                                    
                                                        try:
                                                            cur.execute(query_select, (data[i]['data_esecuzione_prevista'], elenco_tappe[-1], elenco_tipi[-1]))
                                                            #cur1.rowfactory = makeDictFactory(cur1)
                                                            consuntivazioni_uo=cur.fetchall()
                                                        except Exception as e:
                                                            logger.error(query_select)
                                                            logger.error('1:{}, 2:{}, 3:{}'.format(data[i]['data_esecuzione_prevista'], elenco_tappe[-1], elenco_tipi[-1]))
                                                            logger.error(e)
                                                        
                                                        
                                                        
                                                        cur.close()
                                                        cur = con.cursor()
                                                        
                                                        #logger.debug(len(consuntivazioni_uo))
                                                        #exit()
                                                            
                                                        if len(consuntivazioni_uo)==0:
                                                            query_insert='''INSERT INTO UNIOPE.CONSUNT_MACRO_TAPPA (
                                                            ID_MACRO_TAPPA, QTA_ELEM_NON_VUOTATI, CAUSALE_ELEM,
                                                            NOTA, DATA_CONS, ID_PERCORSO,
                                                            ID_VIA, TIPO_ELEMENTO,
                                                            ID_SERVIZIO,
                                                            INS_DATE, MOD_DATE, ORIGINE_DATO) VALUES 
                                                            (:c1, :c2, :c3, 
                                                            :c4, to_date(:c5, 'YYYYMMDD'), :c6,
                                                            (SELECT distinct ID_VIA 
                                                            FROM CONS_PERCORSI_VIE_TAPPE cpvt  
                                                            WHERE ID_TAPPA = :c7), 
                                                            :c8,
                                                            (SELECT DISTINCT ID_SERVIZIO 
                                                                    FROM ANAGR_SER_PER_UO aspu 
                                                                    WHERE ID_PERCORSO = :c9
                                                                    AND to_date(:c10, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE),
                                                            sysdate, NULL, 'EKOVISION')'''
                                                            
                                                            try:
                                                                cur.execute(query_insert, (int(id_tappa), 
                                                                                            (count_elementi-count_fatti),
                                                                                            causale,
                                                                                            nota_consuntivazione, 
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            int(id_tappa), 
                                                                                            tipo_elemento,
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista']
                                                                                            ))
                                                                #cur1.rowfactory = makeDictFactory(cur1)
                                                            except Exception as e:
                                                                logger.error(query_insert)
                                                                logger.error('1:{} 2:{} 3:{} 4:{} 5:{} 6:{} 7:{} 8:{} 9:{} 10:{}'.format(int(id_tappa), 
                                                                                            (count_elementi-count_fatti),
                                                                                            causale,
                                                                                            nota_consuntivazione, 
                                                                                            data[i]['data_esecuzione_prevista'],
                                                                                            data[i]['codice_serv_pred'],
                                                                                            int(id_tappa), 
                                                                                            int(tipo_elemento),
                                                                                            data[i]['codice_serv_pred'],
                                                                                            data[i]['data_esecuzione_prevista'])
                                                                )
                                                                logger.error(e)
                                                                                                            
                                                                
                                                        
                                                        elif len(consuntivazioni_uo)==1:
                                            
                                                            query_update='''
                                                                UPDATE UNIOPE.CONSUNT_MACRO_TAPPA 
                                                                SET QTA_ELEM_NON_VUOTATI=:c1, 
                                                                CAUSALE_ELEM=:c2, 
                                                                NOTA=:c3, 
                                                                MOD_DATE=sysdate, 
                                                                ORIGINE_DATO='EKOVISION'
                                                                WHERE DATA_CONS=to_date(:c4, 'YYYYMMDD') 
                                                                AND ID_MACRO_TAPPA = :c5
                                                                AND TIPO_ELEMENTO = :c6
                                                                '''
                                                            try:
                                                                cur.execute(query_update, ((count_elementi-count_fatti),
                                                                                            causale,
                                                                                            nota_consuntivazione,
                                                                                            data[i]['data_esecuzione_prevista'], 
                                                                                            int(id_tappa),
                                                                                            tipo_elemento))
                                                            except Exception as e:
                                                                logger.error(query_insert)
                                                                logger.error('1:{} 2:{} 3:{} 4:{} 5:{}, 6:{}'.format((count_elementi-count_fatti),
                                                                                                        causale,
                                                                                                        nota_consuntivazione,
                                                                                                        data[i]['data_esecuzione_prevista'], 
                                                                                                        int(id_tappa), 
                                                                                                        tipo_elemento))
                                                                logger.error(e)
                                                            
                                                            
                                                        else:
                                                            logger.error('Problema consuntivazioni doppie su UO')
                                                            logger.error('Id tappa {}'.format(id_tappa))
                                                            logger.error('Tipo elemento {}'.format(tipo_elemento))
                                                            logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                            logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                            logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                            exit()
                                                                
                        
                                                            
                                                            
                                                            
                                                
                                            else:
                                                logger.error('PROBLEMA CONSUNTIVAZIONE')
                                                logger.error('File:{}'.format(filename))
                                                logger.error('Mi sono fermato alla riga {}'.format(i))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                            t+=1
                                            con.commit()
                                            
                                            
                                        if check_tappe_non_trovate==1 or check_tappe_multiple ==1:
                                            query_reimport='''INSERT INTO util.sys_history ("type", "action", description, datetime, id_user,  id_percorso) 
                                                (
                                                    select distinct 'PERCORSO', 'UPDATE', 'Forzatura re-importazione Ekovision', now(),  0, foo.id_percorso 
                                                    from (
                                                        select id_percorso from elem.percorsi p  
                                                        where id_categoria_uso = 3 and cod_percorso = %s
                                                        ) foo
                                                )'''    
                                            try:
                                                curr.execute(query_reimport, (data[i]['codice_serv_pred'],))
                                            except Exception as e:
                                                logger.error(query_reimport)
                                                logger.error('cod percorso: {}'.format(data[i]['codice_serv_pred']))
                                                logger.error(e)
                                            conn.commit()
                                    
                                else:
                                    logger.info('Non processo la scheda perchè antecedente alla data di partenza di Ekovision {}'.format(data_start_ekovision))
                            except Exception as e:
                                logger.error('File:{}'.format(filename))
                                logger.error(e)
                                logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                                logger.error(type(e).__name__)
                                logger.error('Non processo la riga {}'.format(i))
                            i+=1
                        con.commit()
                        # Closing file
                        f.close()
                        logger.info('Chiudo il file {}'.format(filename))
                        logger.info('-----------------------------------------------------------------------------------------------------------------------')
                        #exit()
                        #srv.rename("./"+ filename, "./archive/" + filename)
                        try:
                            srv.rename(filename, "archive/" + filename)
                        except Exception as e:
                            logger.error(e)
                            logger.error('Problema spostamento in archivio del file {}'.format(filename)) 
                            logger.error('Entrare in filezilla e spostare il file a mano')
                            error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it, Riccardo.Piana@amiu.genova.it', os.path.basename(__file__), logger)
                            exit() 
                    except Exception as e:
                        logger.error(e)
                        logger.error('Problema processamemto file {}'.format(filename))
                        logger.error('File spostato nella cartella json_error')
                        f.close()
                        #error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        srv.rename(filename, "json_error/" + filename)
                        error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it, andrea.volpi@ekovision.it, francesco.venturi@ekovision.it', os.path.basename(__file__), logger)
                       


                    #exit()
                    
                    
                    
                    
                    
                    
                    
                else: 
                    logger.warning('Non scarico nessun file')
        
        
        
        
        
        
        
        
        # Closes the connection
        srv.close()
        logger.info('Connessione chiusa')
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema scarico SFTP  
    
    
    logger.debug('Fine ciclo')
    
    
    
    
    
    #exit()
    
 
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      
    