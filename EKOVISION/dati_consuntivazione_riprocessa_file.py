#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) Processo i file già in archivio

2) Processo il file json

3) Tengo traccia di eventuali errori/warning


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


c_handler.setLevel(logging.WARNING)
f_handler.setLevel(logging.INFO)


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



def fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2):
    '''
    Calcolo della fascia turno sulla base degli orari della scheda di lavoro Ekovision
    '''
    fascia_turno=''
    if ora_inizio_lav_2 == '000000' and ora_fine_lav_2 =='000000':
    
        if ora_inizio_lav== '000000' and ora_fine_lav =='000000':
            fascia_turno='D'
        else:
            oi=int(ora_inizio_lav[:2])
            mi=int(ora_inizio_lav[2:4])
            of=int(ora_fine_lav[:2])
            mf=int(ora_fine_lav[2:4])
    else:
        oi=int(ora_inizio_lav[:2])
        mi=int(ora_inizio_lav[2:4])
        of=int(ora_fine_lav_2[:2])
        mf=int(ora_fine_lav_2[2:4])
            
            
    if fascia_turno=='':        
        # calcolo minuti del turno
        if of < oi:
            minuti= 60*(24 - oi) + 60 * of - mi + mf
        else :
            minuti = 60 * (of-oi) - mi + mf 

        
        hh_plus=int(minuti/2/60)
        mm_plus=minuti/2-60*int(minuti/2/60)
        
        # ora media
        if mi+mm_plus >= 60:
            mm=mi+mm_plus-60
            hh=oi+1+hh_plus
        else:
            mm=mi+mm_plus
            hh=oi+hh_plus
        
        #print('{}:{}'.format(hh,mm))
        
        if hh > 5 and hh <= 12:
            fascia_turno = 'M'
        elif hh > 12 and hh <= 20:
            fascia_turno = 'P'
        elif hh > 20 or hh <= 5:
            fascia_turno= 'N'
        
        return fascia_turno




def main():
    
    
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
    
    
    # per ora vado a leggere in archivio (poi probabilmente è da vedere se abbia senso avere 2 flussi distinti)
    cartella_sftp_eko='sch_lav_cons/out/archive'    
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
            
            # qua correggo tutti i casi in cui il tipo elemento era sbagliato a causa del RACC-LAV 
            """
            select_file='''select DISTINCT /*ID_PERCORSO, DATA_CONS, */
                see.nomefile
                from consunt_macro_Tappa i 
                JOIN SCHEDE_ESEGUITE_EKOVISION see ON see.CODICE_SERV_PRED = i.ID_PERCORSO 
                            AND to_char(i.DATA_CONS, 'YYYYMMDD') = see.DATA_ESECUZIONE_PREVISTA 
                where i.data_cons > to_date('20240101', 'yyyymmdd') and --id_percorso = '0500132901' and
                not exists (select 1 from (select distinct tipo_elemento, id_macro_Tappa from cons_elementi ce 
                    inner join cons_micro_tappa cm 
                    on ce.id_elemento = cm.id_elemento) a
                    where i.id_macro_tappa = a.id_macro_tappa
                    and i.tipo_elemento = a.tipo_elemento)
                and origine_dato = 'EKOVISION'
                AND CAUSALE_ELEM = 110
                AND RECORD_VALIDO = 'S' '''
            """
            
                    
            # json che davano errore su un singolo percorso        
            select_file='''select distinct e.nomefile from schede_eseguite_ekovision e
                inner join anagr_Ser_per_uo aspu
                on aspu.id_percorso  = E.CODICE_SERV_PRED
                and to_date(E.DATA_PIANIF_INIZIALE,'yyyymmdd') between ASPU.DTA_ATTIVAZIONE  and ASPU.DTA_DISATTIVAZIONE
                inner join anagr_servizi s
                on s.id_servizio = aspu.id_servizio
                where S.TIPO_SERVIZIO = 'RACCOLTA'
                and e.record_valido  ='S'
                and not exists (select 1 from pr_tmp4 c where c.id_percorso  = aspu.id_percorso and C.DATA_CONS = to_date(E.DATA_PIANIF_INIZIALE,'yyyymmdd') )
                and aspu.id_percorso != '0500132901'
                and e.data_pianif_iniziale like '2024%' '''


            try:
                cur.execute(select_file)
                check_filename=cur.fetchall()
            except Exception as e:
                logger.error(select_file)
                logger.error(e)
            for filename in check_filename:
            #for filename in srv.listdir('./'):
                #logger.debug(filename)
                
                # questo era per correggere il 101 con il 110
                """select_file='''SELECT DISTINCT NOMEFILE
                    FROM SCHEDE_ESEGUITE_EKOVISION see
                    WHERE COD_CAUS_SRV_NON_ESEG_EXT = 101
                    AND RECORD_VALIDO = 'S' and NOMEFILE=:f1'''
                    
                    
                    
                
                
                
                

                try:
                    cur.execute(select_file, (filename,))
                    check_filename=cur.fetchall()
                except Exception as e:
                    logger.error(select_file)
                    logger.error(e)
                                    
                # se non ho già letto il file
                if len(check_filename)==1:
                    logger.info('Sposto il file {} nella cartella per cui debba essere riprocessato'.format(filename))
                
                    try:
                        srv.rename(filename, "../" + filename)
                    except Exception as e:
                        logger.error(e)
                        logger.error('Problema spostamento in archivio del file {}'.format(filename)) 
                        logger.error('Entrare in filezilla e spostare il file a mano')
                        #error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it; Riccardo.Piana@amiu.genova.it', os.path.basename(__file__), logger)
                        exit() 
                """    
                
                logger.info(filename[0])
                try:
                    srv.rename(filename[0], "../" + filename[0])
                except Exception as e:
                    logger.error(e)
                    logger.error('Problema spostamento in archivio del file {}'.format(filename)) 
                    logger.error('Entrare in filezilla e spostare il file a mano')
                    #error_log_mail(errorfile, 'AssTerritorio@amiu.genova.it; Riccardo.Piana@amiu.genova.it', os.path.basename(__file__), logger)
                    exit() 
                    
                    
                    
                    
                '''else: 
                    logger.info('Non scarico il file {} perchè già letto e processato'.format(filename))
                '''
        
        
        
        
        
        
        
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
    