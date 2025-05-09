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
            for filename in srv.listdir('./'):
                #logger.debug(filename)
                select_file='''SELECT * FROM UNIOPE.HIST_SERVIZI_MEZZI_OK 
                WHERE FILENAME=:f1 '''

                try:
                    cur.execute(select_file, (filename,))
                    check_filename=cur.fetchall()
                except Exception as e:
                    logger.error(select_file)
                    logger.error(e)
                                 
                # se non ho già letto il file
                if len(check_filename)==0 and fnmatch.fnmatch(filename, "sch_lav_consuntivi*"):
                    srv.get(filename, path + "/eko_output2/" + filename)
                    logger.info('Scaricato file {}'.format(filename))
                    
                    
                    
                    logger.info ('Inizio processo file'.format(filename))   
                    
                    # imposto a 0 un controllo sulla lettura del file
                    check_lettura=0
                    
                    
                    # Opening JSON file
                    f = open(path + "/eko_output2/" + filename)
                    
                    # returns JSON object as 
                    # a dictionary
                    try:
                        data = json.load(f)
                        
                        
                        
                        i=0
                        while i<len(data):
                            try:
                                logger.info('{} - Leggo dati della scheda di lavoro {}'.format(i, data[i]['id_scheda']))
                                
                                check=0                    
                                
                                if data[i]['data_esecuzione_prevista']>=data_start_ekovision:
                                    #logger.debug(data[i]['cons_ris_tecniche'])
                                    
                                    
                                    
                                    if data[i]['cod_caus_srv_non_eseg_ext']=='' and len(data[i]['cons_ris_tecniche'])>0:
                                        if data[i]['cons_ris_tecniche'][0]['id_giustificativo'] == 0 or data[i]['cons_ris_tecniche'][0]['id_risorsa_tecnica'] > 0:
                                            tt=0
                                            while  tt<len(data[i]['cons_ris_tecniche']):
                                                sportello=data[i]['cons_ris_tecniche'][tt]['cod_matricola_ristec']
                                                logger.debug(sportello)
                                                
                                                
                                                cur2 = con.cursor()
                                                durata = 0
                                                o=0
                                                while o<len(data[i]['cons_ris_tecniche'][tt]['cons_ristec_orari']):
                                                    
                                                    data_ora_start='{} {}'.format(
                                                        data[i]['cons_ris_tecniche'][tt]['cons_ristec_orari'][o]['data_ini'],
                                                        data[i]['cons_ris_tecniche'][tt]['cons_ristec_orari'][o]['ora_ini']
                                                        )
                                                    data_ora_fine='{} {}'.format(
                                                        data[i]['cons_ris_tecniche'][tt]['cons_ristec_orari'][o]['data_fine'],
                                                        data[i]['cons_ris_tecniche'][tt]['cons_ristec_orari'][o]['ora_fine']
                                                        )
                                                    
                                                    fmt='%Y%m%d %H%M%S'
                                                    data_ora_start_ok = datetime.strptime(data_ora_start, fmt)
                                                    data_ora_fine_ok = datetime.strptime(data_ora_fine, fmt)
                                                    # calcolo differenza in minuti ()
                                                    durata+=(data_ora_fine_ok - data_ora_start_ok).total_seconds() / 60.0
                                                    
                                                    o+=1
                                                logger.debug(durata)
                                                
                                                select_query='''SELECT ID_SCHEDA_EKOVISION FROM UNIOPE.HIST_SERVIZI_MEZZI_OK
                                                WHERE ID_SCHEDA_EKOVISION = :m1 '''
                                                try:
                                                    cur2.execute(select_query, (data[i]['id_scheda'], ))
                                                    id_schede=cur2.fetchall()
                                                except Exception as e:
                                                    logger.error(select_query)
                                                    logger.error(e)
                                                
                                                cur2.close()
                                                cur2 = con.cursor()
                                                if len(id_schede)==0:
                                                    insert_query='''INSERT INTO 
                                                    UNIOPE.HIST_SERVIZI_MEZZI_OK (ID_SCHEDA_EKOVISION, SPORTELLO, DURATA, FILENAME)
                                                    VALUES
                                                    (:m1, :m3, :m4, :m5) '''
                                                    try:
                                                        cur2.execute(insert_query, (int(data[i]['id_scheda']), sportello, durata, filename))
                                                    except Exception as e:
                                                        # controllo se si tratta di ditta esterna (in quel caso non devo salvare i dati)
                                                        # altrimenti segnalo l'errore
                                                        cur3 = con.cursor()
                                                        select_query='''SELECT au.ID_ZONATERRITORIALE 
                                                        FROM ANAGR_SER_PER_UO aspu
                                                        JOIN anagr_UO au ON aspu.id_UO = au.id_UO
                                                        WHERE ID_PERCORSO = :p1 and TO_DATE(:p2, 'YYYYMMDD') between aspu.DTA_ATTIVAZIONE 
                                                        and aspu.DTA_DISATTIVAZIONE'''
                                                        try:
                                                            cur3.execute(select_query, (data[i]['codice_serv_pred'],data[i]['data_esecuzione_prevista'] ))
                                                            ii_uu=cur3.fetchall()
                                                        except Exception as e:
                                                            logger.error(select_query)
                                                            logger.error(e)
                                                        for i_u in ii_uu:
                                                            id_zona=i_u[0]
                                                        
                                                        cur3.close()
                                                        if id_zona==7:
                                                            # non c'è errore 
                                                            logger.info('Percorso di ditta esterna non salvo nulla')
                                                        else:
                                                            logger.error(insert_query)
                                                            logger.error
                                                            logger.error('m1:{}, m2:{}, m3:{}, m4:{}'.format(int(data[i]['id_scheda']), sportello, durata, filename))
                                                    
                                                    
                                                else: 
                                                    update_query='''UPDATE
                                                    UNIOPE.HIST_SERVIZI_MEZZI_OK 
                                                    SET SPORTELLO=:m1, DURATA=:m2, FILENAME=:m3
                                                    WHERE ID_SCHEDA_EKOVISION = :m4
                                                    '''
                                                    try:
                                                        cur2.execute(update_query, (sportello, durata, filename,data[i]['id_scheda']))
                                                    except Exception as e:
                                                        logger.error(update_query)
                                                        logger.error
                                                        logger.error('m1:{}, m2:{}, m3:{}, m4:{}'.format( sportello, durata, filename, int(data[i]['id_scheda'])))
                                                    
                                                    
                                                    
                                                cur2.close()    
                                                con.commit()
                                                tt+=1 
                                    
                                    
                                else:
                                    logger.info('Non processo la scheda perchè antecedente alla data di partenza di Ekovision {}'.format(data_start_ekovision))
                            except Exception as e:
                                check=1
                                logger.error('File:{}'.format(filename))
                                logger.error('Non processo la riga {}'.format(i))
                            i+=1
                        #con.commit()
                        
                        
                        # Closing file
                        f.close()
                        logger.info('Chiudo il file {}'.format(filename))
                        logger.info('-----------------------------------------------------------------------------------------------------------------------')
                        #exit()
                        #srv.rename("./"+ filename, "./archive/" + filename)
                    except Exception as e:
                        logger.error(e)
                        logger.error('Problema processamemto file {}'.format(filename))
                        #logger.error('File spostato nella cartella json_error')
                        f.close()
                        #srv.rename("./"+ filename, "./json_error/" + filename)
                        #error_log_mail(errorfile, 'assterritorio@amiu.genova.it; andrea.volpi@ekovision.it; francesco.venturi@ekovision.it', os.path.basename(__file__), logger)
                    
                        
                
                    
                    os.remove(path + "/eko_output2/" + filename)
                
        
        
        
        
        
        
        
        # Closes the connection
        srv.close()
        logger.info('Connessione chiusa')
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema scarico SFTP  
    
    
    logger.debug('Fine ciclo')
    
    
    
    
    
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
    