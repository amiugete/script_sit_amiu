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
                select_file='''SELECT * FROM UNIOPE.EKOVISION_LETTURA_CONSUNT 
                WHERE FILENAME=:f1 and da_riprocessare IS NULL'''

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
                                    
                                    # popolamento hist_servizi
                                    
                                    
                                    #interrogo il WS e ricavo la fascia turno
                                    
                                    # PARAMETRI GENERALI WS
    
    
                                    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

                                    data_json={'user': eko_user, 
                                        'password': eko_pass,
                                        'o2asp' :  eko_o2asp
                                        }
                                        
                                    
                                    
                                
                                        

                                    params={'obj':'schede_lavoro',
                                        'act' : 'r',
                                        'sch_lav_data': data[i]['data_esecuzione_prevista'],
                                        'cod_modello_srv': data[i]['codice_serv_pred'], 
                                        'flg_includi_eseguite': 1,
                                        'flg_includi_chiuse': 1
                                        }

                                    response = requests.post(eko_url, params=params, data=data_json, headers=headers)
                                    #response.json()
                                    #logger.debug(response.status_code)
                                    try:      
                                        response.raise_for_status()
                                        # access JSOn content
                                        #jsonResponse = response.json()
                                        #print("Entire JSON response")
                                        #print(jsonResponse)
                                    except HTTPError as http_err:
                                        logger.error(f'HTTP error occurred: {http_err}')
                                        check=1
                                    except Exception as err:
                                        logger.error(f'Other error occurred: {err}')
                                        logger.error(response.json())
                                        check=1
                                    if check<1:
                                        letture = response.json()
                                        logger.info(letture)
                                        if len(letture['schede_lavoro']) > 0 : 
                                            #controllo se la scheda è la stessa (escludo eventuali soccorsi o schede duplicate per sbaglio)
                                            if data[i]['id_scheda']==letture['schede_lavoro'][0]['id_scheda_lav']:
                                                ora_inizio_lav=letture['schede_lavoro'][0]['ora_inizio_lav']
                                                ora_inizio_lav_2=letture['schede_lavoro'][0]['ora_inizio_lav_2']
                                                ora_fine_lav=letture['schede_lavoro'][0]['ora_fine_lav']
                                                ora_fine_lav_2=letture['schede_lavoro'][0]['ora_fine_lav_2']
                                            if ora_inizio_lav_2=='000000' and ora_fine_lav_2=='000000' :
                                                orario_esecuzione='{} - {}'.format(ora_inizio_lav, ora_fine_lav)
                                            else:
                                                orario_esecuzione='{} - {} / {} - {}'.format(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2)   
                                            logger.info('Orario esecuzione:{}'.format(orario_esecuzione))
                                            fascia_t=fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2)
                                            logger.info('Fascia turno :{}'.format(fascia_t))
                                            # calcolo fascia turno
                                            # caso semplice se c 
                                            
                                    
                                    update_testata='''UPDATE UNIOPE.SCHEDE_ESEGUITE_EKOVISION 
                                        SET ORARIO_ESECUZIONE= :s1, FASCIA_TURNO = :s2
                                        WHERE ID_SCHEDA=:s3'''
                                    
                                    try:
                                        cur.execute(update_testata, (
                                                    orario_esecuzione, fascia_t, data[i]['id_scheda']
                                                ))
                                    except Exception as e:
                                        check=1
                                        logger.error(update_testata)
                                        logger.error('1:{}, 2:{}, 3:{}'.format(
                                            orario_esecuzione, fascia_t, data[i]['id_scheda']
                                        ))
                                        logger.error(e)

                                    con.commit()
                                                                    
                                    
                                    
                                    
                                    
                                    
                                    # consuntivazione 
                                    t=0 # contatore tappe
                                    check_cons=0
                                    while t<len(data[i]['cons_works']):


                                        # alla prima riga correggo eventuali inserimenti già fatti
                                        if t==0:
                                            # devo fare select
                                            update_schede='''UPDATE UNIOPE.CONSUNT_EKOVISION_SPAZZAMENTO 
                                            SET RECORD_VALIDO = 'N' 
                                            WHERE ID_SCHEDA = :s1'''
                                            
                                            try:
                                                cur.execute(update_schede, (
                                                        data[i]['id_scheda'],
                                                    ))
                                            except Exception as e:
                                                check=1
                                                logger.error(update_schede)
                                                logger.error('1:{}'.format(
                                                data[i]['id_scheda']
                                                ))
                                                logger.error(e)
                                                check_lettura+=1
                                        con.commit()
                                        
                                        if t==0:
                                            # devo fare select
                                            update_schede='''UPDATE UNIOPE.CONSUNT_EKOVISION_RACCOLTA 
                                            SET RECORD_VALIDO = 'N' 
                                            WHERE ID_SCHEDA = :s1'''
                                            
                                            try:
                                                cur.execute(update_schede, (
                                                        data[i]['id_scheda'],
                                                    ))
                                            except Exception as e:
                                                check=1
                                                logger.error(update_schede)
                                                logger.error('1:{}'.format(
                                                data[i]['id_scheda']
                                                ))
                                                logger.error(e)
                                                check_lettura+=1
                                        con.commit()
                                        # escludo i NON previsti e NON eseguiti
                                        if int(data[i]['cons_works'][t]['flg_exec'].strip())==1 or  int(data[i]['cons_works'][t]['flg_non_previsto'].strip())==0 :
                                            ################################################################
                                            # Preparo i dati da inserire 
                                            
                                            # causale
                                            # il primo if era dopo ma l'ho sposato sopra (28/11/2024 sarebbero da riprocessare un po di dati)
                                            if int(data[i]['flg_segn_srv_non_effett'].strip())==1:
                                                causale=int(data[i]['cod_caus_srv_non_eseg_ext'].strip())
                                                qualita=0
                                            elif int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC' or data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                    causale=100
                                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                    causale=110
                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                    qualita=int(data[i]['cons_works'][t]['cod_std_qualita'].strip())
                                            #lo sposto prima perchè ci sono alcuni casi in cui int(data[i]['cons_works'][t]['flg_exec'].strip())==1 
                                            # anche se il servizio non è stato effettuato
                                            # elif int(data[i]['flg_segn_srv_non_effett'].strip())==1:
                                            #    causale=int(data[i]['cod_caus_srv_non_eseg_ext'].strip())
                                            #    qualita=0
                                            # se il servizio non fosse stato completato
                                            else :
                                                try:
                                                    causale=int(data[i]['cons_works'][t]['cod_giustificativo_ext'].strip())
                                                    qualita=0
                                                except Exception as e:
                                                    check_cons=1
                                                    logger.warning('ID SCHEDA:{}'.format(data[i]['id_scheda']))
                                                    logger.warning('Causale servizio non effettuato:{}'.format(data[i]['cod_caus_srv_non_eseg_ext']))
                                                    logger.warning('FLG Eseguito:{}'.format(data[i]['cons_works'][t]['flg_exec']))
                                                    logger.warning('PROBLEMA CAUSALE')
                                                    logger.warning(e)
                                                    causale=None
                                            # la causale 999, creata per le preconsuntivazione, in realtà non dovrebbe essere usata.. 
                                            # se fosse arrivato qualcosa lo assimilo alla 102 (percorso non previsto)
                                            if causale == 999:
                                                causale = 102
                                            # vedo se consuntivazione arriva da totem o meno 
                                            if int(data[i]['cons_works'][t]['ts_exec']) == 0:
                                                totem=0
                                            else :
                                                totem=1
                                            
                                            
                                            # riprogrammato
                                            try:
                                                if int(data[i]['cons_works'][t]['flg_riprogrammato']) == 0:
                                                    riprogrammato=0
                                                elif int(data[i]['cons_works'][t]['flg_riprogrammato']) == 1 :
                                                    riprogrammato=1
                                            except Exception as e:
                                                riprogrammato=None
                                                    
                                            
                                            # note
                                            if data[i]['cons_works'][t]['note'] =='':
                                                note=None
                                            else :
                                                note=data[i]['cons_works'][t]['note']
                                                    
                                                    
                                            if data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                #logger.debug('Consuntivazione spazzamento')
                                            
                                                # gestione anomalie causali
                                                if check_cons==1:
                                                    insert_anom='''INSERT INTO UNIOPE.EKOVISION_ANOMALIE_CAUSALI 
                                                    (ID_SCHEDA, COD_TRATTO, POS, FILENAME)
                                                    VALUES
                                                    (:a1, :a2, :a3, :a4)'''
                                                    try:
                                                        cur.execute(insert_anom, (
                                                        data[i]['id_scheda'], 
                                                        int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                        int(data[i]['cons_works'][t]['pos']), 
                                                        filename
                                                    ))
                                                    except Exception as e:
                                                        check=1
                                                        logger.error('Problema inserimeno anomalie')
                                                        
                                                else: 
                                                    delete_anom='''DELETE 
                                                    FROM UNIOPE.EKOVISION_ANOMALIE_CAUSALI 
                                                    WHERE ID_SCHEDA = :a1 AND
                                                    COD_TRATTO=:a2 AND
                                                    POS = :a3
                                                    '''
                                                    try:
                                                        cur.execute(delete_anom, (
                                                        data[i]['id_scheda'], 
                                                        int(data[i]['cons_works'][t]['cod_tratto'].strip()),
                                                        int(data[i]['cons_works'][t]['pos'])
                                                    ))
                                                    except Exception as e:
                                                        check=1
                                                        logger.error('Problema rimozione anomalie')
                                                    
                                                
                                                insert_cons='''INSERT INTO UNIOPE.CONSUNT_EKOVISION_SPAZZAMENTO 
                                                (ID_RECORD,
                                                 ID_SCHEDA, DATA_ESECUZIONE_PREVISTA, CODICE_SERV_PRED,
                                                COD_TRATTO, POSIZIONE, CAUSALE,
                                                QUALITA, NOTE, TOTEM,
                                                RIPROGRAMMATO) 
                                                VALUES
                                                (UNIOPE.CONSUNT_EKOVISION_SPAZZ_SEQ.NEXTVAL,
                                                :c1, :c2, :c3,
                                                :c4, :c5, :c6,  
                                                :c7, :c8, :c9,
                                                :c10
                                                )'''
                                                
                                                try:
                                                    cur.execute(insert_cons, (
                                                        data[i]['id_scheda'], data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                        int(data[i]['cons_works'][t]['cod_tratto'].strip()), int(data[i]['cons_works'][t]['pos']), causale,
                                                        qualita, note, totem,
                                                        riprogrammato
                                                    ))
                                                except Exception as e:
                                                    check=1
                                                    logger.error(insert_cons)
                                                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}, 8:{}, 9:{}, 10:{}'.format(
                                                        data[i]['id_scheda'], data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                        int(data[i]['cons_works'][t]['cod_tratto'].strip()), int(data[i]['cons_works'][t]['pos']), causale,
                                                        qualita, note, totem,
                                                        riprogrammato
                                                    ))
                                                    logger.error(e) 
                                                
                                                
                                                
                                                
                                            elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC' or data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                #logger.debug('Consuntivazione raccolta')
                                                
                                                
                                                # gestione anomalie causali
                                                if check_cons==1:
                                                    insert_anom='''INSERT INTO UNIOPE.EKOVISION_ANOMALIE_CAUSALI 
                                                    (ID_SCHEDA, COD_COMPONENTE, POS, FILENAME)
                                                    VALUES
                                                    (:a1, :a2, :a3, :a4)'''
                                                    try:
                                                        cur.execute(insert_anom, (
                                                        data[i]['id_scheda'], 
                                                        int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                        int(data[i]['cons_works'][t]['pos']), 
                                                        filename
                                                    ))
                                                    except Exception as e:
                                                        check=1
                                                        logger.error('Problema inserimeno anomalie')
                                                        
                                                else: 
                                                    delete_anom='''DELETE 
                                                    FROM UNIOPE.EKOVISION_ANOMALIE_CAUSALI 
                                                    WHERE ID_SCHEDA = :a1 AND
                                                    COD_COMPONENTE=:a2 AND
                                                    POS = :a3
                                                    '''
                                                    try:
                                                        cur.execute(delete_anom, (
                                                        data[i]['id_scheda'], 
                                                        int(data[i]['cons_works'][t]['cod_componente'].strip()),
                                                        int(data[i]['cons_works'][t]['pos'])
                                                    ))
                                                    except Exception as e:
                                                        check=1
                                                        logger.error('Problema rimozione anomalie')
                                                
                                                insert_cons='''INSERT INTO UNIOPE.CONSUNT_EKOVISION_RACCOLTA 
                                                (ID_SCHEDA, DATA_ESECUZIONE_PREVISTA, CODICE_SERV_PRED,
                                                COD_COMPONENTE, POSIZIONE, CAUSALE,
                                                NOTE, TOTEM, RIPROGRAMMATO) 
                                                VALUES
                                                (
                                                :c1, :c2, :c3,
                                                :c4, :c5, :c6,  
                                                :c7, :c8, :c9
                                                )'''
                                                
                                                try:
                                                    cur.execute(insert_cons, (
                                                        data[i]['id_scheda'], data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                        int(data[i]['cons_works'][t]['cod_componente'].strip()), int(data[i]['cons_works'][t]['pos']), causale,
                                                        note, totem, riprogrammato
                                                    ))
                                                except Exception as e:
                                                    check=1
                                                    logger.error(insert_cons)
                                                    logger.error('1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}, 7:{}, 8:{}, 9:{}, 10:{}'.format(
                                                        data[i]['id_scheda'], data[i]['data_esecuzione_prevista'], data[i]['codice_serv_pred'], 
                                                        int(data[i]['cons_works'][t]['cod_componente'].strip()), int(data[i]['cons_works'][t]['pos']), causale,
                                                        note, totem, riprogrammato
                                                    ))
                                                    logger.error(e)
                                                                
                        
                                                            
                                                            
                                                            
                                                
                                            else:
                                                check=1
                                                logger.error('PROBLEMA CONSUNTIVAZIONE')
                                                logger.error('File:{}'.format(filename))
                                                logger.error('Mi sono fermato alla riga {}'.format(i))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                        else:
                                            logger.debug('Tappa non prevista e non effettuata')
                                        t+=1
                                        con.commit()
                                    
                                    
                                    
                                    
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
                    
                       
                    insert_log='''INSERT INTO UNIOPE.EKOVISION_LETTURA_CONSUNT (FILENAME, ERROR) SELECT :c1, :c2
                    FROM DUAL 
                    WHERE NOT EXISTS (SELECT 1 FROM UNIOPE.EKOVISION_LETTURA_CONSUNT WHERE FILENAME = :c3)'''
                    try:
                        cur.execute(insert_log, (
                            filename, check, filename
                        ))
                    except Exception as e:
                        logger.error(insert_log)
                        logger.error('1:{}, 2:{} 3:{}'.format(
                            filename, check, filename
                        ))
                        logger.error(e)
                    
                    update_log = '''UPDATE UNIOPE.EKOVISION_LETTURA_CONSUNT 
                    SET DA_RIPROCESSARE = NULL 
                    WHERE FILENAME = :c1 '''
                    try:
                        cur.execute(update_log, (
                            filename,
                        ))
                    except Exception as e:
                        logger.error(update_log)
                        logger.error('1:{}'.format(
                            filename
                        ))
                        logger.error(e)
                    con.commit()
                    #exit()
                    
                    os.remove(path + "/eko_output2/" + filename)
                    
                    
                    
                    
                    
                #else: 
                #    logger.info('Non scarico il file {} perchè già letto e processato'.format(filename))
        
        
        
        
        
        
        
        
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
    