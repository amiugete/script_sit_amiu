#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi Roberta Fagandini

'''
Lo script gestisce i percorsi stagionali


1) la partenza è la tabella elem.percorsi del SIT che per gli stagionali ha senso resti il punto di partenza 
    in quanto contiene i campi stwitch on e switch off 

NOTA c'è un job che controlla che le date di attivazione e disattivazione degli stagionali siano posticipate nel tempo


MOLTO PRIMA DELL'ATTIVAZIONE DEVO FARE QUESTE COSE:
1) aggiorno le 4 tabelle dello schema anagrafe_percorsi del SIT per creare già il record. Stesso codice, 
    ma nuova versione per mantenere tutti gli storici su Ekovision e su tutti i sistemi

2) faccio la stessa cosa su ANAGR_SER_PER_UO della UO per creare già il record


In questo modo il percorso stagionale è già presente sul DB e posso fare eventuali modifiche a descrizione, 
mezzi, turni e frequenze prima che parta


2 SETTIMANE PRIMA MANDARE NOTIFICA AL TERRITORIO: 
(TODO)


IL GIORNO PRIMA DOVREI CAMBIARE ID_CATEGORIA_USO sul SIT 
(TODO ora lo fa il job)

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
    
    
    select_stagionali = '''select p.id_percorso, p.cod_percorso, p.descrizione, u.descrizione as ut, 
    s.descrizione as servizio, to_char(data_attivazione, 'DD/MM/YYYY') as data_attivazione, 
    case 
    when data_dismissione is null then '01/12/2099'
    else to_char(data_dismissione, 'DD/MM/YYYY')
    end data_dismissione, 
    3 as attivo, 
    id_categoria_uso, 
    ep.cod_percorso
    from elem.percorsi p
    left join elem.percorsi_ut pu on pu.cod_percorso = p.cod_percorso 
    left join topo.ut u on u.id_ut = pu.id_ut and pu.responsabile = 'S'
    left join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso and ep.data_inizio_validita = p.data_attivazione
    join elem.servizi s on s.id_servizio= p.id_servizio
    where stagionalita is not null and data_attivazione > now() 
    and ep.cod_percorso is null
    and id_categoria_uso = 6
    order by p.data_attivazione '''
    
    
 
    try:
        curr.execute(select_stagionali)
        lista_stagionali=curr.fetchall()
    except Exception as e:
        logger.error(select_stagionali)
        logger.error(e)

    
    
    
    insert_percorso1 = '''INSERT INTO anagrafe_percorsi.elenco_percorsi (cod_percorso, descrizione, id_tipo, freq_testata,
        id_turno, durata, codice_cer, versione_testata,
        data_inizio_validita, data_fine_validita, freq_settimane, ekovision)
        (
            select cod_percorso, descrizione, id_tipo, freq_testata, id_turno, durata, codice_cer,
            versione_testata+1, 
            %s, %s, freq_settimane, ekovision	
            from anagrafe_percorsi.elenco_percorsi ep 
            where cod_percorso = %s
            and versione_testata = (select max(ep1.versione_testata) from anagrafe_percorsi.elenco_percorsi ep1 where ep1.cod_percorso = ep.cod_percorso)
        )''' 



    insert_percorso2 = '''INSERT INTO anagrafe_percorsi.elenco_percorsi_old (
    id_percorso_sit, cod_percorso, descrizione, id_tipo,
    freq_testata, versione_uo, data_inizio_validita, data_fine_validita) 
    (
        select %s, cod_percorso, descrizione, id_tipo,
        freq_testata, versione_uo+1, %s, %s 
        from anagrafe_percorsi.elenco_percorsi_old ep where cod_percorso = %s
        and versione_uo = (select max(ep1.versione_uo) 
        from anagrafe_percorsi.elenco_percorsi_old ep1 where ep1.cod_percorso = ep.cod_percorso)
    ) '''
    
    
    
    insert_percorso3 = '''INSERT INTO anagrafe_percorsi.percorsi_ut 
    (cod_percorso, id_ut, id_squadra, responsabile, solo_visualizzazione,
    rimessa, id_turno, durata,
    data_attivazione, data_disattivazione, cdaog3) 
    (
        select cod_percorso, id_ut, id_squadra, responsabile, solo_visualizzazione,
        rimessa, id_turno, durata,
        %s, %s, cdaog3 
        from anagrafe_percorsi.percorsi_ut pu where cod_percorso = %s
        and data_disattivazione = (select max(data_disattivazione) from anagrafe_percorsi.percorsi_ut pu1
        where pu1.cod_percorso = pu.cod_percorso)
    )  '''
    
    
    
    insert_percorso4 = '''INSERT INTO anagrafe_percorsi.date_percorsi_sit_uo 
        (id_percorso_sit, cod_percorso, data_inizio_validita, data_fine_validita)
        VALUES(%s, %s, %s, %s)'''
    
    for ls in lista_stagionali:
        # id_percorso  ls[0]
        # cod_percorso ls[1]
        # data attivazione ls[5]
        # data disattivazione ls[6]
        
        
        
        # INSERT INTO anagrafe_percorsi.elenco_percorsi
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso1, (ls[5], ls[6], ls[1]))
        except Exception as e:
            logger.error(insert_percorso1)
            logger.error(e)
        
        curr1.close()
        
        
        
        # INSERT INTO anagrafe_percorsi.elenco_percorsi_old 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso2, (ls[0], ls[5], ls[6], ls[1]))
        except Exception as e:
            logger.error(insert_percorso2)
            logger.error(e)
        
        curr1.close()
        
        
    
        
        # INSERT INTO anagrafe_percorsi.percorsi_ut 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso3, (ls[5], ls[6], ls[1]))
        except Exception as e:
            logger.error(insert_percorso3)
            logger.error(e)
        
        curr1.close()
    
    
        # anagrafe_percorsi.date_percorsi_sit_uo
        # INSERT INTO anagrafe_percorsi.elenco_percorsi_old 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso4, (ls[0], ls[1], ls[5], ls[6]))
        except Exception as e:
            logger.error(insert_percorso2)
            logger.error(e)
        
        curr1.close()
        
        
        
        conn.commit()
        
        # lanciare procedura o funzione della UO 
    
        try:
            logger.debug(ls[5])
            #exit()
            #strptime
            ret=cur.callproc('UNIOPE.ATTIVA_PERCORSI_STAGIONALI',
                    [ls[1],datetime.strptime(ls[5], '%d/%m/%Y'), datetime.strptime(ls[6], '%d/%m/%Y')])
            logger.debug(ret)
        except Exception as e:
            logger.error(e) 
    
    
    
        con.commit()
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
    