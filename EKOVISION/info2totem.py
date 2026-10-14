#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa di copiare i dati del carburante dal DB di InfoPM a quello del totem 

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect


from datetime import date, datetime, timedelta

from collections import defaultdict



import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *





#import requests

import logging


filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)









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

# libreria per scrivere file csv
import csv



    
     

def main():
      

    
    try:
        if sys.argv[1]== 'prod':
            test=0
        elif sys.argv[1]== 'test':
            test=1
        else: 
            print('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        test=1
    
    
    
    path=os.path.dirname(sys.argv[0]) 
    nome=os.path.basename(__file__).replace('.py','')
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    if test==0:
        logfile='{0}/log/{1}.log'.format(path,nome)
        errorfile='{0}/log/error_{1}.log'.format(path,nome)
    else: 
        logfile='{0}/log/{1}_test.log'.format(path,nome)
        errorfile='{0}/log/error_{1}_test.log'.format(path,nome)
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
    f_handler.setLevel(logging.INFO)


    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)


    cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    c_handler.setFormatter(cc_format)
    f_handler.setFormatter(cc_format)
    
    if test==1:
        logger.info('Ambiente di TEST')
        
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
        
    # Mi connetto al nuovo DB consuntivazione  
    if test ==1:
        nome_db= db_totem_test
    elif test==0:
        nome_db=db_totem
    else:
        logger.error(f'La variabilie test vale {test}. Si tratta di un valore anomalo. Mi fermo qua')
        exit()
        
    logger.info('Connessione al db {} su {}'.format(nome_db, host_totem))
    conn_c = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)
    
    curr = conn_c.cursor()
    
    # connessione al db INFO
    logger.info('Connessione al db INFO su {}'.format(host_infopm))
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    
    parametri_con='{}/{}@//{}:{}/{}'.format(user_infopm,pwd_infopm, host_infopm,port_infopm, service_infopm)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    # per ottimizzare l'inserimento batch
    
    from psycopg2.extras import execute_batch
    
    
    ########################################
    # RIFORNIMENTI CARBURANTE
    query_rif_infopm = '''
    WITH rifornimenti AS 
(select  a.codpompa as cod_pompa,
		decode(a.codpompa,
		'05','SCARPINO'||'_'||a.CODPOMPA ,
		'07','SCARPINO'||'_'||a.CODPOMPA,
		'VOLPARA'||'_'||a.CODPOMPA
		) AS nome_pompa ,
		to_date('18991230','YYYYMMDD') + dataora  as data_ora,
        to_date('18991230','YYYYMMDD') + data  as data,
        a.veicolo,
        --a.CODMECCA AS badge,
        b.CODMECCA AS matricola,
        a.TIPOCARBURANTE AS id_carb, 
        c.DESCRIZIONE AS tipocarb, 
        a.LITRI,
        a.CHILOMETRI, 
        a.CAUSALE 
from sav90.s_rifornimenti a
left join sav90.a_dip     b on to_number( b.coddip) = to_number(a.CODMECCA)
LEFT JOIN sav90.a_carb c ON c.COD_CARB = a.TIPOCARBURANTE 
where to_char(to_date('18991230','YYYYMMDD') + data,'YYYYMMDD') >= to_char(sysdate - 10,'YYYYMMDD')
and litri > 0
and causale <> 22 /* prove inserimenti*/
) 
SELECT * FROM rifornimenti 
    ''' 
    
    
    
     
    upsert_rifornimenti= '''INSERT INTO totem.rifornimenti_amiu (
        cod_pompa, data_ora, datalav,
        veicolo, matricola, idcarb,
        tipocarb, litri, chilometri,
        causale) 
        VALUES 
        (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s
        ) 
        ON CONFLICT (cod_pompa, data_ora) 
        DO UPDATE SET 
        datalav=EXCLUDED.datalav, veicolo=EXCLUDED.veicolo, matricola=EXCLUDED.matricola,
        idcarb=EXCLUDED.idcarb, tipocarb=EXCLUDED.tipocarb, litri=EXCLUDED.litri,
        chilometri=EXCLUDED.chilometri, causale=EXCLUDED.causale;'''
    
    
    
    
    
    
    logger.info("Recupero dati rifornimenti")    
    try:
        cur.execute(query_rif_infopm)
        risultati_uo = cur.fetchall()
    except Exception as e:
        logger.error(query_rif_infopm)
        logger.error(e) 
        
        
    
    records = []
    
    for rs in risultati_uo:
        records.append((
            rs[0], rs[2], rs[3],
            rs[4], rs[5], rs[6],
            rs[7], rs[8], rs[9],
            rs[10]
        ))
        
    logger.info("Scrivo dati rifornimenti su {}".format(nome_db))
    execute_batch(curr, upsert_rifornimenti, records, page_size=1000)
    logger.info("Faccio commit dei dati rifornimenti su {}".format(nome_db))
    conn_c.commit()   
    logger.info("Scrittura dati rifornimenti su {} completata".format(nome_db))   
    
    
               
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn_c.close()
    
    cur.close()
    con.close()
    
    


if __name__ == "__main__":
    main()      