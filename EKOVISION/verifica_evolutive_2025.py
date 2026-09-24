#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa di verificare se a valle delle modifiche fatte nell'ambito delle evolutive 2025, 
torna il conteggio delle componeneti attive per un certo servizio a una certa data

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

from collections import defaultdict

import locale

import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


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


    

    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    # variabile di per l'invio mail 
    check_debug=1
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    mese_anno_oggi=oggi.strftime('%Y%m')
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    check=0
    
    chiusura_ok = 0 # se rimane 0 vuole dire che è tutto chiuso
    
    # cerco il primo mese da controllare
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()

    data_eko = '20231120'
    data_fine = '20260109'

    data_corrente = datetime.strptime(data_eko, '%Y%m%d')
    data_fine_dt = datetime.strptime(data_fine, '%Y%m%d')
    
    query_spazz = '''
        with check_spazz as (
            SELECT
                peir.codice_modello_servizio,
                COUNT(peir.*) AS num_record,
                'eko' as origine
            FROM marzocchir.post_estrazione_itinerari_all peir
            WHERE to_date(%s, 'YYYYMMDD') BETWEEN
                TO_DATE(peir.data_inizio::text, 'YYYYMMDD')
            AND TO_DATE(peir.data_fine::text, 'YYYYMMDD')
            and peir.wk_cod_ext_1 <> '' and cod_tratto_amiu <> ''
            GROUP BY peir.codice_modello_servizio
            union
            SELECT
                vina.codice_modello_servizio,
                COUNT(vina.*) AS num_record,
                'amiu' as origine
            FROM marzocchir.variazioni_itinerari_new_amiu vina 
            WHERE to_date(%s, 'YYYYMMDD') BETWEEN
                TO_DATE(vina.data_inizio::text, 'YYYYMMDD')
            AND TO_DATE(vina.data_fine::text, 'YYYYMMDD')
            and vina.codice_modello_servizio in (select distinct peir.codice_modello_servizio 
                                                FROM marzocchir.post_estrazione_itinerari_all peir
                                                WHERE peir.cod_tratto_amiu <> ''
                                                )
            GROUP BY vina.codice_modello_servizio 
        ) select * from check_spazz 
        order by 1, 3
    '''
    
    

    query_racc = '''
            with check_racc as (
                SELECT
                    peir.codice_modello_servizio,
                    COUNT(peir.*) AS num_record,
                    'eko' as origine
                FROM marzocchir.post_estrazione_itinerari_all peir
                WHERE to_date(%s, 'YYYYMMDD') BETWEEN
                    TO_DATE(peir.data_inizio::text, 'YYYYMMDD')
                AND TO_DATE(peir.data_fine::text, 'YYYYMMDD')
                 and peir.wk_cod_ext_1 <> '' and peir.cod_ext_2 is not null 
                 and peir.cod_componente_amiu is not null
                GROUP BY peir.codice_modello_servizio
                union
                SELECT
                    vina.codice_modello_servizio,
                    COUNT(vina.*) AS num_record,
                    'amiu' as origine
                FROM marzocchir.variazioni_itinerari_new_amiu vina 
                WHERE to_date(%s, 'YYYYMMDD') BETWEEN
                    TO_DATE(vina.data_inizio::text, 'YYYYMMDD')
                AND TO_DATE(vina.data_fine::text, 'YYYYMMDD')
                and vina.codice_modello_servizio in (select distinct peir.codice_modello_servizio 
                                                    FROM marzocchir.post_estrazione_itinerari_all peir
                                                    WHERE peir.cod_componente_amiu is not null
                                                    )
                GROUP BY vina.codice_modello_servizio 
            ) select * from check_racc 
            order by 1, 3
        '''


    query_presenza_percorsi = '''
        SELECT
        vina.codice_modello_servizio,
        COUNT(vina.*) AS num_record,
        'amiu' as origine, vina.codice_tipo_servizio 
    FROM marzocchir.variazioni_itinerari_new_amiu vina 
    WHERE to_date(%s, 'YYYYMMDD') BETWEEN
        TO_DATE(vina.data_inizio::text, 'YYYYMMDD')
    AND TO_DATE(vina.data_fine::text, 'YYYYMMDD')
    and vina.codice_modello_servizio not in (
    select distinct peia.codice_modello_servizio  from marzocchir.post_estrazione_itinerari_all peia 
    where to_date(%s, 'YYYYMMDD') BETWEEN
    TO_DATE(peia.data_inizio::text, 'YYYYMMDD')
    AND TO_DATE(peia.data_fine::text, 'YYYYMMDD')
    and peia.wk_cod_ext_1 <> '' )
    GROUP BY vina.codice_modello_servizio, 
    vina.codice_tipo_servizio
    '''
    
    
    while data_corrente <= data_fine_dt:

        data_str = data_corrente.strftime('%Y%m%d')

        
        logger.info('******************************************************')
        logger.info(f'Verifico che ci siano tutti i percorsi -- Data {data_corrente}')
        # Query raccolta
        curr.execute(query_presenza_percorsi, (data_str, data_str))
        risultati_anomali = curr.fetchall()
        
        if len(risultati_anomali)>0:
            logger.error(f'Il giorno {data_corrente} mancano dei percorsi su Ekovision')
            for r in risultati_anomali: 
                logger.error(r[0]) 
        
        # Query spazzamento
        curr.execute(query_spazz, (data_str, data_str))
        risultati_spazz = curr.fetchall()

        #logger.info(risultati_spazz)
        #i=1
        check_amiu = 0
        #check_eko = 0
        logger.info('******************************************************')
        logger.info(f'IGIENE - GESTIONE TRATTI -- Data {data_corrente}')
        for rs in risultati_spazz:
            #logger.info(rs)
            #if i%2 >0:
            if check_amiu == 0:
                if rs[2]!= 'amiu':
                    logger.error('problema sul giorno {} per il percorso {} che non è presente nell\'anagrafica percorsi di amiu'.format(data_corrente, rs[0]))
                else:
                    check_amiu = 1
                    cod_percorso_amiu = rs[0]
                    num_tratti_amiu = rs[1]
            #else:
            elif check_amiu == 1:
                if rs[2]!= 'eko':
                    logger.error('problema sul giorno {} per il percorso {}, non c\'è cod percorso per eko'.format(data_corrente, cod_percorso_amiu))
                    cod_percorso_amiu = rs[0]
                    num_tratti_amiu = rs[1]
                    #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                else:
                    if rs[0] == cod_percorso_amiu:
                        num_tratti_eko = rs[1]
                        if num_tratti_amiu != num_tratti_eko:
                            logger.error('problema sul giorno {} per il percorso {}, numero tratti amiu diverso da quello eko'.format(data_corrente, rs[0]))
                    else:
                        logger.error('problema sul giorno {} per il percorso {}, non c\'è corrispondenza fra codice percorso amiu ({}) e codice percorso eko'.format(data_corrente, rs[0], cod_percorso_amiu))
                    check_amiu = 0
                    
                #else:
                    #logger.error('problema sul giorno {} per il percorso {}, non c\'è cod percorso per eko'.format(data_corrente, rs[0]))
                    #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    #exit()
                #i+=1

        
        #exit()
        logger.info('******************************************************')
        logger.info(f'RACCOLTA - GESTIONE COMPONENTI -- Data {data_corrente}')
        # Query raccolta
        curr.execute(query_racc, (data_str, data_str))
        risultati_racc = curr.fetchall()
        check_amiu = 0 # mi aspetto di trovare sempre prima il percorso AMIU
        i=1
        for rs in risultati_racc:
            '''if i==10: 
                exit()'''
            #logger.info(f'{i}, {rs}')
            if check_amiu == 0:
                if rs[2]!= 'amiu':
                    logger.error('problema sul giorno {} per il percorso {} che non è presente nell\'anagrafica percorsi di amiu'.format(data_corrente, rs[0]))
                else:
                    check_amiu = 1
                    cod_percorso_amiu = rs[0]
                    num_comp_amiu = rs[1]
            elif check_amiu == 1:
                if rs[2]!= 'eko':
                    logger.error('problema sul giorno {} per il percorso {} non c\'è cod percorso per eko'.format(data_corrente, cod_percorso_amiu))
                    cod_percorso_amiu = rs[0]
                    num_comp_amiu = rs[1]
                    check_amiu = 1 
                else:
                    if rs[0] == cod_percorso_amiu:
                        num_comp_eko = rs[1]
                        if num_comp_amiu != num_comp_eko:
                            logger.error('problema sul giorno {} per il percorso {}, numero componenti amiu diverso da quello eko'.format(data_corrente, rs[0]))
                    else:
                        logger.error('problema sul giorno {} per il percorso {}, non c\'è corrispondenza fra codice percorso amiu ({}) e codice percorso eko'.format(data_corrente, rs[0], cod_percorso_amiu))
                    check_amiu = 0
                    #logger.debug('Rimetto a 0 check_amiu')
                    #exit()
                    #else:
                        #logger.error('problema sul giorno {} per il percorso {}non c\'è cod percorso per eko'.format(data_corrente, rs[0]))
                        #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        #exit()
            i+=1
        




        logger.info('tutto ok per il giorno {}'.format(data_corrente))
        data_corrente += timedelta(days=1)
        
        #logger.debug(versioni)
        # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()      