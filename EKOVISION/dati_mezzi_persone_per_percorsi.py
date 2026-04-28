#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberta Fagandini

'''
Script creato il 23/04/2026 in sostituzione alla trasformazione dati_servizi_percorsi (job notturno di ekovision)

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import requests
from requests.exceptions import HTTPError

import json


import xlsxwriter

import psycopg2

from psycopg2.extras import execute_values

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
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

  
     

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # Mi connetto a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

 
    query= '''
    select pu.cod_percorso, ep.versione_testata, 
  	pu.id_squadra, sc.id_qualifica, 
  	aq.cod_postoorg, sc.quantita, 
 	pu.rimessa, pu.id_ut, 
    ep.data_inizio_validita, ep.data_fine_validita
  	from anagrafe_percorsi.percorsi_ut pu
	join anagrafe_percorsi.elenco_percorsi ep on pu.cod_percorso = ep.cod_percorso and ep.data_fine_validita = pu.data_disattivazione
	left join anagrafe_percorsi.squadre_composizione sc on sc.id_squadra = pu.id_squadra
	left join anagrafe_percorsi.anagr_qualifiche aq on aq.id_qualifica = sc.id_qualifica 
	where pu.id_squadra  <> 15 
	group by pu.cod_percorso , ep.versione_testata, pu.id_squadra, sc.id_qualifica, aq.cod_postoorg, sc.quantita, pu.rimessa, pu.id_ut,
    ep.data_inizio_validita, ep.data_fine_validita
	order by pu.cod_percorso , ep.versione_testata, pu.rimessa desc /* ordinamenteo per avere prima autisti di rimessa*/, 
    aq.cod_postoorg desc /*oridinamento per avere prima gli aura*/
    '''

    query_mezzi = '''select * from anagrafe_percorsi.percorsi_mezzi
    where cod_percorso = %s and versione = %s
    '''

    query_insert = '''
    INSERT INTO etl.persone_mezzi_ekovision
    (cod_percorso, versione, id_qualifica, cod_categoria, flg_autista, cod_mezzo, id_uo, count_ps, data_attivazione, data_disattivazione)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (cod_percorso, versione, id_qualifica, cod_mezzo, id_uo, count_ps)
    /* or you may use [DO NOTHING;] */
    DO UPDATE 
    SET cod_categoria=EXCLUDED.cod_categoria, flg_autista=EXCLUDED.flg_autista, 
    data_attivazione=EXCLUDED.data_attivazione, data_disattivazione=EXCLUDED.data_disattivazione;
    '''

    try:
        curr.execute(query)
        percorsi_mezzi=curr.fetchall()
    except Exception as e:
        logger.error(query)
        logger.error(e)

    # creo una lista di tuple formate da codice percorso e versione
    percorsi = []
    
    logger.info('processo {} percorsi'.format(len(percorsi_mezzi)))
    for pm in percorsi_mezzi:
        logger.info('processo percorso {0} versione {1}'.format(pm[0], pm[1]))
        # per ogni percorso e versione verifico se ho già processato una riga con lo stesso percorso e versione (es. autista di rimessa), 
        # se no la aggiungo alla lista dei percorsi processati e q= 0, altrimenti no e allora definisco qinitq = q dove q è = al valore definito al passo precedente
        # con q e q_init gestisco poi l'inserimento del valore correttop nella colonna count_ps
        if (pm[0], pm[1]) not in percorsi:
            percorsi.append((pm[0], pm[1]))
            q = 0
            q_init = 0
        else:
            q_init = q
        
        logger.debug('q_init è {0}'.format(q_init))
        logger.debug('q è {0}'.format(q))

        persone_mezzi = []
        while q < pm[5]+q_init:
            try:
                curr.execute(query_mezzi, (pm[0], pm[1],))
                mezzi=curr.fetchall()
            except Exception as e:
                logger.error(query_mezzi)
                logger.error(e)


            count_mezzi = len(mezzi)
            logger.debug('il percorso {0} versione {1} ha {2} mezzi'.format(pm[0], pm[1], count_mezzi))
            
            if q < count_mezzi:
                flg_autista = True if mezzi[q][2] is not None else False
                if count_mezzi > 1:
                    cod_mezzo = mezzi[q][2] if mezzi[q][2] is not None else '9999'
                else:
                    cod_mezzo = mezzi[0][2] if mezzi[0][2] is not None else '9999'
            else:
                flg_autista = False

            
            #logger.info('percorso {0} versione {1} ha {2} persone mezzi'.format(pm[0], pm[1], len(persone_mezzi)))
            #faccio insert massivo della lista persone_mezzi ma leggendola al contrario
            try:
                #importato da psycopg2.extras import
                 #execute_values(curr, query_insert, reversed(persone_mezzi))
                curr.execute(query_insert, (pm[0], pm[1], pm[3], pm[4], flg_autista, cod_mezzo, pm[7], q+1, pm[8], pm[9]))
            except Exception as e:
                logger.error(query_insert)
                logger.error(e)

            q+=1


    logger.info('faccio commit')
    conn.commit()

    logger.info("chiudo le connessioni in maniera definitiva")

    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      