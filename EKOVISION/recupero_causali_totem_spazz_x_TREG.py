#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa di verificare che le schede chiuse siano state effettivamente salvate 
nella tabella treg_eko.consunt_ekovision 
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

# libreria per scrivere file csv
import csv


import uuid

def norm_str(s):
    if s is None:
        return None
    s = s.strip()
    return s if s else None
    
     

def main():
      

    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    
    
    
    # connessione al SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    curr = conn.cursor()


    # connessione al DB consuntivazione totem 
    nome_db=db_consuntivazione
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    

    
    # connesione a UNIOPE
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    ####################################################################################
    anno_start = 2026 
    ####################################################################################
    
    
    
    query_tratti=f'''select 
        codice_servizio_pred, 
        data_esecuzione_prevista,
        codice, 
        qualita, 
        causale
        from treg_eko.consunt_ekovision 
        where 
        data_pianif_iniziale > '{anno_start}0101' 
        and tipo_servizio ='S' and 
        qualita > 0 and qualita < 100
        and (
            causale_totem is null
            or LENGTH(causale_totem) > 3
        )
        order by id_scheda'''
    
    
   
    query_id_via_nota='''select distinct codice_modello_servizio as cod_percorso,
        aa.id_via ,
        v.nome , 
        tab.nota, 
        min(tab.data_inizio) as data_inizio,
        max(tab.data_fine) as data_fine
        from 
        (
            SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
        id_asta_percorso, id_elemento_asta_percorso
            FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '20991231')
            union 
            SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
        id_asta_percorso, id_elemento_asta_percorso
            FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '20991231')
            union 
            SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
        id_asta_percorso, id_elemento_asta_percorso
            FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio < coalesce(data_fine, '20991231')
        ) tab
        left join (select id_asta, id_via from elem.aste
        union 
        select id_asta, id_via from history.aste) aa
            on aa.id_asta = tab.codice
        left join topo.vie v on v.id_via = aa.id_via 
        left join topo.comuni c on c.id_comune = v.id_comune 
        left join anagrafe_percorsi.elenco_percorsi ep2 
        on ep2.cod_percorso = tab.codice_modello_servizio 
        and to_date(%s, 'YYYYMMDD') between ep2.data_inizio_validita and ep2.data_fine_validita
        where
        tab.codice = %s
        and codice_modello_servizio = %s
        and %s between tab.data_inizio and tab.data_fine
        group by codice_modello_servizio,
        tab.codice,
        aa.id_via ,
        v.nome , 
        c.cod_istat, 
        tab.nota'''
    
    
    
    """
    query_consuntivazione='''select ve.id_causale from spazzamento.v_effettuati ve 
        left join spazzamento.cons_percorsi_spazz_x_app cpsxa on ve.tappa = cpsxa.id_tappa_raggr 
        where ve.datalav = to_date(%s, 'YYYYMMDD')
        and  punteggio::int = %s
        and ve.idpercorso = %s
        and cpsxa.id_via = %s
        and trim(cpsxa.nota_via) = %s '''
    """
    
    
    query_consuntivazione = '''
        SELECT ve.id_causale
        FROM spazzamento.v_effettuati ve 
        LEFT JOIN spazzamento.cons_percorsi_spazz_x_app cpsxa 
            ON ve.tappa = cpsxa.id_tappa_raggr 
        WHERE ve.datalav = to_date(%s, 'YYYYMMDD')
        AND punteggio::int = %s
        AND ve.idpercorso = %s
        AND cpsxa.id_via = %s
        AND (
                ( %s IS NULL 
                AND NULLIF(TRIM(cpsxa.nota_via), '') IS NULL
                )
            OR ( TRIM(cpsxa.nota_via) = %s )
            )
        '''
    
        
    query_consuntivazione_uo = """
        SELECT *
        FROM UNIOPE.CONSUNT_SPAZZAMENTO_DA_APP_EKO csdae
        LEFT JOIN UNIOPE.CONS_MACRO_TAPPA cmt 
            ON cmt.id_MACRO_TAPPA = csdae.ID_TAPPA
        WHERE DATA_CONS = to_date(:data_cons, 'YYYYMMDD')
        AND id_percorso = :id_percorso
        AND id_via = :id_via
        AND QTA_SPAZZATA = :qta_spazzata
        AND (
                ( :nota_via IS NULL 
                AND NULLIF(TRIM(cmt.NOTA_VIA), '') IS NULL
                )
            OR ( TRIM(cmt.NOTA_VIA) = :nota_via )
            )
            """




        
    
    
    query_update_causale='''UPDATE treg_eko.consunt_ekovision
        set causale_totem = %s
        where tipo_servizio ='SPAZZ' and 
        data_esecuzione_prevista = %s
        and codice_servizio_pred = %s
        and codice = %s and qualita = %s'''
        
    
    
    
    
    
    # cerco i tratti da correggere
    try:
        curr.execute(query_tratti)
        tratti_su_eko=curr.fetchall()
    except Exception as e:
        logger.error(query_tratti)
        logger.error(e)
    
    
    # ciclo sui tratti da correggere
    for tt in tratti_su_eko:
        
        logger.debug(f'''Cerco vie e note per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]}''')
        try:
            curr.execute(query_id_via_nota, (tt[1], tt[2], tt[0], tt[1],))
            tratti_sit=curr.fetchall()
        except Exception as e:
            logger.error(query_id_via_nota)
            logger.error(f'''asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]}''')
            logger.error(e)
        
        for vv in tratti_sit:
            currc = connc.cursor()
            logger.debug(f'''Cerco consuntivazione per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
            
            
            nota_via_param_sit = norm_str(vv[3])

            params_sit = (
                tt[1],         # 'YYYYMMDD'
                tt[3],         # qualita
                tt[0],         # id_percorso
                vv[1],          #id_via,
                nota_via_param_sit,   # per %s IS NULL
                nota_via_param_sit    # per confronto
            )

            
            try:
                currc.execute(query_consuntivazione, params_sit)
                consuntivazione=currc.fetchall()
            except Exception as e:
                logger.error(query_consuntivazione)
                logger.error(f'''asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                logger.error(e)
            
            
            
            if len(consuntivazione)==0:
                logger.warning(f'''Non trovo consuntivazione per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                #logger.debug(consuntivazione)
                # dovrebbe succedere solo prima di maggio, ma nel caso vado a recuperare la causale dalla UO dove però non ci salvavamo le note via
                # quindi rischio di prendere la causale sbagliata se ci sono vie con lo stesso
                
                
                nota_via_param = vv[3].strip() if vv[3] and vv[3].strip() != "" else None

                params = {
                    "data_cons": tt[1],     # stringa YYYYMMDD
                    "id_percorso": tt[0],
                    "id_via": vv[1],
                    "qta_spazzata": tt[3],
                    "nota_via": nota_via_param   # None oppure stringa vera
                }

                try:
                    cur.execute(query_consuntivazione_uo, params)
                    consuntivazione_uo=cur.fetchall()
                except Exception as e:
                    logger.error(query_consuntivazione_uo)
                    logger.error(f'''percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]} e qualita {tt[3]}''')
                    logger.error(e)
                    
                if len(consuntivazione_uo)==0:
                    logger.error(f'''Non trovo consuntivazione UO per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                    causale_recuperata=None
                else:
                    logger.debug(f'''Trovata consuntivazione UO per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                    causale_recuperata=consuntivazione_uo[0][3]
                
            # ho trovato la causale nel db del totem    
            else:
                logger.debug(f'''Trovata consuntivazione per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                causale_recuperata=consuntivazione[0][0]
                
                
                
                
            logger.debug(f'''Aggiorno causale_totem per l'asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via
                            {vv[1]} e nota {vv[3]} con causale {causale_recuperata}''')   
            
            try:
                curr.execute(query_update_causale, (causale_recuperata, tt[1], tt[0], tt[2], tt[3],))
                conn.commit()
            except Exception as e:
                logger.error(query_update_causale)
                logger.error(f'''asta {tt[2]} del percorso {tt[0]} del giorno {tt[1]} con id_via {vv[1]} e nota {vv[3]}''')
                logger.error(e)
            
            
            #exit()
    
   
        
         
    #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()      