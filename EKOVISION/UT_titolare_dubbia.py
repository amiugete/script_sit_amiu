#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberta Fagandini, Roberto Marzocchi

'''
Scopo dello script è 

Passare in rassegna tutte le schede eseguite ekovision 
nel caso in cui il percorso sia della sola rimessa , oppure ci siano più UT in visualizzazione 
verificare per la data di pianificazione quale sia l'UT titolare


Serve per il report dei pesi in cui voglio 1 e 1 sola UT titolare 

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect
from pathlib import Path

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql



import locale

import xlsxwriter

# per leggere file excel
#from python_calamine import CalamineWorkbook
import openpyxl


import psycopg2

import pyodbc

#import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging


from tappa_prevista import *

from crea_dizionario_da_query import *

import uuid


#import datetime 
from datetime import date, datetime, timedelta

import holidays



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

import time

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0
    

# da capire come gestisce datetime e date
  



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

    

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d%H%M%S')
    
    
    
    
    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
      
    # cerco da dove partire per fare il controllo delle schede eseguite ekovision
    
    cerco_max='''select max(nomefile) from consunt.last_check_ut_titolare lcut '''
    
    try:
        curr.execute(cerco_max)
        max_file=curr.fetchone()[0]
    except Exception as e:
        logger.error(cerco_max)
        logger.error(e)
    
    logger.debug('cerco_max {}'.format(max_file))
    
    
    
    select_schede=''' select see.id_scheda, 
        see.codice_serv_pred , 
        see.data_pianif_iniziale, 
        see.nomefile
        from consunt.schede_eseguite_ekovision see 
        where see.data_pianif_iniziale >= '20250101'
        and see.nomefile >= coalesce(%s, '0')
        and starts_with(see.nomefile, 'sch_lav_consuntivi_202')
        order by see.nomefile'''
    
    
    try:
        curr.execute(select_schede, (max_file,))
        schede=curr.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
    
    logger.debug('Len schede {}'.format(len(schede)))
    
    
    cerco_ut_titolare=''' 
    select 
distinct
case 
    /*percorsi dove una UT ci mette l'uomo*/
	when count(*) FILTER (WHERE id_squadra <> 15) = 1 then MAX(id_ut) FILTER (WHERE id_squadra <> 15)
	/*percorsi che hanno 1 sola ut in visualizzazione */
    when count(id_ut) = 1 then max(id_ut)
    /*percorsi della sola rimessa*/
    when count(id_ut) = 0 then -1  
    /*percorsi dove non so cosa fare --> devo calcolare UT titolare */
	else null
end
from anagrafe_percorsi.percorsi_ut pu 
where pu.cod_percorso = %s 
and to_date(%s, 'YYYYMMDD') 
between pu.data_attivazione and pu.data_disattivazione 
and id_ut not in (16, 17)'''




    trovo_ut_titolare='''
    with tappe as (
	SELECT codice_modello_servizio, ordine, objecy_type, 
	    codice, quantita, lato_servizio, percent_trattamento,frequenza,
	    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
	    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
	    id_asta_percorso, id_elemento_asta_percorso
	FROM anagrafe_percorsi.v_percorsi_elementi_tratti where codice_modello_servizio = %s and 
	data_inizio < coalesce(data_fine, '20991231')
	union 
	SELECT codice_modello_servizio, ordine, objecy_type, 
	    codice, quantita, lato_servizio, percent_trattamento,frequenza,
	    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
	    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
	    id_asta_percorso, id_elemento_asta_percorso
	FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where codice_modello_servizio = %s and 
	data_inizio < coalesce(data_fine, '20991231')
	union 
	SELECT codice_modello_servizio, ordine, objecy_type, 
	    codice, quantita, lato_servizio, percent_trattamento,frequenza,
	    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
	    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
	    id_asta_percorso, id_elemento_asta_percorso
	FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where codice_modello_servizio = %s and 
	data_inizio < coalesce(data_fine, '20991231')
    )
    select sum(
    case
        when treg_eko.verify_daily_frequency(
            t.frequenza, 
            %s, 
            (select freq_settimane from anagrafe_percorsi.elenco_percorsi ep 
            where cod_percorso = %s
            and to_date(%s, 'YYYYMMDD') between ep.data_inizio_validita and ep.data_fine_validita - interval '1' day 
            )
        ) = 1
        then te.volume 
        else 0.001 /*nei casi di percorso non in frequenza considero comunque un piccolo volume su cui calcolare ut titolare*/ 
    end
    ), 
    id_ut
    from tappe t 
    join
    (select id_elemento, id_piazzola, tipo_elemento, id_asta  from elem.elementi
    union all 
    select id_elemento, id_piazzola, tipo_elemento, id_asta  from history.elementi)
    e 
    on t.codice = e.id_elemento
    join elem.tipi_elemento te on te.tipo_elemento = e.tipo_elemento 
    left join elem.piazzole p on p.id_piazzola = e.id_piazzola 
    join 
    (select id_asta, id_ut from elem.aste 
    union all 
    select id_asta, id_ut from history.aste 
    ) a on a.id_asta = coalesce(p.id_asta, e.id_asta)
    where %s>=  t.data_inizio 
    and %s < t.data_fine 
    group by id_ut
    order by 1 desc limit 1'''


    update_ut_titolare='''update consunt.schede_eseguite_ekovision
    set id_ut = %s where id_scheda = %s'''
    
    

    max_nome_file = ''
    for ss in schede:
        id_scheda=ss[0]
        codice_serv_pred=ss[1]
        data_pianif_iniziale=ss[2]
        max_nome_file = ss[3]
        
        #logger.debug('id_scheda {} codice_serv_pred {} data_pianif_iniziale {} nomefile {}'.format(id_scheda, codice_serv_pred, data_pianif_iniziale, nomefile))
        # cerco l'UT titolare per il percorso
        try:
            curr.execute(cerco_ut_titolare, (codice_serv_pred, data_pianif_iniziale))
            ut_titolare=curr.fetchone()[0]
        except Exception as e:
            logger.error(cerco_ut_titolare)
            logger.error(e)
        
        if ut_titolare is None:
            logger.info(f'''Cerco UT titolare per 
                        il percorso {codice_serv_pred} del {data_pianif_iniziale} 
                        (id_scheda {id_scheda}''')
            # trovo l'UT titolare calcolando i volumi di lavoro per ciascuna UT e prendendo quella con il volume maggiore
            try:
                curr.execute(trovo_ut_titolare, (codice_serv_pred, 
                                                 codice_serv_pred, 
                                                 codice_serv_pred, 
                                                 data_pianif_iniziale, 
                                                 codice_serv_pred, 
                                                 data_pianif_iniziale,
                                                 data_pianif_iniziale,
                                                 data_pianif_iniziale))
                
                ut_titolare_tmp=curr.fetchone()
                if ut_titolare_tmp is not None:
                    ut_titolare=ut_titolare_tmp[1]
                else:
                    ut_titolare=None
            except Exception as e:
                logger.error(trovo_ut_titolare)
                logger.error(e)
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            # a questo punto dovrei fare update 
            if ut_titolare is not None:
                try:
                    curr.execute(update_ut_titolare, (ut_titolare, id_scheda))
                    # il commit lo faccio alla fine per fare un unico commit            
                except Exception as e:
                    logger.error(update_ut_titolare)
                    logger.error(e)    
    
    
    insert_last_check='''insert into consunt.last_check_ut_titolare(nomefile) values (%s)'''
    
    try:
        curr.execute(insert_last_check, (max_nome_file,))
    except Exception as e:
        logger.error(insert_last_check)
        logger.error(e)
    conn.commit()    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      