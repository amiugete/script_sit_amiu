#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa di copiare i dati per la consuntivazione dal DB della UO 
a quello del totem 

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
    
    # connessione al db della UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    

    # per ottimizzare l'inserimento batch
    
    from psycopg2.extras import execute_batch
    
    
    ########################################
    # RACCOLTA
    query_uo_racc = '''
    SELECT 
    ID_PERCORSO,
    DESCR_PERCORSO,
    ID_TAPPA,
    ID_SERVIZIO,
    DESCR_SERVIZIO,
    ID_UO,
    DESC_UO,
    DITTA_ESTERNA,
    ID_PIAZZOLA,
    ID_VIA,
    NOME_VIA,
    NOME_VIA_ORDINATO,
    CIVICO,
    UTENTE_POSIZIONE,
    TIPO_RIFIUTO,
    TIPO_ELEMENTO,
    NOME_ELEMENTO,
    NUM_ELEMENTI,
    ID_SQUADRA,
    DESCR_SQUADRA,
    FREQUENZA,
    COD_FREQUENZA_PERCORSO, 
    FREQ_SETTIMANE,
    COD_FREQUENZA_TRATTO,
    CRONOLOGIA,
    DATA_INIZIO,
    DATA_FINE, 
    ID_TURNO
    FROM UNIOPE.V_CONS_PERCORSI_RACCOLTA_AMIU
    '''  
    
    upsert_query_racc = '''INSERT INTO raccolta.cons_percorsi_raccolta_amiu (
        id_percorso, desc_percorso, id_tappa, 
        id_servizio, desc_servizio, id_uo,
        desc_uo, ditta_esterna, id_piazzola,
        id_via, nome_via, nome_via_ordinato,
        civico, utente_posizione, tipo_rifiuto,
        tipo_elemento, nome_elemento, num_elementi,
        id_squadra, descr_squadra, frequenza, 
        cod_frequenza_percorso, freq_settimane, cod_frequenza_tratto, 
        cronologia, data_inizio, data_fine,
        data_inserimento, id_turno) 
        values 
        (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            now(), %s
        ) 
         ON CONFLICT (id_tappa, id_uo) 
         DO UPDATE  SET 
         id_percorso=EXCLUDED.id_percorso,
         desc_percorso=EXCLUDED.desc_percorso,
         id_servizio=EXCLUDED.id_servizio,
         desc_servizio=EXCLUDED.desc_servizio,
         id_uo=EXCLUDED.id_uo,
         desc_uo=EXCLUDED.desc_uo,
         ditta_esterna=EXCLUDED.ditta_esterna,
         id_piazzola=EXCLUDED.id_piazzola,
         id_via=EXCLUDED.id_via,
         nome_via=EXCLUDED.nome_via,
         nome_via_ordinato=EXCLUDED.nome_via_ordinato,
         civico=EXCLUDED.civico, 
         utente_posizione=EXCLUDED.utente_posizione, 
         tipo_rifiuto=EXCLUDED.tipo_rifiuto, 
         tipo_elemento=EXCLUDED.tipo_elemento,
         nome_elemento=EXCLUDED.nome_elemento,
         num_elementi=EXCLUDED.num_elementi,
         id_squadra=EXCLUDED.id_squadra,
         descr_squadra=EXCLUDED.descr_squadra,
         frequenza=EXCLUDED.frequenza,
         cod_frequenza_percorso=EXCLUDED.cod_frequenza_percorso,
         freq_settimane=EXCLUDED.freq_settimane,
         cod_frequenza_tratto=EXCLUDED.cod_frequenza_tratto,
         cronologia=EXCLUDED.cronologia,
         data_inizio=EXCLUDED.data_inizio,
         data_fine=EXCLUDED.data_fine,
         data_inserimento=EXCLUDED.data_inserimento, 
         id_turno=EXCLUDED.id_turno;'''
    
    
    logger.info("Recupero dati raccolta da UO")    
    try:
        cur.execute(query_uo_racc)
        risultati_uo = cur.fetchall()
    except Exception as e:
        logger.error(query_uo_racc)
        logger.error(e) 
        
        
    
    records = []
    
    for rs in risultati_uo:
        records.append((
            rs[0], rs[1], rs[2],
            rs[3], rs[4], rs[5],
            rs[6], rs[7], rs[8],
            rs[9], rs[10], rs[11],
            rs[12], rs[13], rs[14],
            rs[15], rs[16], rs[17],
            rs[18], rs[19], rs[20],
            rs[21], rs[22], rs[23],
            rs[24], rs[25], rs[26],
            rs[27]
        ))
        
    logger.info("Scrivo dati raccolta su {}".format(nome_db))
    execute_batch(curr, upsert_query_racc, records, page_size=1000)
    logger.info("Faccio commit dei dati raccolta su {}".format(nome_db))
    conn_c.commit()   
    logger.info("Scrittura dati raccolta su {} completata".format(nome_db))   
    
    
    
    
    
    
    
    ########################################
    # SPAZZAMENTO
    
    
    query_uo_spazz = '''select min(id_macro_tappa) id_tappa_raggr,
	       min(cronologia) cronologia, 
	       id_percorso, desc_percorso, id_servizio, desc_servizio, id_uo, desc_uo, id_turno,
	       id_via, nome_via, nome_via_ordinato, nota_via, 
	       /*lun, mar, mer, gio, ven,sab,dom, */
	       data_inizio, data_fine,
	       PR_GET_DESC_FREQ(frequenza) giorni, 
	       cod_frequenza_percorso, 
	       FREQ_SETTIMANE, cod_frequenza_tratto 
	from (                                 
	select cpvt.id_percorso, ASPU.DESCRIZIONE desc_percorso, aspu.id_servizio, ase.desc_servizio, aspu.id_turno, aspu.id_uo, UO.DESC_UO, 
	       cpvt.id_via, s.nome2 nome_via, s.nome1 nome_via_ordinato,CMT.NOTA_VIA, cmt.id_macro_tappa,  cmt.frequenza,
	       pr_check_frequenza_giorno(cmt.frequenza, 1) lun,
	       pr_check_frequenza_giorno(cmt.frequenza, 2) mar,
	       pr_check_frequenza_giorno(cmt.frequenza, 3) mer,
	       pr_check_frequenza_giorno(cmt.frequenza, 4) gio,
	       pr_check_frequenza_giorno(cmt.frequenza, 5) ven,
	       pr_check_frequenza_giorno(cmt.frequenza, 6) sab,
	       pr_check_frequenza_giorno(cmt.frequenza, 7) dom,
	       fds.COD_FREQUENZa AS cod_frequenza_percorso, 
	       aspu.FREQ_SETTIMANE, fds1.COD_FREQUENZA AS cod_frequenza_tratto,
	       cpvt.cronologia, 
	       /* Modifica 11/06/2024 per gestione stagionali prima di data di attivazione */
		   CASE 
	       	WHEN aps.cod_percorso IS NOT NULL 
	       	AND (SELECT max(dta_attivazione) FROM ANAGR_SER_PER_UO aspu WHERE id_percorso = aps.cod_percorso) > sysdate
	       	THEN aspu.dta_attivazione
	       	ELSE 
	       	cpvt.data_prevista 
	       END AS data_inizio, 
	       /* cpvt.data_prevista data_inizio,*/
	       vp.data_fine_validita data_fine
	from pr_validita_percorsi vp
	inner join cons_percorsi_vie_tappe cpvt
	on VP.ID_PERCORSO = cpvt.id_percorso
	and vp.data_prevista = cpvt.data_prevista
	inner join cons_macro_tappa cmt
	on CMT.ID_MACRO_TAPPA = cpvt.id_tappa
	inner join anagr_ser_per_uo aspu
	on aspu.id_percorso = cpvt.id_percorso
	and vp.data_fine_validita between aspu.dta_attivazione+1 and aspu.dta_disattivazione
	inner join anagr_servizi ase
	on ase.id_servizio = aspu.id_servizio
	inner join anagr_uo uo
	on uo.id_uo = aspu.id_uo
	inner join strade.strade s
	on s.codice_via = cpvt.id_via
	INNER JOIN FREQUENZE_DA_SIT fds
	ON fds.freq_binaria = aspu.frequenza_new
	INNER JOIN FREQUENZE_DA_SIT fds1
	ON fds1.freq_binaria = cmt.FREQUENZA 
	/* Modifica 11/06/2024 per gestione stagionali prima di data di attivazione */
	LEFT JOIN ANAGR_PERCORSI_STAGIONALI aps 
	ON aps.cod_percorso = aspu.id_percorso
	where ase.gestito_app_spazzamento = 'S'
	and aspu.dta_disattivazione > sysdate-365
	--and CMT.FREQUENZA not like 'S%'
	--and aspu.id_percorso IN ('0203009701', '0201238701')
	)
	group by id_percorso, desc_percorso, id_servizio, desc_servizio, id_uo, desc_uo, id_turno,
	       id_via, nome_via, nome_via_ordinato, nota_via,
	       /*lun, mar, mer, gio, ven,sab,dom,*/ 
	       data_inizio, data_fine,
	       PR_GET_DESC_FREQ(frequenza), 
	       cod_frequenza_percorso, 
	       cod_frequenza_tratto, FREQ_SETTIMANE
	--order by id_percorso, frequenza_desc, nome_via_ordinato
 '''
 
 
    upsert_query_spazz = '''INSERT INTO spazzamento.cons_percorsi_spazz_x_app (
            id_tappa_raggr, cronologia, id_percorso,
            desc_percorso, id_servizio, desc_servizio,
            id_uo, desc_uo, id_via,
            nome_via, nome_via_ordinato, nota_via,
            id_turno, cod_frequenza_percorso, freq_settimane,
            cod_frequenza_tratto, data_inizio, data_fine,
            giorni
        ) 
        values 
        (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s   
        ) ON CONFLICT (id_tappa_raggr,id_uo)
        DO UPDATE  
        SET 
        cronologia=EXCLUDED.cronologia, 
        id_percorso=EXCLUDED.id_percorso, 
        desc_percorso=EXCLUDED.desc_percorso, 
        id_servizio=EXCLUDED.id_servizio, 
        desc_servizio=EXCLUDED.desc_servizio, 
        /*id_uo=EXCLUDED.id_uo,*/
        desc_uo=EXCLUDED.desc_uo,
        id_via=EXCLUDED.id_via,
        nome_via=EXCLUDED.nome_via,
        nome_via_ordinato=EXCLUDED.nome_via_ordinato,
        nota_via=EXCLUDED.nota_via,
        id_turno=EXCLUDED.id_turno,
        cod_frequenza_percorso=EXCLUDED.cod_frequenza_percorso,
        freq_settimane=EXCLUDED.freq_settimane,
        cod_frequenza_tratto=EXCLUDED.cod_frequenza_tratto,
        data_inizio=EXCLUDED.data_inizio,
        data_fine=EXCLUDED.data_fine,
        giorni=EXCLUDED.giorni;'''
    
    logger.info("Recupero dati spazzamento da UO")
    try:
        cur.execute(query_uo_spazz)
        risultati_uo = cur.fetchall()
    except Exception as e:
        logger.error(query_uo_spazz)
        logger.error(e)   
    
    
    """
    for rs in risultati_uo:
        #logger.debug(rs)
        id_tappa_raggr=rs[0]
        cronologia=rs[1]
        id_percorso=rs[2]
        desc_percorso=rs[3]
        id_servizio=rs[4]
        desc_servizio=rs[5]
        id_uo=rs[6]
        desc_uo=rs[7]
        id_turno=rs[8]
        id_via=rs[9]
        nome_via=rs[10]
        nome_via_ordinato=rs[11]
        nota_via=rs[12]
        data_inizio=rs[13]
        data_fine=rs[14]
        giorni=rs[15]
        cod_frequenza_percorso=rs[16]
        freq_settimane=rs[17]  
        cod_frequenza_tratto=rs[18] 
        
        curr.execute(upsert_query, (id_tappa_raggr, cronologia, id_percorso,
                                    desc_percorso, id_servizio, desc_servizio,
                                    id_uo, desc_uo, id_via,
                                    nome_via, nome_via_ordinato, nota_via,
                                    id_turno, cod_frequenza_percorso, freq_settimane,
                                    cod_frequenza_tratto, data_inizio, data_fine,
                                    giorni))
    """

    records = []

    for rs in risultati_uo:
        records.append((
            rs[0], rs[1], rs[2],
            rs[3], rs[4], rs[5],
            rs[6], rs[7], rs[9],
            rs[10], rs[11], rs[12],
            rs[8], rs[16], rs[17],
            rs[18], rs[13], rs[14],
            rs[15]
        ))
    logger.info("Scrivo dati spazzamento su {}".format(nome_db))
    execute_batch(curr, upsert_query_spazz, records, page_size=1000)
    logger.info("Faccio commit dei dati spazzamento su {}".format(nome_db))
    conn_c.commit()   
    logger.info("Scrittura dati spazzamento su {} completata".format(nome_db))            
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn_c.close()
    
    


if __name__ == "__main__":
    main()      