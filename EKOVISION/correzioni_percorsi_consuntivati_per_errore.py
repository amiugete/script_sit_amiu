#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Script che sistema la consuntivazione di quei percorsi stagionali che sono stati generati per errore su Ekovision e consuntivati

Input: 
- lista percorsi con errori (RACCOLTA E POI da fare per SPAZZAMENTO)

Cosa fa: 
1) crea una entry in ANAGR_SER_PER_UO attiva solo il giorno della consuntivazione 
2) fa la stessa cosa sull'anagrafica percorsi del sit
 

Output 
- lista componente, data_inizo sbagliata che è da inviare a Ekovision per eliminazione di quelle componenti (almeno per il 2025)


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


from tappa_prevista import *

from crea_dizionario_da_query import *



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
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


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
    
     

def main():
      


    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d')
    
    
    
    # credenziali WS Ekovision
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    

    
    # connessione a SIT
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
   
    
    
    
    
    query_consuntivazioni_errate='''SELECT * FROM TMP_PERC_EKOVISION_ERRORI_R'''
    try:
        cur.execute(query_consuntivazioni_errate)
        elenco_ce=cur.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(query_consuntivazioni_errate)
  
    """"
    le consuntivazioni errate sono state calcolate così
    --CREATE TABLE  TMP_PERC_EKOVISION_ERRORI_R AS         
WITH tmp_schede_percorsi AS 
        (
        SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
        FROM consunt_macro_tappa
        WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD')
        /*UNION
        SELECT ID_PERCORSO || '_' || TO_CHAR(DATA_CONS, 'YYYYMMDD') AS ID_DATA
        FROM consunt_spazzamento
        WHERE DATA_CONS >= TO_DATE('20250101', 'YYYYMMDD') */
        ) SELECT DISTINCT see.ID_SCHEDA, 
        see.CODICE_SERV_PRED AS cod_percorso, 
        see.DATA_ESECUZIONE_PREVISTA
        FROM CONSUNT_EKOVISION_RACCOLTA see
        WHERE see.DATA_ESECUZIONE_PREVISTA >= '202501'
        AND see.record_valido='S' 
        /*ecludo i riprogrammati*/
        AND see.RIPROGRAMMATO = 0
        AND NOT EXISTS (
        SELECT 1
            FROM tmp_schede_percorsi a
            WHERE a.ID_DATA = see.CODICE_SERV_PRED || '_' || see.DATA_ESECUZIONE_PREVISTA 
        )
        /*AND EXISTS 
        (SELECT 1 FROM anagr_ser_per_uo aspu 
        WHERE aspu.ID_PERCORSO = see.CODICE_SERV_PRED  
        AND aspu.dta_disattivazione >= to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') --to_date('202501', 'YYYYMM')
        --AND to_date(see.DATA_ESECUZIONE_PREVISTA, 'YYYYMMDD') BETWEEN aspu.DTA_ATTIVAZIONE AND aspu.DTA_DISATTIVAZIONE
        )*/
    
    
    """
  
    
    
    
    i=0
    for ec in elenco_ce:
        cur1 = con.cursor()
        logger.debug(i)
        logger.debug(f'id_scheda = {ec[0]}')
        logger.debug(f'cod_percorso = {ec[1]}')
        logger.debug(f'data = {ec[2]}')
        insert_uo='''INSERT INTO UNIOPE.ANAGR_SER_PER_UO 
(ID_SER_PER_UO, ID_UO,
ID_PERCORSO, ID_TURNO, 
PROG_PERCORSO, DTA_ATTIVAZIONE, DTA_DISATTIVAZIONE,
DURATA, FAM_MEZZO, 
DESCRIZIONE, ID_SERVIZIO, ID_SQUADRA,
FROM_SIT, FREQUENZA_NEW, FREQ_SETTIMANE) 
(
SELECT 
(SELECT max(id_ser_per_uo) FROM anagr_ser_per_uo aspu0)+ ROW_NUMBER() OVER (ORDER BY id_UO) AS ID_SER_PER_UO, 
aspu.id_UO, 
aspu.ID_PERCORSO,
aspu.ID_TURNO, 
aspu.prog_percorso, 
to_date(:d1, 'YYYYMMDD') AS data_attivazione, 
to_date(:d2, 'YYYYMMDD')+1 AS DTA_DISATTIVAZIONE,
aspu.DURATA,
aspu.FAM_MEZZO, 
aspu.DESCRIZIONE,
aspu.ID_SERVIZIO,
aspu.ID_SQUADRA,
aspu.FROM_SIT, 
aspu.FREQUENZA_NEW,
aspu.FREQ_SETTIMANE
FROM ANAGR_SER_PER_UO aspu WHERE aspu.ID_PERCORSO = :d3
AND aspu.DTA_DISATTIVAZIONE = 
(SELECT max(DTA_DISATTIVAZIONE) FROM ANAGR_SER_PER_UO aspu1 
WHERE  aspu1.ID_PERCORSO = aspu.ID_PERCORSO 
AND aspu1.DTA_DISATTIVAZIONE < to_date(:d4, 'YYYYMMDD')
)
)'''
        try: 
            cur1.execute(insert_uo, (ec[2], ec[2], ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(insert_uo)
        con.commit()
        
        cur1.close()
        
        
        
        #############################################
        # ora devo fare SIT
        
        # elenco_percorsi
        insert1='''INSERT INTO anagrafe_percorsi.elenco_percorsi 
(cod_percorso, descrizione, id_tipo,
freq_testata, id_turno, durata,
codice_cer, versione_testata, 
data_inizio_validita, data_fine_validita, data_fine_ekovision,
data_ultima_modifica, freq_settimane, ekovision,
stagionalita, ddmm_switch_on, ddmm_switch_off, giorno_competenza) 
/**/
(
	select 
	cod_percorso, 
	descrizione, 
	id_tipo, 
	freq_testata,
	id_turno,
	durata,
	codice_cer,
	versione_testata+1 as versione_testata, 
	to_date(%s, 'YYYYMMDD') AS data_inizio_validita, 
	to_date(%s, 'YYYYMMDD')+1 AS data_fine_validita,
	to_date(%s, 'YYYYMMDD')+1 AS data_fine_ekovision,
	now() as data_ultima_modifica,  
	freq_settimane, 
	'f' as ekovision, /* non lo trasmetto ad eko per evitare casini*/
	stagionalita,
	ep.ddmm_switch_on,
	ep.ddmm_switch_off,
	ep.giorno_competenza
	from anagrafe_percorsi.elenco_percorsi ep 
	where cod_percorso = %s
	and ep.data_fine_validita = 
	(select max(data_fine_validita) from anagrafe_percorsi.elenco_percorsi ep1 
		where ep1.cod_percorso = ep.cod_percorso 
		and data_fine_validita < to_date(%s, 'YYYYMMDD')
	)
)'''
        
        
        try: 
            curr.execute(insert1, (ec[2], ec[2], ec[2], ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(insert1)
        
        
        
        update1='''update anagrafe_percorsi.elenco_percorsi ep 
set versione_testata = versione_testata+1,
data_ultima_modifica = now()
	where cod_percorso = %s
	and ep.data_inizio_validita > to_date(%s, 'YYYYMMDD')'''
 
        try: 
            curr.execute(update1, (ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(update1)
            
                        
            
            
        insert2='''INSERT INTO anagrafe_percorsi.percorsi_ut (
cod_percorso, id_ut, id_squadra, 
responsabile, solo_visualizzazione, rimessa,
id_turno, durata, data_attivazione, data_disattivazione, cdaog3) 
/*SELECT*/
(
	select cod_percorso, 
	id_ut, id_squadra,
	responsabile, 
	solo_visualizzazione, 
	rimessa, 
	id_turno, 
	durata,
	to_date(%s, 'YYYYMMDD') as data_attivazione, 
	to_date(%s, 'YYYYMMDD')+1 as data_disattivazione, 
	cdaog3
	from anagrafe_percorsi.percorsi_ut pu 
	where pu.cod_percorso =%s
	and pu.data_disattivazione = 
		(select max(data_disattivazione) from anagrafe_percorsi.percorsi_ut pu1 
			where pu1.cod_percorso = pu.cod_percorso 
			and pu1.data_disattivazione < to_date(%s, 'YYYYMMDD')
		) 
)	'''    
        
        
        try: 
            curr.execute(insert2, (ec[2], ec[2], ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(insert2)
            
            
        insert3='''INSERT INTO anagrafe_percorsi.elenco_percorsi_old
(id_percorso_sit, cod_percorso, descrizione, 
id_tipo, freq_testata,versione_uo,
data_inizio_validita, data_fine_validita
)
(
select 
id_percorso_sit, 
cod_percorso,
descrizione, 
id_tipo, 
epo.freq_testata, 
epo.versione_uo, 
to_date(%s, 'YYYYMMDD') AS data_inizio_validita, 
to_date(%s, 'YYYYMMDD')+1 AS data_fine_validita
from anagrafe_percorsi.elenco_percorsi_old epo 
	where epo.cod_percorso = %s
	and epo.data_fine_validita = 
	(select max(data_fine_validita) from anagrafe_percorsi.elenco_percorsi_old epo1 
		where epo1.cod_percorso = epo.cod_percorso 
		and epo1.data_fine_validita < to_date(%s, 'YYYYMMDD')
	)
)'''

        try: 
            curr.execute(insert3, (ec[2], ec[2], ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(insert3)
            
        
        
        insert4='''insert into anagrafe_percorsi.date_percorsi_sit_uo
(id_percorso_sit, cod_percorso, versioni_uo, 
data_inizio_validita, data_fine_validita)
(select 
id_percorso_sit, 
cod_percorso, 
versioni_uo,
to_date(%s, 'YYYYMMDD') AS data_inizio_validita, 
to_date(%s, 'YYYYMMDD')+1 AS data_fine_validita
from anagrafe_percorsi.date_percorsi_sit_uo dp
where dp.cod_percorso = %s
and dp.data_fine_validita = 
	(select max(data_fine_validita) from anagrafe_percorsi.date_percorsi_sit_uo dp1 
		where dp1.cod_percorso = dp.cod_percorso 
		and dp1.data_fine_validita < to_date(%s, 'YYYYMMDD')
	)
)'''
        try: 
            curr.execute(insert4, (ec[2], ec[2], ec[1], ec[2]))
        except Exception as e:
            logger.error(e)
            logger.error(insert4) 
        
        conn.commit()
        #exit()  
    
    
    
        logger.info('Provo a leggere i dettagli della scheda {}'.format(ec[0]))
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(ec[0]),
                'flg_esponi_consunt' : 1
                }
        
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        #letture2 = response2.json()
        #try: 
        letture2 = response2.json()
        #logger.info(letture2)
        #exit()
        # key to remove
        #key_to_remove = "status"
        del letture2["status"]  
        del letture2['schede_lavoro'][0]['trips']  
        del letture2['schede_lavoro'][0]['risorse_tecniche']
        del letture2['schede_lavoro'][0]['risorse_umane']
        del letture2['schede_lavoro'][0]['serv_conferimenti']
        del letture2['schede_lavoro'][0]['filtri_rfid']        
        #logger.info(letture2)
        #exit()
        #logger.info(json.dumps(letture2).encode("utf-8"))
        
        
        
        
        
        
        
        
        logger.info('Provo a salvare nuovamente la scheda {}'.format(ec[0]))
        
        
        guid = uuid.uuid4()
        params2={'obj':'schede_lavoro',
                'act' : 'w',
                'ruid': '{}'.format(str(guid)),
                'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                }
        #exit()
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        try:
            result2 = response2.json()
            if result2['status']=='error':
                logger.error('Id_scheda = {}'.format(ec[0]))
                logger.error(result2)
        except Exception as e:
            logger.error(e)
            warning_message_mail('Problema scheda {}'.format(ec[0]), 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            
              
        i+=1 




        
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()
    