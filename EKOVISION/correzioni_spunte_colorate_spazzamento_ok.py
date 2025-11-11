#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Script che speriamo vivamente non serva più per corregggere le schede con tappe duplicate
per errori dovuti a itinerari che nel passato sono stati male inseriti su EKOVISION


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ATTENZIONE 

prima di farlo girare occorre scaricare tutti i tratti stradali da Ekovision a mano e importarli nella tabella 
etl.tratti_stradali_ekovision 

questo fino a che non ci faranno estrazione chiesta con ticket 5839 

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!





Input: 
- lista percorsi con errori


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
    
    
    export_eko='20250924_estrazione_tratti_spazz'
    percorsi_da_controllare=[
        '0201258001', '0201010301', '0201243601', '0201012303', '0202243901', '0201252701', '0202242401', '0203007303', '0201241802', '0202004203', '0202244103', '0201251901', '0202001701', '0201255901', '0207000201', '0213241103', '0213244703', '0201016303', '0201255301', '0203008501', '0202002002', '0207003301', '0201250501', '0201036501', '0201013201', '0213244301', '0213248603', '0201253101', '0201016203', '0201236402'
    ]
    
    # 24/10/2025 controllo 0202239201
    export_eko='20251024_estrazione_tratti_spazz'
    percorsi_da_controllare = ['0202239201']
    
    
    
    query_sit= '''select 
codice_modello_servizio,
ordine.ordine,
objecy_type, 
 codice,
ce.id_ekovision,
  quantita, lato_servizio, percent_trattamento,
  freq.frequenza,
  numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
min(data_inizio) as data_inizio, 
/*case 
	when max(data_fine) = '99991231' then null 
	else max(data_fine)
end data_fine, 
*/
max(data_fine) as data_fine,
/*ripasso*/
case 
	when max(data_fine) = '99991231' then ripasso 
	else 0
end ripasso,
to_char(vspe.data_fine_validita, 'YYYYMMDD') as data_fine_validita
from (
	  SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '99991231')
	 union 
	   SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '99991231')	
	 union 
	 SELECT codice_modello_servizio, ordine, objecy_type, 
  codice, quantita, lato_servizio, percent_trattamento,frequenza,
  ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
  codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '99991231') as data_fine
	 FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio < coalesce(data_fine, '99991231')
 ) tab 
 left join etl.tratti_stradali_ekovision ce on tab.codice = ce.id_asta
 left join anagrafe_percorsi.v_servizi_per_ekovision vspe 
 	on vspe.cod_percorso = tab.codice_modello_servizio 
 	and vspe.versione = 
 	(select max(versione) from anagrafe_percorsi.v_servizi_per_ekovision vspe1 where vspe1.cod_percorso = vspe.cod_percorso)
 left join lateral (
  select ordine
  from anagrafe_percorsi.v_percorsi_elementi_tratti a
  where a.codice_modello_servizio = tab.codice_modello_servizio 
    and a.codice = tab.codice
    and a.ripasso = tab.ripasso
    and a.data_fine is null
  limit 1
) ordine on true
left join lateral (
  select fo.freq_binaria as frequenza
  from anagrafe_percorsi.v_percorsi_elementi_tratti a
  left join etl.frequenze_ok fo on fo.cod_frequenza = a.frequenza
  where a.codice_modello_servizio = tab.codice_modello_servizio 
    and a.codice = tab.codice
    and a.ripasso = tab.ripasso
    and a.data_fine is null
  limit 1
) freq on true
where codice_modello_servizio =%s
and data_inizio < '20250924'
 group by codice_modello_servizio,  objecy_type, 
  tab.codice,ce.id_ekovision, quantita, lato_servizio, percent_trattamento,
  ripasso, numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
  vspe.data_fine_validita, ordine.ordine, freq.frequenza
--order by ordine,  ripasso'''
    
    
    query_variazioni_ekovision=f'''
with ps as ({query_sit}
)
select ets.cod_percorso, ets.pos, ets.id_asta, ets.id_tratto_eko,ets.note,ets.data_inizio, ets.data_fine, 
case 
	when count(id_tratto_eko) >1 then 'RIMUOVERE SOLO 1 CASO'
	else 'RIMUOVERE TUTTO'
end DESCR_INTERVENTO
from marzocchir."{export_eko}" ets
left join ps
on trim(ets.cod_percorso)::text = trim(ps.codice_modello_servizio)::text
and coalesce(trim(ets.note), '') = coalesce(trim(ps.nota), '')
/*and ets.pos::int = ps.ordine::int*/
and to_char(ets.data_inizio, 'YYYYMMDD')::text = ps.data_inizio::text
and to_char(ets.data_fine, 'YYYYMMDD')::text = ps.data_fine::text
and ets.id_tratto_eko::int = ps.id_ekovision::int
and ets.id_asta::bigint = ps.codice::bigint
where ets.cod_percorso = %s
and ets.data_inizio <= ets.data_fine 
group by ets.cod_percorso, ets.pos, ets.id_asta, ets.id_tratto_eko,ets.note, ets.data_inizio, ets.data_fine, 
ps.codice_modello_servizio
having ps.codice_modello_servizio is null 
or count(id_tratto_eko)>1
'''


    query_eko=f'''select ets.cod_percorso, ets.pos, ets.id_asta, ets.id_tratto_eko,ets.note, ets.data_inizio, ets.data_fine
              from marzocchir."{export_eko}" ets
              WHERE ets.cod_percorso = %s
              and data_fine >= data_inizio'''
  
  

    outputfile1='{0}/anomalie_output/{1}componenti_da_rimuovere_spazzamento_ok.csv'.format(path,oggi_char)    
    f1= open(outputfile1, "w")
    f1.write('cod_percorso;pos;cod_componente_amiu;id_componente_ekovision;note;data_inizio_sbagliata;data_fine;descr_intervento')
    
    outputfile2='{0}/anomalie_output/{1}componenti_da_aggiornare_spazzamento_ok.csv'.format(path,oggi_char)    
    f2= open(outputfile2, "w")
    f2.write('cod_percorso;pos;cod_componente_amiu;id_componente_ekovision;note;data_inizio_sbagliata;data_fine')
    
    
    
    for pdc in percorsi_da_controllare:
        logger.debug(pdc)
        
        
        try:
            curr.execute(query_sit,(pdc,))
            dettaglio_percorso_sit=curr.fetchall()
        except Exception as e:
            logger.error(e)
        
        try:
            curr.execute(query_eko,(pdc,))
            dettaglio_percorso_eko=curr.fetchall()
        except Exception as e:
            logger.error(e)
            
            
        logger.debug(f'n sit = {len(dettaglio_percorso_sit)}')
        logger.debug(f'n eko = {len(dettaglio_percorso_eko)}')
        #exit()
        
        try:
            curr.execute(query_variazioni_ekovision,(pdc,pdc,))
            dettaglio_percorso=curr.fetchall()
        except Exception as e:
            logger.error(e)
        
        if len(dettaglio_percorso_sit) < len(dettaglio_percorso_eko):    
          if len(dettaglio_percorso)==0:
            logger.warning('Non ho trovato le anomalie')
          for dpe in dettaglio_percorso:       
              logger.debug(f'CASO 1 - {pdc};{dpe[1]};{dpe[2]};{dpe[3]};{dpe[4]};{dpe[5]};{dpe[6]}')
              f1.write(f'\n{pdc};{dpe[1]};{dpe[2]};{dpe[3]};{dpe[4]};{dpe[5]};{dpe[6]};{dpe[7]}')
        else:
          for dpe in dettaglio_percorso:       
              logger.debug(f'CASO 2 - {pdc};{dpe[1]};{dpe[2]};{dpe[3]};{dpe[4]};{dpe[5]};{dpe[6]}')
              f2.write(f'\n{pdc};{dpe[1]};{dpe[2]};{dpe[3]};{dpe[4]};{dpe[5]};{dpe[6]}')
        #exit()
    
    f1.close()
    f2.close()    
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
    