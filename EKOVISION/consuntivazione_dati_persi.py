#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione raccolta di dati persi su EKOVISION

Usato per corregger baco del ticket #6465

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

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging
import uuid

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

# libreria per scrivere file csv
import csv



from descrizione_percorso import *  

    
from tappa_prevista import tappa_prevista

import consuntivazione_raccolta_dati_persi
import consuntivazione_spazzamento_dati_persi

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    # pulisco cartella dati persi
    
    subfolder_amiu='dati_persi'
    
    
    # se non esiste la cartella la creo
    if not os.path.exists(f'{path}/consuntivazioni/{subfolder_amiu}/'):
        os.makedirs(f'{path}/consuntivazioni/{subfolder_amiu}/')
        
    #exit()
    subfolder_ekovision='dati_persi'
    cartella_dati_persi = f'{path}/consuntivazioni/{subfolder_amiu}/'
    
    if subfolder_ekovision=='':
        cartella_eko='sch_lav_cons/in'
    else:
        cartella_eko=f'sch_lav_cons/in/{subfolder_ekovision}'    
    
    
    
    for nome_file in os.listdir(cartella_dati_persi):
        if nome_file.lower().endswith(".csv"):
            path_completo = os.path.join(cartella_dati_persi, nome_file)
            os.remove(path_completo)
            logger.info(f'File eliminato correttamente: {nome_file}')



    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

    
    
    
    
    # prima faccio un giro di pre-consuntivazione per le giornate mancant
    """query_percorsi_correggere='''select 
    id_scheda, ce.codice_servizio_pred,
to_date(data_esecuzione_prevista, 'YYYYMMDD') data_effettiva,
ce.tipo_servizio,
max(u.descrizione) as ut,
za.cod_zona as zona,
count(*) as comp_senza_causale
from treg_eko.consunt_ekovision ce
left join anagrafe_percorsi.v_servizi_per_ekovision vspe 
	on vspe.cod_percorso = ce.codice_servizio_pred 
	and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') between vspe.data_inizio_validita and vspe.data_fine_validita
left join anagrafe_percorsi.elenco_sedi es on es.cod_sede = vspe.cod_sede 
left join anagrafe_percorsi.cons_mapping_uo cmu on cmu.id_uo = es.id_gruppo_coordinamento 
left join topo.ut u on u.id_ut = cmu.id_uo_sit 
left join topo.zone_amiu za on za.id_zona = u.id_zona 
where causale::int = -1
group by id_scheda, ce.codice_servizio_pred,
to_date(data_esecuzione_prevista, 'YYYYMMDD'),
ce.tipo_servizio, 
za.cod_zona 
order by 1 '''
"""



    

    # Usato per corregger baco del ticket #6465
    query_percorsi_correggere='''with cons_eko as (
select id_scheda, 
codice_servizio_pred,
data_esecuzione_prevista,
data_pianif_iniziale,
tipo_servizio,
case
	when 100 = ANY (array_agg(distinct causale::int)::int[]) then 100
    else max(distinct causale::int)
end causale
from treg_eko.consunt_ekovision
group by
id_scheda,
codice_servizio_pred,
data_esecuzione_prevista, 
data_pianif_iniziale,
tipo_servizio 
)
select 
    id_scheda, ce.codice_servizio_pred,
to_date(data_esecuzione_prevista, 'YYYYMMDD') data_effettiva,
ce.tipo_servizio,
max(u.descrizione) as ut,
za.cod_zona as zona,
count(*) as comp_senza_causale
from cons_eko ce
left join anagrafe_percorsi.v_servizi_per_ekovision vspe 
	on vspe.cod_percorso = ce.codice_servizio_pred 
	and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') between vspe.data_inizio_validita and vspe.data_fine_validita
left join anagrafe_percorsi.elenco_sedi es on es.cod_sede = vspe.cod_sede 
left join anagrafe_percorsi.cons_mapping_uo cmu on cmu.id_uo = es.id_gruppo_coordinamento 
left join topo.ut u on u.id_ut = cmu.id_uo_sit 
left join topo.zone_amiu za on za.id_zona = u.id_zona 
where causale::int = -1
group by id_scheda, ce.codice_servizio_pred,
to_date(data_esecuzione_prevista, 'YYYYMMDD'),
ce.tipo_servizio, 
za.cod_zona 
order by 1'''




    # usato per il percorso centro storico + rimessa volpara 
    """query_percorsi_correggere='''select 
    distinct id_scheda, 
    see.codice_serv_pred, 
    to_date(data_esecuzione_prevista, 'YYYYMMDD') data_effettiva,
    'RACC' as tipo_servizio,
    see.data_pianif_iniziale from consunt.schede_eseguite_ekovision see 
where see.codice_serv_pred  = '0508040501'
and see.data_pianif_iniziale  >= '20260113'
    '''
    """
    
    #day=datetime.strptime(data_percorso_input, '%Y%m%d').date()            
    try:
        curr.execute(query_percorsi_correggere)
        lista_percorsi=curr.fetchall()
    except Exception as e:
        logger.error(e)
        
        
    logger.info(f'Ci sono {len(lista_percorsi)} da correggere')
    for aa in lista_percorsi:
        id_scheda= aa[0]
        cod_percorso = aa[1]
        data=aa[2].strftime("%Y%m%d")
        tipo=aa[3]
        logger.info(f'La data testuale è {data}')
        
       
        if tipo=='SPAZZ':
            consuntivazione_spazzamento_dati_persi.main(id_scheda, cod_percorso, data, subfolder_amiu)
        else:
            consuntivazione_raccolta_dati_persi.main(id_scheda, cod_percorso, data, subfolder_amiu)    
        
    
    
    
    logger.info('Invio file con la consuntivazione raccolta via SFTP')
        
    
    


    
    
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd(cartella_eko): #chdir to public
            for nome_file in os.listdir(cartella_dati_persi):
                if nome_file.lower().endswith(".csv"):
                    path_completo = os.path.join(cartella_dati_persi, nome_file)
                    #print(path_completo)
                    srv.put(path_completo) #upload file to SFTP ekovision
                    logger.info(f'File inviato correttamente: {nome_file}')

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error('problema invio SFTP')
        logger.error(e)
        check_ekovision=103 # problema invio SFTP 
        
    
    
    #cancello la subfolder dati persi
    #os.remove(cartella_dati_persi)    
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
 
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()