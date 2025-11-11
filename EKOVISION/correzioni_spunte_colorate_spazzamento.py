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
    
    
    
    percorsi_da_controllare=[
        '0201258001', '0201010301', '0201243601', '0201012303', '0202243901', '0201252701', '0202242401', '0203007303', '0201241802', '0202004203', '0202244103', '0201251901', '0202001701', '0201255901', '0207000201', '0213241103', '0213244703', '0201016303', '0201255301', '0203008501', '0202002002', '0207003301', '0201250501', '0201036501', '0201013201', '0213244301', '0213248603', '0201253101', '0201016203', '0201236402'
    ]
    
    
    query_variazioni_ekovision='''select 
codice_modello_servizio,
ordine.ordine,
objecy_type, 
  /*codice,*/
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
 and %s between tab.data_inizio and tab.data_fine
 group by codice_modello_servizio,  objecy_type, 
  tab.codice,ce.id_ekovision, quantita, lato_servizio, percent_trattamento,
  ripasso, numero_passaggi, nota,
  codice_qualita, codice_tipo_servizio,
  vspe.data_fine_validita, freq.frequenza, ordine.ordine
  order by codice_modello_servizio, data_fine asc, ordine,  ripasso'''
  
  
  

    outputfile1='{0}/anomalie_output/componenti_da_rimuovere_spazzamento.csv'.format(path,nome)    
    f= open(outputfile1, "w")
    f.write('cod_percorso;id_componente_ekovision;data_inizio_sbagliata')
    
    
    
    
    for pdc in percorsi_da_controllare:
        logger.debug(pdc)
        
        
        ########################################################
        # EKOVISION 
        ########################################################
        
        id_scheda=0
        
        # cerco ultimo id_scheda con cui poi interrogherò i WS
        """
        query_id_scheda='''SELECT max(id_scheda) as ID_SCHEDA, 
        max(DATA_ESECUZIONE_PREVISTA ) as max_data 
        FROM SCHEDE_ESEGUITE_EKOVISION see 
        WHERE see.CODICE_SERV_PRED = :p1 
        AND see.RECORD_VALIDO = 'S'
        having max(id_scheda) IS NOT NULL'''
        """
        # cerco gli ultimi 7 id_scheda con cui poi interrogherò i WS
        query_id_scheda = '''
        WITH schede AS (
SELECT rownum, see.ID_SCHEDA, see.DATA_ESECUZIONE_PREVISTA, see.CODICE_SERV_PRED 
FROM SCHEDE_ESEGUITE_EKOVISION see 
WHERE see.CODICE_SERV_PRED = :p1
AND see.RECORD_VALIDO = 'S' 
ORDER BY 3 desc
)
SELECT id_scheda, data_esecuzione_prevista, CODICE_SERV_PRED
FROM schede WHERE rownum < 8
        '''
        try:
            cur.execute(query_id_scheda,(pdc,))
            id_schede=cur.fetchall()
        except Exception as e:
            logger.error(e)
            logger.error(query_id_scheda)
        
        
        logger.debug(f'Schede EKO da analizzare = {len(id_schede)}')
        #exit()
        
        for mdc in id_schede:
            # dichiaro le liste
            componenti_OK = [] # quelle del sit
            componenti_eko=[]  #quelle di Ekovision
        
            id_scheda=mdc[0]
            data_scheda=mdc[1]
        
            logger.debug(f'Scheda {id_scheda} del percorso {mdc[2]} del {mdc[1]}')
            
            
            
            params={'obj':'schede_lavoro',
                        'act' : 'r',
                        'id': id_scheda
                        }
            response = requests.post(eko_url, params=params, data=data, headers=headers)
            #response.json()
            #logger.debug(response.status_code)
            check=0
            try:      
                response.raise_for_status()
                # access JSOn content
                #jsonResponse = response.json()
                #print("Entire JSON response")
                #print(jsonResponse)
            except HTTPError as http_err:
                logger.error(f'HTTP error occurred: {http_err}')
                check=1
            except Exception as err:
                logger.error(f'Other error occurred: {err}')
                logger.error(response.json())
                check=1
            if check<1:
                letture = response.json()
                #logger.debug(letture)
                ss=0
                while ss < len(letture['schede_lavoro']):
                    trips=letture['schede_lavoro'][ss]['trips']
                    # ciclo sulle aste 
                    tr=0
                    while tr < len(trips):
                        waypoints=letture['schede_lavoro'][ss]['trips'][tr]['waypoints']
                        wid=0
                        while wid < len(waypoints):
                            works=letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'] 
                            # ciclo sugli elementi
                            cc=0
                            while cc < len(works):
                                list=[]
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['id_object']))
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['pos']))
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['data_inizio']))
                                list.append(int(letture['schede_lavoro'][ss]['trips'][tr]['waypoints'][wid]['works'][cc]['data_fine']))
                                componenti_eko.append(list)
                                cc+=1
                            wid+=1
                        tr+=1
                    ss+=1 
            
            #logger.debug(componenti_eko)
            logger.debug(f'num componenti_eko {len(componenti_eko)}')
            #exit()
            
            ########################################################
            # SIT 
            ########################################################
            
            try:
                curr.execute(query_variazioni_ekovision,(pdc,data_scheda,))
                dettaglio_percorso=curr.fetchall()
            except Exception as e:
                logger.error(e)
            

            
            
            for dpe in dettaglio_percorso:
                #logger.debug(dpe)
                list=[]
                #list.append(dpe[0])
                list.append(int(dpe[3]))
                list.append(int(dpe[1]))
                list.append(int(dpe[12]))
                #if dpe[13]> data_scheda and dpe[13]!='99991231'and dpe[15]< oggi_char: # percorsi disattivi
                
                if tappa_prevista(datetime.strptime(data_scheda, '%Y%m%d').date(), dpe[7])==1:
                    if dpe[13]>dpe[15]:
                        list.append(99991231)
                    else:
                        list.append(int(dpe[13]))
                #list.append(int(dpe[14]))
                componenti_OK.append(list)
            
            logger.debug(componenti_eko)
            
            logger.debug(f'num componenti_sit {len(componenti_OK)}')
            
            logger.debug(componenti_OK)
            #exit()
            j=0
            for ce in componenti_eko:
                logger.debug(j)
                if ce not in componenti_OK:
                    logger.debug('{0};{1};{2};{3}'.format(pdc, ce[0], ce[1], ce[2]))
                    f.write('\n{0};{1};{2};{3}'.format(pdc, ce[0], ce[1], ce[2]))
                j+=1
            exit()
    
    f.close()
        
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
    