#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Marzocchi

'''
Lo script si occupa di inserimento della causlae "Operatore non AMIU" per i percorsi ditte terze

Lo script parte dal 21/10/2024


In particolare fa: 

- controllo ed eliminazione percorsi duplicati (non dovrebbe più servire a valle di una modifica al job)
- versionamento dei percorsi come da istruzioni 


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


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

# libreria per scrivere file csv
import csv


# per vedere se il percorso era previsto quel giorno
from preconsuntivazione import tappa_prevista

    
     

def main():
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    

    
    #id_scheda = 398690
    
    
    

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    # seleziono tutti i percorsi con squadra ditte terze
    '''query="""select a.cod_percorso, vspe.descrizione, pu.id_ut, pu.id_squadra, pu.cdaog3 
from (select cod_percorso, max(versione) as mv 
	from anagrafe_percorsi.v_servizi_per_ekovision vspe 
	group by cod_percorso) a
join anagrafe_percorsi.v_servizi_per_ekovision vspe on vspe.cod_percorso= a.cod_percorso and a.mv= vspe.versione
join anagrafe_percorsi.percorsi_ut pu on pu.cod_percorso = a.cod_percorso and vspe.data_inizio_validita = pu.data_attivazione 
where data_fine_validita >= (now()::date) /*Controllo anche i percorsi in disattivazione*/
and id_ut in (	119, /*rastrello*/
			 	120, /*revetro*/ 
			 	164, /*ati...*/
			 	166, /*genova insieme*/
			 	167, /*humana*/
			 	169 /*coop_maris*/
			 	/*,163, 121,85*/
			)
and id_squadra = 44"""
    '''
    
    query="""select a.cod_percorso, vspe.descrizione, pu.id_ut, pu.id_squadra, pu.cdaog3 
from (select cod_percorso, max(versione) as mv 
	from anagrafe_percorsi.v_servizi_per_ekovision vspe 
	group by cod_percorso) a
join anagrafe_percorsi.v_servizi_per_ekovision vspe on vspe.cod_percorso= a.cod_percorso and a.mv= vspe.versione
join anagrafe_percorsi.percorsi_ut pu on pu.cod_percorso = a.cod_percorso and vspe.data_inizio_validita = pu.data_attivazione 
where data_fine_validita >= (now()::date) /*Controllo anche i percorsi in disattivazione*/
and id_squadra = 44"""
    
    
    #testo_mail=''
    
    try:
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        curr.execute(query)
        lista_percorsi_dt=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

           
    for lp in lista_percorsi_dt:
        logger.debug(lp[0])
        
        logger.debug(oggi)
        
        

        
        #exit()
        gg=0
        
        while gg <= 14-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            #logger.debug(day)
            # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
            
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': day,
                'cod_modello_srv': lp[0], 
                'flg_includi_eseguite': 0,
                'flg_includi_chiuse': 0
                }
            response = requests.post(eko_url, params=params, data=data_json, headers=headers)
            #response.json()
            #logger.debug(response.status_code)
            try:      
                response.raise_for_status()
                check=0
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
    
    
                if len(letture['schede_lavoro']) > 0 : 
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    logger.debug('Id_scheda non eseguita:{}'.format(id_scheda))
                    
    
                    logger.info('Provo a leggere i dettagli della scheda')
                    
                    
                    params2={'obj':'schede_lavoro',
                            'act' : 'r',
                            'id': '{}'.format(id_scheda),
                            }
                    
                    # salvo i dettagli nella variabile letture2
                    response2 = requests.post(eko_url, params=params2, data=data_json, headers=headers)
                    letture2 = response2.json()
                    # rimuovo l key "status"
                    del letture2["status"]
                    del letture2['schede_lavoro'][0]['trips']
                    del letture2['schede_lavoro'][0]['risorse_tecniche']
                    del letture2['schede_lavoro'][0]['filtri_rfid']    
                    #logger.info(letture2)
                    #logger.debug(letture2)
                    check_inserimento=0
                    try:
                        # se sono previste delle risorse umane verifico se c'è già un giustificativo e se non ci fosse lo aggiungo
                        if len(letture2['schede_lavoro'][0]['risorse_umane'])> 0:
                            logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][0]['id_giustificativo'])
                            
                            if letture2['schede_lavoro'][0]['risorse_umane'][0]['id_giustificativo'] !='3':
                                check_inserimento=1
                                letture2['schede_lavoro'][0]['risorse_umane'][0]['id_giustificativo']='3'
 
                        else:
                            logger.info('Per la scheda {} non ci sono risorse umane'.format(id_scheda))
                    except Exception as e: 
                        logger.warning('Scheda {} - non trovo risorse umane'.format(id_scheda))   
                    
                    
                    # controllo se è da fare (per evitare di rifarlo)
                    if check_inserimento==1:
                        logger.info('Inserisco il giustificativo sulla scheda {}'.format(id_scheda))
                        
                        
                        # la P sta per il personale     
                        params2={'obj':'schede_lavoro',
                                'act' : 'w',
                                'ruid': 'P{}'.format(id_scheda),
                                'json': json.dumps(letture2)
                                }
                        try:
                            response2 = requests.post(eko_url, params=params2, data=data_json, headers=headers)
                            letture2 = response2.json()
                            
                        except Exception as err:
                            logger.error(err)
                            logger.error('Scheda di lavoro senza giustificativo:{}'.format(id_scheda))
                            #exit()
                            #logger.debug(letture2)
                
                        
            gg+=1
    
    '''try: 
        id_scheda=letture['crea_schede_lavoro'][0]['id']
    except Exception as e:
        logger.error(e)
    '''

    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    
    curr.close()
    conn.close()



if __name__ == "__main__":
    main()      