#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della pulizia dell'elenco percorsi generato dai JOB spoon realizzati per Ekovision

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
      


    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    ut=128 #piattaforma dufour

    
    
    
    
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
    
    
    query_serv_predefiniti='''select a.cod_percorso, vspe.descrizione, pu.id_ut, pu.id_squadra, pu.cdaog3 
from (select cod_percorso, max(versione) as mv 
	from anagrafe_percorsi.v_servizi_per_ekovision vspe 
	group by cod_percorso) a
join anagrafe_percorsi.v_servizi_per_ekovision vspe on vspe.cod_percorso= a.cod_percorso and a.mv= vspe.versione
join anagrafe_percorsi.percorsi_ut pu on pu.cod_percorso = a.cod_percorso and vspe.data_inizio_validita = pu.data_attivazione 
where data_fine_validita >= (now()::date) /*Controllo anche i percorsi in disattivazione*/
and id_ut in (	%s
			 )'''
    

    
    try:
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        curr.execute(query_serv_predefiniti, (ut,))
        lista_percorsi_dt=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

    
    
    descrizione=[]
    turno=[]
    operatori=[]       
    for lp in lista_percorsi_dt:
        logger.debug(lp[0])
        
        logger.debug(oggi)
        
        

        
        
        day_check=oggi
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
                #logger.debug(letture2)
                
                descrizione.append(letture2['schede_lavoro'][0]['descr_scheda_lav'][0])
                k=0
                nominativi=''
                while k <len(letture2['schede_lavoro'][0]['risorse_umane']):
                    logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][k])
                    nominativi='''{} {} {}'''.format(
                        nominativi, 
                        letture2['schede_lavoro'][0]['risorse_umane'][k]['cognome'][0],
                        letture2['schede_lavoro'][0]['risorse_umane'][k]['nome'][0]
                    )
                    k+=1
                operatori.append(nominativi.strip())
            
                    
    
    
    
    
    logger.info(descrizione)
    logger.info(nominativi)
    '''
    k=0
    while k <len(letture2['schede_lavoro'][0]['trips'][0]['waypoints']):
        logger.debug(k)
        logger.debug(letture2['schede_lavoro'][0]['trips'][0]['waypoints'][k]['works'][0]['flg_exec_manuale'])
        letture2['schede_lavoro'][0]['trips'][0]['waypoints'][k]['works'][0]['flg_exec_manuale']='1'
        k+=1     
    '''
    #exit()
    
    
    
    




if __name__ == "__main__":
    main()      