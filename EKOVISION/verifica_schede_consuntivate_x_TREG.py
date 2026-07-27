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


import uuid


    
     

def main():
      


    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    mese_anno_oggi=oggi.strftime('%Y%m')
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    check=0
    
    chiusura_ok = 0 # se rimane 0 vuole dire che è tutto chiuso
    
    


    query_percorso_previsto = '''select * from anagrafe_percorsi.elenco_percorsi where cod_percorso = %s
and to_date(%s, 'YYYYMMDD') between data_inizio_validita and data_fine_validita '''
    
    
    
    
    # dal 4/3/2026 non tocco più i dati del 2025 che vanno freezati

    
    anno=2026
    mese=1
    mese_anno_eko = '202601'
    data_eko=datetime.strptime(f'{anno}-{mese}-01', '%Y-%m-%d').date()

    
    #anno=2025
    #mese=12
    logger.debug(f'mese_anno_eko = {mese_anno_eko}')
    logger.debug(f'mese_anno_oggi = {mese_anno_oggi}')
    logger.debug(f'data_eko = {data_eko}')
    logger.debug(f'Oggi-data_eko = {(oggi-data_eko).days}')
   
    #exit()

    
    id_scheda_chiuse_eko=[]
    id_scheda_chiuse_db=[]
    
    
    
    
    # anno e mese sono quelli di ekovision
    start_date = date(anno, mese, 1)


    end_date_finale = date(oggi.year, oggi.month, 1)
    

    locale.setlocale(locale.LC_ALL, "") # prendo la lingua del server

    
    
    mese_mail=start_date.strftime('%B')
    
    logger.debug(mese_mail)
    #exit()



    if oggi.day<5:
        # vado fino al primo del mese corrente
        cinque_giorni_fa = oggi - timedelta(days=5)
        end_date = date(cinque_giorni_fa.year, cinque_giorni_fa.month, 1) 
    else:    
        end_date = date(oggi.year, oggi.month, 1)    

    ###################################
    # modifica manuale da rimuovere
    #end_date = date(oggi.year, 6, 1)


    end_date_mail= end_date - timedelta(days=1)
    
    end_mese_mail=end_date_mail.strftime('%B')
    
    end_anno_mail=end_date_mail.year
    
    logger.info(f'end_date = {end_date}')
    logger.debug(f'end_mese_mail = {end_mese_mail}')
    logger.debug(f'end_anno_mail = {end_anno_mail}')
    #exit()
    
    
    
    # select delle schede presenti in treg_eko.consunt_ekovision
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    curr = conn.cursor()

    
    """
    I WS considerano la data di esecuzione non quella di pianificazione, per cui devo fare quel controllo lì
    query_schede_su_db='''select distinct id_scheda
from treg_eko.consunt_ekovision 
where data_pianif_iniziale > %s and data_pianif_iniziale < %s
and solo_esec is null
order by id_scheda'''
    """
    
    
    query_schede_su_db='''select distinct id_scheda
    from treg_eko.consunt_ekovision 
    where data_esecuzione_prevista > %s and data_esecuzione_prevista < %s
    and solo_esec is null
    order by id_scheda'''
    
    
    try:
        curr.execute(query_schede_su_db, (mese_anno_eko, end_date.strftime('%Y%m%d')))
        schede_su_db=curr.fetchall()
    except Exception as e:
        logger.error(query_schede_su_db)
        logger.error(e)
    
    for sd in schede_su_db:
        id_scheda_chiuse_db.append(sd[0])
    
    

    curr.close()
    
    logger.info('Schede chiuse su DB: {}'.format(len(id_scheda_chiuse_db)))
    
    
    
    # delta time
    delta = timedelta(days=1)

    # iterate over range of dates
    data_mese=start_date


    while data_mese < end_date:
        data_ws=data_mese.strftime('%Y%m%d')
        logger.info(data_ws)
        data_mese += delta
    
    
    

        # provo il WS solo con la data 
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': data_ws,
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
            }
        response = requests.post(eko_url, params=params, data=auth_data_eko, headers=headers)
        #response.json()
        #logger.debug(response.status_code)
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
            #logger.info(letture)
            #exit()
            #logger.info(len(letture['schede_lavoro']))
            
            # leggo tutte le schede di quel giorno
            ss=0
            while ss < len(letture['schede_lavoro']):
                #logger.debug(int(letture['schede_lavoro'][ss]['flg_chiuso']))
                #logger.debug(int(letture['schede_lavoro'][ss]['flg_gest_trip_comp']))
                #logger.debug(int(letture['schede_lavoro'][ss]['flg_gest_trip_tratti']))
                #print(f"scheda {ss} letture = {letture}")
                #exit()
                if int(letture['schede_lavoro'][ss]['flg_chiuso'])==1 and (int(letture['schede_lavoro'][ss]['flg_gest_trip_comp'])==1 or int(letture['schede_lavoro'][ss]['flg_gest_trip_tratti'])==1):
                    #data_nc.append(data_ws)
                    id_scheda_chiuse_eko.append(letture['schede_lavoro'][ss]['id_scheda_lav'])                  
                    #servizio_nc.append(letture['schede_lavoro'][ss]['descr_scheda_lav'])                  
                    #cod_servizio_nc.append(letture['schede_lavoro'][ss]['cod_serv_pred'])
                
                
                ss+=1



    # aggiorno il DB per il prossimo giro
    #logger.debug(id_scheda_chiuse_eko)
    
    
    
    

    
    logger.info('Schede chiuse su Ekovision: {}'.format(len(id_scheda_chiuse_eko)))
    #logger.info('Schede chiuse su DB: {}'.format(len(id_scheda_chiuse_db)))

    
    diff = list(set(id_scheda_chiuse_eko) - set(id_scheda_chiuse_db))
    
    logger.info(f'Lunghezza delle differenze {len(diff)}')
    
    logger.info(f'Schede diff: {diff}')
    #exit()
    
    """diff_a=[]
    for se in id_scheda_chiuse_eko:
        if se not in id_scheda_chiuse_db:
            diff_a.append(se)  
    logger.debug(f'Lunghezza delle differenze {len(diff_a)}')
    """
    
    curr = conn.cursor()
    for dd in diff:
        logger.info(f'Scheda {dd} chiusa in Ekovision ma non in DB: provo a leggere i dettagli')
        
        

        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(dd),
                'flg_esponi_consunt' : 1
                }
        
        response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
       
        letture2 = response2.json()


        # verifico se la scheda era prevista o meno 
        
        cod_percorso = letture2['schede_lavoro'][0]['servizi'][0]['cod_modello']
        data_percorso = letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
        non_effettuato = letture2['schede_lavoro'][0]['servizi'][0]['flg_segn_srv_non_effett']
        id_non_effettuato = letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg']
        # id = 15 non previsto 
        # idd =   festivo
        try:
            curr.execute(query_percorso_previsto, (cod_percorso, data_percorso,))
            controllo_percorso=curr.fetchall()
        except Exception as e:
            logger.error(query_percorso_previsto)
            logger.error(e)




        if len(controllo_percorso)>0:
            logger.info(f'La scheda {dd} è prevista per il percorso {cod_percorso} e la data {data_percorso}')
        
            # verifico se ci sono componenti o tratti
            if len(letture2['schede_lavoro'][0]['trips']) > 0:
                if len(letture2['schede_lavoro'][0]['trips'][0]['waypoints']) > 0:

                    del letture2["status"]  
                    del letture2['schede_lavoro'][0]['trips']  
                    del letture2['schede_lavoro'][0]['risorse_tecniche']
                    del letture2['schede_lavoro'][0]['risorse_umane']
                    del letture2['schede_lavoro'][0]['serv_conferimenti']
                    del letture2['schede_lavoro'][0]['filtri_rfid']        
                    
                    
                    if letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg'] == 15 and letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett'] == '':
                        letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett'] = '...'
                    
                    logger.info('Provo a salvare nuovamente la scheda {}'.format(dd))
                    
                    
                    guid = uuid.uuid4()
                    params2={'obj':'schede_lavoro',
                            'act' : 'w',
                            'ruid': '{}'.format(str(guid)),
                            'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                            }
                    #exit()
                    response2 = requests.post(eko_url, params=params2, data=auth_data_eko, headers=headers)
                    try:
                        result2 = response2.json()
                        if result2['status']=='error':
                            logger.error('Id_scheda = {}'.format(dd))
                            logger.error(result2)
                    except Exception as e:
                        logger.error(e)
                        warning_message_mail('Problema nella chiamata al WS Ekovision per la scheda {}'.format(dd), 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)

                else:
                    logger.info('La scheda {} è chiusa ma non ha waypoints'.format(dd))
            else:
                logger.info('La scheda {} è chiusa ma non ha trips'.format(dd))
        else:
            logger.warning(f'La scheda {dd} percorso {cod_percorso} e la data {data_percorso} non è nel piano programmatico, va bene non ci sia in consunt_ekovision')
         
        
         
    #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()      