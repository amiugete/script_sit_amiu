#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Dato un elenco crea le corrispondenti schede di lavoro

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect, os.path
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

from preconsuntivazione import *

import requests
from requests.exceptions import HTTPError

import json

import logging

#path=os.path.dirname(sys.argv[0]) 



filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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





def main():
    
    
    
    
    
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logging.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    


     # cerco le schede su ekovision
        # PARAMETRI GENERALI WS
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    percorsi_giorni_creare=''
    
    elenco=['0203008303-20240320-304'
            ]
    
    #servizi non attivi
    #'0111002101-20240226-300',
    
    
    perc_snt=[]     #perc_schede_non_trovate
    for ee in elenco:
        check_error=0
        #logger.debug(vv[0])
        
        #logger.debug(oggi)
        #logger.debug(vv[1])
        vv=ee.split('-')
        
        logger.debug(vv)
        
                
               
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': vv[1],
            'cod_modello_srv': vv[0],
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
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
            #logger.info(letture)
            if len(letture['schede_lavoro']) > 0 : 
                id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                #logger.info('Id_scheda:{}'.format(id_scheda))
            else:
                perc_snt.append(vv[0])
                if percorsi_giorni_creare=='':
                    percorsi_giorni_creare='{} - {}'.format(vv[0], vv[1])
                else:
                    percorsi_giorni_creare='{}, {} - {}'.format(percorsi_giorni_creare,vv[0], vv[1])
                    
                    
                    
                    
                curr.close()
                curr = conn.cursor()
                
                query_select_ruid='''select lpad((max(id)+1)::text, 7,'0') 
                from anagrafe_percorsi.creazione_schede_lavoro csl '''
                try:
                    curr.execute(query_select_ruid)
                    lista_ruid=curr.fetchall()
                except Exception as e:
                    logger.error(query_select_ruid)
                    logger.error(e)




                for ri in lista_ruid:
                    ruid=ri[0]

                logger.info('ID richiesta Ekovision (ruid):{}'.format(ruid))
                curr.close()
                
                curr = conn.cursor()
                giason={
                            "crea_schede_lavoro": [
                            {
                                "data_srv": vv[1],
                                "cod_modello_srv": vv[0],
                                "cod_turno_ext": int(vv[2])
                            }
                            ]
                            } 
                params2={'obj':'crea_schede_lavoro',
                        'act' : 'w',
                        'ruid': ruid,
                        'json': json.dumps(giason)
                        }
                
                try:
                    response2 = requests.post(eko_url, params=params2, data=data_json, headers=headers)
                    letture2 = response2.json()
                    #logger.info(letture2)
                    check_creazione_scheda=0
                    id_scheda=letture2['crea_schede_lavoro'][0]['id']
                    check_creazione_scheda=1
                except Exception as e:
                    logger.error(e)
                    logger.error(' - id: {}'.format(ruid))
                    logger.error(' - Cod_percorso: {}'.format(vv[0]))
                    logger.error(' - Data: {}'.format(vv[1]))
                    #logger.error('Id Scheda: {}'.format(id_scheda[k]))
                    # check se c_handller contiene almeno una riga 
                    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.info("chiudo le connessioni in maniera definitiva")
                    #curr.close()
                    #conn.close()
                    #exit()
                
                if check_creazione_scheda ==1:
                    query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                            (id, cod_percorso, "data", id_scheda_ekovision, "check")
                            VALUES(%s, %s, %s, %s, %s);'''
                else: 
                    query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                            (id, cod_percorso, "data", id_scheda_ekovision, "check")
                            VALUES(%s, %s, %s, NULL, %s);'''
                try:
                    if check_creazione_scheda ==1:
                        curr.execute(query_insert, (int(ruid),vv[0], vv[1], id_scheda, check_creazione_scheda))
                    else:
                        curr.execute(query_insert, (int(ruid),vv[0], vv[1], check_creazione_scheda))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
                conn.commit()
    else:
        logger.debug('Percorso {} non previsto il giorno {}'.format(vv[0], vv[1]))
     

    
    
    k=0
    percorso_con_problemi_distinct=[]
    while k<len(perc_snt):
        #logger.debug(k)
        if k==0:
            percorso_con_problemi_distinct.append(perc_snt[k])
            elenco_codici='{0}'.format(perc_snt[k])
        if k > 0 and perc_snt[k]!= perc_snt[k-1]:
            percorso_con_problemi_distinct.append(perc_snt[k])
            elenco_codici='{0} - {1}'.format(elenco_codici, perc_snt[k])
        k+=1
    
    
    # provo a mandare la mail
    try:
        if percorsi_giorni_creare!='':
            # Create a secure SSL context
            context = ssl.create_default_context()



        # messaggio='Test invio messaggio'


            subject = "CREAZIONE SCHEDE LAVORO STRAORDINARIA (errore consuntivazione raccolta)"
            
            ##sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            debug_email='roberto.marzocchi@amiu.genova.it'

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = debug_email
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Creazione schede di lavoro"


            body='''I seguenti percorsi sono stati attivati recentemente e sono privi di schede di lavoro in queste settimane.<br>
            {0}
            <br><br>
            Sono state create <b>automaticamente</b> le schede di lavoro su Ekovision. 
            Verificare il log e controllare a mano eventuali anomalie.
            <br><br>
            Elenco giorni creati: <br>
            {1}
            <br><br>
            AMIU Assistenza Territorio<br>
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(elenco_codici, percorsi_giorni_creare)
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))


            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(path1)
            immagine(message,logoname)
            
            

            
            
            text = message.as_string()

            logger.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            logger.info(invio)
    except Exception as e:
        logger.error(e) # se non fossi riuscito a mandare la mail
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      