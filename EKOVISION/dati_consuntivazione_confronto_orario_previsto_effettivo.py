#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
INPUT 
- una query specifica che restituisce un elenco di ID_SCHEDE 




OUTPUT 
elenco anomalie / correzione 

- orario effettivo scheda != max orario effettivo persone





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

import uuid


    
     

def main():
      
    
    #exit()
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    check=0
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
     
    
    # tutte le schede dal 1 gennaio 2025
    # togliere id scheda
    
    select_schede= """SELECT ID_SCHEDA, CODICE_SERV_PRED, 
DATA_ESECUZIONE_PREVISTA, 
ORARIO_ESECUZIONE 
FROM SCHEDE_ESEGUITE_EKOVISION see 
WHERE RECORD_VALIDO = 'S'
AND DATA_ESECUZIONE_PREVISTA >= 20250101
and ID_SCHEDA = 571125  
ORDER BY 1"""
    
    try:
        cur.execute(select_schede)
        check_schede=cur.fetchall()
    except Exception as e:
        logger.error(select_schede)
        logger.error(e)
    
    
    
    # 
    
    ################################
    # ATTENZIONE ORA è su TEST (da cambiare 2 volte l'URL (lettura e scrittura) 
    #154813
    
    #check_schede=[ [576939]] 
    
    
    check_schede=[ [576939]] 
    
    id_schede_problemi=[]
    orario_effettivo_sbagliato=[]
    orario_effettivo_ok=[]
    
    
    for id_scheda in check_schede:
    
    
        logger.info('Provo a leggere i dettagli della scheda {}'.format(id_scheda[0]))
        
        
        params2={'obj':'schede_lavoro',
                'act' : 'r',
                'id': '{}'.format(id_scheda[0]),
                'flg_esponi_consunt': 1
                }
        
        response2 = requests.post(eko_url_test, params=params2, data=data, headers=headers)
        #letture2 = response2.json()
        #try: 
        letture2 = response2.json()
        
        
        ora_ini_serv=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
        ora_ini_serv2=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio_2']
        ora_fine_serv=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']
        ora_fine_serv2=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine_2']
        
        
        logger.debug('Orari servizio')

        logger.debug(ora_ini_serv)
        logger.debug(ora_fine_serv)
        
        
        
        # gli array conterranno sia gli orari delle persone che dei mezzi
        ora_ini_p=[]
        ora_ini_p2=[]
        ora_fine_p=[]
        ora_fine_p2=[]
        
        logger.debug('Orari persone')
        #logger.debug(letture2['schede_lavoro'][0]['risorse_umane'])
        p=0
        while p < len(letture2['schede_lavoro'][0]['risorse_umane']):
            ora_ini_p.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio'])
            ora_ini_p2.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio_2'])
            ora_fine_p.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine'])
            ora_fine_p2.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine_2'])
            p+=1
        
        p=0
        while p < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
            ora_ini_p.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio'])
            ora_ini_p2.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio_2'])
            ora_fine_p.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine'])
            ora_fine_p2.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine_2'])
            p+=1    
        
        logger.debug(min(ora_ini_p))
        logger.debug(ora_fine_p)
        logger.debug(max(ora_fine_p))
        
        if min(ora_ini_p) != ora_ini_serv  or min(ora_ini_p2) != ora_ini_serv2 or max(ora_fine_p) != ora_fine_serv  or max(ora_fine_p2) != ora_fine_serv2:
            logger.warning('Anomalia')
            id_schede_problemi.append(id_scheda[0])
            orario_effettivo_sbagliato.append('{} - {} / {} - {}'.format(ora_ini_serv, ora_fine_serv, ora_ini_serv2, ora_fine_serv2))
            orario_effettivo_ok.append('{} - {} / {} - {}'.format(min(ora_ini_p), max(ora_fine_p), min(ora_ini_p2), max(ora_fine_p2))) 

            letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']=min(ora_ini_p)
            letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio_2']=min(ora_ini_p2)
            letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']=min(ora_fine_p)
            letture2['schede_lavoro'][0]['servizi'][0]['ora_fine_2']=min(ora_fine_p2)
            
            del letture2["status"]  
            del letture2['schede_lavoro'][0]['trips']  
            del letture2['schede_lavoro'][0]['risorse_tecniche']
            del letture2['schede_lavoro'][0]['risorse_umane']   
            del letture2['schede_lavoro'][0]['filtri_rfid']        
            #logger.info(letture2)
    

            #letture2['schede_lavoro'][0]['flg_imposta_chiuso']='1'

    
            logger.info('Provo a salvare nuovamente la scheda')
            logger.info(letture2)
            
            guid = uuid.uuid4()
            params2={'obj':'schede_lavoro',
                    'act' : 'w',
                    'ruid': '{}'.format(str(guid)),
                    'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                    }
            #exit()
            response2 = requests.post(eko_url_test, params=params2, data=data, headers=headers)
            result2 = response2.json()
            if result2['status']=='error':
                logger.error('Id_scheda = {}'.format(id_scheda))
                logger.error(result2)
            else :
                logger.info(result2['status'])
            
            
        
        else: 
            logger.debug('tutto ok')
        #exit()
        
    if len(id_schede_problemi)> 0:
        try:    
            nome_csv_ekovision="anomalie_orari.csv"
            path_output='{0}/anomalie_output'.format(path)
            if not os.path.exists(path_output):
                os.makedirs(path_output)
            file_variazioni_ekovision="{0}/{1}".format(path_output,nome_csv_ekovision)
            fp = open(file_variazioni_ekovision, 'w', encoding='utf-8')
            fp.write('id_scheda;orario_effettivo_sbagliato;orario_effettivo_ok\n')
            
            i=0
            while i<len(id_schede_problemi):
                fp.write('{};{};{}\n'.format(id_schede_problemi[i], orario_effettivo_sbagliato[i] , orario_effettivo_ok[i]))
                i+=1
            fp.close()
        except Exception as e:
            logger.error(e)
        
        
        
        
        
        
        
        
        
        
        




if __name__ == "__main__":
    main()      