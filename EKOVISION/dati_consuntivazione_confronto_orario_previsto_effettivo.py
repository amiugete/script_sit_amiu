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
    
     
    
    # tutte le schede dal 1 gennaio 2025 a maggio (data in cui abbiamo sospeso l'utilizzo del WS)
    # togliere id scheda
    
    select_schede= """SELECT ID_SCHEDA, CODICE_SERV_PRED, 
        DATA_ESECUZIONE_PREVISTA, DATA_PIANIF_INIZIALE, 
        ORARIO_ESECUZIONE 
        FROM SCHEDE_ESEGUITE_EKOVISION see 
        WHERE RECORD_VALIDO = 'S'
        AND DATA_PIANIF_INIZIALE >= 20250101
        AND DATA_PIANIF_INIZIALE <= 20250515
        and ID_SCHEDA > 478547   
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
    
    check_schede=[ [479078]] 
    
        
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
        
        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
        #letture2 = response2.json()
        #try: 
        letture2 = response2.json()
        
        # controllo che la scheda sia stata eseguita in caso contrario non devo fare nulla
        if int(letture2['schede_lavoro'][0]['servizi'][0]['flg_segn_srv_non_effett'])==0:
            ora_ini_serv=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
            ora_ini_serv2=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio_2']
            ora_fine_serv=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']
            ora_fine_serv2=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine_2']
            
            
            #logger.debug('Orari servizio')

            #logger.debug(ora_ini_serv)
            #logger.debug(ora_fine_serv)
            
            
            
            # gli array conterranno sia gli orari delle persone che dei mezzi
            ora_ini_p=[]
            ora_ini_p2=[]
            ora_fine_p=[]
            ora_fine_p2=[]
            date_inizio=[]
            date_fine=[]
            causali_p=[]
            causali_m=[]
            
            logger.debug('Orari persone')
            #logger.debug(letture2['schede_lavoro'][0]['risorse_umane'])
            p=0
            while p < len(letture2['schede_lavoro'][0]['risorse_umane']):
                causali_p.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['id_giustificativo'])
                if (letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio'])!= (letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine']): 
                    ora_ini_p.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio'])
                    ora_fine_p.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine'])
                    date_inizio.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['data_inizio'])
                    date_fine.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['data_fine'])
                if (letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio_2'])!= (letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine_2']): 
                    ora_ini_p2.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_inizio_2'])
                    ora_fine_p2.append(letture2['schede_lavoro'][0]['risorse_umane'][p]['ora_fine_2'])
                p+=1
            
            p=0
            while p < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                causali_m.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['id_giustificativo'])
                if (letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio'])!= (letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine']): 
                    ora_ini_p.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio'])
                    ora_fine_p.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine'])
                    date_inizio.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['data_inizio'])
                    date_fine.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['data_fine'])
                if (letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio_2'])!= (letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine_2']): 
                    ora_ini_p2.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_inizio_2'])
                    ora_fine_p2.append(letture2['schede_lavoro'][0]['risorse_tecniche'][p]['ora_fine_2'])
                p+=1    
            
            #logger.debug(min(ora_ini_p))
            #logger.debug(ora_fine_p)
            #logger.debug(max(ora_fine_p))
            
            
            if len(ora_ini_p2)==0:
                ora_ini_p2.append('000000')
                ora_fine_p2.append('000000')
            
            check_controllo_ore=1    
            if len(ora_ini_p)==0:
                # non faccio nessun controllo successivo (variabile usata nel successivo IF)
                check_controllo_ore=0
                logger.debug(causali_p)
                logger.debug(causali_m)
                
                if (len(causali_p)>0 and  '3' not in causali_p) and (len(causali_m)>0 and '9' not in causali_m): 
                    logger.debug('Entrato qua')
                    logger.debug(letture2)
                    exit()
                    # controllo le consuntivazioni
                    #tratti / # componenti
                    check
                    t=0 #waypoints
                    while t < len(letture2['schede_lavoro'][0]['waypoints'][t]):
                        #works
                        w=0
                        while t < len(letture2['schede_lavoro'][0]['waypoints'][t]['works'][w]):
                            # problema WS (non espongono le causali) da risolvere lato EKO!!!! 
                            # #Una volta risolto, bisogna verificare se tutte le componenti/tratti sono NON FATTE allora non invio warning
                            w+=1
                        t+=1    
                        
                    # mando una mail
                    query_mail='''SELECT au.mail || ', '|| a.MAIL AS destinatari_mail 
                    FROM ANAGR_UO au
                    LEFT JOIN ANAGR_ZONE a ON a.ID_ZONATERRITORIALE = au.ID_ZONATERRITORIALE
                    WHERE id_uo IN ( 
                        SELECT id_uo FROM anagr_ser_per_uo 
                        WHERE ID_PERCORSO = :m1
                        AND to_date(:m2, 'YYYYMMDD') 
                        BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE 
                    )'''
                    try:
                        cur.execute(query_mail, (letture2['schede_lavoro'][0]['servizi'][0]['cod_modello'], 
                                                letture2['schede_lavoro'][0]['servizi'][0]['data_inizio'],))
                        check_mails=cur.fetchall()
                    except Exception as e:
                        logger.error(query_mail)
                        logger.error(e)
                        
                    for cm in check_mails:
                        destinatari=cm[0]
                                                        
                                                        

                    # controllare le causali (se indicato ditta esterna OK)
                    messaggio='''Da un controllo automatico delle schede Ekovision si nota che il percorso <b>{0}</b> - <b>{1}</b> del {2} (id scheda={3}) è stato eseguito, 
                    ma non sono state indicate nè persone nè mezzi. 
                    <br><br>Si prega di verificare e nel caso correggere le informazioni.
                    '''.format(
                        letture2['schede_lavoro'][0]['servizi'][0]['cod_modello'],
                        letture2['schede_lavoro'][0]['servizi'][0]['descrizione'],
                        datetime.strptime(letture2['schede_lavoro'][0]['servizi'][0]['data_inizio'], '%Y%m%d').strftime('%d/%m/%Y'),
                        id_scheda[0]
                    )
                    destinatari='roberto.marzocchi@amiu.genova.it'
                    logger.warning('messaggio')
                    warning_message_mail(messaggio, destinatari, os.path.basename(__file__), logger, 'ANOMALIA EKOVISION - Scheda eseguita senza personale / mezzi')

            
            if check_controllo_ore==1  and (min(ora_ini_p) != ora_ini_serv  or min(ora_ini_p2) != ora_ini_serv2 or max(ora_fine_p) != ora_fine_serv  or max(ora_fine_p2) != ora_fine_serv2):
                logger.warning('Anomalia')
                id_schede_problemi.append(id_scheda[0])
                orario_effettivo_sbagliato.append('{} - {} / {} - {}'.format(ora_ini_serv, ora_fine_serv, ora_ini_serv2, ora_fine_serv2))
                orario_effettivo_ok.append('{} - {} / {} - {}'.format(min(ora_ini_p), max(ora_fine_p), min(ora_ini_p2), max(ora_fine_p2))) 

                letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']=min(ora_ini_p)
                letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio_2']=min(ora_ini_p2)
                letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']=max(ora_fine_p)
                letture2['schede_lavoro'][0]['servizi'][0]['ora_fine_2']=max(ora_fine_p2)
                letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']=min(date_inizio)
                letture2['schede_lavoro'][0]['servizi'][0]['data_fine']=max(date_fine)
                
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
                response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
                result2 = response2.json()
                if result2['status']=='error':
                    logger.error('Id_scheda = {}'.format(id_scheda))
                    logger.error(result2)
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                else :
                    logger.info(result2['status'])
  
            else: 
                logger.debug('tutto ok')
            #exit()
        #else:
            #servizio non effettuato non devo fare nessun controllo
        
        
    
    
    # finito ciclo scrivo output    
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
        
        
        
        
        
        
        
        
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()        
        




if __name__ == "__main__":
    main()      