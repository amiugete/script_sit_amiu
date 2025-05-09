#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Controllo se mi mancano degli orari di esecuzione di schede eseguite. 
Nel caso interrogo i WS Ekovision e faccio update delle scheda


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


import requests
from requests.exceptions import HTTPError

import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION
import pysftp

import json



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
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.WARNING)
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


import fnmatch



def fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2):
    '''
    Calcolo della fascia turno sulla base degli orari della scheda di lavoro Ekovision
    '''
    fascia_turno=''
    if ora_inizio_lav_2 == '000000' and ora_fine_lav_2 =='000000':
    
        if ora_inizio_lav== '000000' and ora_fine_lav =='000000':
            fascia_turno='D'
        else:
            oi=int(ora_inizio_lav[:2])
            mi=int(ora_inizio_lav[2:4])
            of=int(ora_fine_lav[:2])
            mf=int(ora_fine_lav[2:4])
    else:
        oi=int(ora_inizio_lav[:2])
        mi=int(ora_inizio_lav[2:4])
        of=int(ora_fine_lav_2[:2])
        mf=int(ora_fine_lav_2[2:4])
            
            
    if fascia_turno=='':        
        # calcolo minuti del turno
        if of < oi:
            minuti= 60*(24 - oi) + 60 * of - mi + mf
        else :
            minuti = 60 * (of-oi) - mi + mf 

        
        hh_plus=int(minuti/2/60)
        mm_plus=minuti/2-60*int(minuti/2/60)
        
        # ora media
        if mi+mm_plus >= 60:
            mm=mi+mm_plus-60
            hh=oi+1+hh_plus
        else:
            mm=mi+mm_plus
            hh=oi+hh_plus
        
        #print('{}:{}'.format(hh,mm))
        
        if hh > 5 and hh <= 12:
            fascia_turno = 'M'
        elif hh > 12 and hh <= 20:
            fascia_turno = 'P'
        elif hh > 20 or hh <= 5:
            fascia_turno= 'N'
        
        return fascia_turno




def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logger.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_ekovision='20231120'
    
    
  
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    select_file='''SELECT DISTINCT CODICE_SERV_PRED, DATA_ESECUZIONE_PREVISTA, ID_SCHEDA
FROM SCHEDE_ESEGUITE_EKOVISION see 
WHERE see.RECORD_VALIDO = 'S' AND ORARIO_ESECUZIONE IS NULL 
AND COD_CAUS_SRV_NON_ESEG_EXT IS null'''

    try:
        cur.execute(select_file)
        check_orari=cur.fetchall()
    except Exception as e:
        logger.error(select_file)
        logger.error(e)
                        
               
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    for co in check_orari:
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': co[1],
            'cod_modello_srv': co[0], 
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
            }
        check=0
        response = requests.post(eko_url, params=params, data=data_json, headers=headers)
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
            logger.info('ID scheda da verificare = {}'.format(co[2]))
            logger.info(letture)
            ora_inizio_lav_2=''
            ora_fine_lav_2 = ''
            orario_esecuzione=''
            k=0
            
            while k < len(letture['schede_lavoro']): 
                #controllo se la scheda è la stessa (escludo eventuali soccorsi o schede duplicate per sbaglio)
                if co[2]==letture['schede_lavoro'][k]['id_scheda_lav']:
                    ora_inizio_lav=letture['schede_lavoro'][k]['ora_inizio_lav']
                    ora_inizio_lav_2=letture['schede_lavoro'][k]['ora_inizio_lav_2']
                    ora_fine_lav=letture['schede_lavoro'][k]['ora_fine_lav']
                    ora_fine_lav_2=letture['schede_lavoro'][k]['ora_fine_lav_2']
                    if (ora_inizio_lav_2=='000000' or ora_inizio_lav_2=='') and (ora_fine_lav_2=='000000' or ora_fine_lav_2==''):
                        orario_esecuzione='{} - {}'.format(ora_inizio_lav, ora_fine_lav)
                    else:
                        orario_esecuzione='{} - {} / {} - {}'.format(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2)   
                    logger.info('Orario esecuzione:{}'.format(orario_esecuzione))
                    fascia_t=fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2)
                    logger.info('Fascia turno :{}'.format(fascia_t))
                k+=1
                # calcolo fascia turno
                # caso semplice se c 
                
        if orario_esecuzione != '':
            update_testata='''UPDATE UNIOPE.SCHEDE_ESEGUITE_EKOVISION 
                SET ORARIO_ESECUZIONE= :s1, FASCIA_TURNO = :s2
                WHERE ID_SCHEDA=:s3'''
            
            try:
                cur.execute(update_testata, (
                            orario_esecuzione, fascia_t, co[2]
                        ))
            except Exception as e:
                check=1
                logger.error(update_testata)
                logger.error('1:{}, 2:{}, 3:{}'.format(
                    orario_esecuzione, fascia_t, co[2]
                ))
                logger.error(e)

            con.commit()
        else:
            logger.error('''Per id_cheda {} non trovo l'orario di esecuzione'''.format(co[2]))                                                            
                                    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      
    