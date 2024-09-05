#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Porta a 0 la frequenza e crea l'elenco delle schede da cancellare 


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

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/update_frequenze_per_tipo_servizio.log'.format(path)
errorfile='{}/log/error_update_frequenze_per_tipo_servizio.log'.format(path)
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
    
    
    
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Collegato a DB Oracle. Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    
    
    '''
    77	999	Corso di Formazione (solo INTERO TURNO)
    TOLTI
    
    79	998	Altre Attività Az.li (Vis. medica. az.le, Cso formazione, ecc.)
    TOLTI
    
    69	109	Posizionamento / Man Cestini
    TOLTI
    
    
    
    45	108	Segnaletica Cassonetti
    
    
    55	402	Posizionamento Segnaletica Mobile
    TOLTI 
    
    
    44	107	Consegna Sacchetti
    TOLTI
    
    
    67	802	Consegna/Ritiro mezzi Officine (non per la Valpo)
    TOLTI
    
    
    25 - Servizi a privati
    
    '''
    
    
    id_servizio=45
    new_freq= 'S0000000'
    
    schede_cancellare=''

    gg_indietro=-40

     # cerco le schede su ekovision
        # PARAMETRI GENERALI WS
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    schede_cancellare=''
    

    
    
    
    # QUERY ORIGINALE
    """
    query='''SELECT
        ID_PERCORSO,
        :c1 AS freq_new,
        TO_char(trunc(SYSDATE)+1, 'DD/MM/YYYY') AS data_uo,
        FREQUENZA_NEW, DTA_ATTIVAZIONE, DTA_DISATTIVAZIONE 
        FROM ANAGR_SER_PER_UO aspu WHERE id_servizio IN (
        :c2
        )
        AND  DTA_DISATTIVAZIONE > sysdate 
        AND FREQUENZA_NEW NOT IN (:c3)
        '''
    """        
    
    #QUERY PERSONALIZZATA
    query='''SELECT
        ID_PERCORSO,
        :c1 AS freq_new,
        TO_char(trunc(SYSDATE)+1, 'DD/MM/YYYY') AS data_uo,
        FREQUENZA_NEW, DTA_ATTIVAZIONE, DTA_DISATTIVAZIONE 
        FROM ANAGR_SER_PER_UO aspu 
        WHERE aspu.id_percorso in ('0203002401')'''
    
    testo_mail=''
    
    try:
        # query originale
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        #QUERY PERSONALIZZATA
        cur.execute(query, (new_freq,))
        lista_variazioni=cur.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)


           
    for vv in lista_variazioni:
        check_error=0
        logger.debug(vv[0])
        # ora devo cercare per quel codice percorso i percorsi attivi sulla UO
        query_uo='''SELECT aspu.ID_SER_PER_UO, 
        aspu.ID_UO, 
        au.DESC_UO,
        as2.DESC_SQUADRA 
        FROM ANAGR_SER_PER_UO aspu 
        JOIN ANAGR_UO au ON au.ID_UO = aspu.ID_UO 
        JOIN ANAGR_SQUADRE as2 ON as2.ID_SQUADRA = aspu.ID_SQUADRA
        WHERE ID_PERCORSO = :c1 AND aspu.DTA_DISATTIVAZIONE > sysdate '''
        try:
            cur.execute(query_uo, (vv[0],))
            percorsi_da_mod=cur.fetchall()
            #macro_tappe.append(tappa[2])
        except Exception as e:
            check_error=2
            logger.error('''{}'''.format(vv[0]))
            logger.error(query_uo)
            logger.error(e)
        for pp in percorsi_da_mod:
            cur0 = con.cursor()
            logger.debug(vv[0])
            logger.debug(int(pp[1]))
            logger.debug(vv[1])
            logger.debug(vv[2])
            try:
                risp=0
                """ret=cur0.callproc('UNIOPE.CAMBIO_FREQUENZA',
                         [vv[0],int(pp[1]),vv[1], vv[2], risp])
                logger.debug(ret)
                #exit()
                testo_mail='''{0}<li>
                Cod_percorso={1},
                Nuova frequenza UO = {2},
                UO = {3}
                Squadra = {4}</li>'''.format(testo_mail,vv[0], vv[1], pp[2], pp[3])"""
            except Exception as e:
                check_error=3
                #logger.error(query_o3)
                #logger.error(vv[0],pp[1],vv[1], vv[2], risp)
                logger.error(e)  
            cur0.close() 
            
            # applico le modifiche sulla UO
            testo_mail='AAA'
            #con.commit()
        
        
       
        
    
    
   
        # devo cercare le schede per id_ser_per uo > 33349 e <=33390 
        
          
        gg=gg_indietro
        while gg <= 14-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            #logger.debug(day)
            # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
            
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': day,
                'cod_modello_srv': vv[0]
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
                    logger.info('Id_scheda:{}'.format(id_scheda))
                    
                    if schede_cancellare=='':
                        schede_cancellare='{}'.format(id_scheda)
                    else:
                        schede_cancellare='{},{}'.format(schede_cancellare,id_scheda)
            gg+=1 
     

    
    
    if testo_mail!='':
        # Create a secure SSL context
        context = ssl.create_default_context()



    # messaggio='Test invio messaggio'


        subject = "ALERT MAIL - Percorsi per cui è stata modificata la frequenza sulla UO a partire da domani"
        
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Cambio frequenze"


        body='''E' stata variata la frequenza di uno o più percorsi sulla UO sulla base del servizio. <br>
        Le seguenti modifiche sono state apportate anche sulla UO <ul> {0} </ul>
        <br><br>
        Elenco schede da cancellare: <br>
        {1}
        <br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(testo_mail, schede_cancellare)
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        

        
        
        text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      