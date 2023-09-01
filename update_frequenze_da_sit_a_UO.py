#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script usa il trigger presente su SIT che scrive i dati in:
- schema etl
- tabella frequenze_percorsi_history

Eventualmente da integrare con altro
'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

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


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/update_frequenze_da_sit_a_UO.log'.format(path)
errorfile='{}/log/error_update_frequenze_da_sit_a_UO.log'.format(path)
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





def main():
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Collegato a DB Oracle. Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    
    


    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

    
    
    query='''select p.cod_percorso, 
fo.freq_binaria as new_freq_uo, 
to_char(fph.changed_on::date + interval '1 day', 'dd/mm/yyyy') as data_uo,
fph.id
from etl.frequenze_percorsi_history fph 
join elem.percorsi p on p.id_percorso = fph.id_percorso 
join etl.frequenze_ok fo on fo.cod_frequenza = fph.frequenza_new 
where fph.uo = 'f'; '''

    testo_mail=''
    
    try:
        curr.execute(query)
        lista_variazioni=curr.fetchall()
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
        WHERE ID_PERCORSO = :c1 AND aspu.DTA_DISATTIVAZIONE > SYSDATE'''
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
                ret=cur0.callproc('UNIOPE.CAMBIO_FREQUENZA',
                         [vv[0],int(pp[1]),vv[1], vv[2], risp])
                logger.debug(ret)
                testo_mail='''{0}<li>
                Cod_percorso={1},
                Nuova frequenza UO = {2},
                UO = {3}
                Squadra = {4}</li>'''.format(testo_mail,vv[0], vv[1], pp[2], pp[3])
            except Exception as e:
                check_error=3
                #logger.error(query_o3)
                #logger.error(vv[0],pp[1],vv[1], vv[2], risp)
                logger.error(e)  
            cur0.close() 
            
            # applico le modifiche sulla UO
            con.commit() 
        if check_error==0: 
            update_sit='''update etl.frequenze_percorsi_history fph set uo='t' where id=%s'''
            try:
                curr.execute(update_sit, (vv[3],))
            except Exception as e:
                logger.error('SIT NON AGGIORNATO!!! VERIFICARE UO E CAMBIARE IL DATO SU SIT A MANO')
                logger.error(e)
            conn.commit()

    
    
    if testo_mail!='':
        # Create a secure SSL context
        context = ssl.create_default_context()



    # messaggio='Test invio messaggio'


        subject = "ALERT MAIL - Percorsi per cui è stata modificata la frequenza su SIT"
        
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


        body='''E' stata variata la frequenza di uno o più percorsi su SIT. <br>
        Le seguenti modifiche sono state apportate anche sulla UO <ul> {0} </ul>
        <br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(testo_mail)
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path)
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
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      