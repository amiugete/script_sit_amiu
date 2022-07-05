#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge le date di disattivazioni su U.O. ed effettua l'allineamento con SIT:


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql


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
logfile='{}/log/check_disattivazioni.log'.format(path)
errorfile='{}/log/error_check_disattivazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)




'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''


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
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()


    nome_db=db #db_test
    
    # connessione a PostgreSQL
    logging.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    cp_s=[] # codice percorso SIT
    cp_u=[] # codice percorso UO
    des_s=[] # descrizione SIT
    data_u=[] # data UO


    # cerco i percorsi attivi su SIT
    query_percorsi=''' select id_percorso, cod_percorso, descrizione, 
    data_dismissione 
    from elem.percorsi where id_categoria_uso = 3 and data_dismissione is null
    order by cod_percorso'''

    try:
        curr.execute(query_percorsi)
        percorsi=curr.fetchall()
    except Exception as e:
        logging.error(query_percorsi)
        logging.error(e)
    logging.debug(len(percorsi))
    for pp in percorsi:
        cod_percorso=pp[1]

        query_UO= '''SELECT ID_PERCORSO, 
        DTA_DISATTIVAZIONE
        FROM ANAGR_SER_PER_UO aspu 
        WHERE ID_PERCORSO = :cod_perc
        AND DTA_DISATTIVAZIONE = 
        (SELECT max(DTA_DISATTIVAZIONE) FROM ANAGR_SER_PER_UO aspu1 WHERE ID_PERCORSO = aspu.ID_PERCORSO) 
        AND DTA_DISATTIVAZIONE < SYSDATE AND ID_SERVIZIO !=9
        GROUP BY ID_PERCORSO, 
        DTA_DISATTIVAZIONE'''
        try:
            cur.execute(query_UO, [cod_percorso])
            percorso_UO=cur.fetchall()
        except Exception as e:
            logging.error(query_UO)
            logging.error(e)
        

        # verifico se non ci fossero dati discordanti
        anomalie_date=[]
        if len (percorso_UO)>1:
            anomalie_date.append(cod_percorso)
            logger.warning('Il percorso {0} ha diverse date di disattivazione su UO'.format(cod_percorso))


        for pu in percorso_UO:
            #id_ser_per_uo=
            cod_percorso_uo=pu[0]
            data_uo=pu[1]
            logger.info('Il percorso {0} ha data di disattivazione {1}'.format(cod_percorso_uo, data_uo))
            cp_u.append(cod_percorso_uo)
            cp_s.append(cod_percorso)
            data_u.append(data_uo)
            des_s.append(pp[2])

            query_update='''UPDATE elem.percorsi set id_categoria_uso= 4, data_dismissione = %s 
            WHERE id_percorso=%s '''
            curr1 = conn.cursor() 
            try:
                curr1.execute(query_update, (data_uo,pp[0]))
            except Exception as e:
                logging.error(query_update)
                logging.error(e)
            curr1.close()
            conn.commit()


    if len(cp_u)>0:
        nome_file_ut='percorsi_disattivati_su_UO.xlsx'
        file_ut="{0}/report/{1}".format(path, nome_file_ut)
        workbook = xlsxwriter.Workbook(file_ut)
        w1 = workbook.add_worksheet('Dis_UO_att_SIT')

        w1.set_tab_color('red')

        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True})
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})
        text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 1, 15)
        w1.set_column(2, 2, 50)
        w1.set_column(3, 4, 25)
        w1.set_column(5, 5, 15)


        w1.write(0, 0, 'COD_PERCORSO_UO', title) 
        w1.write(0, 1, 'COD_PERCORSO_SIT', title) 
        w1.write(0, 2, 'DESCRIZIONE SIT', title) 
        w1.write(0, 3, 'SERVIZIO', title) 
        w1.write(0, 4, 'UT', title) 
        w1.write(0, 5, 'DATA DISATTIVAZIONE', title) 
          
        i=0
        #r=1
        while i< len(cp_u):
            query_uo2='''SELECT ID_SER_PER_UO, 
                ID_PERCORSO, 
                as2.DESC_SERVIZIO,au.DESC_UO,
                DTA_DISATTIVAZIONE
                FROM ANAGR_SER_PER_UO aspu 
                JOIN ANAGR_UO au ON au.ID_UO = aspu.ID_UO 
                JOIN ANAGR_SERVIZI as2 ON as2.ID_SERVIZIO = aspu.ID_SERVIZIO 
                WHERE ID_PERCORSO = :cod_perc
                AND DTA_DISATTIVAZIONE = 
                (SELECT max(DTA_DISATTIVAZIONE) FROM ANAGR_SER_PER_UO aspu1 WHERE ID_PERCORSO = aspu.ID_PERCORSO
                ) AND aspu.ID_SERVIZIO !=9 '''
            try:
                cur.execute(query_uo2, [cp_u[i]])
                percorso_UO2=cur.fetchall()
            except Exception as e:
                logging.error(query_uo2)
                logging.error(e)
    


            for pu2 in percorso_UO2: 
                servizio=pu2[2]
                ut=pu2[3]


            w1.write(i+1, 0, cp_u[i], text)
            w1.write(i+1, 1, cp_s[i], text)
            w1.write(i+1, 2, des_s[i], text)
            w1.write(i+1, 3, servizio, text)
            w1.write(i+1, 4, ut, text)
            w1.write(i+1, 5, data_u[i], date_format)           
            
            i+=1
        
        workbook.close()
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "Percorsi da disattivare su SIT"
        body = '''
        <html>
            <head></head>
            <body>
        <p>
        Mail generata automaticamente dal codice python check_disattivazioni.py che gira ogni mattina su server amiugis.
        </p><p>
Esistono dei percorsi che risultano disattivati su UO non su SIT . <br> Visualizza l'allegato e controlla i dati che sono stati aggiornati in automatico.
Se ci sono dei dubbi contatta le UT di riferimento 
</p><br><p>
AMIU Assistenza Territorio
</p>
  </body>
</html>
'''
        sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email #debug_mail
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Anomalie percorsi disattivati"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "html"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_ut, nome_file_ut)
        
        #text = message.as_string()

        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)
        
    cur.close()
    curr.close()

if __name__ == "__main__":
    main()   