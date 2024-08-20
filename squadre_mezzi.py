#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge allinea mezzi e squadre dall'anagrafica percorsi (nuovo schema SIT / UO) al

La parte delle squadre è ancora da realizzare

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


def main():


    nome_db=db #db_test
    
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    
    query_mezzi_squadre= '''select dpsu.id_percorso_sit, dpsu.cod_percorso, 
        array_agg(pu.id_squadra), string_agg(distinct pu.cdaog3, ', ')  
        from anagrafe_percorsi.date_percorsi_sit_uo dpsu
        join anagrafe_percorsi.elenco_percorsi ep 
            on ep.cod_percorso = dpsu.cod_percorso 
            and ep.data_inizio_validita = dpsu.data_inizio_validita 
            and ep.data_fine_validita = dpsu.data_fine_validita
        join anagrafe_percorsi.percorsi_ut pu on ep.cod_percorso = pu.cod_percorso 
            and ep.data_inizio_validita = pu.data_attivazione  
            and ep.data_fine_validita = pu.data_disattivazione 
        where dpsu.data_fine_validita > now()
        group by dpsu.id_percorso_sit, dpsu.cod_percorso'''


    # 1 faccio un controllo sui mezzi doppi
    query_pg='''{} having count(distinct pu.cdaog3) > 1'''.format(query_mezzi_squadre)
    try:
        curr.execute(query_pg)
        check=curr.fetchall()
    except Exception as e:
        logging.error(query_pg)
        logging.error(e)
    
    if len(check) > 0: 
        logging.error('Ci sono dei percorsi che prevedono mezzi diversi. Per ora non è consentito.. lo sarà')
        logging.error(query_pg)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
    
    
    
    # faccio update dei mezzi
    
    try:
        curr.execute(query_mezzi_squadre)
        mezzi_squadre=curr.fetchall()
    except Exception as e:
        logging.error(query_mezzi_squadre)
        logging.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()


    for ms in mezzi_squadre:
        update_percorsi_sit='''update elem.percorsi set famiglia_mezzo = %s
        where id_percorso= %s and famiglia_mezzo != %s'''
    
        
        try:
            curr.execute(update_percorsi_sit, (ms[3], ms[0], ms[3]))
        except Exception as e:
            logging.error(query_mezzi_squadre)
            logging.error(e)
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()    
        

    exit();
    if len(percorsi_anomali_uo)>0:
        file_ut="{0}/report/anomalie_turni.xlsx".format(path)
        workbook = xlsxwriter.Workbook(file_ut)
        w1 = workbook.add_worksheet('Anomalie turni')

        w1.set_tab_color('red')

        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True, 'bg_color': '#ccffee'})
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ccffee'})
        text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 0, 15)
        w1.set_column(1, 1, 60)
        w1.set_column(2, 3, 20)
        w1.set_column(4, 4, 25)
        w1.set_column(5, 5, 10)
        w1.set_column(6, 6, 35)


        w1.write(0, 0, 'COD_PERCORSO', title) 
        w1.write(0, 1, 'DESCRIZIONE', title) 
        w1.write(0, 2, 'SERVIZIO', title) 
        w1.write(0, 3, 'DATA ATTIVAZIONE', title) 
        w1.write(0, 4, 'UO', title) 
        w1.write(0, 5, 'ID_TURNO', title) 
        w1.write(0, 6, 'DESCRIZIONE_TURNO', title) 
        
        i=0
        r=1
        while i< len(percorsi_anomali_uo):
            select_anomalie= '''SELECT aspu.ID_PERCORSO, aspu.DESCRIZIONE, as2.DESC_SERVIZIO,
            aspu.DTA_ATTIVAZIONE, au.DESC_UO, aspu.ID_TURNO, at2.DESCR_ORARIO  
            FROM ANAGR_SER_PER_UO aspu 
            JOIN ANAGR_TURNI at2 ON at2.ID_TURNO =aspu.ID_TURNO
            JOIN ANAGR_UO au ON au.ID_UO = aspu.ID_UO 
            JOIN ANAGR_SERVIZI as2 ON as2.ID_SERVIZIO = aspu.ID_SERVIZIO 
            WHERE ID_PERCORSO IN (:id_perc) AND 
            (aspu.DTA_DISATTIVAZIONE > SYSDATE OR (
            aspu.DTA_DISATTIVAZIONE = (SELECT max(DTA_DISATTIVAZIONE) FROM ANAGR_SER_PER_UO WHERE ID_PERCORSO=aspu.ID_PERCORSO AND ID_UO=aspu.ID_UO)
            and id_percorso IN (SELECT ID_PERCORSO FROM CONS_PERCORSI_STAGIONALI))) '''
            try:
                cur.execute(select_anomalie, [percorsi_anomali_uo[i]])
                anomalie=cur.fetchall()
            except Exception as e:
                logging.error(query_pg)
                logging.error(e)
            for aa in anomalie:
                j=0
                while j<len(aa):
                    if i%2==0:
                        if j==3:
                            w1.write(r, j, aa[j], date_format)
                        else:
                            w1.write(r, j, aa[j], text)
                    else:
                        if j==3:
                            w1.write(r, j, aa[j], date_format_dispari)
                        else:
                            w1.write(r, j, aa[j], text_dispari)
                    j+=1
                r+=1
            i+=1
        
        workbook.close()
        ################################
        # predisposizione mail
        ################################

        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "Anomalie turni su UO"
        body = '''Mail generata automaticamente dal codice python turni.py che gira su server amiugis.\n\n
Esistono dei percorsi su UO dove c'è una discordanza di turni fra un UT e l'altra. \nVisualizza l'allegato e contatta le UT di riferimento\n\n\n\n
AMIU Assistenza Territorio
'''
        #sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Anomalie turni"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_ut, 'anomalie_turni.xlsx')
        
        #text = message.as_string()

        # Now send or store the message
        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)
        


if __name__ == "__main__":
    main()   