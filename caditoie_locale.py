#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script importa i dati dai geopackage caricati sul cloud di MERGIN al DB PostGIS

Lavora su:

- caditoie_2022 

Si riferisce al progetto denominato caditoie 

'''


import os, sys, getopt, re  # ,shutil,glob
import inspect, os.path

from credenziali import *


import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/caditoie.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)


import xlsxwriter



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



sender_email = user_mail
receiver_email='assterritorio@amiu.genova.it'
debug_email='roberto.marzocchi@amiu.genova.it'


def main(argv):
    

    logging.info('Leggo gli input')
    try:
        opts, args = getopt.getopt(argv,"hi:",["input="])
    except getopt.GetoptError:
        logging.error('caditoie_locale.py -i <input sqlite3 file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('caditoie_locale.py -i <input sqlite3 file>')
            sys.exit()
        elif opt in ("-i", "--input"):
            sqlite_file = arg
            logging.info('Geopackage file = {}'.format(sqlite_file))
    
    
    logging.info('Connessione al db PostgreSQL SIT ')
    connp = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    
    currp = connp.cursor()
    connp.autocommit = True
    
    nuove_caditoie_georeferenziate=[]


    logging.info('Connessione al GeoPackage')
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()
    for row in cur.execute('''SELECT fid, geom, id_amiu, 
                           tipo_dicca, tipo_amiu, note, 
                           tipo_occlusione, foto, id_via, id_tratto 
                           FROM "caditoie_2022"'''):
        id=int(row[0])
        
        # cerco se la nota è già sul DB 
        sql_select='SELECT fid from caditoie.caditoie_2022 where fid=%s;'
        
        try:
            currp.execute(sql_select, (id))
            check_cad=currp.fetchall()
        except Exception as e:
            logging.error(sql_select)
            logging.error(e)
        #logging.debug(len(check_nota))
        if len(check_cad)==1:
            #faccio update
            sql_update='''UPDATE caditoie.caditoie_2022
            SET geom=%s, id_amiu=%s, tipo_dicca=%s, tipo_amiu=%s, note=%s, tipo_occlusione=%s, foto=%s, id_via=%s, id_tratto=%s
            WHERE fid=%s;'''
            try:
                currp.execute(sql_update, ((x for x in row[1:]), id))
            except Exception as e:
                logging.error(sql_select)
                logging.error(e)
        else:
            nuove_caditoie_georeferenziate.append(row[0])
            sql_insert='''INSERT INTO caditoie.caditoie_2022
            (fid, geom, id_amiu, tipo_dicca, tipo_amiu, note, tipo_occlusione, foto, id_via, id_tratto)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
            try:
                currp.execute(sql_insert, (x for x in row))
            except Exception as e:
                logging.error(e)
            
            


    currp.close()

    """
    currp = connp.cursor()




    # se ci sono nuove note compilo excel e lo mando per mail
    if len(nuove_caditoie_georeferenziate)>0:
        #num_giorno=datetime.datetime.today().weekday()
        #giorno=datetime.datetime.today().strftime('%A')
        giorno_file=datetime.datetime.today().strftime('%Y%m%d')
        logging.debug(path)
        #logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
        nome_file='{}_nuove_caditoie_georeferenziate.xlsx'.format(giorno_file)
        file_note="{0}/report/{1}".format(path, nome_file)
        workbook = xlsxwriter.Workbook(file_note)
    
        w1 = workbook.add_worksheet('Nuove note odierne')

        w1.set_tab_color('red')


        title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
        text = workbook.add_format({'text_wrap': True})
        date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})
        
        #text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
        #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

        w1.set_column(0, 0, 9)
        w1.set_column(1, 2, 30)
        w1.set_column(3, 3, 50)
        w1.set_column(4, 4, 20)



        w1.write(0, 0, 'ID_CADITOIA', title) 
        w1.write(0, 1, 'INDIRIZZO', title) 
        w1.write(0, 2, 'RIFERIMENTO', title) 
        w1.write(0, 3, 'NOTA', title) 
        w1.write(0, 4, 'DATA ORA', title)  
        
        i=0
        r=1
        while i< len(nuove_caditoie_georeferenziate):
            w1.write(r, 0, int(nuove_caditoie_georeferenziate[i]), text)
            sql_select_p = '''SELECT riferimento, via, numero_civico
            FROM geo.v_piazzole_geom WHERE id_piazzola=%s; '''
            try:
                currp.execute(sql_select_p, (piazzole_nuove[i],))
                dettagli_piazzola=currp.fetchall()
            except Exception as e:
                logging.error(sql_select_p)
                logging.error(e)
            for dp in dettagli_piazzola:
                w1.write(r, 1, '{},{}'.format(dp[1],dp[2]), text) 
                w1.write(r, 2, '{}'.format(dp[0]), text) 
            w1.write(r, 3, note_nuove[i], text)
            w1.write(r, 4, date_nuove[i], date_format)
            r+=1
            i+=1
        
        
        
        
       
        
        
        
        
        
        
        workbook.close()


        



        ################################
        # predisposizione mail
        ################################
        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "Nuove annotazioni bilaterali"
        body = '''
Visualizza in allegato le nuove note relative alle nuove piazzole bilaterali in fase di installazione e/o l'elenco delle nuove piazzole installate. \n\n
Mail generata automaticamente dal codice python progettazione_locale.py che gira giornalmente su server amiugis. 
In caso di problemi si prega di non rispondere a questa mail, bensì di contattarci alla mail assistenzaterritorio@amiu.genova.it 
\n\n\n\n
AMIU Assistenza Territorio
'''
       

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = debug_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Nuove annotazioni/installazioni odierne bilaterali"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        allegato(message, file_note, nome_file)
        
        #text = message.as_string()

        # Now send or store the message
        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio) 

    else:
        ################################
        # predisposizione mail
        ################################
        # Create a secure SSL context
        context = ssl.create_default_context()

        subject = "Non si sono georeferenziate nuove caditoie"
        body = '''Mail generata automaticamente dal codice python progettazione_locale.py che gira giornalmente su server amiugis. 
Il codice ha girato ma non sono state georeferenziate nuove caditoie
\n\n\n\n
AMIU Assistenza Territorio
'''
        
        #cc_mail='pianar@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = debug_email
        #message["Cc"] = cc_mail
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Nuove annotazioni bilaterali"

        
                        
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        # aggiunto allegato (usando la funzione importata)
        #allegato(message, file_note, nome_file)
        
        #text = message.as_string()

        # Now send or store the message
        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)


    
    currp.close()
    
    """
    
    # CHIUDO LA CONNESSIONE
    connp.close()

    
    
    
    

if __name__ == "__main__":
    main(sys.argv[1:])  
