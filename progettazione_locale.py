#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script importa i dati dai geopackage caricati sul cloud di MERGIN al DB PostGIS

Lavora su:

- annotazioni 
- installazioni

Si riferisce al progetto denominato installazioni_bilaterali_genova 

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
logfile='{}/log/installazione_bilaterali_genova.log'.format(path)
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



#sender_email = user_mail
receiver_email='assterritorio@amiu.genova.it'
debug_email='roberto.marzocchi@amiu.genova.it'


def main(argv):
    

    logging.info('Leggo gli input')
    try:
        opts, args = getopt.getopt(argv,"hi:",["input="])
    except getopt.GetoptError:
        logging.error('progettazione_locale.py -i <input sqlite3 file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('progettazione_locale.py -i <input sqlite3 file>')
            sys.exit()
        elif opt in ("-i", "--input"):
            sqlite_file = arg
            logging.info('Geopackage file = {}'.format(sqlite_file))
    
    
    logging.info('Connessione al db PostgreSQL SIT PROG')
    connp = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    
    currp = connp.cursor()
    connp.autocommit = True
    
    note_nuove=[]
    date_nuove=[]
    piazzole_nuove=[]

    installate_nuove=[]
    i_elementi_nuovi=[]
    i_date_nuove=[]
    m_nuove=[]
    t_nuove=[]


    logging.info('Connessione al GeoPackage')
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()
    for row in cur.execute('SELECT * FROM note_installazione ORDER BY update_time;'):
        id=int(row[0])
        id_piazzola=int(row[1])
        nota=row[2]
        dataora=row[3]
        #logging.debug("{} - {}".format(id,nota))
        #logging.debug("{} - {}".format(id_piazzola,dataora))
        
        # cerco se la nota è già sul DB 
        sql_select='SELECT * from ne.note_installazione where id_piazzola=%s and update_time=%s;'
        
        try:
            currp.execute(sql_select, (id_piazzola,dataora,))
            check_nota=currp.fetchall()
        except Exception as e:
            logging.error(sql_select)
            logging.error(e)
        #logging.debug(len(check_nota))
        if len(check_nota)==1:
            #faccio update
            sql_update='''UPDATE ne.note_installazione
            SET note=%s
            WHERE id_piazzola=%s and update_time=%s;'''
            try:
                currp.execute(sql_update, (nota,id_piazzola,dataora,))
            except Exception as e:
                logging.error(sql_select)
                logging.error(e)
        else:
            note_nuove.append(nota)
            date_nuove.append(dataora)
            piazzole_nuove.append(id_piazzola)
            sql_insert='''INSERT INTO ne.note_installazione
            (id, id_piazzola, note, update_time)
            VALUES(%s, %s, %s, %s);'''
            try:
                currp.execute(sql_insert, (id,id_piazzola,nota,dataora,))
            except Exception as e:
                logging.error(e)
            
            


    currp.close()
    currp = connp.cursor()

    # faccio la stessa cosa con le nuove piazzole installate

    for row in cur.execute('SELECT * FROM elementi_installati ORDER BY time;'):
        id_elemento=int(row[0])
        if row[1]==1:
            installata='true'
        elif row[1]==0:
            installata='false'
        else:
            installata='false'
        dataora=row[2]
        matr=row[3]
        tag=row[4]
        #logging.debug("{} - {}".format(id,nota))
        #logging.debug("{} - {}".format(id_piazzola,dataora))
        
        # cerco se la nota è già sul DB 
        sql_select='SELECT * from ne.elementi_installati where id_elemento=%s;'
        
        try:
            currp.execute(sql_select, (id_elemento,))
            check_inst=currp.fetchall()
        except Exception as e:
            logging.error(sql_select)
            logging.error(e)
        #logging.debug(len(check_nota))
        if len(check_inst)==1:
            #faccio update
            sql_update='''UPDATE ne.elementi_installati
            SET installata=%s, matricola=%s,
            tag=%s
            WHERE id_elemento=%s;'''
            try:
                currp.execute(sql_update, (installata, matr, tag, id_elemento,))
            except Exception as e:
                logging.error(e)
        else:
            installate_nuove.append(installata)
            i_elementi_nuovi.append(id_elemento)
            i_date_nuove.append(dataora)
            m_nuove.append(matr)
            t_nuove.append(tag)
            sql_insert='''INSERT INTO ne.elementi_installati
            (id_elemento, installata, matricola, tag, time)
            VALUES(%s, %s, %s, %s, %s);'''
            try:
                currp.execute(sql_insert, (id_elemento, installata, matr, tag, dataora,))
            except Exception as e:
                logging.error(sql_insert)
                logging.error(e)
    
    
    currp.close()


    currp = connp.cursor()

    # se ci sono nuove note compilo excel e lo mando per mail
    if len(note_nuove)>0 or len(installate_nuove)>0:
        #num_giorno=datetime.datetime.today().weekday()
        #giorno=datetime.datetime.today().strftime('%A')
        giorno_file=datetime.datetime.today().strftime('%Y%m%d')
        logging.debug(path)
        #logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
        nome_file='{}_installazioni_bilaterali.xlsx'.format(giorno_file)
        file_note="{0}/report/{1}".format(path, nome_file)
        workbook = xlsxwriter.Workbook(file_note)
        if len(note_nuove)>0:
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



            w1.write(0, 0, 'PIAZZOLA', title) 
            w1.write(0, 1, 'INDIRIZZO', title) 
            w1.write(0, 2, 'RIFERIMENTO', title) 
            w1.write(0, 3, 'NOTA', title) 
            w1.write(0, 4, 'DATA ORA', title)  
            
            i=0
            r=1
            while i< len(note_nuove):
                w1.write(r, 0, int(piazzole_nuove[i]), text)
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
        
        
        
        
        if len(installate_nuove)>0:
            w1 = workbook.add_worksheet('Piazzole installate oggi')

            w1.set_tab_color('green')

            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})

            title = workbook.add_format({'bold': True, 'bg_color': '#F9FF33', 'valign': 'vcenter', 'center_across': True,'text_wrap': True})
            text = workbook.add_format({'text_wrap': True})
            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm'})
            
            #text_dispari= workbook.add_format({'text_wrap': True, 'bg_color': '#ffcc99'})
            #date_format_dispari = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm', 'bg_color': '#ffcc99'})

            w1.set_column(0, 0, 9)
            w1.set_column(1, 2, 30)
            w1.set_column(3, 4, 10)
            w1.set_column(5, 5, 20)
            w1.set_column(6, 7, 10)


            w1.write(0, 0, 'PIAZZOLA', title) 
            w1.write(0, 1, 'INDIRIZZO', title) 
            w1.write(0, 2, 'RIFERIMENTO', title)
            w1.write(0, 3, 'ELEMENTO', title)
            w1.write(0, 4, 'INSTALLATO', title) 
            w1.write(0, 5, 'DATA ORA', title)
            w1.write(0, 6, 'MATRICOLA', title)
            w1.write(0, 7, 'TAG', title)  
            
            i=0
            r=1
            while i< len(installate_nuove):
                w1.write(r, 3, int(i_elementi_nuovi[i]), text)
                sql_select_p = '''SELECT id_piazzola, riferimento, via, numero_civico
                FROM geo.v_piazzole_geom WHERE id_piazzola=(SELECT id_piazzola from elem.v_elementi WHERE id_elemento=%s); '''
                try:
                    currp.execute(sql_select_p, (i_elementi_nuovi[i],))
                    dettagli_piazzola=currp.fetchall()
                except Exception as e:
                    logging.error(sql_select_p)
                    logging.error(e)
                for dp in dettagli_piazzola:
                    w1.write(r, 1, '{},{}'.format(dp[2],dp[3]), text) 
                    w1.write(r, 0, '{}'.format(dp[0]), text)
                    w1.write(r, 2, '{}'.format(dp[1]), text) 
                w1.write(r, 4, installate_nuove[i], text)
                w1.write(r, 5, i_date_nuove[i], date_format)
                w1.write(r, 6, m_nuove[i], date_format)
                w1.write(r, 7, t_nuove[i], date_format)
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

        subject = "Non ci sono nuove annotazioni bilaterali"
        body = '''Mail generata automaticamente dal codice python progettazione_locale.py che gira giornalmente su server amiugis. 
Il codice ha girato ma non sono state rilevate nuove annotazioni /installazioni
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
    connp.close()

    
    
    
    

if __name__ == "__main__":
    main(sys.argv[1:])  
