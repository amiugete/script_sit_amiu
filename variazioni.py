#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica le variazioni e manda CSV a assterritorio@amiu.genova.it giornalmemte con la sintesi delle stesse 
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import datetime
import holidays
from workalendar.europe import Italy


from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail


#import requests

import logging
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





import csv

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    #filename=logfile,
    level=logging.DEBUG)





def main():
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True


    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    
    holiday_list = []
    holiday_list_pulita=[]
    for holiday in holidays.Italy(years=[(oggi.year -1), (oggi.year)]).items():
        #print(holiday[0])
        #print(holiday[1])
        holiday_list.append(holiday)
        holiday_list_pulita.append(holiday[0])
    
    
    # AGGIUNGO LA FESTA PATRONALE
    logging.debug('Anno corrente = {}'.format(oggi.year))
    fp = datetime.datetime(oggi.year, 6, 24)
    festa_patronale=datetime.date(fp.year, fp.month, fp.day)
    holiday_list_pulita.append(festa_patronale)
    
    if num_giorno==0:
        num=3
        # controllo se venerdì era festivo
        ven = oggi - datetime.timedelta(days = num)
        ven=datetime.date(ven.year, ven.month, ven.day)
        if ven in holiday_list_pulita:
            num=4
            gio = oggi - datetime.timedelta(days = num)
            gio=datetime.date(gio.year, gio.month, gio.day)
            if gio in holiday_list_pulita:
                num=5
    elif num_giorno in (5,6):
        num=0
        logging.info('Oggi è {0}, lo script non gira'.format(giorno))
        exit()
    else:
        num=1
        # se oggi è festa
        if oggi in holiday_list_pulita:
            num=0
            logging.info('Oggi è giorno festivo, lo script non gira'.format(giorno))
            exit()
        ieri=oggi - datetime.timedelta(days = num)
        ieri=datetime.date(ieri.year, ieri.month, ieri.day)
        #logging.debug('Ieri = {}'.format(ieri))
        #logging.debug(holiday_list_pulita)
        if ieri in holiday_list_pulita:
            # se ieri era lunedì (es. Pasquetta)
            logging.debug('Ieri {}'.format(ieri.strftime('%A')))
            if ieri.weekday()==0:
                num=4 # da ven in poi
            # se ieri era martedì
            elif ieri.weekday()==1:
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e lunedì (caso di Natale lunedì e S. Stefano Martedì)
                if altroieri in holiday_list_pulita:
                    num=5
            # altrimenti
            else: 
                num=2
                # verifico altro ieri 
                altroieri=oggi - datetime.timedelta(days = num)
                altroieri=datetime.date(altroieri.year, altroieri.month, altroieri.day)
                # se altro ieri era festivo e non lunedì (caso di Natale martedì/mercoledì o di due feste vicine)
                if altroieri in holiday_list_pulita:
                    num=3
                    
    
    logging.debug('num = {}'.format(num))
    #exit()                
                    
    
    
    '''******************************************************************************************************
    NON SONO COMPRESI I PERCORSI STAGIONALI per cui vanno re-importate le variazioni in fase di attivazione 
    ********************************************************************************************************'''
    query='''select distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut
        from util.sys_history h
        inner join elem.percorsi p 
        on h.id_percorso = p.id_percorso 
        inner join elem.percorsi_ut pu 
        on pu.cod_percorso =p.cod_percorso 
        inner join elem.servizi s 
        on s.id_servizio =p.id_servizio
        inner join topo.ut u 
        on u.id_ut = pu.id_ut 
        where h.datetime > (current_date - INTEGER '{0}') 
        and h.datetime < current_date 
        and (
        (h."type" IN ('PERCORSO') 
        and h.action IN ('UPDATE_ELEM', 'UPDATE')
        ) or 
        (h."type" IN ('ASTA PERCORSO') 
        and h.action IN ('INSERT', 'UPDATE', 'DELETE')
        )
        )
        and pu.responsabile = 'S'
        and p.id_categoria_uso in (3)
        and (p.data_dismissione is null or p.data_dismissione > current_date )
        union 
        select p2.cod_percorso , p2.descrizione, s2.descrizione as servizio, u2.descrizione  as ut
        from elem.percorsi p2 
        join elem.servizi s2 on s2.id_servizio = p2.id_servizio 
        inner join elem.percorsi_ut pu2 
        on pu2.cod_percorso =p2.cod_percorso
        inner join topo.ut u2 
        on u2.id_ut = pu2.id_ut 
        where pu2.responsabile = 'S'
        and p2.id_categoria_uso in (3)
        and p2.data_attivazione > (current_date - INTEGER '{0}')
        UNION
        select distinct p3.cod_percorso , p3.descrizione, s3.descrizione as servizio, u3.descrizione  as ut
        from elem.elementi e
        join (
        select datetime, description, id_piazzola, split_part(replace(description, 'Elementi tipo ', ''), ' ',1) as tipo_elemento 
        from util.sys_history sh 
        where type='PIAZZOLA_ELEM' and action = 'UPDATE' and description ilike 'Elementi tipo%' and datetime > (current_date - INTEGER '{0}')
        and id_percorso is null 
        ) b on b.id_piazzola=e.id_piazzola and b.tipo_elemento::int = e.tipo_elemento and date_trunc('second', e.data_inserimento) != date_trunc('second', b.datetime)  
        join elem.elementi_aste_percorso eap on eap.id_elemento = e.id_elemento 
        join elem.aste_percorso ap on eap.id_asta_percorso = ap.id_asta_percorso 
        join elem.percorsi p3 on p3.id_percorso = ap.id_percorso 
        join elem.servizi s3 on s3.id_servizio = p3.id_servizio 
        inner join elem.percorsi_ut pu3 
        on pu3.cod_percorso =p3.cod_percorso
        inner join topo.ut u3 
        on u3.id_ut = pu3.id_ut 
        where pu3.responsabile = 'S'
        and p3.id_categoria_uso in (3)
        order by ut, servizio
        '''.format(num)
    


    try:
        curr.execute(query)
        lista_variazioni=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    cod_percorso=[]
    descrizione=[]
    servizio=[]
    ut=[]

           
    for vv in lista_variazioni:
        logging.debug(vv[0])
        cod_percorso.append(vv[0])
        descrizione.append(vv[1])
        servizio.append(vv[2])
        ut.append(vv[3])
        insert_query='''
            update elem.percorsi set data_attivazione = (current_date)
            where data_attivazione < now() and 
            cod_percorso='{}'
        '''.format(vv[0])

        try:
            curr.execute(insert_query)
	        #lista_variazioni=curr.fetchall()
        except Exception as e:
            logging.error(insert_query)
            logging.error(e)                                            

    

    

       
    if len(cod_percorso)>0:
        logging.info('Oggi ci sono {} variazioni. Creo nuovo file'.format(len(cod_percorso)))
        nome_file="{0}_variazioni.xlsx".format(giorno_file)
        file_variazioni="{0}/variazioni/{1}".format(path,nome_file)
        
        
        workbook = xlsxwriter.Workbook(file_variazioni)
        w = workbook.add_worksheet()

        w.write(0, 0, 'cod_percorso') 
        w.write(0, 1, 'descrizione') 
        w.write(0, 2, 'servizio') 
        w.write(0, 3, 'ut') 
        
        '''
        w.write(1, 0, 1234.56)  # Writes a float
        w.write(2, 0, 'Hello')  # Writes a string
        w.write(3, 0, None)     # Writes None
        w.write(4, 0, True)     # Writes a bool
        '''
        
        #f = open(file_variazioni, "w")
        #f.write('cod_percorso;descrizione;servizio;ut_resp\n')
    


    i=0
    while i<len(cod_percorso):
        #f.write('"{}";"{}";"{}";"{}"\n'.format(cod_percorso[i],descrizione[i],servizio[i],ut[i]))
        w.write(i+1,0,'{}'.format(cod_percorso[i]))
        w.write(i+1,1,'{}'.format(descrizione[i]))
        w.write(i+1,2,'{}'.format(servizio[i]))
        w.write(i+1,3,'{}'.format(ut[i]))
        i+=1

    if len(cod_percorso)>0:
        #f.close()
        workbook.close()

    #exit() # per ora esco qua e non vado oltre

    
    # Create a secure SSL context
    context = ssl.create_default_context()



   # messaggio='Test invio messaggio'


    subject = "Variazioni odierne - File automatico"
    if num==1:
        gg_text='''dell'ultimo giorno (ieri)'''
    else:
        gg_text='''degli ultimi {} giorni'''.format(num)
    body = """Report giornaliero delle variazioni {} .<br><br><br>
    L'applicativo che gestisce l'estrazione delle utenze è stato realizzato dal gruppo Gestione Applicativi del SIGT.<br> 
    Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {}<br><br>
    Giorno {}<br><br>
    AMIU Assistenza Territorio<br>
     <img src="cid:image1" alt="Logo" width=197>
    <br>
    """.format(gg_text, user_mail, giorno_file)
    #sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File giornaliero con le variazioni"


        
                        
    # Add body to email
    message.attach(MIMEText(body, "html"))


    #aggiungo logo 
    logoname='{}/img/logo_amiu.jpg'.format(path)
    immagine(message,logoname)
    
    
    # aggiunto allegato (usando la funzione importata)
    allegato(message, file_variazioni, nome_file)
    # Add body to email
    message.attach(MIMEText(body, "plain"))
    
    
    text = message.as_string()

    logging.info("Richiamo la funzione per inviare mail")
    invio=invio_messaggio(message)
    logging.info(invio)


if __name__ == "__main__":
    main()