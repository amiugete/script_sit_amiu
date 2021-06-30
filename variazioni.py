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
    filename=logfile,
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


    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    if num_giorno==0:
        num=3
    elif num_giorno in (5,6):
        num=0
        logging.info('Oggi è {0}, lo script non gira'.format(giorno))
        exit()
    else:
        num=1
    
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
        and h.action IN ('UPDATE_ELEM')
        ) or 
        (h."type" IN ('ASTA PERCORSO') 
        and h.action IN ('INSERT', 'UPDATE')
        )
        )
        and pu.responsabile = 'S'
        and (p.data_dismissione is null or p.data_dismissione > current_date)
        order by ut, servizio'''.format(num)
    


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
    body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
    sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "File giornaliero con le variazioni"

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    #filename = file_variazioni  # In same directory as script


    ctype, encoding = mimetypes.guess_type(file_variazioni)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(file_variazioni)
        # Note: we should handle calculating the charset
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "image":
        fp = open(file_variazioni, "rb")
        attachment = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "audio":
        fp = open(file_variazioni, "rb")
        attachment = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(file_variazioni, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename=nome_file)
    message.attach(attachment)

    '''
    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())


    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {nome_file}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    '''
    
    
    text = message.as_string()






    with smtplib.SMTP_SSL(smtp_mail, port_mail, context=context) as server:
        server.login(user_mail, pwd_mail)
        server.sendmail(user_mail, receiver_email, text)
        # TODO: Send email here




if __name__ == "__main__":
    main()