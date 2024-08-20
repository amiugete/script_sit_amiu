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

import cx_Oracle

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


from crea_dizionario_da_query import *


import csv

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

'''#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/variazioni_importazioni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    filename=logfile,
    level=logging.DEBUG)
'''


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/percorsi_nuova_versione.log'.format(path)
errorfile='{}/log/error_percorsi_nuova_versione.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)



oggi1=datetime.datetime.today().strftime('%d/%m/%Y')



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






def main():
    # carico i mezzi sul DB PostgreSQL
    test= 0
    
    if test==1:
        db_name=db_test
    else:
        db_name=db
        
        
    logger.info('Connessione al db {0}'.format(db_name))
    conn = psycopg2.connect(dbname=db_name,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    
    
    
    check_error=0
    
    
    
    # cerco i percorsi da modificare oggi per domani
    
    query_pdm='''select sh.id_user , p.id_percorso, 
        replace(cod_percorso ,
        (select to_char((now()+ INTERVAL '1 DAY'),'_DDMMYYYY')), '') as codice, 
        cod_percorso,
        descrizione, su.name
        from elem.percorsi p
        join util.sys_history sh on sh.id_percorso = p.id_percorso and sh.type ilike 'PERCORSO' and sh."action" ilike 'insert' 
        left join util.sys_users su on sh.id_user = su.id_user 
        where cod_percorso like (select to_char((now()+ INTERVAL '1' DAY),'%_DDMMYYYY'))
        and id_categoria_uso = 2
        order by 1'''
    
    
    try:
        curr.execute(query_pdm)
        lista_percorsi_new=curr.fetchall()
    except Exception as e:
        check_error+=1
        logger.error(e)
        logger.error(query_pdm)

    #inizializzo gli array
    id_percorsi_new=[]
    cod_percorsi=[]
    descrizioni_new=[]
    descrizioni_old=[]
    
    logger.debug(len(lista_percorsi_new))    
    lista_percorsi= '<ul>'
    for pn in lista_percorsi_new:
        logger.debug(pn[2])
        id_percorsi_new.append(pn[1])
        cod_percorsi.append(pn[2])
        descrizioni_new.append(pn[4])

        curr1 = conn.cursor()
        
        # cerco la vecchia descrizione
        query_descrizione_old='''select descrizione from elem.percorsi where cod_percorso = %s and id_categoria_uso = 3'''
        
        try:
            curr1.execute(query_descrizione_old, (pn[2],))
            descr_old=curr1.fetchall()
        except Exception as e:
            check_error+=1
            logger.error(e)
            logger.error(query_descrizione_old)
            
        for do in descr_old:
            descrizioni_old.append(do[0])
            lista_percorsi= '{0}<li>{1} - {2}</li>'.format(lista_percorsi, pn[2], do[0], )
        
        
        curr1.close()
        
        # dismetto vecchia versione del percorso
        curr1 = conn.cursor()
        if check_error==0:
            query_dismissione='''update elem.percorsi set data_dismissione = date(now())+1 ,id_categoria_uso = 4
    where cod_percorso = %s and id_categoria_uso = 3'''
        
            try:
                curr1.execute(query_dismissione, (pn[2],))
                #lista_variazioni=curr.fetchall()
            except Exception as e:
                check_error+=1
                logger.warning('Update codice percorso = {}'.format(pn[2]))
                logger.error(query_dismissione)
                logger.error(e)                                            

        curr1.close()
        #conn.commit()
            
        
        # attivo nuova versione del percorso
        curr1 = conn.cursor()
        if check_error==0:
            query_update='''update elem.percorsi pp
                set data_attivazione = date(now())+1,
                id_categoria_uso = 3,
                versione=((select max(versione) from elem.percorsi where cod_percorso =%s) +1),
                descrizione = (select descrizione from elem.percorsi where cod_percorso =%s 
                and versione = (select max(versione) from elem.percorsi where cod_percorso =%s) ),
                stagionalita= (select stagionalita from elem.percorsi where cod_percorso =%s
                and versione = (select max(versione) from elem.percorsi where cod_percorso =%s)),
                ddmm_switch_on =(select ddmm_switch_on from elem.percorsi where cod_percorso =%s
                and versione = (select max(versione) from elem.percorsi where cod_percorso =%s)),
                ddmm_switch_off =(select ddmm_switch_off from elem.percorsi where cod_percorso =%s
                and versione = (select max(versione) from elem.percorsi where cod_percorso =%s)),
                cod_percorso = %s
                where pp.cod_percorso = %s
                and pp.id_categoria_uso = 2'''
            
            try:
                curr1.execute(query_update, (pn[2],pn[2],pn[2],pn[2],pn[2],pn[2],pn[2],pn[2],pn[2],pn[2],pn[3]))
                #lista_variazioni=curr.fetchall()
            except Exception as e:
                check_error+=1
                logger.warning('Update codice percorso = {}'.format(pn[3]))
                logger.error(query_update)
                logger.error(e)   
        
        curr1.close()
        #conn.commit()                             
        
        
        # scrivo history
        curr1 = conn.cursor()
        if check_error==0:
            descrizione='Nuova versione percorso codice {0}'.format(pn[2])
            insert_history='''INSERT INTO util.sys_history
("type", "action", description, datetime, id_user, id_percorso)
VALUES('PERCORSO', 'UPDATE', %s, CURRENT_TIMESTAMP, %s, %s)'''

            try:
                curr1.execute(insert_history, (descrizione,pn[0],pn[1]))
                #lista_variazioni=curr.fetchall()
            except Exception as e:
                check_error+=1
                logger.error(insert_history)
                logger.error(e)   
        
        
        curr1.close()
        #conn.commit()   

        # delete percorsi sospesi
        curr1 = conn.cursor()
        if check_error==0:
            delete_sospesi='''DELETE etl.percorsi_sospesi WHERE id_percorso=%s'''
            try:
                curr1.execute(delete_sospesi, (int(pn[1]),))
                #lista_variazioni=curr.fetchall()
            except Exception as e:
                check_error+=1
                logger.error(delete_sospesi)
                logger.error(e)   
            
    
        curr1.close()
        conn.commit() 




    # invio mail
    if len(lista_percorsi_new) > 0:
        #mando mail
        logger.info('mando mail')
        lista_percorsi='{0}</ul>'.format(lista_percorsi)
        
        
        # Create a secure SSL context
        context = ssl.create_default_context()



        # messaggio='Test invio messaggio'


        subject = "Nuova versione percorsi"
        
        body = """Report giornaliero dei percorsi per cui domani entreranno in funzione nuove versioni dei file<br><br> 
        
        {0}
        
        L'applicativo che gestisce le nuove versioni dei percorsi è stato realizzato dal gruppo Gestione Applicativi del SIGT.<br> 
        Segnalare tempestivamente eventuali malfunzionamenti inoltrando la presente mail a {1}<br><br>
        Giorno {2}<br><br>
        AMIU Assistenza Territorio<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        """.format(lista_percorsi, user_mail, oggi1)
        ##sender_email = user_mail
        debug_email='roberto.marzocchi@amiu.genova.it'
        if test == 1:
            receiver_email=debug_email 
        else:
            receiver_email='assterritorio@amiu.genova.it'
        

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Nuova versione percorsi"


            
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path)
        immagine(message,logoname)
        
    
        

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        
            
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger.info("Chiudo definitivamente le connesioni al DB")
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)

if __name__ == "__main__":
    main()