#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2024
# Roberto Marzocchi

'''
Ci sono delle piazzole eliminate con interventi rimasti aperti --> li imposto come abortiti


'''


from doctest import ELLIPSIS_MARKER
import os, sys, getopt, re
from dbus import DBusException  # ,shutil,glob
import requests
from requests.exceptions import HTTPError







import json


import inspect, os.path




import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)

sys.path.append(parentdir)

#print(parentdir)
#exit()
#sys.path.append('../')

from credenziali import *
from invio_messaggio import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

giorno_file=datetime.datetime.today().strftime('%Y%m%d%H%M')

# nome dello script python
nome=os.path.basename(__file__).replace('.py','')


#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/{}.log'.format(path, nome)
errorfile='{}/error_{}.log'.format(path, nome)
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




# MAIL - libreria per invio mail
import email, smtplib, ssl
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText


#################################################
try:
    logger.debug(len(sys.argv))
    if sys.argv[1]== 'prod':
        test=0
    else: 
        logger.error('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
        exit()
except Exception as e:
    logger.info('Non ci sono parametri, sono in test')
    test=1


debug_email= 'roberto.marzocchi@amiu.genova.it'
if test==1:
    hh=host
    dd=db_test
    mail_notifiche_apertura='magmaco@amiu.genova.it'
    mail_notifiche_apertura=debug_email
    und_test='_TEST'
    oggetto= ' (TEST)'
    incipit_mail='''<p style="color:red"><b>Questa mail proviene dagli applicativi di TEST (SIT e Gestione oggetti).
     NON si tratta di un reale intervento</b></p>'''
else:
    hh=host
    dd=db
    mail_notifiche_apertura='magmaco@amiu.genova.it'
    und_test=''
    oggetto =''
    incipit_mail=''
#################################################


def connect():
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=dd,
                        port=port,
                        user=user_manut,
                        password=pwd_manut,
                        host=hh)
    return conn



def main():

    #################################################################
    """logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=dd,
                        port=port,
                        user=user,
                        password=pwd,
                        host=hh)
    """
    conn=connect()
    curr = conn.cursor()
    curr1 = conn.cursor()
    
    

    #conn.autocommit = True
    #################################################################

    query_select = '''select id, 
    tipo,
    descrizione,
    stato,
    stato_descrizione,  
elemento_id, 
piazzola_id, 
utente,
su.email,
to_char(data_creazione, 'DD/MM/YYYY') as data_creazione , 
stato,
stato_descrizione, 
odl 
from gestione_oggetti.v_intervento vi 
left join util.sys_users su on su."name"=utente
where tipo_elemento is null and stato in (1,5)'''



    footer='''<hr>
<p>Questa è una mail automatica inviata dall'applicativo Gestione Oggetti.
L'applicativo è raggiungibile al seguente <a href="http://{0}/GestioneOggetti{1}/#/interventi">indirizzo</a>.<br>
Si prega di NON RISPONDERE alla presente mail. In caso di problemi con l'applicativo scrivere a assterritorio@amiu.genova.it
</p>'''.format(host, und_test)


    #####################################################################
    
    try:
        curr.execute(query_select)
        lista_interventi_apertura=curr.fetchall()
    except Exception as e:
        logger.error(e)

    c=0
    try:
        if len(lista_interventi_apertura) > 0:
            logger.info('Ci sono interventi da abortire')
            c=1
    except Exception as e:
        logger.info('Non ci sono interventi da abortire')

    if c==1:
        for ii in lista_interventi_apertura:
    
            query_abort='''insert into gestione_oggetti.intervento_tipo_stato_intervento 
                (tipo_stato_intervento_id, intervento_id, data_ora)
                VALUES 
                (2, %s, now())'''
            try:
                curr.execute(query_abort, (ii[0],))
            except Exception as e:
                logger.error(e)  
            
            
            query_update2='''UPDATE gestione_oggetti.intervento
	SET odl_id=NULL
	WHERE id=%s;''' 
            try:
                curr.execute(query_abort, (ii[0],))
            except Exception as e:
                logger.error(e) 
                
                 
            # compongo la mail
            body='''{0}
        L'utente {1} ({2}) in data {3} aveva creato il seguente intervento:<br>
        <ul>
        <li> Tipo intervento:   {4}</li>
        <li> Descr Intervento:  {5}</li>
        <li> Piazzola:          {6}</li>
        <li> Stato intervento   {7}</li>
        </ul>
        La piazzola è stata rimossa, quindi l'intervento è stato automaticamente abortito.
        {8}
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''.format(incipit_mail, ii[7], ii[8], ii[9], ii[1], ii[2], 
                   ii[6], ii[11], footer)

            logger.debug(body)  
            #exit()

            # messaggio='Test invio messaggio'


            subject = "PIAZZOLA ELIMINATA - INTERVENTO {} ABORTITO ".format(ii[0])
            #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
            #sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            #debug_email='roberto.marzocchi@amiu.genova.it'

            #receiver_email=mail_notifiche_apertura

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = 'no_reply@amiu.genova.it'
            message["To"] = receiver_email
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Nuovo intervento"


                
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))



            # aggiunto allegato (usando la funzione importata)
            #allegato(message, file_variazioni, nome_file)
            # Add body to email
            #message.attach(MIMEText(body, "plain"))
            

            logoname='{}/img/logo_amiu.jpg'.format(parentdir)
            immagine(message,logoname)

            #text = message.as_string()

            logging.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            

            if invio==200:
                query_update='''UPDATE gestione_oggetti.email
                SET data_invio=now(), "destinatario_A"=%s 
                WHERE id=%s
                '''
                try:
                    curr1.execute(query_update, (receiver_email,ii[16]))
                except Exception as e:
                    logger.error(e)
                    
                    
                #logging.info(invio)
                # COMMIT
                logger.info('Faccio il commit')
                conn.commit()
            else:
                logging.error('Problema invio mail. Error:{}'.format(invio))
        # se non era da inviare la mail imposto comunque una data ma non metto l'indirizzo
       

    curr.close()
    curr1.close()
    conn.close()
    
    
    
if __name__ == "__main__":
    main() 