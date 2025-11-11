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
logfile='{}/log/{}.log'.format(path, nome)
errorfile='{}/log/error_{}.log'.format(path, nome)
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
    hh=host_test
    dd=db_test
    mail_notifiche_apertura='roberto.marzocchi@amiu.genova.it'
    mail_notifiche_apertura=debug_email
    und_test='_TEST'
    oggetto= ' (TEST)'
    incipit_mail='''<p style="color:red"><b>Questa mail proviene dagli applicativi di TEST (SIT e Gestione oggetti).
     NON si tratta di un reale intervento</b></p>'''
else:
    hh=host
    dd=db
    mail_notifiche_apertura='roberto.marzocchi@amiu.genova.it'
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

    query_select = '''select * from gestione_oggetti.v_intervento vi 
where stato not in (1,5)'''


    
    try:
        curr.execute(query_select)
        lista_interventi_spostare=curr.fetchall()
    except Exception as e:
        logger.error(e)

    c=0
    try:
        if len(lista_interventi_spostare) > 0:
            logger.info('Ci sono interventi da spostare')
            c=1
    except Exception as e:
        logger.info('Non ci sono interventi da spostare')

    if c==1:
        logger.debug('Sono qua')
        for ii in lista_interventi_spostare:
            logger.debug(ii[0])
            query_sposta='''WITH moved AS (
                DELETE FROM gestione_oggetti.intervento
                WHERE id = %s
                RETURNING id, descrizione, tipo_priorita_id, data_creazione, elemento_id, piazzola_id, note_chiusura, odl_id, utente
            )
            INSERT INTO gestione_oggetti.intervento_hist (id, descrizione, tipo_priorita_id, data_creazione, elemento_id, piazzola_id, note_chiusura, odl_id, utente)
            SELECT id, descrizione, tipo_priorita_id, data_creazione, elemento_id, piazzola_id, note_chiusura, odl_id, utente
            FROM moved '''
            try:
                curr1.execute(query_sposta, (ii[0],))
            except Exception as e:
                logger.error(e)  
                logger.error(query_sposta)
                logger.error(f'Intervento id {ii[0]}')
            
            
            
    # COMMIT
    logger.info('Faccio il commit')
    conn.commit()
                   

    curr.close()
    curr1.close()
    conn.close()
    
    
    
if __name__ == "__main__":
    main() 