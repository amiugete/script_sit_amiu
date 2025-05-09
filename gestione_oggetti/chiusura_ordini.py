#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script gestisce la chiusura ordini che non avviene usando l'applicativo AMIU 
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



#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/chiusura_ordini.log'.format(path)
errorfile='{}/error_chiusura_ordini.log'.format(path)
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

if test==1:
    hh=host
    dd=db_test
    mail_notifiche_apertura='roberto.marzocchi@amiu.genova.it'
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
                        user=user,
                        password=pwd,
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
    curr2 = conn.cursor()
    
    
    

    #conn.autocommit = True
    #################################################################

    query_select = ''' select * from gestione_oggetti.odl o where chiuso = 0'''



    #####################################################################
    
    try:
        curr.execute(query_select)
        lista_odl=curr.fetchall()
    except Exception as e:
        logger.error(e)

    
    
    for oo in lista_odl:
        query_check='''select a.tipo_stato_intervento_id, a.intervento_id, a.data_ora 
            from gestione_oggetti.intervento_tipo_stato_intervento a
            join 
            (select intervento_id,
            max(data_ora) 
            from gestione_oggetti.intervento_tipo_stato_intervento itsi 
            where intervento_id in (select id from gestione_oggetti.intervento i where odl_id =%s)
            group by intervento_id) as b
            on a.intervento_id=b.intervento_id and data_ora = max'''               

        try:
            curr1.execute(query_check, (oo[0],))
            lista_int=curr1.fetchall()
        except Exception as e:
            logger.error(e)

        stato=1
        logger.debug(oo[0])
        for ii in lista_int:
            logger.debug(ii[0])
            if ii[0] in [1,5]: # 1 Aperto # 5 preso in carico #2 abortito # 3 chiuso # 4 chiuso con riserva
                stato=0 
        
        
        update='''UPDATE gestione_oggetti.odl set chiuso=%s where id=%s'''
        curr2.execute(update,(stato,oo[0],))
        logger.info('UPDATE ODL n {0} con stato {1}'.format(oo[0], stato))
        conn.commit()





    curr.close()
    curr1.close()
    curr2.close()
    conn.close()




if __name__ == "__main__":
    main()  