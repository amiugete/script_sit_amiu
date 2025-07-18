
#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 202
# ChatGPT + Roberto Marzocchi

'''




'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql


import psycopg2


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


import time

from datetime import datetime



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



TRESHOLD_RUNTIME_SEC = 200
INTERVALLO_SEC = 30


def get_query_attive():
    nome_db=db
    #logger.info('Connessione al db {}'.format(nome_db))

    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    cur = conn.cursor()
    cur.execute("""
        SELECT pid, usename, datname, state, 
               now() - query_start AS runtime,
               wait_event_type, query
        FROM pg_stat_activity
        WHERE state != 'idle'
        ORDER BY runtime DESC;
    """)
    righe = cur.fetchall()
    cur.close()
    conn.close()
    return righe

def monitora_query():
    i=0
    while True:
        query_attive = get_query_attive()
        if i==0 or i%1000 == 0:
            logger.info(f"Controllo {i} in corso su {len(query_attive)} query attive in questo momento")
        for q in query_attive:
            pid, usename, datname, state, runtime, wait_event_type, query = q
            runtime_sec = runtime.total_seconds()
            if runtime_sec > TRESHOLD_RUNTIME_SEC:
                msg = f"[PID {pid}] {usename}@{datname} - {runtime_sec:.1f}s - {query[:100]}"
                logging.warning(msg)
                warning_message_mail(msg, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
        i+=1        
        time.sleep(INTERVALLO_SEC)



def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    monitora_query()




if __name__ == "__main__":
    main()
    
 
  