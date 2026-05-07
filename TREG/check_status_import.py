#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi - Roberta Fagandini

'''
Lo script verifica se l'import dei dati di raccolta e spazzamento su TREG è andato a buon fine. In particolare:
 - seleziona tutti gli elementi nella tabella treg_eko.check_status_import
 - per ognuno di questi elementi verifica se il processo è ancora in corso o se è terminato
 - se è terminato ma ancora nella tabella vuol dire che l'import non è andato a buon fine e quindi faccio il rollback della chiamata
'''

import os, sys, re  # ,shutil,glob
import errno
import inspect, os.path

import csv

import psycopg2

import requests
from requests.exceptions import HTTPError
from urllib.parse import urlencode

import cx_Oracle

from datetime import date, datetime, timedelta

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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

from treg_env import *
import uuid

def processo_attivo(pid):
    try:
        os.kill(pid, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return False  # il processo non esiste
        elif e.errno == errno.EPERM:
            return True   # esiste ma non hai permessi
    else:
        return True

def main():

    query_select = 'select * from treg_eko.check_status_import;'

    delete_importid = '''
        DELETE FROM treg_eko.check_status_import
        WHERE import_id=%s;
    '''


    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()

    try:
        curr.execute(query_select)
        importTREG=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_select)
        logger.error(e)

    token=token_treg(logger)
    logger.debug(token)

    for it in importTREG:
        logger.info('Controllo import con PID {0}'.format(it[0]))
        if processo_attivo(int(it[0])):
            logger.info('Il processo con PID {0} è ancora attivo'.format(it[0]))
        else:
            logger.info('Il processo con PID {0} non è più attivo quindi faccio il rollback'.format(it[0]))
            api_url_rollback='{}atrif/api/v1/tobin/b2b/process/rifqt-{}/rollback-upload/av1'.format(url_ws_treg, it[3])
            guid_roll = uuid.uuid4()
            body_rollback={
                'id': str(guid_roll),
                'importId': str(it[2]),
            }
            response_roll = requests.post(api_url_rollback, json=body_rollback, headers={'accept':'*/*', 
                'mde': 'PROD',
                'Authorization': 'EIP {}'.format(token),
                'Content-Type': 'application/json'})
            
            if response_roll.status_code == 200:
                logger.info('la chiamata di rollback ha dato esito positivo: {}'.format(response_roll.text))

                try:
                    curr.execute(delete_importid, (str(it[2]),))
                    conn.commit()
                except Exception as e:
                    logger.error(delete_importid)
                    logger.error(e)
            else:
                logger.error('la chiamata di rollback ha dato esito negativo ({}): {}'.format(response_roll.status_code, response_roll.text))
            

    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()

if __name__ == "__main__":
    main()   
            