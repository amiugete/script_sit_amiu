#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi, Roberta Fagandini

'''
Scopo dello script è controllare che ci siano dati recenti provenienti da Tellus



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect







from datetime import date, datetime, timedelta



import psycopg2



currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



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



    

def main():
    
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
    
    

        
    select_elementi = '''with last_date as 
(select extract(epoch from (now()-max(data_ora)))/3600 as delay,
max(data_ora) as last_timestamp 
from tellus.dettaglio_eventi
) 
select to_char(last_timestamp, 'DD/MM/YYYY HH24:MI:SS') from last_date where delay > 24'''



    try:
        curr.execute(select_elementi)
        elementi=curr.fetchall()
    except Exception as e:
        logger.error(select_elementi)
        logger.error(e)


    for e in elementi:
        messaggio=f'''L'ultimo messaggio proventiente da Tellus risale al {e[0]}. Verificare i WS o contattare Tellus.'''
        
        logger.warning(messaggio)
        warning_message_mail(messaggio, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger, 'WARNING - Dati Tellus non aggiornati')
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    

    curr.close()
    conn.close()
    
    
    
if __name__ == "__main__":
    main()      