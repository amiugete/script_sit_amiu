#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica se ha girato variazioni_importazioni.py senza errori e nel caso si comporta di conseguenza 
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


from credenziali import *

#import report_settimanali_percorsi_ok 


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


# per mandare file a EKOVISION
import pysftp

#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))



path=os.path.dirname(sys.argv[0]) 
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

logger2 = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger2.addHandler(c_handler)
logger2.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)





def cfr_tappe(tappe_sit, tappe_uo, logger):
    ''' Effettua il confronto fra le tappe di SIT e quelle di UO'''
    #logger.info('Richiamo la funzione cfr_tappe')
    check=0
    if len(tappe_sit) == len(tappe_uo) :
        k=0
        while k < len(tappe_sit):
            #logger.debug(tappe_sit[k][0])
            #logger.debug(tappe_uo[k][0])
            
            # nume_seq 0
            if tappe_sit[k][0]!=tappe_uo[k][0]:
                check=1
            # id_via 1
            if tappe_sit[k][1]!=tappe_uo[k][1]:
                check=1    
            # riferimento 3
            if (tappe_uo[k][3] is None and tappe_sit[k][3] is None) or ( (not tappe_uo[k][3] or re.search("^\s*$", tappe_uo[k][3])) and (not tappe_sit[k][3] or re.search("^\s*$", tappe_sit[k][3])) ):
                check1=0
            else:
                if tappe_sit[k][3]!=tappe_uo[k][3]:
                    check=1
                    logger.warning('rif SIT = .{}., rif UO = {}'.format(tappe_sit[k][3], tappe_uo[k][3]))
                    
                
            # frequenza 4
            if tappe_sit[k][4]!=tappe_uo[k][4]:
                check=1   
            # tipo_el 5
            if tappe_sit[k][5]!=tappe_uo[k][5]:
                check=1   
            #id_el 6
            if tappe_sit[k][6]!=tappe_uo[k][6]:
                check=1 
            # ripasso
            if tappe_sit[k][8]!=tappe_uo[k][8]:
                check=1  
            # nota via  7
            if (tappe_uo[k][7] is None and tappe_sit[k][7] is None) or ( (not tappe_uo[k][7] or re.search("^\s*$", tappe_uo[k][7])) and (not tappe_sit[k][7] or re.search("^\s*$", tappe_sit[k][7])) ):
                check1=0
            else:
                if tappe_sit[k][7]!=tappe_uo[k][7] and tappe_uo[k][6] is None: # questo controllo va fatto solo nel caso di spazzamenti (ripasso is null)
                    check=1
                    logger.warning('SIT =  {}, UO = {}'.format(tappe_sit[k][7], tappe_uo[k][7]))
            
            k+=1
    else:
        check=1
    return check



def main():
    logger2.info('Il PID corrente è {0}'.format(os.getpid()))
    # carico i mezzi sul DB PostgreSQL
    logger2.info('Connessione al db')
    try: 
        conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    except Exception as e:
        logger2.errror(e)
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger2)
        exit()
    curr = conn.cursor()
    #conn.autocommit = True
    
    

    oggi=datetime.datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=datetime.date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    num_giorno=datetime.datetime.today().weekday()
    giorno=datetime.datetime.today().strftime('%A')
    giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    oggi1=datetime.datetime.today().strftime('%d/%m/%Y')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    
    
    select_query='''select check_error_uo, 
        check_error_ekovision
        from anagrafe_percorsi.log_trasferimenti_giornalieri
        where giorno=to_date(%s, 'YYYYMMDD')'''
        
    try:
        curr.execute(select_query, (giorno_file,))
        errori=curr.fetchall()
    except Exception as e:
        logger2.error(e)
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger2)
        exit()        
    
    
    run_script=0
    
    
    w_m_eko='''NOTA BENE: Se ora l'importazione è andata a buon fine c'è da avvisare anche Ekovision di rilanciara a mano il job notturno'''
    if len(errori)==0:
        # non ha girato devo far rigirare il tutto
        w_m='Oggi non ha ancora girato lo script che invia le modifiche ai percorsi alla UO e ad Ekovision'
        logger2.warning(w_m)
        # devo rilanciare lo script
        run_script=1
        w_m2=w_m_eko
    
    for e in errori:
        if e[0]==0 and e[1]==0:
            logger2.info('Tutto OK non faccio nulla')

        else:
            w_m='Oggi ha girato lo script che invia le modifiche ai percorsi alla UO e ad Ekovision, ma ha dato errori'
            logger2.warning(w_m)
            # devo rilanciare lo script
            run_script=1
            if e[1]!=0:
                w_m2=w_m_eko
            else:
                w_m2=''
            
    if run_script>0:
        logger2.info('************************************************************')
        import variazioni_importazioni
        variazioni_importazioni.main()
        logger2.info('************************************************************')

        messaggio_mail='{} <br><b>{}</b>'.format(w_m, w_m2)
        warning_message_mail(messaggio_mail, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger2)
    
    
    ##################################################################################################
    #                               CHIUDO LE CONNESSIONI
    ################################################################################################## 
    logger2.info("Chiudo definitivamente le connesioni al DB")
    conn.close()

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger2)

if __name__ == "__main__":
    main()