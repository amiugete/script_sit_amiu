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
logfile='{}/log/variazioni_importazioni.log'.format(path)
errorfile='{}/log/error_variazioni_importazioni.log'.format(path)
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
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


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
            # nota via  7
            #logger.debug('SIT =  {}, UO = {}'.format(tappe_sit[k][7], tappe_uo[k][7]))
            if (tappe_uo[k][7] is None and tappe_sit[k][7] is None) or ( (not tappe_uo[k][7] or re.search("^\s*$", tappe_uo[k][7])) and (not tappe_sit[k][7] or re.search("^\s*$", tappe_sit[k][7])) ):
                check1=0
                
            else:
                if tappe_sit[k][7].strip()!=tappe_uo[k][7].strip():
                    check=1
                    logger.warning('SIT =  {}, UO = {}'.format(tappe_sit[k][7], tappe_uo[k][7]))
            
            k+=1
    else:
        check=1
    return check






def main():
    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    id_p= 155762
    cod_p= '0213244201'
    oggi1='21/11/2022'
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    
    # PRIMA VERIFICO SE CI SIANO DIFFERENZE CHE GIUSTIFICHINO IMPORTAZIONE
    curr1 = conn.cursor()
    sel_sit='''select vt.num_seq, id_via::int, coalesce(vt.numero_civico, ' ') as  numero_civico , 
    coalesce(riferimento,' ') as riferimento, fo.freq_binaria as frequenza,vt.tipo_elemento, vt.id_elemento::int,
    coalesce(vt.nota_asta, ' ') as nota_asta
    from etl.v_tappe vt 
    join etl.frequenze_ok fo on fo.cod_frequenza = vt.frequenza_asta::int 
    where id_percorso = %s  
    order by num_seq , numero_civico, id_elemento, nota_asta '''
    try:
        curr1.execute(sel_sit, (id_p,))
        #logger.debug(query_sit1, max_id_macro_tappa, vv[4] )
        #curr1.rowfactory = makeDictFactory(curr1)
        tappe_sit=curr1.fetchall()
    except Exception as e:
        logger.error(sel_sit, id_p )
        logger.error(e)
    
    
    cur1 = con.cursor()
    sel_uo='''SELECT VTP.CRONOLOGIA NUM_SEQ,VTP.ID_VIA, NVL(VTP.NUM_CIVICO,' ') as  NUMERO_CIVICO,
    NVL(VTP.RIFERIMENTO, ' ') as RIFERIMENTO,
    VTP.FREQELEM,VTP.TIPO_ELEMENTO, TO_NUMBER(VTP.ID_ELEMENTO) AS ID_ELEM_INT,
        NVL(VTP.NOTA_VIA, '') as NOTA_VIA
    FROM V_TAPPE_ELEMENTI_PERCORSI VTP
    inner join (select MAX(CPVT.DATA_PREVISTA) data_prevista, CPVT.ID_PERCORSO
        from CONS_PERCORSI_VIE_TAPPE CPVT
    where CPVT.DATA_PREVISTA<=TO_DATE(:t1,'DD/MM/YYYY') 
    group by CPVT.ID_PERCORSO) PVT
    on PVT.ID_PERCORSO=VTP.ID_PERCORSO 
    and vtp.data_prevista = pvt.data_prevista
    where VTP.ID_PERCORSO=:t2
    ORDER BY VTP.CRONOLOGIA,NUMERO_CIVICO,ID_ELEM_INT, NOTA_VIA
    ''' 
    try:
        cur1.execute(sel_uo, (oggi1, cod_p))
        #cur1.rowfactory = makeDictFactory(cur1)
        tappe_uo=cur1.fetchall()
    except Exception as e:
        logger.error(sel_uo, oggi1, cod_p)
        logger.error(e)
    
    curr1.close()  
    cur1.close()      
    logger.debug('Trovate {} tappe su SIT per il percorso {}'.format(len(tappe_sit),cod_p))
    logger.debug('Trovate {} tappe su UO per il percorso {}'.format(len(tappe_uo),cod_p))
    
    logger.debug(tappe_sit[1][1])
    logger.debug(tappe_uo[1][1])

    
                
    
    if cfr_tappe(tappe_sit, tappe_uo, logger)==0 :
        logger.info('Percorso {} già importato con data antecedente. Non ci sono state modifiche sostanziali.'.format(cod_p))
    else: 
        logger.info('Percorso {} già importato con data antecedente ma da reimportare (ci sono cose che non tornano).'.format(cod_p))
    
    
    
    
    
    
    
    
    
    
    
    
    
    

    # CHIUDO LE CONNESSIONI 
    logger.info("Chiudo definitivamente le connesioni al DB")
    con.close()
    conn.close()


if __name__ == "__main__":
    main()