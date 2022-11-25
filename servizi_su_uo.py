#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script è utile per accoppiare i servizi alle UT su UO in maniera massiva e ordinata evitando delle cavolate

Da usare con cautela

Lo script fa degli insert massivi sulle seguenti 3 tabelle: 

- ORDINE SERVIZI UO (per un servizio, definita SERVUZIO BASE deve essere popolato a mano)    
 
- FORZA_LAVORO_UO

- DAT_FORZA_LAVORO_UO

'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter
#from xlsxwriter.utility import xl_rowcol_to_cell

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

import locale
#locale.setlocale(locale.LC_ALL, 'it_IT.UTF-8')
locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')
import calendar

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
logfile='{}/log/servizi_su_uo.log'.format(path)
errorfile='{}/log/error_servizi_su_uo.log'.format(path)
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


    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()
    
    #input
    servizio_base= 108
    altri_servizi= [135,136]
    UT = [16,17,231,26,45,49,59,85,87,90]


    #ORDINE SERVIZI UO (quello base deve essere popolato a mano)    
 
    query='''SELECT ID_SERVIZIO, ID_UO, ORDINE
    FROM ORDINE_SERVIZI_UO osu 
    WHERE ID_UO= :uo and ID_SERVIZIO = :serv_base'''
    
    
    
    cur1 = con.cursor()
    i=0
    while i < len (UT):
        try:
            cur.execute(query, (UT[i],servizio_base,))
            #cur.rowfactory = makeDictFactory(cur)
            ordine_servizi=cur.fetchall()
        except Exception as e:
            logger.error(query, UT[i],servizio_base)
            logger.error(e)
        

        for os in ordine_servizi:
            #ciclo sui servizi (mantengo lo stesso ordine)
            k=0
            while k < len (altri_servizi):
                query1='''SELECT ID_SERVIZIO, ID_UO, ORDINE FROM UNIOPE.ORDINE_SERVIZI_UO 
                WHERE ID_SERVIZIO = :id_serv AND ID_UO=:id_uo  AND ORDINE=:ordine'''
                data = dict(id_serv= altri_servizi[k], id_uo= os[1], ordine= os[2])
                try:
                    cur1.execute(query1, data)
                    #cur.rowfactory = makeDictFactory(cur)
                    servizi_UT=cur1.fetchall()
                except Exception as e:
                    logger.error(query1, data)
                    logger.error(e)
                if len(servizi_UT)==0:                    
                    query_insert='''INSERT INTO UNIOPE.ORDINE_SERVIZI_UO (ID_SERVIZIO, ID_UO, ORDINE) VALUES (:id_serv, :id_uo, :ordine)'''
                    #logger.debug(query_insert, data)
                    cur1.execute(query_insert, data)
                k+=1
                         
        # ciclo sulle UT
        i+=1
    
    con.commit()
    cur1.close()
    cur.close()
    
    
    cur = con.cursor()
    
    ###############################################################################
    # FORZA_LAVORO_UO
    # DAT_FORZA_LAVORO_UO


    s_completo=altri_servizi
    s_completo.append(servizio_base)
    turni=['M', 'P', 'N']
    i=0
    while i < len (UT):
        
        k=0
        while k < len (s_completo):
            
            t=0
            while t<len(turni):
                data=dict(id_uo=UT[i], id_serv=s_completo[k], tt=turni[t])
                
                ###############################################################################
                # FORZA_LAVORO_UO
                
                query1='''SELECT ID_UO, ID_SERVIZIO, TURNO, QUANTITA FROM FORZA_LAVORO_UO 
                WHERE ID_UO = :id_uo AND ID_SERVIZIO = :id_serv AND TURNO = :tt
                ''' 
                try:
                    cur.execute(query1, data)
                    #cur.rowfactory = makeDictFactory(cur)
                    dflu=cur.fetchall()
                except Exception as e:
                    logger.error(query1, data)
                    logger.error(e)
                delete1= '''DELETE FROM UNIOPE.FORZA_LAVORO_UO
                WHERE ID_UO = :id_uo AND  ID_SERVIZIO = :id_serv AND TURNO = :tt
                '''
                
                insert1= '''INSERT INTO UNIOPE.FORZA_LAVORO_UO
                (ID_UO, ID_SERVIZIO, TURNO, QUANTITA)
                VALUES (:id_uo, :id_serv, :tt, 0)'''
                
                if len(dflu)==0:
                    cur.execute(insert1, data)
                elif len(dflu)>1:
                    cur.execute(delete1, data)
                    logger.debug(delete1, data)
                    cur.execute(insert1, data)

                ###############################################################################
                # DAT_FORZA_LAVORO_UO
                
                query1='''SELECT ID_UO, ID_SERVIZIO, TURNO, QUANTITA FROM DAT_FORZA_LAVORO_UO 
                WHERE ID_UO = :id_uo AND ID_SERVIZIO = :id_serv AND TURNO = :tt
                ''' 
                try:
                    cur.execute(query1, data)
                    #cur.rowfactory = makeDictFactory(cur)
                    flu=cur.fetchall()
                except Exception as e:
                    logger.error(query1, data)
                    logger.error(e)
                delete1= '''DELETE FROM UNIOPE.DAT_FORZA_LAVORO_UO
                WHERE ID_UO = :id_uo AND ID_SERVIZIO = :id_serv AND TURNO = :tt
                '''
                
                insert1= '''INSERT INTO UNIOPE.DAT_FORZA_LAVORO_UO
                (ID_UO, ID_SERVIZIO, TURNO, QUANTITA)
                VALUES (:id_uo, :id_serv, :tt, 0)
                '''
                
                if len(flu)==0:
                    cur.execute(insert1, data)
                    #logger.debug(insert1, data)
                elif len(flu)>1:
                    cur.execute(delete1, data)
                    logger.debug(delete1, data)
                    cur.execute(insert1, data)
                    #logger.debug(insert1, data)

                
                
            
            
                    
                t+=1
            k+=1
        i+=1
    
    
    
    
    
    con.commit()
    cur.close()
    con.close()
    
    


if __name__ == "__main__":
    main()