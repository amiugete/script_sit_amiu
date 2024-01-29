#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script gestisce l'invio dei dati dei pesi a EKOVISION. 

I dati sono quelli che da ECOS vengono copiati in TB_PESI_PERCORSI (Uniope)

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
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

# libreria per scrivere file csv
import csv



# function to return a named tuple
def makeNamedTupleFactory(cursor):
    columnNames = [d[0].lower() for d in cursor.description]
    import collections
    Row = collections.namedtuple('Row', columnNames)
    return Row


# funzionde per restituire un dizionario
def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow    
     

def main():
      


    # preparo gli array 
    barcode=[]
    cod_sede_impianto=[]
    peso=[]
    data_conferimento=[]
    ora_conferimento=[]
    cod_amiu=[]
    data_insert=[]

    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
    
     # Mi connetto al DB oracle UO
    logger.info('Connessione al db {}'.format(service_uo))
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    


    
    cur = con.cursor()
    cur1 = con.cursor()

    


    """******************************************************************************
    PRIMO INSERIMENTO 
    """
    
    """query_pesi='''SELECT PERCORSO AS BARCODE, 
concat(CONCAT(ad.IMP_COD_ECOS,'_'), ad.UNI_COD_ECOS) AS COD_SEDE_IMPIANTO,
tpp.PESO, tpp.DATA_CONFERIMENTO, 
NVL(tpp.ORA_CONFERIMENTO, '00:00:00') AS ORA_CONFERIMENTO, 
cod_protocollo,
concat(concat(tpp.ANNO_PROTOCOLLO,'_'), tpp.NUM_PROTOCOLLO) AS COD_AMIU, 
tpp.INS_DATE 
FROM TB_PESI_PERCORSI tpp 
JOIN ANAGR_DESTINAZIONI ad ON ad.ID_DESTINAZIONE = TPP.DESTINAZIONE 
WHERE tpp.DATA_PERCORSO >= to_date ('20231120', 'YYYYMMDD')
AND tpp.PROVENIENZA != 'TERZI'
ORDER BY NUM_PROTOCOLLO  '''"""
    
    
    
    
    query_pesi='''SELECT PERCORSO AS BARCODE, 
concat(CONCAT(ad.IMP_COD_ECOS,'_'), ad.UNI_COD_ECOS) AS COD_SEDE_IMPIANTO,
tpp.PESO, tpp.DATA_CONFERIMENTO,
NVL(tpp.ORA_CONFERIMENTO, '00:00:00') AS ORA_CONFERIMENTO, 
cod_protocollo,
concat(concat(tpp.ANNO_PROTOCOLLO,'_'), tpp.NUM_PROTOCOLLO) AS COD_AMIU, 
tpp.INS_DATE 
FROM TB_PESI_PERCORSI tpp 
JOIN ANAGR_DESTINAZIONI ad ON ad.ID_DESTINAZIONE = tpp.DESTINAZIONE 
WHERE (tpp.INS_DATE >= (SELECT max(DATA_INSERT) FROM INVIO_PESI_EKOVISION ipe )
OR concat(concat(tpp.ANNO_PROTOCOLLO,'_'), tpp.NUM_PROTOCOLLO) > (SELECT max(COD_AMIU) FROM INVIO_PESI_EKOVISION ipe )
) AND tpp.PROVENIENZA != 'TERZI' AND num_protocollo IS NOT null and PERCORSO IS NOT NULL
ORDER BY NUM_PROTOCOLLO '''
    
    
    
    try:
        cur.execute(query_pesi) 
        cur.rowfactory = makeDictFactory(cur)    
        pesi=cur.fetchall()     
    except Exception as e:
        logger.error(query_pesi)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
   
    for pp in pesi:
        if int(pp['COD_PROTOCOLLO'])!=838:
            logger.error('''C'è un codice protocollo ({0}) diverso da 838 verificare'''.format(pp['COD_PROTOCOLLO']))
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
        else:
            barcode.append(pp['BARCODE'])
            cod_sede_impianto.append(pp['COD_SEDE_IMPIANTO'])
            peso.append(pp['PESO'])
            data_conferimento.append(pp['DATA_CONFERIMENTO'].strftime("%Y%m%d"))
            ora_conferimento.append(pp['ORA_CONFERIMENTO'].replace(':',''))
            cod_amiu.append(pp['COD_AMIU'])
            data_insert.append(pp['INS_DATE'])
            #max_progr=t_i[2]
    
    
    
    
    try:    
        nome_csv_ekovision="conferimenti_{0}.csv".format(giorno_file)
        file_pesi_ekovision="{0}/pesi/{1}".format(path,nome_csv_ekovision)
        fp = open(file_pesi_ekovision, 'w', encoding='utf-8')
                      
        fieldnames = ['barcode', 'cod_sede_impianto', 'data_conferimento', 'ora_conferimento', 'peso_netto_kg', 'cod_amiu']
      
        '''
        
        myFile = csv.DictWriter(fp, delimiter=';', fieldnames=dizionario[0].keys(), quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        # Write the header defined in the fieldnames argument
        myFile.writeheader()
        # Write one or more rows
        myFile.writerows(dizionario)
        
        # senza usare dizionario
        '''
        #myFile = csv.writer(fp, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        myFile = csv.writer(fp, delimiter=';')
        myFile.writerow(fieldnames)
        
        k=0 
        while k < len(barcode):
            if k%1000==0:
                logger.debug('''preparazione file csv - {0} rows'''.format(k))
            
            row=[barcode[k],cod_sede_impianto[k], data_conferimento[k], ora_conferimento[k], peso[k], cod_amiu[k]]
            myFile.writerow(row)
            k+=1
            
        fp.close()
        check_ekovision=200
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni


    #exit()
    logger.info('Invio file con la preconsuntivazione via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('sch_lav_cons/in/'): #chdir to public
            srv.put(file_pesi_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP  
    
    
    
    if check_ekovision==200 and len(barcode)>0:
        insert_max_id='''INSERT INTO INVIO_PESI_EKOVISION
        (COD_AMIU, DATA_INSERT, DATA_INVIO )
        VALUES
        (:c1, to_date(:c2, 'YYYYMMDD HH24MISS'), SYSDATE)'''
        try:
            cur1.execute(insert_max_id, (max(cod_amiu),max(data_insert).strftime("%Y%m%d %H%M%S")))
        except Exception as e:
            logger.error(insert_max_id)
            logger.error(max(cod_amiu))
            logger.error(max(data_insert).strftime("%Y%m%d %H%M%S"))
            logger.error(e)
            
        
        con.commit()   
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    logger.info("Chiusura cursori e connessioni")
    
    cur.close()
    cur1.close()
    con.close()




if __name__ == "__main__":
    main()      