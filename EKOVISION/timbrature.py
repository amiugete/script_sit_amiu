#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script della gestione e invio dei dati delle timbrature

DA esipertbo (dblink su UO) minvio i dati sul db dwh per creare un progressivo e gestire le date/ora di aggiornamento

Da dwh spedisco i dati in modo incrementale a Ekovision



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
    
    cod_ris=[]
    data_timbr=[]
    progr=[]
    orario=[]
    verso=[]
    tipo_ris=[]
    
    
    
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
    
        
    # Mi connetto a dwh (PostgreSQL) per poi recuperare le mail
    nome_db=db_dwh
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    
    curr = conn.cursor()
    curr1 = conn.cursor()
    cur = con.cursor()
    
    select_data='''select to_char(max(data_ora)::date - interval '1' day, 'YYYYMMDD')::int as data 
from personale_ekovision.invio_timbrature_ekovision ite''' 
    
    try:
        curr.execute(select_data)     
        data=curr.fetchall()     
    except Exception as e:
        logger.error(select_x_invio)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
    
    for d in data:
        data_start=int(d[0])
    curr.close()
    curr = conn.cursor()
    
    logger.debug(data_start)
    #exit()        
    # ciclo su elenco vie / note consuntivate
    query_timbrature='''SELECT ID_PERSONA,
        CLTIMBRA AS "DATA", lpad(to_char(MTTIMBRA), 4,'0') AS "ORARIO",
        trim(CDVERSOT) AS VERSO,
        trim(FLMANUAL) AS NOTE 
        FROM esipertbo.v_timbr_eko@sipedb a
        WHERE (cltimbra > :dd) 
        OR (trim(FLMANUAL) IS NOT NULL AND cltimbra > to_char(trunc((sysdate - interval '2' MONTH), 'MONTH'), 'YYYYMMDD'))
        ORDER BY 2,3 '''
    
    
                
    try:
        cur.execute(query_timbrature, (data_start,))
        cur.rowfactory = makeDictFactory(cur)
        timbrature=cur.fetchall()
    except Exception as e:
        logger.error(query_timbrature)
        logger.error(e)

    i=0
    for tt in timbrature:
        i+=1
        if i%1000==0:
            logger.debug(i)
        # faccio il controllo di quanto ho su dwh
        
        check_timbratura_esiste = '''SELECT cod_ris, "data", progr, orario, verso, note, data_ultima_modifica
        FROM personale_ekovision.ris_timbrature
        where cod_ris=%s and "data" = %s  and orario = %s''' 
                
        try:
            curr.execute(check_timbratura_esiste, (int(tt["ID_PERSONA"]), int(tt["DATA"]), tt["ORARIO"]))     
            check_t_e=curr.fetchall()     
        except Exception as e:
            logger.error(check_timbratura_esiste)
            logger.error('Codice persona = {}'.format(tt["ID_PERSONA"]))
            logger.error('Data = {}'.format(tt["DATA"]))
            logger.error('Ora = {}'.format(tt["ORARIO"]))
            logger.error(e)
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
        
        if len(check_t_e)==0:
            #insert
            query_insert= '''INSERT INTO personale_ekovision.ris_timbrature
(cod_ris, "data", orario, verso, note, data_ultima_modifica)
VALUES ( %s, %s, %s, %s, %s, now() ) '''
            try:
                curr1.execute(query_insert, (int(tt["ID_PERSONA"]), int(tt["DATA"]), tt["ORARIO"],tt["VERSO"], tt["NOTE"] ))     
            except Exception as e:
                logger.error(query_insert)
                logger.error('Codice persona = {}'.format(int(tt["ID_PERSONA"])))
                logger.error('Data = {}'.format(int(tt["DATA"])))
                logger.error('Ora = {}'.format(tt["ORARIO"]))
                logger.error('Verso = {}'.format(tt["VERSO"]))
                logger.error('Nota = {}'.format(tt["NOTE"]))
                logger.error(e)
                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
        else:
            for tp in  check_t_e: # tp sta per timbratura su PostgreSQL
                if tp[4]!=tt["VERSO"] or tp[5]!= tt["NOTE"] : # allora faccio update
                    query_update='''UPDATE personale_ekovision.ris_timbrature
SET cod_ris=%s, "data"=%s, orario=%s, verso=%s, note=%s, data_ultima_modifica=now()
WHERE progr=%s ; '''
                    try:
                        curr1.execute(query_update, (int(tt["ID_PERSONA"]), int(tt["DATA"]), tt["ORARIO"],tt["VERSO"], tt["NOTE"], tp[2]))     
                    except Exception as e:
                        logger.error(query_insert)
                        logger.error('Codice persona = {}'.format(int(tt["ID_PERSONA"])))
                        logger.error('Data = {}'.format(int(tt["DATA"])))
                        logger.error('Ora = {}'.format(tt["ORARIO"]))
                        logger.error(e)
                        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                        exit()
                #altrimenti non faccio nulla    
    conn.commit()
    logger.info('Fine aggiornamento DWH')            
           

     
    curr.close()
    curr1.close()
    curr = conn.cursor()
    
    check_ekovision=0
    select_x_invio='''select cod_ris, "data"::int, progr, orario,
verso, 'D' as tipo_ris
from personale_ekovision.ris_timbrature rt 
where (progr > (select max(progr) from personale_ekovision.invio_timbrature_ekovision ite) 
or data_ultima_modifica > (select max(data_ora) from personale_ekovision.invio_timbrature_ekovision ite))
and cod_ris in (select id_persona from personale_ekovision.personale p )
order by progr''' 
    
    try:
        curr.execute(select_x_invio)     
        timbrature_x_invio=curr.fetchall()     
    except Exception as e:
        logger.error(select_x_invio)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
   
    for t_i in timbrature_x_invio:
        
        cod_ris.append(t_i[0])
        data_timbr.append(t_i[1])
        progr.append(int(t_i[2]))
        orario.append(t_i[3])
        verso.append(t_i[4])
        tipo_ris.append(t_i[5])
        #max_progr=t_i[2]
    
    
    
    
    try:    
        nome_csv_ekovision="timbrature_{0}.csv".format(giorno_file)
        file_preconsuntivazioni_ekovision="{0}/timbrature/{1}".format(path,nome_csv_ekovision)
        fp = open(file_preconsuntivazioni_ekovision, 'w', encoding='utf-8')
                      
        fieldnames = ['cod_ris', 'data', 'progr', 'orario','verso',
                        'tipo_ris']
      
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
        while k < len(progr):
            row=[int(cod_ris[k]), int(data_timbr[k]), int(progr[k]), orario[k],verso[k],
                        tipo_ris[k] ]
            myFile.writerow(row)
            k+=1
            
        fp.close()
        check_ekovision=200
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni


    if check_ekovision==200:
        logger.info('Invio file con le timbrature via SFTP')
        try: 
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
        password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
        log="/tmp/pysftp.log")

            with srv.cd('timbrature/in/'): #chdir to public
                srv.put(file_preconsuntivazioni_ekovision) #upload file to nodejs/

            # Closes the connection
            srv.close()
        except Exception as e:
            logger.error(e)
            check_ekovision=103 # problema invio SFTP  
    
    curr.close()
    curr = conn.cursor()
    
    
    if check_ekovision==200 and len(progr)>0:
        insert_max_id='''INSERT INTO personale_ekovision.invio_timbrature_ekovision
        (progr, data_ora)
        VALUES
        (%s, now())'''
        try:
            curr.execute(insert_max_id, (max(progr),))
        except Exception as e:
            logger.error(insert_max_id)
            logger.error(e)
            
        
        conn.commit()   
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    logger.info("Chiusura cursori e connessioni")
    curr.close()
    conn.close()
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      