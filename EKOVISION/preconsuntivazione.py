#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della pre-consuntivazione sulla base delle frequenze presenti su SIT

Fa un co
Se ci sono delle frequenze di aste / piazzole < frequenze della testata del percorso scrive file per ekovision



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

import inspect
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
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)






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


from tappa_prevista import tappa_prevista

    
     

def main():
      


    # preparo gli array 
    
    cod_percorso=[]
    data=[]
    id_turno=[]
    id_componente=[]
    id_tratto=[]
    flag_esecuzione=[]
    causale=[]
    nota_causale=[]
    sorgente_dati=[]
    data_ora=[]
    lat=[]
    long=[]
    ripasso=[]
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

    
    
    # monday = 0
    #gg_sett=datetime.today().weekday()
    #logger.debug(gg_sett)
    #exit()
    gg=0
    while gg <= 7-datetime.today().weekday():
        day=oggi + timedelta(gg)
        logger.debug(day)
        
        # ciclo su elenco aste con differenze --> s.riempimento = 0 solo spazzamento / lavaggio
        # and p.id_percorso in (170330, 199028)
        query_spazz='''select p.cod_percorso, 
                p.id_turno, 
                ap.id_asta, 
                fo.freq_binaria as freq_asta, 
                fo2.freq_binaria  as freq_percorso, 
                fo3.freq_binaria  as differenza, 
                p.data_attivazione::date, 
                p.data_dismissione::date, 
                coalesce(ap.ripasso_fittizio,0) as ripasso_fittizio               
                from elem.aste_percorso ap 
                join elem.percorsi p on p.id_percorso = ap.id_percorso 
                join etl.frequenze_ok fo on fo.cod_frequenza = ap.frequenza
                join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
                left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-ap.frequenza)
                join elem.servizi s on s.id_servizio = p.id_servizio
                where p.id_categoria_uso in (3,6) and ap.frequenza is not null 
                and ap.frequenza <> p.frequenza
                and s.riempimento = 0
                order by p.cod_percorso, ap.num_seq; '''
                
        try:
            curr.execute(query_spazz)
            lista_aste=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(e)

        for aa in lista_aste:
            
            #aa[3] frequenza asta 
            #aa[4] frequenza percorso
            #logger.debug(aa[3])
            #logger.debug(tappa_prevista(day, aa[3]))
            #logger.debug(aa[4])
            #logger.debug(tappa_prevista(day, aa[4]))
            if (tappa_prevista(day, aa[4])==1 # frequenza percorso
                and tappa_prevista(day, aa[3])==-1 # frequenza asta
                and aa[6] <= day # data attivazione
                and (aa[7] is None or aa[7] > day) # data dismissione
                ):
                cod_percorso.append(aa[0])
                data.append(day.strftime("%Y%m%d"))
                id_turno.append(aa[1])
                id_componente.append(None)
                id_tratto.append(aa[2])
                flag_esecuzione.append(2)
                causale.append(999)
                nota_causale.append(None)
                sorgente_dati.append('SIT')
                data_ora.append(None)
                lat.append(None)
                long.append(None)
                ripasso.append(aa[8])
           
        # ciclo su elenco aste con differenze --> s.riempimento = 0 solo spazzamento / lavaggio
        # per collaudo and p.id_percorso in (191684,200437,199857)
        query_racc='''select p.cod_percorso, p.id_turno, 
            eap.id_elemento, 
            fo.freq_binaria as freq_elemento, 
            fo2.freq_binaria  as freq_percorso, 
            fo3.freq_binaria  as differenza,
            eap.ripasso, 
            p.data_attivazione::date, 
            p.data_dismissione::date 
            from elem.aste_percorso ap 
            join elem.percorsi p on p.id_percorso = ap.id_percorso 
            join elem.elementi_aste_percorso eap on ap.id_asta_percorso = eap.id_asta_percorso 
            join etl.frequenze_ok fo on fo.cod_frequenza = eap.frequenza::int
            join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
            join elem.servizi s on s.id_servizio = p.id_servizio 
            left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-eap.frequenza::int)
            where p.id_categoria_uso in (3,6) and ap.frequenza is not null 
            and eap.frequenza::int <> p.frequenza and s.riempimento > 0 
            order by p.cod_percorso, ap.num_seq '''
                
        try:
            curr.execute(query_racc)
            lista_elementi=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(e)

        for aa in lista_elementi:
            
            #aa[3] frequenza asta 
            #aa[4] frequenza percorso
            #logger.debug(aa[3])
            #logger.debug(tappa_prevista(day, aa[3]))
            #logger.debug(aa[4])
            #logger.debug(tappa_prevista(day, aa[4]))
            if (tappa_prevista(day, aa[4])==1 
                and tappa_prevista(day, aa[3])==-1
                and aa[7] <= day # data attivazione
                and (aa[8] is None or aa[8] > day) # data dismissione
                ):
                cod_percorso.append(aa[0])
                data.append(day.strftime("%Y%m%d"))
                id_turno.append(aa[1])
                id_componente.append(aa[2])
                id_tratto.append(None)
                flag_esecuzione.append(2)
                causale.append(999)
                nota_causale.append(None)
                sorgente_dati.append('SIT')
                data_ora.append(None)
                lat.append(None)
                long.append(None)
                ripasso.append(aa[6])   
    
        
        
        
        
        
        gg+=1

    check_ekovision=200
    '''
    #creo un dizionario 
    
    # Creating an empty dictionary
    dizionario = {}
    # Adding list as value
    dizionario["cod_percorso"] = cod_percorso
    dizionario["data"] = data
    
    logger.debug(dizionario)
    # Adding list as value
    exit()
    '''
    try:    
        nome_csv_ekovision="preconsuntivazioni_{0}.csv".format(giorno_file)
        file_preconsuntivazioni_ekovision="{0}/preconsuntivazioni/{1}".format(path,nome_csv_ekovision)
        fp = open(file_preconsuntivazioni_ekovision, 'w', encoding='utf-8')
                      
        fieldnames = ['cod_percorso', 'data', 'id_turno', 'id_componente','id_tratto',
                        'flag_esecuzione', 'causale', 'nota_causale', 'sorgente_dati', 'data_ora', 'lat', 'long', 'ripasso' ]
      
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
        while k < len(cod_percorso):
            row=[cod_percorso[k], data[k], id_turno[k], id_componente[k],id_tratto[k],
                        flag_esecuzione[k], causale[k], nota_causale[k], sorgente_dati[k], data_ora[k], lat[k], long[k], ripasso[k]]
            myFile.writerow(row)
            k+=1
        '''
        matrice=[tuple(cod_percorso), tuple(data), tuple(id_turno), tuple(id_componente),tuple(id_tratto),
                        tuple(flag_esecuzione), tuple(causale), tuple(nota_causale), tuple(sorgente_dati), tuple(data_ora), tuple(lat), tuple(long)]
        myFile.writerows(matrice)
        '''
        fp.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni



    logger.info('Invio file con la preconsuntivazione via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('sch_lav_cons/in/'): #chdir to public
            srv.put(file_preconsuntivazioni_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP  
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      