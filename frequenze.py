#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge le frequenze del SIT espresse come numeri interi e le traduce nel formato atteso dal SW U.O. 
'''



import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/frequenze.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)


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




def long_set(codice_binario):
    '''
        Dato un codice binario di 7 numeri dove il primo è lunedì, il secondo martedì, etc.  
            1. restituisce il long
            2. restituisce il conto dei giorni 
    '''
    array_giorni=['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    long_giorni=''
    i=0
    conto_giorni=0
    while i<7:
        if codice_binario[i:(i+1)]=='1':
            conto_giorni+=1
            if long_giorni=='':
                long_giorni='{}'.format(array_giorni[i])
            else:
                long_giorni='{}-{}'.format(long_giorni,array_giorni[i])
        i+=1
    
    return long_giorni, conto_giorni


def long_mese(codice_binario):
    '''
        Dato un codice binario di 4 numeri dove il primo è la prima settimana, il secondo è la seconda settimana etc.  
            1. restituisce il long dei mesi
            2. restituisce il conto delle settimane 
    '''
    #array_giorni=['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom']
    long_mesi=''
    i=0
    conto_mesi=0
    while i<4:
        if codice_binario[i:(i+1)]=='1':
            conto_mesi+=1
            if long_mesi=='':
                long_mesi='{}'.format((i+1))
            else:
                long_mesi='{}-{}'.format(long_mesi,i+1)
        i+=1
    return long_mesi, conto_mesi


def codice_mensile(codice_binario1, codice_binario2):
    '''
        Dati i due codici binari restituisce il codice mensile
    '''
    cod=''
    i=0
    while i<7:
        if codice_binario1[i:(i+1)]=='1':
            s=0
            while s<4:
                if codice_binario2[s:(s+1)]=='1':
                    if cod=='':
                        cod='{}{}'.format(s+1,i+1)
                    else:
                        cod='{}_{}{}'.format(cod,s+1,i+1)
                s+=1 
        i+=1
    
    return cod


        


def main():
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True

    # rimuovo la tabella etl.frequenze_ok_1 che serve per vedere le differenze
    curr0 = conn.cursor()
    query0='drop table etl.frequenze_ok_1'
    try:
        curr0.execute(query0)
    except Exception as e:
        logging.error(e)

    curr0.close()

    # la ricreo pulita
    query1='''create table etl.frequenze_ok_1 as
        select * from etl.frequenze_ok 
        order by cod_frequenza'''
    curr0 = conn.cursor()
    try:
        curr0.execute(query1)
    except Exception as e:
        logging.error(e)

    curr0.close()



    #create table query
    select_frequenze = ''' select distinct ap.frequenza, ap.frequenza::bit(12) as fbin, f.*
        from elem.aste_percorso ap 
        left join elem.frequenze f
        on cast (f.cod_frequenza as text) = cast (ap.frequenza as text)
        where ap.frequenza is not null
    union               
    select distinct ap.frequenza::integer, ap.frequenza::integer::bit(12) as fbin,f.*
        from elem.elementi_aste_percorso ap 
        left join elem.frequenze f
        on cast (f.cod_frequenza as text) = cast (ap.frequenza as text)
        where ap.frequenza is not null
    union  
    select distinct ap.frequenza::integer, ap.frequenza::integer::bit(12) as fbin,f.*
        from elem.percorsi ap 
        left join elem.frequenze f
        on cast (f.cod_frequenza as text) = cast (ap.frequenza as text)
        where ap.frequenza is not null  
    order by 1    '''

    
    try:
        curr.execute(select_frequenze)
        frequenze_su_db=curr.fetchall()
    except Exception as e:
        logging.error(e)


    i=1       
    for f in frequenze_su_db:
        logging.debug('************************\n{}'.format(i))
        try:
            freq_int=f[0]
            freq_bin=str(f[1])
            logging.debug(freq_bin)
            bin_uo=f[6]
            logging.debug(bin_uo)
            
            #mese
            freq_mensile=freq_bin[7:11] 
            freq_mensile_ok=freq_mensile[::-1]
            giorni=freq_bin[0:7]
            giorni_ok=giorni[::-1]
            logging.debug('Giorni {}'.format(giorni_ok))
            logging.debug('Mese {}'.format(freq_mensile_ok))
            if int(freq_mensile)==0:
                logging.debug('S')
                stringa_binaria='S{}'.format(giorni_ok)
                logging.debug(giorni_ok)
                descrizione_long=long_set(giorni_ok)[0]
                conto_giorni=long_set(giorni_ok)[1]
                logging.debug(descrizione_long)
                logging.debug(conto_giorni)
                descrizione_short='{} GG settimana'.format(conto_giorni)
                num=conto_giorni
                
                if bin_uo != None and bin_uo != stringa_binaria:
                    logging.error('S Non torna')
                    logging.error(bin_uo)
                    logging.error(stringa_binaria)
            else:
                logging.debug('M')
                giorni_settimana=long_set(giorni_ok)[0]
                conto_giorni=long_set(giorni_ok)[1]
                mesi=long_mese(freq_mensile_ok)[0]
                num_mesi=long_mese(freq_mensile_ok)[1]
                cod_mese=codice_mensile(giorni_ok,freq_mensile_ok)
                logging.debug(cod_mese)
                descrizione_short='{} GG mese'.format(conto_giorni)
                descrizione_long='{} {}'.format(mesi,giorni_settimana)
                num=conto_giorni*num_mesi/4
                stringa_binaria='M{}'.format(cod_mese)
                if bin_uo != None and bin_uo[0:1]=='S':
                    logging.error('M Non torna')

            curr_s = conn.cursor()
            query_select="SELECT cod_frequenza FROM  etl.frequenze_ok WHERE cod_frequenza={0}".format(freq_int)
            try:
                curr.execute(query_select)
                sel=curr.fetchall()
            except Exception as e:
                logging.error(e)
            curr_s.close()

            if len(sel)==0:
                query_insert='''
                INSERT INTO etl.frequenze_ok
                (cod_frequenza, descrizione_short, descrizione_long, num_frequenza, freq_binaria, num_giorni)
                VALUES({0}, '{1}', '{2}', {0}, '{3}', {4});
                '''.format(freq_int, descrizione_short, descrizione_long, stringa_binaria, num)
                logging.debug(query_insert)
                curr1 = conn.cursor()
                curr1.execute(query_insert)
                curr1.close()
        except Exception as e:
            logging.warning(e)
            logging.warning(query_insert)
            #logging.error('Null value')
        i+=1
        

    
    # trasferisco i dati sul DB UO 


    freq_SIT=[]
    freq_UO=[]
    query_select='''select cod_frequenza as "FREQUENZA_SIT", freq_binaria as "FREQUENZA_UO" 
    from etl.frequenze_ok 
    order by cod_frequenza
    '''
    curr0 = conn.cursor()
    try:
        curr0.execute(query_select)
        frequenze_su_sit=curr0.fetchall()
    except Exception as e:
        logging.error(e)
    
    curr0.close()

    for ff in frequenze_su_sit:
        freq_SIT.append(ff[0])
        freq_UO.append(ff[1])
    

    


    ####################################################################################
    # controllo cosa è cambiato

    query2= '''select * from etl.frequenze_ok 
where cod_frequenza not in (select cod_frequenza from etl.frequenze_ok_1)'''

    ################################
    # predisposizione mail
    ################################

    # Create a secure SSL context
    context = ssl.create_default_context()

    subject = "FREQUENZA DA AGGIUNGERE A CODICE DI UO"
    body = '''Mail generata automaticamente dal codice python frequenze.py che gira su amiugis\n\n\n
    Frequenze da aggiungere:\n'''
    sender_email = user_mail
    receiver_email='assterritorio@amiu.genova.it'
    debug_email='roberto.marzocchi@amiu.genova.it'
    cc_mail='calvello@amiu.genova.it'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = cc_mail
    message["Subject"] = subject
    #message["Bcc"] = debug_email  # Recommended for mass emails
    message.preamble = "Variazione frequenze"

    
    # ciclo su variazioni
    
    curr0 = conn.cursor()
    try:
        curr0.execute(query2)
        frequenze_nuove=curr0.fetchall()
    except Exception as e:
        logging.error(e)
    curr0.close()

    check=0
    for fff in frequenze_nuove:
        body='{} cod_frequenza (SIT)= {} freq_binaria (UO) = {}\n'.format(body, fff[0], fff[4])
        check=1


    if check>0:
        # Add body to email
        message.attach(MIMEText(body, "plain"))

        text = message.as_string()

        logging.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logging.info(invio)
        
        
    
        ################################################################################################
        # ora siccome è cambiato qualcosa accedo su UO e ricreo il mapping frequenze


        # connessione Oracle
        cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
        parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
        logging.debug(parametri_con)
        con = cx_Oracle.connect(parametri_con)
        logging.info("Versione ORACLE: {}".format(con.version))



        cur = con.cursor()
        query='''TRUNCATE TABLE UNIOPE.CONS_MAPPING_FREQUENZE'''.format()
        try:
            cur.execute(query)
        except Exception as e:
            logging.error(query)
            logging.error(e)
        cur.close()
        #con.commit()


        cur = con.cursor()
        i=0
        while i < len(freq_SIT):
            query='''INSERT INTO UNIOPE.CONS_MAPPING_FREQUENZE
    (FREQUENZA_SIT, FREQUENZA_UO)
    VALUES({}, '{}')'''.format(freq_SIT[i], freq_UO[i])
            try:
                logging.debug(query)
                cur.execute(query)
                con.commit()
            except Exception as e:
                logging.error(query)
                logging.error(e)
            i+=1

        cur.close()
        con.close()
    else:
        logging.info("Non ci sono nuove frequenze quindi chiudo tutto")





    curr.close()
    conn.close()
	    

if __name__ == "__main__":
    main()