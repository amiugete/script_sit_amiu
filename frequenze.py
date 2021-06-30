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

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import db, port, user, pwd, host


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/frequenze.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    #filename=logfile,
    level=logging.ERROR)


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

            query_insert='''
            INSERT INTO marzocchir.frequenze_ok
            (cod_frequenza, descrizione_short, descrizione_long, num_frequenza, freq_binaria, num_giorni)
            VALUES({0}, '{1}', '{2}', {0}, '{3}', {4});
            '''.format(freq_int, descrizione_short, descrizione_long, stringa_binaria, num)
            logging.debug(query_insert)
            curr1 = conn.cursor()
            curr1.execute(query_insert)
        except Exception as e:
            logging.error(e)
            logging.error(query_insert)
            #logging.error('Null value')
        i+=1
        curr1.close()
    curr.close()
    conn.close()
	    

if __name__ == "__main__":
    main()