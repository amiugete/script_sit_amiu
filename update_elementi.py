#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script sostituisce gli elementi di un certo tipo con un altro per un comune specifico
'''


import os, sys, re  # ,shutil,glob
import inspect, os.path

import xlsxwriter


#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import datetime

from credenziali import db, port, user, pwd, host


#import requests

import logging


filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/update.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='a', # overwrite or append
    #filename=logfile,
    level=logging.INFO)





def main():
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    '''
    comune torriglia id = 20
    tipo elemento RSU 1000 id= 1
    sostituito con 3 da 360 id=22
    

    '''

    select='''select e.id_piazzola, count(tipo_elemento) 
        from elem.elementi e 
        join elem.piazzole p 
        on p.id_piazzola = e.id_piazzola 
        join elem.aste a 
        on a.id_asta = p.id_asta 
        join topo.vie v 
        on v.id_via =a.id_via 
        where tipo_elemento = 1
        and v.id_comune = 20
        and (p.data_eliminazione is null or p.data_eliminazione > now())
        group by e.id_piazzola'''
    
    try:
        curr.execute(select)
        lista_piazzole=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #piazzola=[]
    #count=[]
    cont=1
    curr2=conn.cursor()
    for pp in lista_piazzole:
        logging.debug(pp[0])
        
        '''********************************
        tipo_elemento=22
        ********************************'''
        
        insert = '''INSERT INTO elem.elementi
(tipo_elemento, id_piazzola,  id_cliente, privato, peso_reale, peso_stimato, id_utenza, data_ultima_modifica, percent_riempimento, freq_stimata)
VALUES(22, {0}, '-1'::integer, 0, 0, 0, '-1'::integer, '2021/08/03', 75, 3)'''.format(pp[0])
        logging.debug(insert)
        '''********************************
        tipo_elemento=22
        ********************************'''
        delete1= '''delete from elem.elementi_aste_percorso eap 
        where id_elemento in (
        select id_elemento from elem.elementi e 
        where id_piazzola={} and tipo_elemento=1
        );'''.format(pp[0])
        delete2= '''DELETE FROM elem.elementi
WHERE id_piazzola={} and tipo_elemento=1;'''.format(pp[0])

        logging.debug(delete2)
        '''********************************
        numero da sostituitre3
        ********************************'''
        num_nuovi_elementi=3*pp[1]
        logging.info('{} - Piazzola: {} - Ne inserisco {}'.format(cont, pp[0], num_nuovi_elementi))
        k=0
        while k<num_nuovi_elementi:
            curr2.execute(insert)
            k+=1
        curr2.execute(delete1)
        curr2.execute(delete2)
        cont+=1
        
            
            
        #exit
    '''********************************
    attivare o disattivare
    ********************************'''
    #conn.commit()





if __name__ == "__main__":
    main()     