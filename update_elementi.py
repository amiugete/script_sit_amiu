#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script sostituisce gli elementi di un certo tipo con un altro
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
    level=logging.DEBUG)





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

    select='''select e.id_piazzola, count(tipo_elemento) 
        from elem.elementi e 
        join elem.piazzole p 
        on p.id_piazzola = e.id_piazzola 
        join elem.aste a 
        on a.id_asta = p.id_asta 
        join topo.vie v 
        on v.id_via =a.id_via 
        where tipo_elemento = 12
        and v.id_comune = 17
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

    curr2=conn.cursor()
    for pp in lista_piazzole:
        logging.debug(pp[0])
        insert = '''INSERT INTO elem.elementi
(tipo_elemento, id_piazzola,  id_cliente, privato, peso_reale, peso_stimato, id_utenza, data_ultima_modifica, percent_riempimento, freq_stimata)
VALUES(40, {0}, '-1'::integer, 0, 0, 0, '-1'::integer, '2021/08/03', 75, 3)'''.format(pp[0])
        logging.debug(insert)
        delete= '''DELETE FROM elem.elementi
WHERE id_piazzola={} and tipo_elemento=12'''.format(pp[0])
        logging.debug(delete)
        if pp[1]==1:
            curr2.execute(delete)
            #curr2.execute(insert)
            #curr2.execute(insert)
            logging.debug('Ne inserisco 2')
        elif pp[1]==2:
            curr2.execute(delete)
            #curr2.execute(insert)
            #curr2.execute(insert)
            #curr2.execute(insert)
            logging.debug('Ne inserisco 3')
        #exit






if __name__ == "__main__":
    main()     