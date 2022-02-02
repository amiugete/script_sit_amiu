#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi



import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import csv

import psycopg2


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/bilaterali_progettazione.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)





def main():
    
    
    # 
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    


    piazzole_vetro=[21430, 22152, 22155, 22389, 22391, 22392, 22393, 22394, 22395, 22397, 22406, 22409, 22413, 23657, 23661, 23662, 23663, 23664, 23667, 23795, 23802, 24894, 24895, 24899, 24900, 24903, 24905, 27623, 27625, 27629, 27632, 27639, 27641, 27642, 27644, 27645, 27646, 27649, 27652, 27655, 28731]
    
    
    i=0
    while i < len(piazzole_vetro):
        query='''INSERT INTO elem.elementi
    (id_elemento,
    tipo_elemento, id_piazzola,
    id_asta,
    id_cliente, privato, peso_reale, peso_stimato,
    id_utenza, modificato_da, data_ultima_modifica, percent_riempimento, freq_stimata)
    VALUES((select min(id_elemento)-1 from elem.elementi),
    12, %s,
    (select id_asta from elem.piazzole where id_piazzola = %s),
    '-1'::integer, 0, 0, 0,
    '-1'::integer, 'Battaglia-Marzocchi', '2021/11/10 00:00:00', 90,3);'''
        curr.execute(query, (piazzole_vetro[i],piazzole_vetro[i] , ))
        i+=1
    
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn.commit()
    ########################################################################################
    
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()   