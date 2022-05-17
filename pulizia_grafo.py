#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per fare update delle aste delle piazzole
'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


import psycopg2

import datetime

from urllib.request import urlopen
import urllib.parse


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('./')
from credenziali import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/pulizia_grafo.log'.format(path)
errorfile='{}/log/error_pulizia_grafo.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''


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


#########################################################
ambiente='sit_test'
#########################################################



def main():



    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=ambiente,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    select_aste_da_pulire='''
    select s.* from 
    (
        select g.id,
        g.geoloc, 
        st_length(g.geoloc)::int as lung,
        --start
        string_agg(distinct g1.id::text, ',') as start_id,
        count(distinct g1.id) as conto_start,
        string_agg(distinct g2.id::text, ',') as end_id,
        count(distinct g2.id) as conto_end
        from geo.grafostradale g 
        -- destra
        join geo.grafostradale g1 on g.id != g1.id and st_touches(st_startpoint(g.geoloc), g1.geoloc)
        --sinistra
        join geo.grafostradale g2 on g.id != g2.id and st_touches(st_endpoint(g.geoloc), g2.geoloc)
        --where st_length(g.geoloc) < 10
        group by g.id,
        g.geoloc
    ) s 
    where ((s.conto_start= 1 and s.conto_end>1) or (s.conto_start> 1 and s.conto_end=1)) and lung < 10
    order by id'''


    try:
        curr.execute(select_aste_da_pulire)
        lista_aste=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #ut=[]

           
    for a in lista_aste:
        id_asta_rimuovere=a[0]
        conto_aste_start=a[4]
        conto_aste_end=a[6]
        if conto_aste_start==1:
            nuova_asta=a[3]
        elif conto_aste_end ==1:
            nuova_asta=a[5]
        else:
            logging.error('Non dovrei mai entrare qua dentro')

        # cerco piazzole  e se sì applico la funzione
        

        # cerco altri elementi e se sì applico la funzione


        # cerco percorsi di spazzamento che ci passano --> elimino e scrivo nella history 

