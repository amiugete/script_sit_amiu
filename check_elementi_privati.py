#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script importa in automatico alcuni elementi su una lista di piazzole date in un file CSV
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import csv

import psycopg2

import datetime

from credenziali import *


#import requests

import logging



#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/check_pre_import_legno_plastica.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode='w', # overwrite or append
    filename=logfile,
    level=logging.INFO)





def main():

    logging.info('Lettura file CSV')

    
    id_piazzola=[]
    civico=[]
    riferimento=[]
    
    with open('input/piazzole_imballaggi.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.info(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                #logging.debug(len(row))
                if len(row):
                    #logging.debug(line_count)
                    #logging.debug(len(row))
                    id_piazzola.append(int(row['ID_Piazzola']))
                    civico.append(row['Civico'])
                    riferimento.append(row['Riferimento'])
                line_count += 1
        #logging.debug(id_piazzola)
        logging.info('Lette {} righe nel file CSV'.format(len(id_piazzola)))

    

    # # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
            port=port,
            user=user,
            password=pwd,
            host=host)

    curr = conn.cursor()
    conn.autocommit = True


    # num_giorno=datetime.datetime.today().weekday()
    # giorno=datetime.datetime.today().strftime('%A')
    # giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    # logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    # if num_giorno==0:
    #     num=3
    # elif num_giorno in (5,6):
    #     num=0
    #     logging.info('Oggi è {0}, lo script non gira'.format(giorno))
    #     exit()
    # else:
    #     num=1
    
    # query='''select distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut
    #     from util.sys_history h
    #     inner join elem.percorsi p 
    #     on h.id_percorso = p.id_percorso 
    #     inner join elem.percorsi_ut pu 
    #     on pu.cod_percorso =p.cod_percorso 
    #     inner join elem.servizi s 
    #     on s.id_servizio =p.id_servizio
    #     inner join topo.ut u 
    #     on u.id_ut = pu.id_ut 
    #     where h.datetime > (current_date - INTEGER '{0}') 
    #     and h.datetime < current_date 
    #     and h."type" = 'PERCORSO' 
    #     and h.action = 'UPDATE_ELEM'
    #     and pu.responsabile = 'S'
    #     order by ut, servizio'''.format(num)
    

    i=0
    while i < len(id_piazzola):
        query='''select e.id_asta, e.id_cliente, e.posizione, e.privato, e.numero_civico_old, 
ep.id_utenzapap, e.numero_civico, e.lettera_civico, e.colore_civico, e.note, e.riferimento, 
ep.id_elemento_privato, 
ep.descrizione 
from elem.elementi e
join elem.elementi_privati ep 
on e.x_id_elemento_privato = ep.id_elemento_privato
left join utenze.utenze u on 
ep.id_utenzapap = u.id_utenza 
where e.id_piazzola = {}
group by  e.id_asta, e.id_cliente, e.posizione, e.privato, e.numero_civico_old, 
ep.id_utenzapap, e.numero_civico, e.lettera_civico, e.colore_civico, e.note, e.riferimento, 
ep.id_elemento_privato, 
ep.descrizione, u.data_cessazione
order by u.data_cessazione, ep.id_utenzapap desc
limit 1'''.format(id_piazzola[i])
        try:
            curr.execute(query)
            parametri_elemento=curr.fetchall()
        except Exception as e:
            logging.error(e)
        if len(parametri_elemento) > 1:
            logging.error('La piazzola {} contiene più di un elemento privato'.format(id_piazzola[i]))
        for vv in parametri_elemento: 
            logging.info(vv)
        i+=1



if __name__ == "__main__":
    main()