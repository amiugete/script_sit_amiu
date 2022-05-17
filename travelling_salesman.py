#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge le date di disattivazioni su U.O. ed effettua l'allineamento con SIT:


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


import openrouteservice as ors


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/travelling_salesman.log'.format(path)
errorfile='{}/log/error_travelling_salesman.log'.format(path)
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
from allegato_mail import *


def main():

    nome_db=db #db_test
    
    # connessione a PostgreSQL
    logging.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    cer=200101
    riempimento=0
    numero=50
    num_veicoli=1

    query='''select * from (
        select row_number() over() as id, ci.id_piazzola::int, 
        ci.indirizzo_idea, concat(vpd.via,' ', vpd.civ, ' - ', vpd.riferimento) as indirizzo_amiu,
        count(ci.id_elemento_idea) as num, sum(ci.volume_contenitore) as vol, avg(ci.val_riemp) as riempimento_medio,
        string_agg(ci.data_ultimo_agg::text, ',') as aggiornamenti, st_x(st_transform(ci.geoloc,4326)) as lon, 
        st_y(st_transform(ci.geoloc,4326)) as lat
        from idea.censimento_idea ci 
        left join elem.v_piazzole_dwh vpd on vpd.id_piazzola::text  = ci.id_piazzola
        where ci.cod_cer_mat = %s
        group by ci.id_piazzola, ci.geoloc, ci.indirizzo_idea, vpd.via, vpd.civ, vpd.riferimento
        ) foo 
        where riempimento_medio > %s
        order by riempimento_medio desc
        limit %s'''


    try:
        curr.execute(query, (cer,riempimento,numero))
        piazzole=curr.fetchall()
    except Exception as e:
        logging.error(query)
        logging.error(e)


    depot = [-19.818474, 34.835447]



    # Define the vehicles
    # https://openrouteservice-py.readthedocs.io/en/latest/openrouteservice.html#openrouteservice.optimization.Vehicle
    vehicles = list()
    for idx in range(num_veicoli):
        vehicles.append(
            ors.optimization.Vehicle(
                id=idx,
                start=list(reversed(depot)),
                end=list(reversed(depot)),
                capacity=[300]#,
                #time_window=[1553241600, 1553284800]  # Fri 8-20:00, expressed in POSIX timestamp
            )
        )

    # Next define the delivery stations
    # https://openrouteservice-py.readthedocs.io/en/latest/openrouteservice.html#openrouteservice.optimization.Job
    deliveries = list()
    '''
    0 id
    1  id_piazzola	
    2 indirizzo_idea	
    3 indirizzo_amiu	
    4 num	
    5 vol	
    6 riempimento_medio	
    7 aggiornamenti	
    8 lon	
    9 lat
    '''
    for d in piazzole:
        deliveries.append(
            ors.optimization.Job(
            #ors.optimization.ShipmentStep(
                id=d[1],
                #description=d[1],
                location=[d[8], d[9]],
                service=120*d[4]  #  2 minuti per ogni elemento
                #pickup=[d[4]/1000]
                #time_windows=[[
                #    int(delivery.Open_From.timestamp()),  # VROOM expects UNIX timestamp
                #    int(delivery.Open_To.timestamp())
                #]]
            )
        )
    
    ll = []
    for d in piazzole:
        ll.append([d[8], d[9]])
    #print(deliveries)
    #exit()

    ors_client = ors.Client(key=ORS_KEY)  # Get an API key from https://openrouteservice.org/dev/#/signup
    

    from openrouteservice import distance_matrix


    request = {'locations': ll,
           'profile': 'driving-hgv',
           'optimized': True}

    pubs_matrix = ors_client.distance_matrix(**request)




    result = ors_client.optimization(
        jobs=deliveries,
        vehicles=vehicles,
        geometry=False#, 
        #matrix= pubs_matrix
    )
    
    #print(result['routes'])

    #result['routes']

    for route in result['routes']:
        for step in route["steps"]:
            if step.get("job", "Depot")=='Depot':
                print('Depot')
            else:
                p=int(step.get("job", "Depot"))
                print(p)
                qq='''select id_piazzola, concat(vpd.via,' ', vpd.civ, ' - ', vpd.riferimento) as indirizzo_amiu
                from elem.v_piazzole_dwh vpd
                where id_piazzola =%s'''
                try:
                    curr.execute(qq, (p,))
                    dettaglio=curr.fetchall()
                except Exception as e:
                    logging.error(e)
                for dp in dettaglio:
                    print('{} - {}'.format(dp[0], dp[1]))
                #print(step.get("job", "Depot"))

    curr.close()
    conn.close()

if __name__ == "__main__":
    main()   