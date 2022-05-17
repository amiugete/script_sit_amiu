#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga il WS di IDEA che registra i conferimenti
'''


import os, sys, getopt, re  # ,shutil,glob
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path

import datetime


import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('../')
from credenziali import *
from recupera_token import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/conferimenti_horus.log'.format(path)
errorfile='{}/error_conferimenti_horus.log'.format(path)
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




def main():
    #################################################################
    logger.info("Recupero il token")
    token1=token()
    #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    #################################################################
    api_url='{}/svotamenti'.format(url_idea)
    headers1 = {'''Authorization: Token {0}'''.format(token1)}
    
    # per ora re-importo tutto, poi sarà da sistematre 
    '''query_truncate="TRUNCATE TABLE idea.conferimenti_horus CONTINUE IDENTITY RESTRICT;"
    try:
        curr.execute(query_truncate)
    except Exception as e:
        logger.error(e)
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn.commit()
    ########################################################################################
    curr.close()
    curr = conn.cursor()
    '''
    #print(headers1)
    #exit()
    p=1
    check=0
    
    #giorno='{}000000'.format((datetime.datetime.today()-datetime.timedelta(days = 1)).strftime('%Y%m%d'))
    giorno = '20220228000000'

    logger.debug("From date:{}".format(giorno))
    
    while check<1:
        logger.info('Page index {}'.format(p))
        response = requests.get(api_url, params={'date_from': giorno, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
        #response.json()
        logger.debug(response.status_code)
        try:      
            response.raise_for_status()
            # access JSOn content
            #jsonResponse = response.json()
            #print("Entire JSON response")
            #print(jsonResponse)
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}')
            check=500
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            logger.error(response.json())
            check=500
        if check<1:
            letture = response.json()
            
            colonne=letture['meta']['columns']
            
            logger.debug(len(colonne))
            logger.debug(colonne)
            
            
            logger.debug('Lette {} righe dalle API'.format(len(letture['data'])))
            if len(letture['data'])>=3:
                logger.info(letture['data'][2][14])
            #exit()
            if len(letture['data'])==0:
                check=100
            i=0
            while i < len(letture['data']):
                # 16 lat 17 long
                # 9 codice elemento IDEA
                # 14 data 
                # 6 codice badg 
                # 7 id_user
                if float(letture['data'][i][16])>0:
                    #id_isola
                    id_pdr=letture['data'][i]['id_pdr']
                    lat=letture['data'][i]['lat']
                    lon=letture['data'][i]['lng']
                    cod_cont=letture['data'][i]['cod_contenitore']
                    riempimento=letture['data'][i]['livello_riempimento']
                    data_ora_svuotamento=datetime.datetime.strptime(letture['data'][i]['data_ora_svuotamento'], "%Y%m%d%H%M%S").strftime("%Y/%m/%d %H:%M:%S")
                    p_netto=letture['data'][i]['peso_netto']
                    p_lordo=letture['data'][i]['peso_lordo']
                    p_tara=letture['data'][i]['peso_tara']
                    query_select='''SELECT * FROM idea.svuotamenti 
                    WHERE cod_cont=%s and data_ora_svuotamento=%s'''
                    try:
                        curr.execute(query_select, (cod_cont, data_ora_svuotamento))
                        conferimento=curr.fetchall()
                    except Exception as e:
                        logger.error(e)
                    curr.close()
                    curr = conn.cursor()
                    # se c'è già la entry faccio 
                    if len(conferimento)>0: 
                        query_update='''UPDATE idea.svuotamenti
                        SET id_piazzola=%s, riempimento=%s, peso_netto=%s, peso_lordo=%s, peso_tara=%s
                        geoloc=st_transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),3003)
                        WHERE id_elemento_idea=%s and data_ora_conferimento=%s;'''
                        try:
                            curr.execute(query_update, (id_pdr, riempimento, p_netto, p_lordo, p_tara, lon, lat, cod_cont, data_ora_svuotamento))
                        except Exception as e:
                            logger.error(e)
                    else:
                        query_insert='''INSERT INTO idea.svuotamenti
                        (id_piazzola, id_elemento_idea, riempimento, data_ora_svuotamento, peso_netto, peso_lordo, peso_tara, geoloc)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, 
                        st_transform(ST_SetSRID(ST_MakePoint(%s, %s),4326),3003));;'''
                        try:
                            curr.execute(query_insert, (id_pdr, cod_cont, data_ora_svuotamento, riempimento, p_netto, p_lordo, p_tara, lon, lat))
                        except Exception as e:
                            logger.error(e)
                    ########################################################################################
                    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                    #conn.commit()
                    ########################################################################################
                #print(i,letture['data'][i][9], letture['data'][i][10], letture['data'][i][14], letture['data'][i][16],letture['data'][i][17])
                i+=1
            p+=1
   
    logger.info("Chiudo definitivamente la connesione al DB")
    curr.close()
    conn.close()
    
    
    #while i
    
    
    
    
if __name__ == "__main__":
    main()   