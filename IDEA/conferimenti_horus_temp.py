#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga il WS di IDEA che registra i conferimenti
'''


import os, sys, getopt, re  # ,shutil,glob

import argparse
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
logfile='{}/conferimenti_horus_temp.log'.format(path)
errorfile='{}/error_conferimenti_horus_temp.log'.format(path)
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
    api_url='{}/conferimentihorus'.format(url_idea)
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
    
    '''
    id_piazzola=''
    try:
        opts, args = getopt.getopt(argv,"hm:p:",["ifile=","ofile="])
    except getopt.GetoptError:
        print('conferimenti_horus.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('conferimenti_horus.py -m <mode> [-p <piazzola>')
            sys.exit()
        elif opt in ("-m", "--mode"):
            mode = arg
        elif opt in ("-p", "--piazzola"):
            id_piazzola = arg
    logger.info('Mode is {}'.format(mode))
    logger.info('Piazzola {}'.format(id_piazzola))
    

    parser = argparse.ArgumentParser(description = "conferimenti_horus.py -m <mode> [-p <piazzola>]")
    parser.add_argument("-h", "--Help", help = 'conferimenti_horus.py -m <mode> [-p <piazzola>]', required = False, default = '')
    parser.add_argument("-m", "--mode", help = 'Mode: standard or piazzola', required = False, default = 'standard')
    parser.add_argument("-p", "--piazzola", help = 'id_piazzola', required = False, default = '')
    
    argument = parser.parse_args()
    status = False
    
    if argument.Help:
        print("You have used '-H' or '--Help' with argument: {0}".format(argument.Help))
        status = True
    if argument.mode:
        mode=argument.mode
        status = True
    if argument.piazzola:
        id_piazzola=argument.piazzola
        status = True
    
    if mode == 'standard':
        giorno='{}000000'.format((datetime.datetime.today()-datetime.timedelta(days = 1)).strftime('%Y%m%d'))
    elif mode == 'piazzola':
        giorno='20220101000000'
        if id_piazzola == '':
            logger.error('With mode "piazzola" id_piazzola is mandatory')
            sys.exit(1)  
    else: 
        logger.error('Mode non recognized')
        sys.exit(1)

    '''


    giorno1='20220101000000'
    giorno2='20220401000000'
    logger.debug("From date:{}".format(giorno1))
    while check<1:
        logger.info('Page index {}'.format(p))
        response = requests.get(api_url, params={'date_from': giorno1, 'date_to': giorno2, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
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
                #if (i % 100)==0:
                #    logger.debug(i)
                if float(letture['data'][i][16])>0:
                    #id_isola
                    #id_isola=letture['data'][i][2]
                    id_idea=letture['data'][i][0]
                    id_elemento=letture['data'][i][9]
                    #descrizione_elemento=letture['data'][i][10]
                    #cod_cer=letture['data'][i][11]
                    cod_rifiuto=letture['data'][i][12]
                    id_badge=letture['data'][i][6]
                    id_user=letture['data'][i][7]
                    id_categoria=letture['data'][i][13]
                    data_conferimento=datetime.datetime.strptime(letture['data'][i][14], "%Y%m%d%H%M%S").strftime("%Y/%m/%d %H:%M:%S")
                    query_select='''SELECT * FROM idea.conferimenti_horus 
                    WHERE id_idea=%s'''
                    try:
                        curr.execute(query_select, (id_idea,))
                        conferimento=curr.fetchall()
                    except Exception as e:
                        logger.error(e)
                    curr.close()
                    curr = conn.cursor()
                    # se c'è già la entry faccio 
                    if len(conferimento)>0: 
                        query_update='''UPDATE idea.conferimenti_horus
                        set cod_prodotto=%s, id_badge=%s, id_user=%s, id_categoria=%s, id_elemento=%s, data_ora_conferimento=%s
                        WHERE id_idea=%s;'''
                        try:
                            curr.execute(query_update, (cod_rifiuto, id_badge, id_user, id_categoria, id_elemento, data_conferimento, id_idea))
                        except Exception as e:
                            logger.error(e)
                    else:
                        query_insert='''INSERT INTO idea.conferimenti_horus
        (id_elemento, cod_prodotto, id_badge, id_user, id_categoria, data_ora_conferimento, id_idea)
        VALUES(%s, %s, %s, %s, %s , %s, %s);'''
                        try:
                            curr.execute(query_insert, (id_elemento, cod_rifiuto, id_badge, id_user, id_categoria, data_conferimento, id_idea))
                        except Exception as e:
                            logger.error(e)
                    ########################################################################################
                    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                    conn.commit()
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