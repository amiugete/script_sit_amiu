#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga i WS di Bucher


'''


import os, sys, getopt, re  # ,shutil,glob
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path




import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('../')
from credenziali import *

#import requests
import datetime
import time


import logging

from invio_messaggio import *


filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/veicoli.log'.format(path)
errorfile='{}/error_veicoli.log'.format(path)
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
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
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

    logging.info("START READ WS")
    api_url=url_bucher_token
    logger.debug(payload_bucher)
    response = requests.post(api_url, json=payload_bucher)
    logger.debug(response)
    #response.json()
    logger.info("Status code: {0}".format(response.status_code))
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
    token=response.json()['token']

    logger.info("Mi connetto al WS GET {}". format(url_bucher))
    
    
    #exit()
    

    ms = datetime.datetime.now()
    endtime= int(time.mktime(ms.timetuple())) #* 1000


    # recupero lo starttime dai dati che ho già a sistema
    query_st='''select extract(epoch from max(data_ora)at time zone 'cet')::int as startime,
    extract(epoch from now() at time zone 'cet')::int as endtime,
    max(data_ora)
    from spazz_bucher.messaggi m '''
    try:
        curr.execute(query_st)
        st0=curr.fetchall()
    except Exception as e:
        logger.error(e)
    curr.close()
    curr = conn.cursor()
    # se c'è già la entry faccio 
    if len(st0)>0:
        for st1 in st0:
            #starttime=st1[0]
            #endtime=st1[1]
            maxt=st1[2]
        '''logging.debug('Postgres time')
        logging.debug(starttime)
        logging.debug(endtime)
        logging.debug('Python time')
        #starttime=datetime.datetime.strptime(maxt, '%Y-m-%d %H:%M:%S')'''
        starttime= int(time.mktime(maxt.timetuple())) #* 1000
    else:
        yesterday = ms - datetime.timedelta(hours = 3)
        starttime= int(time.mktime(yesterday.timetuple())) #* 1000
    #exit()

    logging.debug(starttime)
    logging.debug(endtime)
    #exit()

    #response = requests.get(url_bucher, params={'starttime':starttime, 'endtime': endtime, 'X-Auth-Token':token}, headers={'accept': 'application/json'})
    response = requests.get(url_bucher, params={'starttime':starttime, 'endtime': endtime}, headers={'X-Auth-Token': token})
    #response = requests.get(url_bucher, headers={'X-Auth-Token': token})
    logger.debug(response.url)

    logger.info("Status code: {0}".format(response.status_code))
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
    
    logger.debug("Printing Entire Get Request")
    

    letture = response.json()
    logger.debug(letture)
    logger.debug(len(letture))
    i=0
    if len(letture)>1:
        while i<len(letture):
            colonne=letture[i]
            #logger.debug(len(colonne))
            #logger.debug(colonne)
            asset_id=letture[i]['asset']['asset_id']
            sportello=letture[i]['asset']['name']
            sn=letture[i]['asset']['sn']
            note=letture[i]['asset']['note']
            query_select="SELECT * FROM spazz_bucher.mezzi where sn = %s"
            try:
                curr.execute(query_select, (sn,))
                serialnumbers=curr.fetchall()
            except Exception as e:
                logger.error(e)
            curr.close()
            curr = conn.cursor()
            # se c'è già la entry faccio 
            if len(serialnumbers)>0: 
                query_update='''UPDATE spazz_bucher.mezzi
                set id=%s, sportello=%s, note= %s
                WHERE sn=%s;'''
                try:
                    curr.execute(query_update, (asset_id, sportello, note, sn))
                except Exception as e:
                    logger.error(e)
            else:
                query_insert='''INSERT INTO spazz_bucher.mezzi
    (id, sportello, note, sn)
    VALUES(%s, %s, %s, %s);'''
                try:
                    curr.execute(query_insert, (asset_id, sportello, note, sn))
                except Exception as e:
                    logger.error(e)
            ########################################################################################
            # da testare sempre prima senza fare i commit per verificare che sia tutto OK
            conn.commit()
            ########################################################################################

            # leggo la parte restante del messaggio
            sweeper_mode=letture[i]['sweeper']['status']
            route_id=letture[i]['route_id']
            trk_id=letture[i]['trkId']
            driver_id=letture[i]['operator_id']
            lat=letture[i]['latitude']
            lon=letture[i]['longitude']
            s_time=letture[i]['sample_time']
            



            query_select="SELECT * FROM spazz_bucher.messaggi where sportello = %s and data_ora=%s"
            try:
                curr.execute(query_select, (sportello,s_time))
                serialnumbers=curr.fetchall()
            except Exception as e:
                logger.error(e)
            curr.close()
            curr = conn.cursor()
            # se c'è già la entry faccio 
            if len(serialnumbers)>0: 
                query_update='''UPDATE spazz_bucher.messaggi
                set routeid =%s, driverid =%s, sweeper_mode =%s, geoloc=ST_SetSRID(ST_MakePoint(%s, %s),4326), trk_id=%s
                WHERE sportello = %s and data_ora=%s;'''
                try:
                    curr.execute(query_update, (route_id, driver_id, sweeper_mode, lon, lat, trk_id, sportello, s_time))
                except Exception as e:
                    logger.error(e)
            else:
                query_insert='''INSERT INTO spazz_bucher.messaggi
                (routeid, driverid, sweeper_mode, geoloc, sportello, data_ora, trk_id)
                VALUES(%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, %s, %s);'''
                try:
                    curr.execute(query_insert, (route_id, driver_id, sweeper_mode, lon, lat, sportello, s_time, trk_id))
                except Exception as e:
                    logger.error(e)
            ########################################################################################
            # da testare sempre prima senza fare i commit per verificare che sia tutto OK
            logger.info('Faccio commit')
            conn.commit()
            ########################################################################################

            
            
            
            # da aggiornare ultimo utilizzo e ultimo spazzamento nella tabella dei mezzi


            i+=1
    else:
        logger.warning('''Non c'è nulla da leggere''')
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info('Fine script')

if __name__ == "__main__":
    main()   