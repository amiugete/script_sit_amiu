#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga il WS di Aebi Schmidt per recuperare i messaggi per ciascun serial number 
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

#import requests
import datetime

import logging

from invio_messaggio import *


filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/messaggi.log'.format(path)
errorfile='{}/error_messaggi.log'.format(path)
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
f_handler.setLevel(logging.INFO)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)




def main():


    #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    curr1= conn.cursor()
    curr2= conn.cursor()
    #conn.autocommit = True
    #################################################################

    query_select="SELECT id FROM spazz_schmidt.serialnumbers"
    try:
        curr.execute(query_select, ())
        serialnumbers=curr.fetchall()
    except Exception as e:
        logger.error(e)

    
    for sn in serialnumbers:
        logging.debug(sn[0])

        # imposto il check a 0
        check=0
    
        while check < 1:
            query_start='''SELECT 
            CASE 
                WHEN max(id) is null THEN 0
                ELSE max(id)
            END start_id
            FROM spazz_schmidt.messaggi
            WHERE serialnumber_id=%s;'''
            try:
                curr1.execute(query_start, (sn[0],))
                start_id=curr1.fetchall()
            except Exception as e:
                logger.error(e)
            
            # parto a leggere da quello StartIndex
            for si in start_id:
                start_index=si[0]



            #################################################################
            logger.info("Mi connetto al WS {0} per leggere l'ultimo messaggio della spazzatrice con SN {1} ". format(url_schmidt, sn[0], start_index))
            api_url='{0}SerialNumbers/{1}/Messages/Last'.format(url_schmidt, sn[0])
            from requests.auth import HTTPBasicAuth
            auth=HTTPBasicAuth(user_schmidt, pwd_schmidt)
            response = requests.get(api_url, auth=auth, headers={'accept': 'text/json'})
            logger.info("Status code: {0}".format(response.status_code))
            try:      
                response.raise_for_status()
                # access JSOn content
                #jsonResponse = response.json()
                #print("Entire JSON response")
                #print(jsonResponse)
            except HTTPError as http_err:
                logger.error(f'HTTP error occurred: {http_err}')
                check0=500
            except Exception as err:
                logger.error(f'Other error occurred: {err}')
                logger.error(response.json())
                check0=500
            letture = response.json()
            lastid=letture['id']


            #logger.info('''L'ultimo messaggio ha id {}'''.format(lastid))
            if lastid==start_index:
                logger.info('''Ho letto fino all'ultimo messaggio con id {}'''.format(lastid))
                check=1
            #exit()
        
            #################################################################
            logger.info("Mi connetto al WS {0} per leggere i messaggi della spazzatrice con SN {1} a partire da startIndex {2}". format(url_schmidt, sn[0], start_index))
            api_url='{0}SerialNumbers/{1}/Messages'.format(url_schmidt, sn[0])
            from requests.auth import HTTPBasicAuth
            auth=HTTPBasicAuth(user_schmidt, pwd_schmidt)
            response = requests.get(api_url, auth=auth, headers={'accept': 'text/json'}, params={'startIndex': start_index})
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
            letture = response.json()
            i=0
            while i<len(letture):
                colonne=letture[i]
                #logger.debug(len(colonne))
                #logger.debug(colonne)
                logger.debug(letture[i])
                id=letture[i]['id']
                lv_id=letture[i]['lastValidId']
                header=letture[i]['header']
                if header==None:
                    logger.debug('''Non è specificato il codice percorso e/o l'operatore''')
                    driverid = None
                    driver2id = None
                    routeId = None
                else: 
                    #"driverId": "string",
                    #"driver2Id": "string",
                    #"routeId": "string",
                    driverid=letture[i]['header']['driverId']
                    driver2id=letture[i]['header']['driver2Id']
                    routeId=letture[i]['header']['routeId']
                logger.debug('***************************************************')
                vehicleRoute=letture[i]['vehicleRoute']
                if vehicleRoute != None:
                    logger.debug('Vehicle route:', vehicleRoute)
                    lat=letture[i]['vehicleRoute']['geoLat']
                    if lat == None:
                        logger.warning('Lat is null')
                        exit()
                    #DD = int(float(lat)/100)
                    #SS = float(lat) - DD * 100
                    #latdec=DD + SS/60
                    # valido per latitudini positive
                    if lat[-1]=='N':
                        lat=lat.replace('N','')
                        latdec=int(float(lat)/100) + (float(lat) - int(float(lat)/100) * 100)/60
                    elif lat[-1]=='S':
                        lat=lat.replace('S','')
                        latdec=-int(float(lat)/100) + (float(lat) - int(float(lat)/100) * 100)/60
                    else:
                        logger.error('Problem with latitude')
                    lon=letture[i]['vehicleRoute']['geoLon'].replace('E', '')
                    londec=int(float(lon)/100) + (float(lon) - int(float(lon)/100) * 100)/60
                    logger.debug('lat = {0}, lon = {1}'.format(latdec, londec))
                    print(datetime.datetime.now())
                    data=letture[i]['vehicleRoute']['sysDate']
                    logger.debug(data)
                    gg=int(int(data[0:3])/4)
                    mm=data[3:5]
                    aaaa=1985+int(data[5:7])
                    
                
                    ora=letture[i]['vehicleRoute']['sysTime']
                    logger.debug(ora)
                    hh=ora[0:2] 
                    minuti=ora[2:4]
                    ss=int(ora[4:7])/4

                    data_ora='{}/{}/{} {}:{}:{}'.format(aaaa,mm,gg, hh, minuti, ss)
                    logger.debug(data_ora)
                    #exit()
                    sweeper=letture[i]['sweeper']
                    if sweeper!=None:
                        sweepermode=letture[i]['sweeper']['swprMode']
                    else:
                        sweepermode=None
                    # controllo che non ci sia già un messaggio con quell'id
                    query_select="SELECT * FROM spazz_schmidt.messaggi where id = %s and serialnumber_id=%s"
                    try:
                        curr.execute(query_select, (id,sn[0]))
                        serialnumbers=curr.fetchall()
                    except Exception as e:
                        logger.error(e)
                    curr.close()
                    curr = conn.cursor()
                    # se c'è già la entry faccio 
                    if len(serialnumbers)==0 and londec > 0:
                        query_insert="""
                        INSERT INTO spazz_schmidt.messaggi
                        (id, last_valid_id, serialnumber_id, routeid, driverid, driver2id, geoloc, data_ora, data_ora_inserimento, sweeper_mode)
                        VALUES(%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, now(), %s);           
                        """
                        try:
                            curr.execute(query_insert, (id, lv_id, sn[0], routeId, driverid, driver2id, londec, latdec, data_ora,sweepermode))
                        except Exception as e:
                            logger.error(e)
                    

                        ########################################################################################
                        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                        conn.commit()
                        ######################################################################################## """
            
                logger.debug('Mode = {}'.format(sweepermode))
                #exit()
                i+=1


    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
   
    logger.info("Chiudo definitivamente la connesione al DB")
    curr2.close()
    curr1.close()
    curr.close()
    conn.close()
    
    
    #while i
    
    
    
    
if __name__ == "__main__":
    main() 