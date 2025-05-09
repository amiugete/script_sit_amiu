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
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
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

    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    nstep = 5000
    
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

    query_select="""SELECT id FROM spazz_schmidt.serialnumbers where api_attive = 't' 
    and 
    id in ( '100001111',  '100001114') order by id desc"""
    try:
        curr.execute(query_select, ())
        serialnumbers=curr.fetchall()
    except Exception as e:
        logger.error(e)

    start_index=0
    start_index_old=0
    for sn in serialnumbers:
        logging.debug(sn[0])

        # imposto il check a 0
        check=0
        # al primo step settp last_id=0
        lastid = 0
        
        
        while check < 1:
            logger.debug('check = {}'.format(check))
            if check==0:
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
                start_index_old=start_index
                for si in start_id:
                    start_index=si[0]
                    #start_index=416301

                if start_index==start_index_old:
                    start_index = start_index+nstep


            #################################################################
            #dovrei farlo solo al primo step
            if lastid == 0:  
                check_last=0
                logger.info("Mi connetto al WS {0} per leggere l'ultimo messaggio della spazzatrice con SN {1} ". format(url_schmidt, sn[0], start_index))
                api_url='{0}SerialNumbers/{1}/Messages/last'.format(url_schmidt, sn[0])
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
                    messaggio_http_err='HTTP error occurred: {}'.format(http_err)
                    #warning_message_mail(messaggio_http_err, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.warning(messaggio_http_err)
                    check_last=500
                except Exception as err:
                    messaggio_other_err='Other error occurred: {}'.format(err)
                    #warning_message_mail(messaggio_other_err, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.warning(messaggio_other_err)
                    logger.warning(response.json())
                    check_last=500
                if check_last==0:
                    try:
                        letture = response.json()
                    except Exception as e:
                        logger.error(e)
                        logger.error('Problema lettura JSON ultimo messaggio della spazzatrice con SN {0} '.format(sn[0]))
                    #print(letture)
                    try:
                        lastid=letture['id']
                    except Exception as e:
                        lastid=-1
                        check=1
            
            
            
            check_start=0      
            if start_index == 0 and lastid > 0: 
                 
                #################################################################
                logger.info("Mi connetto al WS {0} per leggere il primo messaggio della spazzatrice con SN {1} ". format(url_schmidt, sn[0], start_index))
                api_url='{0}SerialNumbers/{1}/Messages/first'.format(url_schmidt, sn[0])
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
                    messaggio_http_err='HTTP error occurred: {}'.format(http_err)
                    #warning_message_mail(messaggio_http_err, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.warning(messaggio_http_err)
                    check_start=500
                except Exception as err:
                    messaggio_other_err='Other error occurred: {}'.format(err)
                    #warning_message_mail(messaggio_other_err, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.warning(messaggio_other_err)
                    logger.error(response.json())
                    check_start=500
                if check_start==0:
                    try:
                        letture = response.json()
                    except Exception as e:
                        logger.warning(e)
                        logger.warning('Problema lettura JSON primo messaggio della spazzatrice con SN {0} '.format(sn[0]))
                        start_index=nstep
                    #print(letture)
                    try:
                        firstid=letture['id']
                    except Exception as e:
                        logger.warning(e)
                        firstid=0
                        check=1
                
                    # se lo start_index è minore del primo messaggio allora lo inizializzo al primo id che posso leggere    
                    if start_index < firstid:
                        start_index=firstid
                 
            if check_start>0 or check_last>0:
                check=1
                logger.warning('Non procedo con le letture')
                            
            logger.info('''L'ultimo messaggio ha id {0}. Start_index = {1}'''.format(lastid, start_index))
            if lastid==start_index:
                logger.info('''Ho letto fino all'ultimo messaggio con id {}'''.format(lastid))
                check=1
            #exit()
        
            if lastid>0 and check <1:
                #################################################################
                logger.info("Mi connetto al WS {0} per leggere {3} messaggi della spazzatrice con SN {1} a partire da startIndex {2}". format(url_schmidt, sn[0], start_index, nstep))
                api_url='{0}SerialNumbers/{1}/Messages'.format(url_schmidt, sn[0])
                from requests.auth import HTTPBasicAuth
                auth=HTTPBasicAuth(user_schmidt, pwd_schmidt)
                #response = requests.get(api_url, auth=auth, headers={'accept': 'text/json'}, params={'startIndex': start_index, 'endIndex': start_index + nstep})
                # provo a togliere l'endIndex come suggeritomi
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
                try:
                    letture = response.json()
                    i=0
                    contatore=0
                    while i<len(letture):
                        colonne=letture[i]
                        #logger.debug(len(colonne))
                        #logger.debug(colonne)
                        logger.debug(letture[i])
                        #print(letture[i])
                        id=int(letture[i]['id'])
                        lv_id=int(letture[i]['lastValidId'])
                        header=letture[i]['header']
                        londec=0
                        #sweepermode=0
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
                            routeId = None
                            
                            data=letture[i]['header']['sysDate']
                            logger.debug(data)
                            gg=int(int(data[0:3])/4)
                            mm=data[3:5]
                            aaaa=1985+int(data[5:7])
                            
                        
                            ora=letture[i]['header']['sysTime']
                            logger.debug(ora)
                            hh=ora[0:2] 
                            minuti=ora[2:4]
                            ss=int(ora[4:7])/4

                            data_ora='{}/{}/{} {}:{}:{}'.format(aaaa,mm,gg, hh, minuti, ss)
                            logger.debug(data_ora)
                            # Il routeId non è nella sezione header ma in quella vehicleRoute -- > spostato sotto
                            #routeId=letture[i]['header']['routeId']
                        logger.debug('***************************************************')
                        vehicleRoute=letture[i]['vehicleRoute']
                        if vehicleRoute != None:
                            logger.debug('Vehicle route:', vehicleRoute)
                            routeId=letture[i]['vehicleRoute']['routeId']
                            lat=letture[i]['vehicleRoute']['geoLat']
                            lon=letture[i]['vehicleRoute']['geoLon']
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
                                latdec=-(int(float(lat)/100) + (float(lat) - int(float(lat)/100) * 100)/60)
                            else:
                                logger.error('Problem with latitude')
                            if lon[-1]=='E':
                                lon=letture[i]['vehicleRoute']['geoLon'].replace('E','')
                                londec=int(float(lon)/100) + (float(lon) - int(float(lon)/100) * 100)/60
                            elif lat[-1]=='W':
                                lon=letture[i]['vehicleRoute']['geoLon'].replace('W','')
                                londec=-(int(float(lon)/100) + (float(lon) - int(float(lon)/100) * 100)/60)
                            else:
                                logger.error('Problem with longitude')
                            logger.debug('lat = {0}, lon = {1}'.format(latdec, londec))
                            #print(datetime.datetime.now())
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
                        # leggo sweeper
                        sweeper=letture[i]['sweeper']
                        #print('Sweeper = ')
                        #print(sweeper)
                        if sweeper!=None:
                            sweepermode=letture[i]['sweeper']['swprMode']
                        else:
                            sweepermode=0
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
                        #print(len(serialnumbers),driverid)
                        if len(serialnumbers)==0 and londec > 0:
                            contatore+=1
                            query_insert="""
                            INSERT INTO spazz_schmidt.messaggi
                            (id, last_valid_id, serialnumber_id, routeid, driverid, driver2id, geoloc, data_ora, data_ora_inserimento, sweeper_mode)
                            VALUES(%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s, now(), %s)
                            ON CONFLICT (id, serialnumber_id) DO NOTHING ;           
                            """
                            try:
                                curr.execute(query_insert, (id, lv_id, sn[0], routeId, driverid, driver2id, londec, latdec, data_ora, sweepermode))
                            except Exception as e:
                                logger.error(e)
                            #print(query_insert)
                        '''elif len(serialnumbers)==0 and londec == 0 and (header!=None or vehicleRoute!=None):
                            query_insert="""
                            INSERT INTO spazz_schmidt.messaggi
                            (id, last_valid_id, serialnumber_id, routeid, driverid, driver2id, geoloc, data_ora, data_ora_inserimento, sweeper_mode)
                            VALUES(%s, %s, %s, %s, %s, %s, (select distinct geoloc from spazz_schmidt.messaggi where id=%s), %s, now(), %s);           
                            """
                            try:
                                curr.execute(query_insert, (id, lv_id, sn[0], routeId, driverid, driver2id, lv_id, data_ora,sweepermode))
                            except Exception as e:
                                logger.error(e)
                            #print(query_insert, (id, lv_id, sn[0], routeId, driverid, driver2id, lv_id, data_ora,sweepermode))
                        
                        '''
                        ########################################################################################
                        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                        conn.commit()
                        ######################################################################################## """
                        if sweepermode!=None:
                            logger.debug('Mode = {}'.format(sweepermode))
                        # da ricommentare per test
                        #exit()
                        i+=1
                    # se non ci sono dati sposto in avanti lo start_index    
                    if contatore==0 and start_index!=lastid:
                        logger.warning('Non ho letto nulla. Check={}'.format(check))
                        start_index=start_index+nstep
                        check=-1
                        logger.warning('Modifico start index e imposto il check a -1')
                    else:
                        check=0
                    
                        
                except Exception as e:
                    logger.error(e)
                    logger.error('Problema lettura messaggi della spazzatrice con SN {0} '.format(sn[0]))
                    check=203 # errore non beccato 


    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
   
    logger.info("Chiudo definitivamente la connesione al DB")
    curr2.close()
    curr1.close()
    curr.close()
    conn.close()
    
    
    
    
if __name__ == "__main__":
    main() 