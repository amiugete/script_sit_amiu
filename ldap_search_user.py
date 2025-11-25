#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga LDAP per recuperare info dell'utente


'''


import os, sys, getopt, re
from tkinter import E, Entry  # ,shutil,glob
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

# funzioni per inviare messaggi mail
from invio_messaggio import *

#import requests
import datetime
import time

import ldap

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/ldap.log'.format(path)
errorfile='{}/log/ldap_error.log'.format(path)
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
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    ###################################################################

    # faccio update solo se già non ci fosse una mail
    query_select="select * from util.sys_users su where domain_name = 'DSI' and (email is null or email='')"
    try:
        curr.execute(query_select)
        users=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #piazzola=[]
    #count=[]

    #curr1=conn.cursor()
    for uu in users:
        user1=uu[1]
        id1=uu[4]
        try:
            connect = ldap.initialize(ldap_url)
            connect.set_option(ldap.OPT_REFERRALS, 0)
            connect.simple_bind_s(ldap_login, ldap_pwd)
            criteria = "(&(objectClass=user)(sAMAccountName={0}))".format(user1)
            attributes = ['sAMAccountName', 'mail']
            result = connect.search_s('DC=amiu,DC=genova,DC=it',
                                ldap.SCOPE_SUBTREE, criteria, attributes)
            #print(result)
            sAn=result[0][1]['sAMAccountName'][0].decode('utf-8')
            #print(sAn)
            mail=result[0][1]['mail'][0].decode('utf-8')
            query_update='UPDATE util.sys_users SET email= %s WHERE id_user=%s'
            curr.execute(query_update,(mail, id1,))
            conn.commit()
            
        
        except Exception as e:
            logger.error(f'Non si riesce a configurare in maniera automatica la mail per l\'utente:{user1}')
            logger.error('Verificare il seguente utenti su SIT e aggiungere la MAIL alla tabella util.sys_users, perchè non si riesce a configurare in maniera automatica')
            logger.error(f'Errore riscontrato:{e}')
            #logger.error(e)
    

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")

    curr.close()
    conn.close()



if __name__ == "__main__":
    main() 