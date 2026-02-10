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
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

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


# update con la mail
query_update='UPDATE util.sys_users SET email= %s WHERE id_user=%s'

query_update_ns='UPDATE util_ns.sys_users SET email= %s WHERE id_user=%s'


query_upsert_ns="""INSERT INTO util_ns.sys_users (
    domain_name, 
    "name",
    id_role,
    last_access,
    id_user,
    email) 
    VALUES 
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    )
    ON CONFLICT (id_user)
    DO UPDATE  
    SET domain_name=EXCLUDED.domain_name, 
    "name"=EXCLUDED."name", 
    id_role=EXCLUDED.id_role, 
    last_access=EXCLUDED.last_access, 
    email=EXCLUDED.email;"""


# upsert della tabella sys_users_addons (poi sarà da spostare)
query_upsert_addons="""INSERT INTO util_ns.sys_users_addons 
(id_user, 
esternalizzati, sovrariempimenti,
sovrariempimenti_admin,
coge, utenze) 
VALUES (%s, 
NULL, NULL, 
NULL, 
NULL, NULL) ON CONFLICT DO NOTHING;"""



def main():
     #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    logger.info('Connessione al db SIT TEST')
    conn_test = psycopg2.connect(dbname=db_test,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    curr_ns = conn.cursor()
    curr_test = conn_test.cursor()
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
            
            # update mail
            curr.execute(query_update,(mail, id1,))

            curr_ns.execute(query_update_ns,(mail, id1,))
            
            
            conn.commit()
            
        
        except Exception as e:
            logger.error(f'Non si riesce a configurare in maniera automatica la mail per l\'utente:{user1}')
            logger.error('Verificare il seguente utenti su SIT e aggiungere la MAIL alla tabella util.sys_users, perchè non si riesce a configurare in maniera automatica')
            logger.error(f'Errore riscontrato:{e}')
            #logger.error(e)
    
    
    # faccio secondo giro su tutti utenti 
    logger.info('Faccio  giro su tutti utenti per aggiungerli la riga in sys_users_addons')
    query_select2="select * from util.sys_users su where domain_name = 'DSI' "
    try:
        curr.execute(query_select2)
        users=curr.fetchall()
    except Exception as e:
        logging.error(e)
    
    for uu in users:
        user1=uu[1]
        id1=uu[4]
        # upsert sys_users_addons
        curr.execute(query_upsert_ns,(uu[0],uu[1],uu[2],uu[3],uu[4], uu[5],))
        curr.execute(query_upsert_addons,(id1,))
        
        conn.commit()
    
    # faccio secondo giro su tutti utenti in test
    logger.info('Faccio  giro su tutti utenti di test per aggiungerli la riga in sys_users_addons')

    try:
        curr_test.execute(query_select2)
        users_test=curr_test.fetchall()
    except Exception as e:
        logging.error(e)
    
    for uut in users_test:
        user1_test=uut[1]
        id1_test=uut[4]
        # upsert sys_users_addons
        curr_test.execute(query_upsert_ns,(uu[0],uu[1],uu[2],uu[3],uu[4], uu[5],))
        curr_test.execute(query_upsert_addons,(id1_test,))
        
        conn_test.commit()
        
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")

    curr.close()
    curr_ns.close()
    curr_test.close()
    conn.close()
    conn_test.close()



if __name__ == "__main__":
    main() 