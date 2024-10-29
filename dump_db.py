#! /usr/bin/env python
# -*- coding: utf-8 -*-
#   Gter Copyleft 2020
#   Roberto Marzocchi



import os,sys,re #,shutil,glob
#import time
#import urllib
#import datetime #importo tutta la libreria
from datetime import date



from credenziali import *


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

import logging
path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)



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

import psycopg2

from invio_messaggio import *

schema_da_escludere="backup"

# per sistemi linux funziona sicuro
home=os.getenv("HOME")


logger.info(home)


# Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
nome_db=db
logger.info('Connessione al db {}'.format(nome_db))
conn = psycopg2.connect(dbname=nome_db,
                    port=port,
                    user=user,
                    password=pwd,
                    host=host)


logger.info('Step 0 - Lista DB')
today = date.today()

# dd/mm/YY
d1 = today.strftime("%Y%m%d")



'''
cur = conn.cursor()
'''
curr=conn.cursor()
'''query1="SELECT datname FROM pg_database WHERE datname not in ('template0', 'template1');"
print(query1)
cur.execute(query1)
list_db = cur.fetchall()
'''
list_db=['sit', 'sit_prog']
count=0
for row in list_db:
    #se volessi tutti i db
    # db=row[0]
    nome_db=row
    # questa parte direi che non serve
    query2="REVOKE CONNECT ON DATABASE \"{}\" FROM public;".format(nome_db)
    #print(query2)
    #curr.execute(query2)
    #print(db)
    os.environ['PGPASSWORD'] = pwd # visible in this process + all children
    #export = "export PGPASSWORD=\"mnl1076postgres\""
    #os.system(export)
    
    
    
    if not os.path.exists('{}/backup_db'.format(home)):
            os.makedirs("{}/backup_db".format(home))
    
    if not os.path.exists('{}/backup_db/{}'.format(home,d1)):
            os.makedirs("{}/backup_db/{}".format(home,d1))      
            
    '''
    if count==0:
        os.system('mkdir backup_db/{0}'.format(d1))
    count+=1
    '''
    
    
    home=os.getenv("HOME")
    # faccio il backup del DB
    dump_string='pg_dump -h {3} -U {4} -N {5} -F c {0} -f /{2}/backup_db/{1}/{1}_{0}.backup'.format(nome_db,d1,home, host, user, schema_da_escludere)
    #logger.debug(dump_string)
    ret=os.system(dump_string)
    logger.info('Risultato bakcup del db {}: {}'.format(nome_db,ret))
    
    # solo per il DB del SIT
    if int(today.strftime("%d"))==1 and db =='SIT':
        logger.info ('Oggi è il primo del mese. Faccio anche backup dello schema backup del SIT')
        # insert 
        try:
            move_query='''WITH selection AS (
                        DELETE FROM idea.conferimenti_horus
                        WHERE data_ora_conferimento < (now()::date - interval '3' month)
                        RETURNING *
                    )
                    INSERT INTO backup.idea_conferimenti_storici
                    SELECT * FROM selection;'''
            curr.execute(move_query)
            conn.commit()
            # faccio il backup del solo schema backuo (opzione -n minuscolo fa il contrario di -N)
            dump_string_backup='pg_dump -h {3} -U {4} -n {5} -F c {0} -f /{2}/backup_db/{1}/{1}_{0}_schema_{5}.backup'.format(nome_db,d1,home, host, user, schema_da_escludere)
            ret1=os.system(dump_string_backup)
            logger.info('Risultato backup di {}: {}'.format(schema_da_escludere, ret1))
        except Exception as e:
            logger.error(move_query)
            logger.error(e)


    else:
        logger.info('Non è il primo del mese')
    

'''
cur.close
conn.close
'''
error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
logger.info("chiudo le connessioni in maniera definitiva")


curr.close()
conn.close()