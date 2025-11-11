#! /usr/bin/env python
# -*- coding: utf-8 -*-
#   Copyleft 2021 Update 2025
#   Roberto Marzocchi, Roberta Fagandini


''''
ATTENZIONE: 

siccome si usa pg_dump che su questo server non è aggiornato, in caso di modifiche bisogna compilare

pyinstaller --onefile dump_db.py

e poi copiare l'eseguibile (dist/dump_db) sul server sitdb 

scp dist/dump_db assterritorio@172.24.4.39:/home/assterritorio/


'''




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


# per sistemi linux funziona sicuro
home=os.getenv("HOME")
logger.info(home)





logger.info('Step 0 - Lista DB')
today = date.today()

# dd/mm/YY
d1 = today.strftime("%Y%m%d")

 
        
        
#cartelle_bkp=['backup_db', 'backup_db_new']        
cartelle_bkp=['backup_db_new']        




for bkp_fld in cartelle_bkp:
    
    # creazione tabelle
    if not os.path.exists('{}/{}'.format(home, bkp_fld)):
            os.makedirs("{}/{}".format(home, bkp_fld))

    if not os.path.exists('{}/{}/{}'.format(home,bkp_fld, d1)):
            os.makedirs("{}/{}/{}".format(home,bkp_fld, d1)) 
    
    
    # definisco i DB DI AMIUPOSTGRES 
     
    list_db={'sit': host, 'sit_prog': host_prog, 'dwh': host_dwh, 'consuntivazione':host_totem}
    schema_da_escludere=["backup", "backup", "", ""]
    i=0
    for d, h in list_db.items():
        #se volessi tutti i db
        nome_db=d
        host_db = h

        # questa parte direi che non serve
        #query2="REVOKE CONNECT ON DATABASE \"{}\" FROM public;".format(nome_db)
        #print(query2)
        #curr.execute(query2)
        #print(db)
        os.environ['PGPASSWORD'] = pwd # visible in this process + all children
        #export = "export PGPASSWORD=\"mnl1076postgres\""
        #os.system(export)
        
        
        # solo per il DB del SIT (Eccezione cablata)
        if int(today.strftime("%d"))==1 and nome_db =='sit':
            logger.info ('Oggi è il primo del mese e sono su sit. Faccio anche backup dello schema backup del SIT')
            # sposto dei dati sul backup
            
            try: 
                # Mi connetto a SIT (PostgreSQL) 
                logger.info ('''Prima di fare il backup sposto un po' di dati pesanti nello schema backup''')
                #nome_db=db
                logger.info('Connessione al db {}'.format(nome_db))
                conn = psycopg2.connect(dbname=nome_db,
                                    port=port,
                                    user=user,
                                    password=pwd,
                                    host=host_db) 
                curr=conn.cursor()

            except Exception as e:
                logger.error(e)
            
            
            
            
            
            try:
                
                logger.info ('''Pulizia conferimenti Id&A''')   
                # questo pezzo non funziona
                       
                move_query='''WITH selection AS (
                            DELETE FROM idea.conferimenti_horus
                            WHERE data_ora_conferimento < (now()::date - interval '3' month)
                            RETURNING *
                        )
                        INSERT INTO backup.idea_conferimenti_storici
                        SELECT * FROM selection'''
                curr.execute(move_query)
                
                # riscrivo così
                """insert_query='''INSERT INTO backup.idea_conferimenti_storici
                        SELECT * from idea.conferimenti_horus
                        WHERE data_ora_conferimento < (now()::date - interval '3' month)'''
                        
                delete_query= '''DELETE FROM idea.conferimenti_horus
                        WHERE data_ora_conferimento < (now()::date - interval '3' month)'''
                
                curr.execute(insert_query) 
                curr.execute(delete_query)
                """
                # essendo dentro un try fa il commit solo se nessuna delle due query va in errore quindi è sicura
                conn.commit()
            except Exception as e:
                logger.error(move_query)
                #logger.error(insert_query)
                #logger.error(delete_query)
                logger.error(e)
            
            
            try:    
                logger.info ('''Pulizia posizioni Tellus''')
                move_query2='''WITH selection AS (
                    DELETE FROM tellus.posizioni
                    WHERE data_ora < (now()::date - interval '61' day)
                    RETURNING *
                )
                INSERT INTO backup.tellus_posizioni
                SELECT * FROM selection'''
                curr.execute(move_query2)
                
                """
                insert_query2= '''INSERT INTO backup.tellus_posizioni
                SELECT * FROM tellus.posizioni
                    WHERE data_ora < (now()::date - interval '61' day)'''
                    
                delete_query2='''DELETE FROM tellus.posizioni
                    WHERE data_ora < (now()::date - interval '61' day)'''
                """
                
                conn.commit()
            except Exception as e:
                logger.error(move_query2)
                #logger.error(insert_query2)
                #logger.error(delete_query2)
                logger.error(e)
            
                
            logger.info('chiudo le connessioni al DB SIT')
            curr.close()
            conn.close()
            try:    
                # faccio il backup del solo schema backup (opzione -n minuscolo fa il contrario di -N)
                logger.info('faccio il backup dello schema backup')
                dump_string_backup='pg_dump -h {3} -U {4} -n {5} -F c {0} -f /{2}/{6}/{1}/{1}_{0}_schema_{5}.backup'.format(nome_db,d1,home, host_db, user, schema_da_escludere[i], bkp_fld)
                ret1=os.system(dump_string_backup)
                logger.info('Risultato backup di {}: {}'.format(schema_da_escludere[i], ret1))
            except Exception as e:
                logger.error(e)


        else:
            logger.info('Non è il primo del mese')
        
                
        '''
        if count==0:
            os.system('mkdir backup_db/{0}'.format(d1))
        count+=1
        '''
        
        # faccio il backup del DB escludendo lo schema da escludere
        

        if schema_da_escludere[i]=='':
            dump_string='pg_dump -h {3} -U {4} -F c {0} -f /{2}/{5}/{1}/{1}_{0}.backup'.format(nome_db, d1, home,
                                                                                                    host_db,
                                                                                                    user,
                                                                                                    bkp_fld)
        else:
            dump_string='pg_dump -h {3} -U {4} -N {5} -F c {0} -f /{2}/{6}/{1}/{1}_{0}.backup'.format(nome_db, d1, home,
                                                                                                        host_db,
                                                                                                        user,
                                                                                                        schema_da_escludere[i], 
                                                                                                        bkp_fld)
        #logger.debug(dump_string)
        ret=os.system(dump_string)
        logger.info('Risultato backup del db {}: {}'.format(nome_db,ret))
        
        
        
        i+=1

    schema_da_escludere=""
    
    
    
    # definisco i DB DI AMIUGIS 
    
    logger.info('Inizio il backup dei DB sul server {}'.format(host_amiugis))
    list_db=['lizmap_mappe', 'lizmap_dwh', 'lizmap_mappenew3', "api_db"]
    schema_da_escludere=['', '', '', ''] # array dove per ogni DB metto eventuali schemi da escludere
    i=0
    for row in list_db:
        
        # qua devo metterci la password di amiugis 
        os.environ['PGPASSWORD'] = pwd_webroot # visible in this process + all children
        
        
        # faccio il backup del DB escludendo lo schema da escludere
        if schema_da_escludere[i]=='':
            dump_string='pg_dump -h {3} -U {4} -F c {0} -f /{2}/{5}/{1}/{1}_{0}.backup'.format(row, d1, home,
                                                                                                    host_amiugis,
                                                                                                    user_webroot,
                                                                                                    bkp_fld)
        else:
            dump_string='pg_dump -h {3} -U {4} -N {5} -F c {0} -f /{2}/{6}/{1}/{1}_{0}.backup'.format(row, d1, home,
                                                                                                        host_amiugis,
                                                                                                        user_webroot,
                                                                                                        schema_da_escludere[i],
                                                                                                        bkp_fld)
        #logger.debug(dump_string)
        ret=os.system(dump_string)
        logger.info('Risultato bakcup del db {}: {}'.format(row,ret))
        i+=1








error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
logger.info("chiudo le connessioni in maniera definitiva")


