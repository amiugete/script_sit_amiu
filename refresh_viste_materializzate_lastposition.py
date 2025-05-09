#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberta Fagandini

'''
Script per aggiornare le viste materializzate con l'ultima posizione
dei mezzi di raccolta e spazzamento (DB SIT etl.mv_spazzamento_last24 e etl.mv_raccolta_last24)
'''

import os, sys
import psycopg2
import logging

from credenziali import *

from invio_messaggio import *


path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

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


def move_mv_amiugis(logger, mv, gid, geom):
    '''Questa funzione che chiamo anche da altre parti sposta una vista materializzata su amiugis 
    Input:
    - logger
    - vista materializzata
     
    
    '''
    conn_web = psycopg2.connect(dbname=db_web,
                        port=port,
                        user=user_webroot,
                        password=pwd_webroot,
                        host=host_amiugis)
    
    curr_web = conn_web.cursor()
    # ora creo la tabella su amiugis per questioni di performance
    query_dblink='''select dblink_connect('conn_dblink{0}', 'sit')'''.format(mv)
    try:
        curr_web.execute(query_dblink)
    except Exception as e:
        logger.error(query_dblink)
        logger.error(e)
    
    
    
    query_dblink1='''drop table if exists gps.{0}'''.format(mv) 

    try:
        curr_web.execute(query_dblink1)
    except Exception as e:
        logger.error(query_dblink1)
        logger.error(e)

    if 'spazzamento' in mv:

        query_dblink2='''create table gps.{0} as 
        select * from dblink('conn_dblink{0}', '
        SELECT id, tipo_mezzo, sportello::int, sweeper_mode, 
        data_ora, geom, id_comune,
        comune, prefisso_utenti 
        FROM etl.{0};') as t1
        (id integer, tipo_mezzo varchar, sportello integer, sweeper_mode integer,
        data_ora timestamp, geom geometry(point,4326) ,
        id_comune int, comune varchar, prefisso_utenti varchar)'''.format(mv)

    else:
        logger.debug('sono qua')
        query_dblink2='''create table gps.{0} as 
        select * from dblink('conn_dblink{0}', '
        SELECT id, tipo_mezzo, sportello::int,
        data_ora, geom, id_comune,
        comune, prefisso_utenti 
        FROM etl.{0};') as t1
        (id integer, tipo_mezzo varchar, sportello integer, 
        data_ora timestamp, geom geometry(point,4326) ,
        id_comune int, comune varchar, prefisso_utenti varchar)'''.format(mv)

    try:
        curr_web.execute(query_dblink2)
    except Exception as e:
        logger.error(query_dblink2)
        logger.error(e)
        
    query_dblink3='''ALTER TABLE gps.{0} 
    ADD CONSTRAINT {0}_pk PRIMARY KEY ({1})'''.format(mv,gid)

    try:
        curr_web.execute(query_dblink3)
    except Exception as e:
        logger.error(query_dblink3)
        logger.error(e)

    query_dblink4='''CREATE INDEX {0}_geom_idx
    ON gps.{0}
    USING GIST ({1})'''.format(mv, geom)
    
    try:
        curr_web.execute(query_dblink4)
    except Exception as e:
        logger.error(query_dblink4)
        logger.error(e)
        
    query_dblink5='''select dblink_disconnect('conn_dblink{0}')'''.format(mv)
    
    try:
        curr_web.execute(query_dblink5)
    except Exception as e:
        logger.error(query_dblink5)
        logger.error(e)
        
        
    # faccio commit
    conn_web.commit()
    
    
    # CHIUSURA connessione
    curr_web.close()
    conn_web.close()
    
    
    
    
    
    
    
    

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # Mi connetto a SIT (PostgreSQL)
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    
    

    mViews = ['mv_spazzamento_last24com','mv_raccolta_last24com']

    for mv in mViews:
        logger.info('Iizio refresh vista materializzata {}'.format(mv))
        
        query = 'REFRESH MATERIALIZED VIEW CONCURRENTLY etl.{};'. format(mv)

        try:
            curr.execute(query)
            logger.info('La vista {} è stata aggiornata correttamente'.format(mv))
        except Exception as e:
            logger.error(query)
            logger.error(e)
        conn.commit()

        logger.info('Mi connetto tramite DBLINK e lavoro su amiugis')

        move_mv_amiugis(logger, mv, 'sportello', 'geom')
        
        logger.info('Fine aggiornamento tabella {} su amiugis'.format(mv));
    
    curr.close()
    conn.close()
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)


if __name__ == "__main__":
    main()   