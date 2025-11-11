#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione spazzamento:



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import requests
from requests.exceptions import HTTPError

import json


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

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
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)


# libreria per invio mail
import email, smtplib, ssl
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from invio_messaggio import *

# libreria per scrivere file csv
import csv





    
     

def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
  


    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi_dt=datetime.today()
    oggi=oggi_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    #oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))

        
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    curr1 = conn.cursor()

    #mViews = ['mv_spazzamento_last30dd', 'mv_spazzamento_last60dd']

    mViews = ['mv_spazzamento_last60dd']
    for mv in mViews:


        logger.info('Inizio AGGIORNAMENTO vista etl.{0}'.format(mv))

        ### REFRESH VISTA MATERIALIZZATA POSIZIONE LAST 30 DD ###
        query_refresh = 'REFRESH MATERIALIZED VIEW CONCURRENTLY etl.{0};'.format(mv)
        try:
            curr.execute(query_refresh)
            logger.debug('La vista etl.{0} è stata aggiornata correttamente'.format(mv))
        except Exception as e:
            logger.error(query_refresh)
            logger.error(e)
        conn.commit()

        
        
        
        
        logger.info('Inizio aggiornamento tabella {} su amiugis'.format(mv))
        
        
        
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

        query_dblink2='''create table gps.{0} as 
        select * from dblink('conn_dblink{0}', 'select id, tipo_mezzo, sportello::int, 
        data_ora, data, sweeper_mode, geom from etl.{0};') 
        AS t1(id integer, tipo_mezzo varchar, sportello integer, 
        data_ora timestamp, "data" date, sweeper_mode integer,
        geom geometry(point,4326))'''.format(mv)

        try:
            curr_web.execute(query_dblink2)
        except Exception as e:
            logger.error(query_dblink2)
            logger.error(e)


        # faccio commit
        conn_web.commit()
        
        
        
        query_dblink3='''ALTER TABLE gps.{0} 
        ADD CONSTRAINT {0}_pk PRIMARY KEY ({1})'''.format(mv, 'id')

        try:
            curr_web.execute(query_dblink3)
        except Exception as e:
            logger.error(query_dblink3)
            logger.error(e)

        query_dblink4='''CREATE INDEX {0}_geom_idx
        ON gps.{0}
        USING GIST ({1})'''.format(mv, 'geom')
        
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



        query_dblink8='''drop table if exists gps.{0}_pref'''.format(mv) 

        try:
            curr_web.execute(query_dblink8)
        except Exception as e:
            logger.error(query_dblink8)
            logger.error(e)



        # nella query devo escudere eventuali punti a cavallo fra 2 comuni lo faccio con il group by e l'having
        query_dblink6='''create table gps.{0}_pref as 
        SELECT r.id, r.tipo_mezzo, r.sportello, r.data_ora, r."data", r.sweeper_mode, r.geom, 
        max(c.prefisso_utenti) as prefisso_utenti 
        FROM gps.{0} r
        join gps.v_confini_comuni_area_pref c on st_intersects(r.geom, c.geom)
        group by r.id, r.tipo_mezzo, r.sportello, r.data_ora, r."data", r.sweeper_mode, r.geom
        having count(c.prefisso_utenti)=1
        '''.format(mv)

        try:
            curr_web.execute(query_dblink6)
        except Exception as e:
            logger.error(query_dblink6)
            logger.error(e)
            
        # faccio commit
        conn_web.commit()
        
            
        query_dblink7='''ALTER TABLE gps.{0}_pref 
        ADD CONSTRAINT {0}_pref_pk PRIMARY KEY (id)'''.format(mv)

        try:
            curr_web.execute(query_dblink7)
        except Exception as e:
            logger.error(query_dblink7)
            logger.error(e)

        query_dblink7='''CREATE INDEX {0}_pref_geom_idx
        ON gps.{0}_pref
        USING GIST (geom)'''.format(mv)

        try:
            curr_web.execute(query_dblink7)
        except Exception as e:
            logger.error(query_dblink7)
            logger.error(e)
        

        query_drop='''drop table if exists gps.{0}'''.format(mv) 

        try:
            curr_web.execute(query_drop)
        except Exception as e:
            logger.error(query_drop)
            logger.error(e)
        
        
        # faccio commit
        conn_web.commit()
        
        logger.info('Fine aggiornamento tabella {} su amiugis'.format(mv))
   
    curr_web.close()
    conn_web.close()
    #exit()
    exportKml = 0
    if exportKml != 0:
        data_start = oggi - timedelta(days=30)
        logging.debug('un mese fa era il {}'.format(data_start))

        #data_start = '2024-06-01' #data in cui è iniziata la consuntivazione
        #date_format = '%Y-%m-%d'
        #data_start_ok = datetime.strptime(data_start, date_format)
        #interval = oggi - data_start_ok

        interval = oggi - data_start
        
        logger.debug(interval.days)
        gg=-(interval.days)
        logger.debug(gg)
        while gg <= -1: # arrivo fino a ieri
            day_check=oggi + timedelta(gg)
            

            anno_file=day_check.strftime('%Y')
            mese_file=day_check.strftime('%m')
            giorno_file=day_check.strftime('%Y%m%d')
            #oggi1=datetime.today().strftime('%d/%m/%Y')
            logger.debug(giorno_file)

            
            
            nome_table0='tracce_gps_{}_step0'.format(giorno_file)
            query= '''
                create table etl.{} as 
                select 'Spazzatrice Bucher' as name, concat('Mezzo: ', sportello, ' - Routeid: ', routeid::text) as description, 
                /*'<href>https://maps.google.com/mapfiles/kml/pushpin/red-pushpin.png</href>' as icon,*/
                driverid, data_ora as timestamp, geoloc as geom 
                from spazz_bucher.messaggi m 
                where m.sweeper_mode =1 and data_ora::date= to_date(%s, 'YYYYMMDD')
                union 
                select 'Spazzatrice Schmidt' as name, concat('Mezzo: ', s.equip_id, ' - Routeid: ', routeid::text) as description, 
                /*s.equip_id as sportello, routeid,*/
                /*'<href>https://maps.google.com/mapfiles/kml/pushpin/blue-pushpin.png</href>' as icon,*/
                driverid,  data_ora, geoloc as geom
                from spazz_schmidt.messaggi m
                join spazz_schmidt.serialnumbers s on s.id = m.serialnumber_id 
                where m.sweeper_mode =1 and data_ora::date= to_date(%s, 'YYYYMMDD')'''.format(nome_table0)
            
            
            try:
                curr.execute(query, (giorno_file, giorno_file))
                #lista_causali=currc.fetchall()
            except Exception as e:
                logger.error(query)
                logger.error(e)

            # generazione indice per migliorare le prestazioni
            crea_indice='''CREATE INDEX idx_geom ON etl.{} USING gist (geom);'''.format(nome_table0)

            try:
                curr.execute(crea_indice)
                #lista_causali=currc.fetchall()
            except Exception as e:
                logger.error(crea_indice)
                logger.error(e)
            gg+=1
            conn.commit()
            
            logger.debug('Tabella step 0 e indice creati')

            # step 1 creao tabella con intersect sui comuni (consente di filtrare i dati per comune evitando che tutti vedano tutto)
            nome_table1='tracce_gps_{}_step1'.format(giorno_file)
            query_step1='''create table etl.{} as select c.id as id_comune, c.descrizione as comune, dg.* from etl.{} dg
    join geo.confini_comuni_area c on st_intersects(dg.geom, st_transform(c.geoloc, 4326))'''.format(nome_table1, nome_table0)


            #logger.debug(query_step1)
            try:
                curr.execute(query_step1)
                #lista_causali=currc.fetchall()
            except Exception as e:
                logger.error(query)
                logger.error(e)
            conn.commit()

            logger.debug('Tabella step 1 creata')
            
            query_comuni='''select distinct a.comune, aa.descr_ambito as ambito 
            from etl.{} a 
            join topo.comuni c on c.id_comune=a.id_comune
            join topo.ambiti aa on aa.id_ambito= c.id_ambito'''.format(nome_table1)
            
            try:
                curr.execute(query_comuni)
                lista_comuni=curr.fetchall()
            except Exception as e:
                logger.error(query)
                logger.error(e)
            
            
            
            for c in lista_comuni:
                logger.debug(c[1])
                logger.debug(c[0])
                
                export_folder_step0='{}Posizioni GPS Mezzi'.format(sftpuser_folder)
                
                ie=os.path.exists(export_folder_step0)
                logger.debug(ie)
                if not ie:
                    logger.debug('Entro qua 1')
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder_step0)



                export_folder_step1='{}/Igiene'.format(export_folder_step0) # Igiene
                
                ie=os.path.exists(export_folder_step1)
                logger.debug(ie)
                if not ie:
                    logger.debug('Entro qua 1')
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder_step1)
                    
            
            
            
            
            
                    
                export_folder_step2 = '''{}/{}'''.format(export_folder_step1, c[1]) # ambito
            
                if not os.path.exists(export_folder_step2):
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder_step2)
                    
                    
                    
                export_folder='''{}/{}'''.format(export_folder_step2, c[0]) # comune
                
                if not os.path.exists(export_folder):
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder)
                
                
                export_folder='''{}/{}'''.format(export_folder, anno_file) # anno
                
                if not os.path.exists(export_folder):
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder)
                
                
                export_folder='''{}/{}'''.format(export_folder, mese_file) # mese
                
                if not os.path.exists(export_folder):
                    # Create a new directory because it does not exist
                    os.makedirs(export_folder)
                
                
                #exit()
                
                kml_name='{0}/{1}_posizioni_gps.kml'.format(export_folder,giorno_file)
                
            
            
            
                # windows {0}\\bin\\ogr2ogr.exe             0=qgis_path
                # linux /usr/bin/ogr2ogr
                
                comando='''/usr/bin/ogr2ogr -f "KML" -overwrite "{7}" PG:"host={0} user={1}  dbname={2} password={3} port={4}" -sql "select * from etl.{5} where comune ilike '{6}'"  -nlt CONVERT_TO_LINEAR -nln {8}_tracce_gps'''.format(host, user, nome_db, pwd, port, nome_table1, c[0], kml_name, giorno_file )
                
                
                
                logger.debug(comando)
                ret=os.system(comando)
                
                if ret != 0:
                    logger.error(ret)
                    logger.error('Giorno {} - File {}'.format(giorno_file , kml_name))
                
            # cancello la tabella temporanea
            drop_table0='''drop table etl.{}'''.format(nome_table0)
            drop_table1='''drop table etl.{}'''.format(nome_table1)
            try:
                curr.execute(drop_table0)
                curr.execute(drop_table1)
                #lista_causali=currc.fetchall()
            except Exception as e:
                logger.error(query)
                logger.error(e)
            conn.commit()
        

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      