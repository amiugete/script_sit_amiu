#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script importa i dati dai geopackage caricati sul cloud di MERGIN al DB PostGIS

Lavora su:

- annotazioni 
- installazioni

Si riferisce al progetto denominato installazioni_bilaterali_genova 

'''


import os, sys, getopt, re  # ,shutil,glob
from credenziali import *


import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/installazione_bilaterali_genova.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)




from osgeo import ogr, gdal

PG_CONN = {
    'user': os.getenv(user),
    'password': os.getenv(pwd),
    'port': os.getenv(port),
    'host': os.getenv(host),
    'db': os.getenv(db_prog),
}

def pg_to_gpkg(tablename):
    """Processes the requests to generate geopackages
    Args:
        tablename (text): table id
    """

    connString = f"PG: db='{PG_CONN['db']}' host='{PG_CONN['host']}' user='{PG_CONN['user']}' password='{PG_CONN['password']}'"

    conn = ogr.Open(connString)

    gdal.SetConfigOption('PG_USE_COPY', 'YES')

    print(f"Processing {tablename}")

    # # get the data
    sql = f"SELECT * FROM {tablename}"
    ogr_lyr = conn.ExecuteSQL(sql)
    print(f"Found {len(ogr_lyr)} elements")

    # # generate filename and dir
    curdir = os.path.dirname(os.path.abspath(__file__))
    exportdir = os.path.join(curdir, 'export')
    filename = f"{tablename}.gpkg"

    if not os.path.exists(exportdir):
        os.makedirs(exportdir)

    out_file = os.path.join(exportdir, filename)

    # # generate the geopackage
    _options = gdal.VectorTranslateOptions(
        **{'layerName': tablename, 'SQLStatement': sql, 'format': 'GPKG'}
    )

    if len(ogr_lyr) > 0:
        if os.path.exists(out_file):
            os.remove(out_file)
        src_ds = gdal.OpenEx(connString, gdal.OF_VECTOR)
        gdal.VectorTranslate(out_file, src_ds, options=_options)
    return


def main():
    
    logging.info('Leggo gli input')
    try:
        opts, args = getopt.getopt(argv,"hm:",["mail="])
    except getopt.GetoptError:
        logging.error('progettazione_locale.py -i <input sqlite3 file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('progettazione_locale.py -i <input sqlite3 file> ')
            sys.exit()
        elif opt in ("-i", "--input"):
            sqlite_file = arg
            logging.info('Geopackage file = {}'.format(mail))
    
    
    logging.info('Connessione al db PostgreSQL')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
    
    
    curr = conn.cursor()
    conn.autocommit = True
    
    logging.info('Connessione al GeoPackage')
    con = sqlite3.connect(sqlite_file)
    cur = con.cursor()
    for row in cur.execute('SELECT * FROM note_installazione ORDER BY update_time'):
        print(row[1])
    
    
    curr.close()
    conn.close()

    
    
    
    

if __name__ == "__main__":
    main()  
