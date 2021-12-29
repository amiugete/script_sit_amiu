#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
sTAMPA FILTRO PER MUNICIPI SU MAPPA LIZMAP
'''


import os,sys, getopt
import inspect, os.path



import psycopg2

import datetime

from urllib.request import urlopen
import urllib.parse

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from credenziali import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')


logfile='{}/log/{}mappa_lizmap.log'.format(path, giorno_file)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)




def main():




    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True




    url_map='https://amiugis.amiu.genova.it/mappe/lizmap/www/index.php/view/map/'
    repository='repo1'
    project='piazzole_dwh'
    epsg=3857
    crs='EPSG:{}'.format(epsg)

    query3 = '''select id, nome_municipio, 
replace(replace(replace(st_extent(st_transform(geom,{}))::text,'BOX(',''),')',''),' ',',')
from geo.municipi_area_comune mac 
group by id, nome_municipio'''.format(epsg)

    print(query3)
    curr3 = conn.cursor()


    try:
        curr3.execute(query3)
        lista_ut2=curr3.fetchall()
    except Exception as e:
        logging.error(e)

    for uu in lista_ut2:

        params={ 'repository' : repository,
                    'project': project,
                    #'layers': 'BTTT',
                    #'layers': 'v_piazzole_dwh',
                    #'layerid':'v_piazzole_dwh_d8c75ba3_ed30_4159_9832_44336d033e65',
                    'bbox': uu[2],
                    'crs':crs,
                    #'filter': '''v_piazzole_dwh+:+"municipio"+ILIKE+'{}'+'''.format(uu[1])
                    #'typename': 'v_piazzole_dwh',
                    'filter': '''v_piazzole_dwh:"municipio" ILIKE '%s' ''' %(uu[1])
                    }
                
        url_2 = urllib.parse.urlencode(params)
        url_ok = '{}?{}'.format(url_map, url_2)
        print(uu[0])
        print(url_ok)

    curr.close()





if __name__ == "__main__":
    main()   