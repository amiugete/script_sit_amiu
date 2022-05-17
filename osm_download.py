#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Getting started from a Gter Script 2020/2021
# Rossella Ambrosino, Roberta Fagandini e Roberto Marzocchi


# AMIU copyleft 2021
# reviwed by Roberto Marzocchi



import sys
import requests
import json
import os
import psycopg2
from credenziali  import *


import logging


path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/download_grafo_osm.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)



logging.info('*'*20 + ' NUOVA ESECUZIONE ' + '*'*20)


#query sul database OSM per estrazione del tag highway 
overpass_url = "http://overpass-api.de/api/interpreter"

#e="10.646374350606367" n="43.969977647278014" s="43.74798936510469" w="10.288957076844582
# order is s, w, n, e

comune='Genova'
osm_file='{}/osm_file/{}.osm'.format(path, comune)

logging.info('Connessione al db')
conn = psycopg2.connect(dbname=db,
                    port=port,
                    user=user,
                    password=pwd,
                    host=host)

curr = conn.cursor()
conn.autocommit = True
bbox_query='''select 
st_xmin(st_extent(st_transform(geoloc,4326))),
st_xmax(st_extent(st_transform(geoloc,4326))),
st_ymin(st_extent(st_transform(geoloc,4326))),
st_ymax(st_extent(st_transform(geoloc,4326)))
from geo.confini_comuni_area cca 
where descrizione ilike '{}';'''.format(comune)

try:
    curr.execute(bbox_query)
    bbox=curr.fetchall()
except Exception as e:
    logging.error(e)


#inizializzo gli array
#piazzola=[]
#count=[]

#curr1=conn.cursor()
for pp in bbox:
    w=pp[0]
    e=pp[1]
    s=pp[2]
    n=pp[3]




##########################################################

# metodo con il geocoded area

# test='''{{{{geocodeArea:{0}}}}}'''.format(comune)
# logging.debug(test)
# overpass_query ='''
# <osm-script output="xml" timeout="25">
# <id-query {{{{geocodeArea:{0}}}}} into="area_0"/>
#     <union>
#         <query type="node">
#             <has-kv k="highway"/>
#             <area-query from="area_0"/>
#         </query>
#         <query type="way">
#             <has-kv k="highway"/>
#             <area-query from="area_0"/>
#         </query>
#         <query type="relation">
#             <has-kv k="highway"/>
#             <area-query from="area_0"/>
#         </query>
#     </union>
#     <union>
#         <item/>
#         <recurse type="down"/>
#     </union>
#     <print mode="body"/>
# </osm-script>
# '''.format(comune)

##########################################################

bbox='<bbox-query e="{0}" n="{1}" s="{2}" w="{3}"/>'.format(e,n,s,w)
logging.debug(bbox)

overpass_query ='''
<osm-script output="xml" timeout="25">
    <union>
        <query type="node">
            <has-kv k="highway"/>
            {0}
        </query>
        <query type="way">
            <has-kv k="highway"/>
            {0}
        </query>
        <query type="relation">
            <has-kv k="highway"/>
            {0}
        </query>
    </union>
    <union>
        <item/>
        <recurse type="down"/>
    </union>
    <print mode="body"/>
</osm-script>
'''.format(bbox)




# overpass_query ='''
# <osm-script output="xml" timeout="25">
#     <union>
#         <query type="node">
#             <has-kv k="highway"/>
#             <bbox-query e="10.646374350606367" n="43.969977647278014" s="43.74798936510469" w="10.288957076844582"/>
#         </query>
#         <query type="way">
#             <has-kv k="highway"/>
#             <bbox-query e="10.646374350606367" n="43.969977647278014" s="43.74798936510469" w="10.288957076844582"/>
#         </query>
#         <query type="relation">
#             <has-kv k="highway"/>
#             <bbox-query e="10.646374350606367" n="43.969977647278014" s="43.74798936510469" w="10.288957076844582"/>
#         </query>
#     </union>
#     <union>
#         <item/>
#         <recurse type="down"/>
#     </union>
#     <print mode="body"/>
# </osm-script>
# '''

logging.info('Lancio query')
response = requests.get(overpass_url, params={'data': overpass_query})
                        
if response.ok:
    logging.info('Query eseguita con successo!')
else:
    logging.error('Query fallita!')
    logging.error(response)
    os._exit(1)

                        
logging.info('Recupero dati')
try:                      
    data = response.text
except:
    logging.error('Recupero dati fallito')
    os._exit(1)

    
#scrive il risultato della query su un file data.osm
logging.info('Scrivo file .osm')
with open(osm_file, "w") as file:
    file.write(data)
file.close()


logging.info('osm 2 pgrouting')
#Import in Postgres del file data.osm
p = """osm2pgrouting -f {0} -h {1} -U {2} -d {3} -p {4} -W {5}  --schema {6} --conf={7} --clean""".format(osm_file,
                                                                                                  'localhost',
                                                                                                  user_pgrouting,
                                                                                                  db_pgrouting,
                                                                                                  port,
                                                                                                  pwd_pgrouting,
                                                                                                  'network',
                                                                                                  '{}/osm_import/mapconfig_for_cars.xml'.format(path))
 #"""osm2pgrouting -f data.osm -h localhost -U postgres -d city_routing -p 5432 -W postgresnpwd  --schema network --conf=/usr/share/osm2pgrouting/mapconfig_rail.xml"""
  
os.system(p)



logging.info('*'*20 + ' ESCO NORMALMENTE' + '*'*20) 