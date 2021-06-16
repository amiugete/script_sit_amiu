#!/usr/bin/env python
#  originally developed by Rossella Ambrosino and Roberto Marzocchi 2021 (Gter)

# modified by Roberto Marzocchi (AMIU)



import requests
import json
import os
from credenziali import *
import logging
logging.basicConfig(
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filename='log/download_OSM.log',   #mancano permessi
    level=logging.INFO)

logging.info('*'*20 + ' NUOVA ESECUZIONE ' + '*'*20)


#query sul database OSM per estrazione del tag highway 
overpass_url = "http://overpass-api.de/api/interpreter"

#e="10.646374350606367" n="43.969977647278014" s="43.74798936510469" w="10.288957076844582

'''
Estensione GENOVA
w 8.6673965305308442,  s 44.3785184681189833 : e 9.0956375982374134,  n 44.5198453628836077


'''


# order is s, w, n, e
overpass_query_old = """
[timeout:900][maxsize:1073741824][out:xml];
(node["highway"="*"](43.74798936510469, 10.288957076844582, 43.969977647278014,10.646374350606367);
way["highway"="*"](43.74798936510469, 10.288957076844582, 43.969977647278014,10.646374350606367);
rel["highway"="*"](43.74798936510469, 10.288957076844582, 43.969977647278014,10.646374350606367);
);
(._;>;);
out meta;
"""
overpass_query ='''
<osm-script output="xml" timeout="25">
    <union>
        <query type="node">
            <has-kv k="highway"/>
            <bbox-query e="9.0956375982374134" n="44.5198453628836077" s="44.3785184681189833" w="8.6673965305308442"/>
        </query>
        <query type="way">
            <has-kv k="highway"/>
            <bbox-query e="9.0956375982374134" n="44.5198453628836077" s="44.3785184681189833" w="8.6673965305308442"/>
        </query>
        <query type="relation">
            <has-kv k="highway"/>
            <bbox-query e="9.0956375982374134" n="44.5198453628836077" s="44.3785184681189833" w="8.6673965305308442"/>
        </query>
    </union>
    <union>
        <item/>
        <recurse type="down"/>
    </union>
    <print mode="body"/>
</osm-script>
'''


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
osm_file='genova.osm'
with open(osm_file, "w") as file:
    file.write(data)
file.close()


'''
logging.info('osm 2 pgrouting')
#Import in Postgres del file data.osm
p = """osm2pgrouting -f {0} -h {1} -U {2} -d {3} -p {4} -W {5}  --schema {6} --conf={7}""".format(osm_file,
                                                                                                  host,
                                                                                                  user,
                                                                                                  dbname,
                                                                                                  port,
                                                                                                  password,
                                                                                                  schema,
                                                                                                  conf)
 #"""osm2pgrouting -f data.osm -h localhost -U postgres -d city_routing -p 5432 -W postgresnpwd  --schema network --conf=/usr/share/osm2pgrouting/mapconfig_rail.xml"""
  
os.system(p)
'''


logging.info('*'*20 + ' ESCO NORMALMENTE' + '*'*20) 