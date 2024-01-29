#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge il grafo temporanea 
'''

import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import psycopg2


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/grafo.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)



########################################################
# input

#comune='recco'
#ut=2032
#quartiere=2037
#via_senzanome=200001

comune='sori'
ut=189
quartiere=2037
via_senzanome=650001



########################################################


def main():
    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True


    '''
    Faccio una select in cui 
    
    - splitto tutte le multilinestring in linestring (laddove lo sono), 
    - cerco un id da dare alle asteci sommo il max(id_asta)
    - calcolo la lunghezza

    e in generale recupero tutte le informazioni da aggiungere sulle tabelle che costituiscono il grafo
    '''



    query='''select (row_number() OVER (order by osm_id, (st_dump(st_transform(geom , 3003))).geom)) + (select max(id_asta) from elem.aste a) as  id_asta,
			case 
			when id_via is not null then id_via
			else {}
			end id_via, osm_id, gr.maxwidth, tho.transitabilita, 
			--ST_LineMerge(
        	--ST_SnapToGrid(st_transform(geom , 3003),0.001)) as geom, 
            (st_dump(st_transform(geom , 3003))).geom as geom,
			round(st_length((st_dump(st_transform(geom , 3003))).geom)) as length_m
            from marzocchir.grafo_{} gr 
            join marzocchir.transitabilita_highway_osm tho 
            on tho.highway_osm = gr.highway  '''.format(via_senzanome, comune)



    try:
        curr.execute(query)
        lista_aste=curr.fetchall()
    except Exception as e:
        logging.error(e)
    for aa in lista_aste:
        id_asta=aa[0]
        id_via=aa[1]
        check_update=0
        try:
            larghezza=float(aa[3])*100
            check_update=1
        except:
            logging.debug('Larghezza non specificata')
        osm_id=aa[2]
        transitabilita=aa[4]
        geom=aa[5]
        lunghezza=aa[6]

        

        curr2 = conn.cursor()

        insert_attrib= '''INSERT INTO elem.aste
            (id_asta, id_via, num_seq, lung_asta, larg_asta,
            lung_marc_d, larg_marc_d, alt_marc_d, lung_marc_p, larg_marc_p, alt_marc_p, lung_port_d, larg_port_d, lung_port_p, larg_port_p, 
            lung_ped_d, larg_ped_d, lung_ped_p, larg_ped_p,
            portata, freq_mercati, freq_fiere, 
            id_ut, id_quartiere, id_circoscrizione, 
            id_tipologia_asta, id_transitabilita,
            id_uu, area_porto, cod_nodo1, cod_nodo2,
            verso_asta, senso_marcia_12, senso_marcia_21,
            semaforo_nodo1, semaforo_nodo2, cod_comune, cod_traffico_12, cod_traffico_21, old_idelem)
            VALUES({5}, {0}, null, {1}, null,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
            null, null, null,
            {2}, {3}, null,
            null, {4},
            null, null, null, null,
            12, 1, 1,
            null, null, null, null, null, null);'''.format(id_via,lunghezza, ut, quartiere, transitabilita, id_asta)

        logging.debug(insert_attrib)
        curr2.execute(insert_attrib)
        curr2.close()


        curr1 = conn.cursor()
        if osm_id is None:
            insert_geom='''INSERT INTO geo.grafostradale
            (id, geoloc)
            VALUES({0}, '{1}');'''.format(id_asta, geom)
        else:
            insert_geom='''INSERT INTO geo.grafostradale
            (id, geoloc, osm_id)
            VALUES({0}, '{1}', {2});'''.format(id_asta, geom, osm_id)

        logging.debug(insert_geom)
        curr1.execute(insert_geom)
        curr1.close()
        
        
        if check_update==1:
            curr3 = conn.cursor()
            update_attrib='''update elem.aste 
            set larg_asta={} 
            where id_asta= {}'''.format(larghezza, id_asta)
            logging.debug(update_attrib)
            curr3.execute(update_attrib)
            curr3.close()

        
        ########################################################################################
        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
        #conn.commit()
        ########################################################################################
        
        #exit()


if __name__ == "__main__":
    main()   