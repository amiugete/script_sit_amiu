#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script elimina le piazzole composte da solo un elemento che non deve essere in piazzola (cestini raccolta carta) 
'''



import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/pulizia_elementi_piazzole.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)





def indice(val, array):
    '''
    Cerco l'indice all'interno di un array
    '''
    ind = -1
    i=0
    while i < len(array):
        if array[i]==val:
            ind=i
        i+=1
    if ind == -1:
        logging.error('Il valore {} non è incluso nell\'array {}'.format(val, array))
    return ind


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

    select='''
        select e.id_elemento, e.tipo_elemento, e.id_asta, e.id_piazzola, e.riferimento, e.note
        from elem.elementi e 
        join elem.piazzole p 
        on p.id_piazzola = e.id_piazzola 
        where tipo_elemento in (122, 135, 132, 133, 137, 138, 136,131, 16,134)
        and e.id_piazzola is not null 
        and (p.data_eliminazione is null or p.data_eliminazione > now())
        order by tipo_elemento 
    '''
    

    id_tabella=[122, 135, 132, 133, 137, 138, 136,131, 16,134]

    tabella=['cestini','athenaalter', 'athenaet', 'athenaidem','athenateseo4','heritagesquare', 'metalco','miniplaza', 'sabaudo110', 'sabaudo55']



    try:
	    curr.execute(select)
	    lista_elementi=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #piazzola=[]
    #count=[]

    #curr1=conn.cursor()
    for pp in lista_elementi:
        # check piazzola per capire se elominarla
        sel_piazzola= '''select count(distinct tipo_elemento) from elem.elementi where id_piazzola = {}'''.format(pp[3])
        curr1 = conn.cursor()
        try:
	        curr1.execute(sel_piazzola)
	        lista_piazzole=curr1.fetchall()
        except Exception as e:
            logging.error(e)
        for cc in lista_piazzole:
            if cc[0]>1:
                logging.warning('Piazzola {} non eliminata'.format(pp[3]))
            else:
                #eliminare piazzola
                query3='''update elem.piazzole set data_eliminazione = now() where id_piazzola ={}'''.format(pp[3])
                curr3 = conn.cursor()
                try:
                    curr3.execute(query3)
                except Exception as e:
                    logging.error(e)
                    logging.error(query3)
                curr3.close()
        curr1.close()

        # ora devo vedere se quell'elemento c'è già oppure no e poi nel caso in cui non ci sia crearlo laddove c'è la piazzola 
        # (se gli elementi erano 2 li distanzio leggermente)
        kk=indice(pp[1], id_tabella)

        query='''select * from geo.{} 
        where id={}'''.format(tabella[kk],pp[0])

        id_elemento=0
        curr2=conn.cursor()
        try:
	        curr2.execute(query)
	        elemento=curr2.fetchall()
        except Exception as e:
            logging.error(e)
        for ee in elemento:
            id_elemento=ee[0]
        if id_elemento==0:
            #logging.info('Elemento da aggiungere')
            query4='''insert into geo.{0} (id, geoloc) values ({1}, (select geoloc from geo.piazzola where id = {2}))'''.format(tabella[kk], pp[0], pp[3])
            #logging.debug(query4)
            curr4 = conn.cursor()
            try:
                curr4.execute(query4)
            except Exception as e:
                logging.error(e)
                logging.error(query4)
            curr4.close()
            query5='''update elem.elementi set 
                    riferimento = (select riferimento from elem.piazzole where id_piazzola = {0}),
                    note =(select note from elem.piazzole where id_piazzola = {0})
                    where id_elemento = {1}'''.format(pp[3], pp[0])
            #logging.debug(query5)
            curr5 = conn.cursor()
            try:
                curr5.execute(query5)
            except Exception as e:
                logging.error(e)
                logging.error(query5)
            curr5.close()

        else: 
            logging.info('Id {}= {}'.format(tabella[kk],id_elemento))
        #logging.debug(pp[0])
        curr2.close()
        #exit()

    curr.close()
    conn.close()
if __name__ == "__main__":
    main()     