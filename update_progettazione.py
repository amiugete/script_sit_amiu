#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge 2 file csv:
1)  piazzole da rendere bilaterali
    1.a eliminare elementi non privati che sono presenti
    1.b aggiungere elementi bilaterali

2) piazzole da eliminare (aggiunge data di eliminazione)

'''

import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

import csv

import psycopg2


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *


#import requests

import logging

path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/bilaterali_progettazione.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)



########################################################
# input
file_csv= '{}/bilaterali/progettazione_bilaterale.csv'.format(path)
file_delete_csv= '{}/bilaterali/eliminare.csv'.format(path)
file_legenda_csv= '{}/bilaterali/legenda.csv'.format(path)

########################################################


def main():
    
    
    # leggo il file CSV


    piazzola_del=[]
    with open(file_delete_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.debug(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                piazzola_del.append(row[0])
                line_count += 1
        logging.debug('Processed {} lines.'.format(line_count))


    piazzola_l=[]
    fatto_l=[]
    tipo_l=[]
    grandezza=[]
    with open(file_legenda_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.debug(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                piazzola_l.append(row[0])
                fatto_l.append(row[1])
                tipo_l.append(row[2])
                grandezza.append(row[3])
                line_count += 1
        logging.debug('Processed {} lines.'.format(line_count))


    

        
    
    # leggo il file CSV


    piazzola=[]
    rsu=[]
    carta=[]
    multi=[]
    umido=[]
    n_rsu=[]
    n_carta=[]
    n_multi=[]
    n_umido=[]
    with open(file_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.debug(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                piazzola.append(row[0])
                rsu.append(row[1])
                carta.append(row[2])
                multi.append(row[3])
                umido.append(row[4])
                n_rsu.append(float(row[5]))
                n_carta.append(float(row[6]))
                n_multi.append(float(row[7]))
                n_umido.append(float(row[8]))
                line_count += 1
        logging.debug('Processed {} lines.'.format(line_count))
    
    
    
    
    
    
    # 
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    
    



    logging.info('Inizio fase 0')
    logging.info(len(piazzola_l))
    logging.info(len(grandezza))

    i=0
    while i < len(piazzola_l):
        query = 'update elem.piazzole '
        if  fatto_l[i]=='Bilaterali':
            if tipo_l[i] == 'L':
                if grandezza[i]=='3700':
                    query= "{} set prog = 'TLB'".format(query)
                else:
                    query= "{} set prog = 'TLR'".format(query)
            elif tipo_l[i] == 'P' or tipo_l[i] == 'NO':
                if grandezza[i]=='3700':
                    query= "{} set prog = 'TPB'".format(query)
                else:
                    query= "{} set prog = 'TPR'".format(query)
        elif fatto_l[i]=='Eliminate':
            if tipo_l[i] == 'L':
               query= "{} set prog = 'CL',  motivazione ='R'".format(query) 
            elif tipo_l[i] == 'P' or tipo_l[i] == 'NO':
                query= "{} set prog = 'CP' ,  motivazione ='R'".format(query)
        else:
            query='{} set prog = NULL '.format(query)
        query='{} where id_piazzola = {} '.format(query, piazzola_l[i])
        logging.debug(query)
        curr.execute(query)
        i+=1

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn.commit()
    ########################################################################################

    
    exit()

    logging.info('Inizio fase 1')
    i=0
    while i < len(piazzola_del):
        query = '''update elem.piazzole set data_eliminazione = now()
        where id_piazzola = {} '''.format(piazzola_del[i])
        curr.execute(query)
        i+=1

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    #conn.commit()
    ########################################################################################
    
    curr.close()
    #exit()
    logging.info('Inizio fase 2')

    curr = conn.cursor()
    i=0
    while i < len(piazzola):
        select ='''from elem.elementi e 
        where tipo_elemento in (
        select tipo_elemento from elem.tipi_elemento te 
        where tipo_rifiuto in (1,3,4,5)
        ) and id_elemento not in (
        select id_elemento from elem.elementi_privati ep )
        and id_piazzola ={}'''.format(piazzola[i])
        query1='''delete from elem.elementi_aste_percorso 
        where id_elemento in (select id_elemento {})'''.format(select)
        query2='''delete {}'''.format(select)
        logging.debug(query1)
        curr.execute(query1)
        logging.debug(query2)
        curr.execute(query2)


        query_type_rsu='''select tipo_elemento, percent_riempimento, freq_stimata from elem.tipi_elemento 
        where tipologia_elemento IN ('B', 'R') 
        and tipo_rifiuto=1
        and volume={}'''.format(rsu[i])
        curr1=conn.cursor()
        try:
            curr1.execute(query_type_rsu)
            tipi_rsu=curr1.fetchall()
        except Exception as e:
            logging.error(e)
        for aa in tipi_rsu:
            tipo_rsu=aa[0]
            percent_riempimento = aa[1]
            freq_stimata=aa[2]
        curr1.close()
        k=0
        while k<n_rsu[i]:
            query_rsu= '''INSERT INTO elem.elementi
                        (tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
                         id_cliente, posizione, dimensione, privato, peso_reale, 
                         peso_stimato, numero_civico_old, riferimento, coord_lat, coord_long, 
                         id_utenza, nome_attivita, modificato_da, data_ultima_modifica, percent_riempimento,
                         x_id_elemento_privato, freq_stimata, numero_civico, lettera_civico, colore_civico, note)
                        VALUES ({0}, {1}, NULL, (select id_asta from elem.piazzole where id_piazzola={1}), NULL,
                         '-1'::integer, NULL, NULL, 0, 0,
                          0, NULL, NULL, NULL, NULL,
                           '-1'::integer,  NULL, NULL, NULL, {2},
                            NULL, {3}, NULL, NULL, NULL, NULL);'''.format(tipo_rsu, piazzola[i], percent_riempimento, freq_stimata)
            curr.execute(query_rsu)
            k+=1


        query_type_carta='''select tipo_elemento, percent_riempimento, freq_stimata from elem.tipi_elemento 
        where tipologia_elemento IN ('B', 'R') 
        and tipo_rifiuto=3
        and volume={}'''.format(carta[i])
        curr1=conn.cursor()
        try:
            curr1.execute(query_type_carta)
            tipi_carta=curr1.fetchall()
        except Exception as e:
            logging.error(e)
        for aa in tipi_carta:
            tipo_carta=aa[0]
            percent_riempimento = aa[1]
            freq_stimata=aa[2]
        curr1.close()
        k=0
        while k<n_carta[i]:
            query_carta= '''INSERT INTO elem.elementi
                        (tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
                         id_cliente, posizione, dimensione, privato, peso_reale, 
                         peso_stimato, numero_civico_old, riferimento, coord_lat, coord_long, 
                         id_utenza, nome_attivita, modificato_da, data_ultima_modifica, percent_riempimento,
                         x_id_elemento_privato, freq_stimata, numero_civico, lettera_civico, colore_civico, note)
                        VALUES ({0}, {1}, NULL, (select id_asta from elem.piazzole where id_piazzola={1}), NULL,
                         '-1'::integer, NULL, NULL, 0, 0,
                          0, NULL, NULL, NULL, NULL,
                           '-1'::integer,  NULL, NULL, NULL, {2},
                            NULL, {3}, NULL, NULL, NULL, NULL);'''.format(tipo_carta, piazzola[i], percent_riempimento, freq_stimata)
            curr.execute(query_carta)
            k+=1



        query_type_multi='''select tipo_elemento, percent_riempimento, freq_stimata from elem.tipi_elemento 
        where tipologia_elemento IN ('B', 'R') 
        and tipo_rifiuto=4
        and volume={}'''.format(multi[i])
        curr1=conn.cursor()
        try:
            curr1.execute(query_type_multi)
            tipi_multi=curr1.fetchall()
        except Exception as e:
            logging.error(e)
        for aa in tipi_multi:
            tipo_multi=aa[0]
            percent_riempimento = aa[1]
            freq_stimata=aa[2]
        curr1.close()
        k=0
        while k<n_multi[i]:
            query_multi= '''INSERT INTO elem.elementi
                        (tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
                         id_cliente, posizione, dimensione, privato, peso_reale, 
                         peso_stimato, numero_civico_old, riferimento, coord_lat, coord_long, 
                         id_utenza, nome_attivita, modificato_da, data_ultima_modifica, percent_riempimento,
                         x_id_elemento_privato, freq_stimata, numero_civico, lettera_civico, colore_civico, note)
                        VALUES ({0}, {1}, NULL, (select id_asta from elem.piazzole where id_piazzola={1}), NULL,
                         '-1'::integer, NULL, NULL, 0, 0,
                          0, NULL, NULL, NULL, NULL,
                           '-1'::integer,  NULL, NULL, NULL, {2},
                            NULL, {3}, NULL, NULL, NULL, NULL);'''.format(tipo_multi, piazzola[i], percent_riempimento, freq_stimata)
            curr.execute(query_multi)
            k+=1


        query_type_umido='''select tipo_elemento, percent_riempimento, freq_stimata from elem.tipi_elemento 
        where tipologia_elemento IN ('B', 'R') 
        and tipo_rifiuto=5
        and volume={}'''.format(umido[i])
        curr1=conn.cursor()
        try:
            curr1.execute(query_type_umido)
            tipi_umido=curr1.fetchall()
        except Exception as e:
            logging.error(e)
        for aa in tipi_umido:
            tipo_umido=aa[0]
            percent_riempimento = aa[1]
            freq_stimata=aa[2]
        curr1.close()
        k=0
        while k<n_umido[i]:
            query_umido= '''INSERT INTO elem.elementi
                        (tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
                         id_cliente, posizione, dimensione, privato, peso_reale, 
                         peso_stimato, numero_civico_old, riferimento, coord_lat, coord_long, 
                         id_utenza, nome_attivita, modificato_da, data_ultima_modifica, percent_riempimento,
                         x_id_elemento_privato, freq_stimata, numero_civico, lettera_civico, colore_civico, note)
                        VALUES ({0}, {1}, NULL, (select id_asta from elem.piazzole where id_piazzola={1}), NULL,
                         '-1'::integer, NULL, NULL, 0, 0,
                          0, NULL, NULL, NULL, NULL,
                           '-1'::integer,  NULL, NULL, NULL, {2},
                            NULL, {3}, NULL, NULL, NULL, NULL);'''.format(tipo_umido, piazzola[i], percent_riempimento, freq_stimata)
            curr.execute(query_umido)
            k+=1

        i+=1


    '''
    Faccio una select sulle piazzole 
    '''




    '''curr2.execute(insert_attrib)
    curr2.close()'''


    
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    #conn.commit()
    ########################################################################################
    
    #exit()


if __name__ == "__main__":
    main()   