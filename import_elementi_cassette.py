#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script importa in automatico alcuni elementi su una lista di piazzole date in un file CSV
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import csv

import psycopg2

import datetime

from credenziali import *


#import requests

import logging



#LOG

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))

#path=os.path.dirname(sys.argv[0]) 
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/log/import_legno_plastica.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #filename=logfile,
    level=logging.INFO)





def main():

    logging.info('Lettura file CSV')

    
    id_piazzola=[]
    civico=[]
    riferimento=[]
    
    with open('input/piazzole_imballaggi.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                logging.info(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                #logging.debug(len(row))
                if len(row):
                    #logging.debug(line_count)
                    #logging.debug(len(row))
                    id_piazzola.append(int(row['ID_Piazzola']))
                    civico.append(row['Civico'])
                    riferimento.append(row['Riferimento'])
                line_count += 1
        #logging.debug(id_piazzola)
        logging.info('Lette {} righe nel file CSV'.format(len(id_piazzola)))
        logging.info('I dati sono inseriti su SIT e nel campo "modificato_da" abbiamo inserito il valore "Importato da script" ')

    

    # # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
            port=port,
            user=user,
            password=pwd,
            host=host)

    curr = conn.cursor()
    conn.autocommit = True


    # num_giorno=datetime.datetime.today().weekday()
    # giorno=datetime.datetime.today().strftime('%A')
    # giorno_file=datetime.datetime.today().strftime('%Y%m%d')
    # logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))
    
    # if num_giorno==0:
    #     num=3
    # elif num_giorno in (5,6):
    #     num=0
    #     logging.info('Oggi è {0}, lo script non gira'.format(giorno))
    #     exit()
    # else:
    #     num=1
    
    # query='''select distinct p.cod_percorso , p.descrizione, s.descrizione as servizio, u.descrizione  as ut
    #     from util.sys_history h
    #     inner join elem.percorsi p 
    #     on h.id_percorso = p.id_percorso 
    #     inner join elem.percorsi_ut pu 
    #     on pu.cod_percorso =p.cod_percorso 
    #     inner join elem.servizi s 
    #     on s.id_servizio =p.id_servizio
    #     inner join topo.ut u 
    #     on u.id_ut = pu.id_ut 
    #     where h.datetime > (current_date - INTEGER '{0}') 
    #     and h.datetime < current_date 
    #     and h."type" = 'PERCORSO' 
    #     and h.action = 'UPDATE_ELEM'
    #     and pu.responsabile = 'S'
    #     order by ut, servizio'''.format(num)
    

    i=0
    while i < len(id_piazzola):
        query='''select e.id_asta, e.id_cliente, e.posizione, e.privato, e.numero_civico_old, 
            ep.id_utenzapap, e.numero_civico, e.lettera_civico, e.colore_civico, e.note, e.riferimento, 
            ep.id_elemento_privato, 
            ep.descrizione 
            from elem.elementi e
            join elem.elementi_privati ep 
            on e.x_id_elemento_privato = ep.id_elemento_privato
            left join utenze.utenze u on 
            ep.id_utenzapap = u.id_utenza 
            where e.id_piazzola = {}
            group by  e.id_asta, e.id_cliente, e.posizione, e.privato, e.numero_civico_old, 
            ep.id_utenzapap, e.numero_civico, e.lettera_civico, e.colore_civico, e.note, e.riferimento, 
            ep.id_elemento_privato, 
            ep.descrizione, u.data_cessazione
            order by u.data_cessazione, ep.id_utenzapap desc
            '''.format(id_piazzola[i])
        try:
            curr.execute(query)
            parametri_elemento=curr.fetchall()
        except Exception as e:
            logging.error(e)
        if len(parametri_elemento) > 1:
            logging.warning('La piazzola {} contiene più di un elemento privato'.format(id_piazzola[i]))
        if len(parametri_elemento) < 1:
            logging.warning('La piazzola {} non contiene elementi privati. Si consiglia di controllare l\'esattezza delle informazioni su SIT'.format(id_piazzola[i]))
        c=0
        for vv in parametri_elemento:  
            #modificato_da
            #data_ultima_modifica
            #freq_stimata = 3
            if c==0:
                if vv[1] != None:
                    id_cliente= vv[1]
                else:
                    id_cliente = -1
                
                if vv[2] != None:
                    posizione= vv[2]
                else:
                    posizione = 0
                
                #if vv[5] != None:
                #    id_utenza= vv[5]
                #else:
                #    id_utenza = -1
                
                if riferimento[i] != None:
                    rif = riferimento[i]
                else: 
                    if vv[10] != None:
                        rif= vv[10]
                    else: 
                        rif = 'nd'
                
                #posizione, privato, numero_civico_old
                #    id_utenza, numero_civico, lettera_civico, colore_civico, 
                #    note
                logging.debug(len(vv))
                if civico[i] != None and vv[4] != None:
                    if vv[4].lower() != civico[i].lower():
                        logging.info('Piazzola {} - incongruenza civici con file excel:\n - numero civico letto = {}\n - numero civico csv = {}'.format(id_piazzola[i],vv[4],civico[i]))
                else:
                    logging.info('Piazzola {} incongruenza / assenza civici su file excel :\n - numero civico letto = {}\n - numero civico csv = {}'.format(id_piazzola[i],vv[4],civico[i]))
                

                
                #campi= ''' id_asta, id_cliente, posizione, privato, id_utenza, id_piazzola, modificato_da, data_ultima_modifica, freq_stimata, riferimento'''
                #valori= '''{0},{1},{2},1,{3},{4}, 'Importato da script SIT', now(), 3 '''.format(vv[0], id_cliente, posizione, id_utenza, id_piazzola[i])
                
                for tipo in (170, 178):
                    insert_query= ''' INSERT INTO elem.elementi (tipo_elemento, id_asta, id_cliente, posizione, privato, 
                    id_piazzola, peso_reale, peso_stimato, percent_riempimento, modificato_da, data_ultima_modifica, freq_stimata, riferimento)
                    VALUES (%s, %s, %s, %s, 1, %s, 0, 0, 90, 'Importato da script', now(), 3 , %s )'''
                    logging.debug(insert_query)
                    curr2 = conn.cursor()
                    curr2.execute(insert_query, (tipo, vv[0], id_cliente, posizione, id_piazzola[i], rif))
                    curr2.close()


                upd= 'UPDATE elem.elementi SET'
                cond = 'WHERE id_piazzola = {} and tipo_elemento in (170, 178)'. format(id_piazzola[i])
                # numero_civico_old
                if vv[4] != None:
                    update_query='''UPDATE elem.elementi SET numero_civico_old = %s 
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[4], id_piazzola[i]))
                    curr3.close()
                else:
                    if civico[i] != None:
                        update_query='''UPDATE elem.elementi SET numero_civico_old = %s 
                        WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                        curr3 = conn.cursor()
                        curr3.execute(update_query, (civico[i], id_piazzola[i]))
                        curr3.close()


                if vv[5] != None:
                    update_query='''UPDATE elem.elementi SET id_utenza = %s 
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[5], id_piazzola[i]))
                    curr3.close()

                # numero_civico, 6
                if vv[6] != None:
                    update_query='''UPDATE elem.elementi SET numero_civico = %s 
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[6], id_piazzola[i]))
                    curr3.close()
                # lettera_civico, 7
                if vv[7] != None:
                    update_query='''UPDATE elem.elementi SET lettera_civico = %s
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[7], id_piazzola[i]))
                    curr3.close()
                # colore_civico, 8
                if vv[8] != None:
                    update_query='''UPDATE elem.elementi SET colore_civico = %s 
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[8], id_piazzola[i]))
                    curr3.close()
                # note, 9
                if vv[9] != None:
                    update_query='''UPDATE elem.elementi SET note = %s 
                    WHERE id_piazzola = %s and tipo_elemento in (170, 178)'''
                    logging.debug(update_query)
                    curr3 = conn.cursor()
                    curr3.execute(update_query, (vv[9], id_piazzola[i]))
                    curr3.close()
                
                #check per non fare doppia importazione nel caso in cui in precedenza ci fosse più di un cliente
                c+=1
            
        i+=1

    curr.close()
    conn.close()

                                



if __name__ == "__main__":
    main()