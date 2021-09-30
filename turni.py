#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script legge i turni attivi su U.O. ed effettua l'allineamento con SIT:
1. verifica se il turno esiste 
caso A in SIT il turno esiste e id è corretto allinea solo la descrizione
caso B in SIT il turno esiste, ma id non è corretto, per cui occorre aggiornare i turni con il vecchio id e usare quello nuovo
caso C in SIT il turno non esiste e lo crea 


Quindi allinea i turni di tutti i percorsi da U.O. a SIT
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
logfile='{}/log/turni.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)


def main():
    # Mi connetto al DB oracle
    cx_Oracle.init_oracle_client("C:\oracle\instantclient_19_10") # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))
    cur = con.cursor()


    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True


    
    id_uo=[]
    h_s=[]
    h_e=[]
    m_s=[]
    m_e=[]
    codice=[]
    fascia=[]

    query='''SELECT
    id_turno, codice_turno, DESCRIZIONE, FASCIA_TURNO, DESCR_ORARIO,
    INIZIO_ORA, FINE_ORA, INIZIO_MINUTI, FINE_MINUTI
    FROM ANAGR_TURNI at2 
    WHERE DTA_DISATTIVAZIONE > SYSDATE 
    ORDER BY ID_TURNO '''
    try:
        cur.execute(query)
        lista_turni=cur.fetchall()
    except Exception as e:
        logging.error(query)
        logging.error(e)
    cur.close()

    for t_uo in lista_turni:
        logging.debug('Id_turno (uo): {} '.format(t_uo[0]))
        logging.debug('Tupla di {} '.format(len(t_uo)))
        id_uo.append(t_uo[0])
        h_s.append(t_uo[5])
        h_e.append(t_uo[6])
        m_s.append(t_uo[7])
        m_e.append(t_uo[8])
        codice.append(t_uo[1])
        fascia.append(t_uo[3])

    # query_pg='''select id_turno, descrizione, cod_turno 
    # from id_turnoelem.turni t 
    # where inizio_ora ={0}
    # and fine_ora = {1}
    # and inizio_minuti={2}
    # and fine_minuti= {3} '''.format(t_uo[5], t_uo[6], t_uo[7], t_uo[8])


    # PRIMO CICLO: sui turni esistenti

    query_pg='''select id_turno, descrizione, cod_turno,
    inizio_ora, fine_ora, inizio_minuti, fine_minuti
    from elem.turni t '''
    try:
        curr.execute(query_pg)
        turno=curr.fetchall()
    except Exception as e:
        logging.error(query_pg)
        logging.error(e)
    logging.debug(len(turno))
    for t_sit in turno:
        if t_sit[0] in id_uo:
            k=id_uo.index(t_sit[0])
            if int(t_sit[3])==int(h_s[k]) and int(t_sit[4])==int(h_e[k]) and int(t_sit[5])==int(m_s[k]) and int(t_sit[6])==int(m_e[k]):
                logging.info('ID {} confermato'.format(t_sit[0]))
            else: 
                text='''ID {0} non corrisponde 
                Inizio SIT {1}:{2} Fine SIT: {3}:{4} - 
                Inizio UO {5}:{6} - Fine UO {7}:{8}'''.format(t_sit[0], t_sit[3], t_sit[5], t_sit[4], t_sit[6], int(h_s[k]), m_s[k], h_e[k], m_e[k])

                #text='''ID {} non corrisponde 
                #Inizio SIT {} Fine SIT: {} - '''.format(t_sit[0], t_sit[3], t_sit[4])

                logging.warning(text)
                #logging.warning('ID {0} non corrisponde Inizio SIT {1}:{2} Fine SIT: {3}:{4} - Inizio UO {5}:{6} - Fine UO {7}:{8}'.format(t_sit[0], t_sit[3], t_sit[5], t_sit[4], t_sit[6], h_s[k], m_s[k], h_e[k], m_e[k]))

        else:
            # sarebbe da rimuovere
            logging.info('ID {} da rimuovere'.format(t_sit[0]))


        
    # SECONDO CICLO: su Oracle per aggiungere ciò che non c'è
    i=0
    while i<len(id_uo):
        if fascia[i]=='M':
            f='A'
        else:
            f=fascia[i]
        if int(m_s[i])==0 and int(m_e[i])==0:
            cod_turno='{} {}/{}'.format(codice[i], int(h_s[i]), int(h_e[i]))
        else: 
            if int(m_s[i])==0:
               cod_turno='{}  {}/{}:{}'.format(codice[i], int(h_s[i]), int(h_e[i]), m_e[i])
            elif int(m_e[i])==0:
                cod_turno='{}  {}/{}'.format(codice[i], int(h_s[i]), m_s[i], int(h_e[i]))
            else:
                cod_turno='{}  {}:{}/{}:{}'.format(codice[i], int(h_s[i]), m_s[i], int(h_e[i]), m_e[i])
        logging.debug('I:{} - Descrizione: {} - Codice: {} '.format(id_uo[i], f, cod_turno ))
        i+=1




    # da impostare ciclo sui percorsi

    

if __name__ == "__main__":
    main()   