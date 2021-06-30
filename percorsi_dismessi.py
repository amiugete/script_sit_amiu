#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica tutti i percorsi dismessi in SIT ma non in UO
'''


import os,sys
import inspect, os.path
# da sistemare per Linux
import cx_Oracle





import psycopg2

import datetime

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

logging.basicConfig(
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    filemode ='w',
    filename='{}\log\{}_{}_conversione_oracle_19.log'.format(path, giorno_file, user),
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

    
    query='''select cod_percorso, max(versione) as versione, descrizione, data_attivazione, max(data_dismissione) as data_dismissione 
        from elem.percorsi p 
        where id_categoria_uso in (4,5) 
        and cod_percorso not in (select cod_percorso from elem.percorsi where cod_percorso=p.cod_percorso and id_categoria_uso=3)
        and cod_percorso like '0%'
        and length(cod_percorso) in (10,11)
        group by cod_percorso, descrizione, data_attivazione
        order by cod_percorso '''
    


    try:
	    curr.execute(query)
	    lista_dismessi=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    cod_percorso=[]
    data_dismissione=[]
           
    for vv in lista_dismessi:
        logging.debug(vv[0])
        cod_percorso.append(vv[0])
        data_dismissione.append(vv[4])


    # connessione Oracle
    cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_10")
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo,host_uo,port_uo,service_uo)
    logging.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logging.info("Versione ORACLE: {}".format(con.version))



    cur = con.cursor()


    i=0
    k=1
    logging.info('*****************************************************')
    logging.info('''CENSIMENTO PERCORSI DISMESSI SU SIT non ancora dismessi in U.O. \n "NUM", "ID_PERCORSO","DTA_ATTIVAZIONE", "DTA_DISATTIVAZIONE","DESCRIZIONE", "FAM_MEZZO", "FROM_SIT", "FREQUENZA_NEW", "UT", "SERVIZIO" ''')
    while i< len(cod_percorso):
        query='''
        SELECT a.ID_PERCORSO, DTA_ATTIVAZIONE, DTA_DISATTIVAZIONE, DESCRIZIONE, FAM_MEZZO, FROM_SIT, FREQUENZA_NEW , au.DESC_UO, as2.DESC_SERVIZIO 
        FROM 
        (
        SELECT * 
        FROM ANAGR_SER_PER_UO 
        WHERE ID_PERCORSO  in ('{}') 
        ORDER BY DTA_ATTIVAZIONE DESC
        ) a
        JOIN ANAGR_UO au 
        ON au.ID_UO = a.ID_UO
        JOIN ANAGR_SERVIZI as2 
        ON as2.ID_SERVIZIO = a.ID_SERVIZIO
        WHERE ROWNUM = 1 AND a.ID_SERVIZIO <>9 AND DTA_DISATTIVAZIONE > SYSDATE '''.format(cod_percorso[i])
        cur.execute(query)
        #cur.execute('select * from all_tables')
        #k=0
        
        for result in cur:
            #if result[7] < = '':
            logging.info('''{}, {}, {}, {}, {}, {}, {}, {}'''.format(k, result[0],result[1], result[2], result[3],
              result[4],result[5], result[6]))
            k+=1
        i+=1
            
    
    cur.close()





if __name__ == "__main__":
    main()