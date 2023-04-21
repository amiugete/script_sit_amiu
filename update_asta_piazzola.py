#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per fare update delle aste delle piazzole
'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle


import xlsxwriter


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


logfile='{}/log/{}update_grafo_piazzola.log'.format(path, giorno_file)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)





def update_asta_piazzola(piazzola, asta_old, asta_new, ambiente):


    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db')
    conn = psycopg2.connect(dbname=ambiente,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True
    # cerco i percorsi che passano da quella piazzola
    query='''select id_elemento, id_percorso, id_categoria_uso, cod_percorso from elem.v_percorsi_per_elemento vppe 
 where id_elemento in (select id_elemento from elem.elementi e where id_piazzola in (%s))'''
    
    try:
        curr.execute(query,(piazzola,))
        lista_percorsi=curr.fetchall()
    except Exception as e:
        logging.error(e)


    #inizializzo gli array
    #ut=[]

           
    for u in lista_percorsi:
        #logging.debug(vv[0])
        #ut.append(vv[0])
        id_percorso=u[1]
        cod_percorso=u[3]
        id_elemento=u[0]

        descrizione_update = 'Percorso {0} codice {1} aggiornato da script python update_asta_piazzola.py'.format(id_percorso, cod_percorso)

        insert_history='''INSERT INTO util.sys_history ("type","action","description","datetime","id_piazzola","id_percorso", "id_user") VALUES
	 ('PERCORSO','UPDATE_ELEM', %s, now(), %s, %s, -1);'''
        curr2= conn.cursor()
        try:
            curr2.execute(insert_history,(descrizione_update,piazzola,id_percorso,))
        except Exception as e:
            logging.error(e)

        curr2.close()
        query2='''select * from elem.aste_percorso ap where id_percorso = %s and id_asta=%s'''
        curr2= conn.cursor()
        
        try:
            curr2.execute(query2,(id_percorso, asta_new,))
            lista_aste_percorsi=curr2.fetchall()
        except Exception as e:
            logging.error(e)
            
        ''' Procedo in due modi diversi'''
        if len(lista_aste_percorsi)==0:
            logging.info("Non c'è la nuova asta {0}, nel percorso {1} (id={2}), la aggiungo".format(asta_new, cod_percorso, id_percorso))
            curr3= conn.cursor()
             #update num_seq
            update =  '''update elem.aste_percorso 
            set num_seq=num_seq+1 
            where id_percorso=%s and num_seq >=(select min(num_seq) FROM elem.aste_percorso where id_percorso = %s and id_asta=%s);'''
            try:
                curr3.execute(update,(id_percorso, id_percorso, asta_old,))
                conn.commit()
            except Exception as e:
                logging.error(e)
            
            insert= '''insert into elem.aste_percorso (id_asta, num_seq, x_cod_percorso, lato_servizio, percent_trattamento, tipo, frequenza, 
            carico_scarico, id_percorso, metri_trasf, tempo_trasf, senso_perc, lung_trattamento, nota) SELECT %s, min(num_seq), x_cod_percorso,
            lato_servizio, percent_trattamento, tipo, frequenza, 
            carico_scarico, id_percorso, metri_trasf, tempo_trasf, senso_perc, lung_trattamento, nota
            FROM elem.aste_percorso where id_percorso = %s and id_asta=%s group by x_cod_percorso,
            lato_servizio, percent_trattamento, tipo, frequenza, 
            carico_scarico, id_percorso, metri_trasf, tempo_trasf, senso_perc, lung_trattamento, nota;'''

            try:
                curr3.execute(insert,(asta_new, id_percorso, asta_old,))
                conn.commit()
            except Exception as e:
                logging.error(e)
            
            curr3.close()    
            
            
            curr3= conn.cursor()
            update_eap='''update elem.elementi_aste_percorso 
                    set id_asta_percorso = (select id_asta_percorso from elem.aste_percorso ap where id_percorso = %s and id_asta=%s)
                    where id_elemento = %s and id_asta_percorso in
                    (select id_asta_percorso from elem.aste_percorso where id_percorso=%s);'''
            try:
                curr3.execute(update_eap, (id_percorso, asta_new, id_elemento,id_percorso,))
                conn.commit()
            except Exception as e:
                logging.error(e)
            
            curr3.close()

            
            
            ''' 
            Se 0 Cambio il tipo della vecchia asta da servizio a trasferimento
            '''
            curr3= conn.cursor()
            
            test_eap='''select count(*) from elem.elementi_aste_percorso eap where id_asta_percorso in (
                            select id_asta_percorso from elem.aste_percorso ap where id_percorso = %s and id_asta=%s 
                        )'''
            try:
                curr3.execute(test_eap, (id_percorso, asta_old))
                lista_test=curr3.fetchall()
            except Exception as e:
                logging.error(e)
            
            for tt in lista_test:
                check_test=tt[0]
            
            if check_test == 0:
                update_ap='''update elem.aste_percorso
                set tipo = 'trasferimento' 
                where id_percorso = %s and id_asta=%s '''
                
                try:
                    curr3.execute(update_ap, (id_percorso, asta_old))
                    conn.commit()
                except Exception as e:
                    logging.error(e)
            else:
                logging.info('''Non cambio tipo asta perchè c'è altra piazzola''')
                
                
            curr3.close()
            
            
            
        elif len(lista_aste_percorsi)>0:
            logging.info("La nuova asta {0} c'è già nel percorso {1}, faccio semplicemente update".format(asta_new, cod_percorso))
            c=0
            for ap in lista_aste_percorsi:
                if c==0:
                    asta_percorso=ap[0]
                    update_eap='''update elem.elementi_aste_percorso 
                    set id_asta_percorso = %s
                    where id_elemento = %s and id_asta_percorso in
                    (select id_asta_percorso from elem.aste_percorso where id_percorso=%s)'''
                    curr3= conn.cursor()
                    curr3.execute(update_eap, (asta_percorso, id_elemento,id_percorso,))
                    conn.commit()
                    curr3.close()
                c+=1   
        '''else: 
            print('Cod percorso: {}'.format(cod_percorso))
            print('Id percorso: {}'.format(id_percorso))
            print('Asta: {}'.format(asta_new))
            print("Da capire come gestire")      
        '''
        curr2.close()
        
    update_piazzola = '''update elem.piazzole p
        set id_asta= %s
        where id_piazzola in (%s);'''
    
    try:
        curr.execute(update_piazzola,(asta_new, piazzola,))
        conn.commit()
    except Exception as e:
        logging.error(e)
        
        
    update_elementi = '''update elem.elementi 
    set id_asta= %s
    where id_piazzola in (%s);'''
    
    try:
        curr.execute(update_elementi,(asta_new, piazzola,))
        conn.commit()
    except Exception as e:
        logging.error(e)


    curr.close()




def main():
    # parte propedeutica all'eventuale GUI
    if os.name == 'nt':
        domain=os.environ['userdomain']
    else: 
        logging.warning('Non sono in windows')
        domain= 'ND'
    user=os.getlogin()
    logging.info('''Dominio ={0}, Utente={1}'''.format(domain, user))
    ############################################
    #INPUT (da rendere dinamici per  fare WS)
    piazzola = 21779
    asta_old = 2435110007
    asta_new = 2452600009
    ambiente = 'sit' # sit_test, #sit_prog
    #############################################
    update_asta_piazzola(piazzola, asta_old, asta_new, ambiente)




if __name__ == "__main__":
    main()   