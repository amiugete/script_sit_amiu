#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Script per allineare SIT con SIT prog:
- piazzole eliminate da SIT e non da SIT prog
- 
- 

'''


import os,sys, getopt
import inspect, os.path
# da sistemare per Linux
import cx_Oracle

#import openpyxl
#from pathlib import Path


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


logfile='{}/log/{}_allineamento_sit_prog.log'.format(path, giorno_file)

logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)





def main():




    # carico i mezzi sul DB PostgreSQL
    logging.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    logging.info('Connessione al db SIT PROG')
    conn_p = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr_p = conn_p.cursor()

    
    ''''Cerco piazzole eliminate da SIT dopo il 01-01-2021'''

    p_e_s=[]
    de_s=[]
    query= '''SELECT id_piazzola, data_eliminazione 
    FROM elem.piazzole p 
    WHERE data_eliminazione > '2021-01-01' '''

    try:
        curr.execute(query)
        dettagli_piazzola=curr.fetchall()
    except Exception as e:
        logging.error(e)
    
    
    
    for p in dettagli_piazzola:
        p_e_s.append(p[0])
        de_s.append(p[1])
    
    
    i=0
    while i < len(de_s):
        update_prog=''' update elem.piazzole
        set data_eliminazione=%s,
        prog = NULL,
        motivazione = NULL
        where id_piazzola=%s
        '''
        curr_p.execute(update_prog, (de_s[i],p_e_s[i]))
        i+=1


    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################

    logging.info("{} piazzole eliminate".format(len(de_s)))

    curr.close()
    curr_p.close()


    
    

    
    
    '''Cerco piazzole create su SIT'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()

 
 
    '''Prima di tutto devo cercare gli elementi bilaterali e usare numeri negativi come per le piazzole'''

    query_elementi='''select id_elemento from elem.elementi e where tipo_elemento in 
    (
    select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
    )
    and id_elemento > 0'''


    try:
        #logging.info(query_elementi)
        curr_p.execute(query_elementi, ('%'+'bilat'+'%',))
        elementi_bilaterali=curr_p.fetchall()
    except Exception as e:
        logging.error(e)

    curr_p1 = conn_p.cursor()
    k=1
    for e in elementi_bilaterali:
        update_id='''UPDATE elem.elementi 
        SET id_elemento=(select least(0,min(id_elemento))-%s from elem.elementi e )
        where id_elemento=%s'''
        curr_p1.execute(update_id, (k,e))
        k+=1

    #####################
    ## il -k è da sistemare

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################


    curr_p1.close()
    curr_p.close()

    curr_p = conn_p.cursor()


    logging.info("Terminato check su {} elementi bilaterali già creati".format(len(elementi_bilaterali)))



    ''' Cerco se la campana del vetro c'era già prima'''

    query_elementi='''select id_elemento, id_piazzola from elem.elementi where id_piazzola in 
        (
        select distinct id_piazzola from elem.elementi e where tipo_elemento in 
        (
        select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
        )
        ) and tipo_elemento = 12'''



    try:
        #logging.info(query_elementi)
        curr_p.execute(query_elementi, ('%'+'bilat'+'%',))
        elementi_vetro_bilaterali=curr_p.fetchall()
    except Exception as e:
        logging.error(e)

    curr_p1 = conn_p.cursor()
    k=1
    for e in elementi_vetro_bilaterali:
        select_sit='''select * from elem.elementi 
        WHERE id_elemento=%s and id_piazzola=%s and tipo_elemento=12'''

        
        try:
            #logging.info(query_elementi)
            curr.execute(select_sit, (e[0],e[1]))
            elementi_vetro_sit=curr.fetchall()
        except Exception as e:
            logging.error(e)
        

        select_esiste_sit='''select * from elem.elementi 
        WHERE id_elemento=%s'''

        
        try:
            #logging.info(query_elementi)
            curr.execute(select_esiste_sit, (e[0],))
            elemento_esiste_sit=curr.fetchall()
        except Exception as e:
            logging.error(e)

        #print(len(elementi_vetro_sit))
        if len(elementi_vetro_sit) < 1 and len(elemento_esiste_sit)==1:
            logging.info('id_elemento {}'.format(e[0]))
            update_id='''UPDATE elem.elementi 
            SET id_elemento=(select least(0,min(id_elemento))-%s from elem.elementi e )
            where id_elemento=%s'''
            curr_p1.execute(update_id, (k,e[0]))
            k+=1
        elif len(elemento_esiste_sit)<1:
            logging.info('id_elemento {} sarebbe da eliminare'.format(e[0]))




    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################

    logging.info("Terminato check su {} elementi del vetro in piazzole bilaterali già creati".format(k))




    curr_p1.close()
    curr_p.close()
    curr.close()

    curr = conn.cursor()
    curr_p = conn_p.cursor()







    '''Cerco aste create su SIT'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()


    query_max_id_prog='''select max(id_asta) from elem.aste'''

    try:
        curr_p.execute(query_max_id_prog)
        max_id_q=curr_p.fetchall()
    except Exception as e:
        logging.error(e)

    for mp in max_id_q:
        max_id=mp[0]
    

    


    




    curr.close()
    curr_p.close()

    curr_p = conn_p.cursor()
    curr = conn.cursor()




    select_aste='''SELECT id_asta, id_via, num_seq, lung_asta, larg_asta, lung_marc_d,
    larg_marc_d, alt_marc_d, lung_marc_p, larg_marc_p, alt_marc_p, lung_port_d,
    larg_port_d, lung_port_p, larg_port_p, lung_ped_d, larg_ped_d, lung_ped_p,
    larg_ped_p, portata, freq_mercati, freq_fiere, id_ut, id_quartiere,
    id_circoscrizione, id_tipologia_asta, id_transitabilita, id_uu, area_porto, cod_nodo1,
    cod_nodo2, verso_asta, senso_marcia_12, senso_marcia_21, semaforo_nodo1, semaforo_nodo2,
    cod_comune, cod_traffico_12, cod_traffico_21, old_idelem
    FROM elem.aste WHERE id_asta > %s;
    '''

    #40

    try:
        curr.execute(select_aste, (max_id,))
        id_a=curr.fetchall()
    except Exception as e:
        logging.error(e)


    curr1 = conn.cursor()
    curr_p1 = conn_p.cursor()



    query_insert='''INSERT INTO elem.aste
        (id_asta, id_via, num_seq, lung_asta, larg_asta, lung_marc_d,
        larg_marc_d, alt_marc_d, lung_marc_p, larg_marc_p, alt_marc_p, lung_port_d,
        larg_port_d, lung_port_p, larg_port_p, lung_ped_d, larg_ped_d, lung_ped_p,
        larg_ped_p, portata, freq_mercati, freq_fiere, id_ut, id_quartiere, 
        id_circoscrizione, id_tipologia_asta, id_transitabilita, id_uu, area_porto, cod_nodo1, 
        cod_nodo2, verso_asta, senso_marcia_12, senso_marcia_21, semaforo_nodo1, semaforo_nodo2,
        cod_comune, cod_traffico_12, cod_traffico_21, old_idelem)
        VALUES (%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s);'''


    
    for aa in id_a:
        #args_str = ','.join(curr_p1.mogrify(("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in pp)
        #print(args_str)
        #31
        #print(len(aa))
        curr_p.execute(query_insert, (aa[0], aa[1],aa[2],aa[3],aa[4],aa[5],aa[6], aa[7],aa[8],aa[9],aa[10], aa[11],aa[12],aa[13],aa[14],aa[15],aa[16],aa[17],aa[18],aa[19],aa[20],aa[21],aa[22],aa[23],aa[24],aa[25],aa[26],aa[27],aa[28],aa[29],aa[30],aa[31],aa[32],aa[33],aa[34],aa[35],aa[36],aa[37],aa[38],aa[39]))
        
    

        selezione_geom_aste='''SELECT id, idelem, mi_style, mi_prinx, geoloc, osm_id
        FROM geo.grafostradale WHERE id = %s;
        '''
    



        try:
            curr1.execute(selezione_geom_aste,(aa[0],))
            id_ag=curr1.fetchall()
        except Exception as e:
            logging.error(e)


        insert_geo='''INSERT INTO geo.grafostradale
        (id, idelem, mi_style, mi_prinx, geoloc, osm_id)
        VALUES(%s, %s, %s, %s, %s, %s);
        '''
        for ag in id_ag:
            curr_p1.execute(insert_geo, (ag[0],ag[1],ag[2],ag[3],ag[4],ag[5]))

       
       ########################################################################################
        # da testare sempre prima senza fare i commit per verificare che sia tutto OK
        conn_p.commit()
        ########################################################################################
       
        curr1.close()
        curr1 = conn.cursor()




        curr_p1.close()
        curr_p1 = conn_p.cursor()








    curr.close()
    curr= conn.cursor()





    logging.info("{} aste aggiunte".format(len(id_a)))



    ''' Devo cercare le piazzole e gli elementi associati creati'''


    query_max_id_prog='''select max(id_piazzola) from elem.piazzole'''

    try:
        curr_p.execute(query_max_id_prog)
        max_id_q=curr_p.fetchall()
    except Exception as e:
        logging.error(e)

    for mp in max_id_q:
        max_id=mp[0]
    

    curr_p.close()
    curr_p = conn_p.cursor()



 
    c_e=0
    query='''SELECT id_piazzola, riferimento, descrizione, posizione, numero_civico, num_file_elementi,
    id_asta, note, id_censimento, id_review, modificata_da, data_ultima_modifica, flg_lateralizzazione_rsu,
    flg_lateralizzazione_carta, flg_lateralizzazione_plastica, path_photo, accuracy, colore_civico, lettera_civico,
    numero_civico_orig, data_mod_indirizzo, suolo_privato, id_via, data_eliminazione, foto,
    ecopunto, id_originario, prog, motivazione, lato, riportata_prod
    FROM elem.piazzole WHERE id_piazzola > %s; '''

    try:
        curr.execute(query, (max_id,))
        id_p=curr.fetchall()
    except Exception as e:
        logging.error(e)


    curr1 = conn.cursor()
    curr_p1 = conn_p.cursor()




    query_insert=''' INSERT INTO elem.piazzole
        (id_piazzola, riferimento, descrizione, posizione, numero_civico, num_file_elementi,
        id_asta, note, id_censimento, id_review, modificata_da, data_ultima_modifica, flg_lateralizzazione_rsu, 
        flg_lateralizzazione_carta, flg_lateralizzazione_plastica, path_photo, accuracy, colore_civico, lettera_civico, 
        numero_civico_orig, data_mod_indirizzo, suolo_privato, id_via, data_eliminazione, foto, 
        ecopunto, id_originario, prog, motivazione, lato, riportata_prod)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
    
    for pp in id_p:
        #args_str = ','.join(curr_p1.mogrify(("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in pp)
        #print(args_str)
        #31
        curr_p.execute(query_insert, (pp[0], pp[1],pp[2],pp[3],pp[4],pp[5],pp[6], pp[7],pp[8],pp[9],pp[10], pp[11],pp[12],pp[13],pp[14],pp[15],pp[16], pp[17],pp[18],pp[19],pp[20],pp[21],pp[22],pp[23],pp[24],pp[25],pp[26],pp[27],pp[28],pp[29],pp[30]))
        
    


        selezione_geom_piazzole='''SELECT id, mi_style, mi_prinx, geoloc, coord_lat, coord_long
            FROM geo.piazzola where id = %s;
        '''
    

        # da completare


        try:
            curr1.execute(selezione_geom_piazzole,(pp[0],))
            id_pg=curr1.fetchall()
        except Exception as e:
            logging.error(e)


        for pg in id_pg:
            insert_geo='''INSERT INTO geo.piazzola
            (id, mi_style, mi_prinx, geoloc, coord_lat, coord_long)
            VALUES(%s,%s,%s,%s,%s,%s);
            '''
            curr_p1.execute(insert_geo, (pg[0],pg[1],pg[2],pg[3],pg[4],pg[5]))

        curr1.close()
        curr1 = conn.cursor()
        
        curr_p1.close()
        curr_p1 = conn_p.cursor()

        seleziono_elementi='''SELECT id_elemento, tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem, 
        id_cliente, posizione, dimensione, privato, peso_reale, peso_stimato,
        numero_civico_old, riferimento, coord_lat, coord_long, id_utenza, nome_attivita,
        modificato_da, data_ultima_modifica, percent_riempimento, x_id_elemento_privato, freq_stimata, numero_civico,
        lettera_civico, colore_civico, note
        FROM elem.elementi WHERE id_piazzola =%s;
        '''
        
        

        try:
            curr1.execute(seleziono_elementi,(pp[0],))
            id_e=curr1.fetchall()
        except Exception as e:
            logging.error(e)


        insert_elementi='''INSERT INTO elem.elementi
            (id_elemento,tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
            id_cliente, posizione, dimensione, privato, peso_reale, peso_stimato,
            numero_civico_old, riferimento, coord_lat, coord_long, id_utenza, nome_attivita,
            modificato_da, data_ultima_modifica, percent_riempimento, x_id_elemento_privato, freq_stimata, numero_civico,
            lettera_civico, colore_civico, note)
            VALUES(%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s);'''

        for ee in id_e:
            curr_p1.execute(insert_elementi,(ee[0],ee[1],ee[2],ee[3],ee[4],ee[5],ee[6],ee[7],ee[8],ee[9],ee[10],ee[11],ee[12],ee[13],ee[14],ee[15],ee[16],ee[17],ee[18],ee[19],ee[20],ee[21],ee[22],ee[23],ee[24],ee[25],ee[26]))
            c_e+=1
            #27

    logging.info("{} piazzole aggiunte, con {} elementi".format(len(id_p), c_e))

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
   
    curr1.close()
    curr_p1.close()
    curr.close()
    curr_p.close()
    
    
    ''' Cerco piazzole modificate su SIT'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()
    curr_p1 = conn_p.cursor()
    curr_p2 = conn_p.cursor()
    ''' Parto dagli elementi di SIT '''


    select_elementi_sit='''SELECT 
    id_elemento, tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
    id_cliente, posizione, dimensione, privato, peso_reale, peso_stimato,
    numero_civico_old, riferimento, coord_lat, coord_long, id_utenza, nome_attivita,
    modificato_da, data_ultima_modifica, percent_riempimento, x_id_elemento_privato, freq_stimata, numero_civico,
    lettera_civico, colore_civico, note
    FROM elem.elementi;'''

    #27

    try:
        curr.execute(select_elementi_sit)
        id_e=curr.fetchall()
    except Exception as e:
        logging.error(e)

    c=0
    for ee in id_e:
        '''Cerco se esiste in SIT PROG'''
        select_prog='''select * from elem.elementi where id_elemento = %s'''
        try:
            curr_p.execute(select_prog,(ee[0],))
            id_e1=curr_p.fetchall()
        except Exception as e:
            logging.error(e)
        # se c'è l'elemento non lo tocco
        #altrimenti
        if len(id_e1)<1:
            select_prog2='''select * from elem.elementi where id_piazzola = %s and tipo_elemento in 
            (
            select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
            ) '''
            try:
                #logging.info(query_elementi)
                curr_p1.execute(select_prog2, (ee[2], '%'+'bilat'+'%'))
                id_e_bil=curr_p1.fetchall()
            except Exception as e:
                logging.error(e)
            # se non ci sono bilaterali nella stessa piazzola
            if len(id_e_bil)<1:
                curr_p2.execute(insert_elementi,(ee[0],ee[1],ee[2],ee[3],ee[4],ee[5],ee[6],ee[7],ee[8],ee[9],ee[10],ee[11],ee[12],ee[13],ee[14],ee[15],ee[16],ee[17],ee[18],ee[19],ee[20],ee[21],ee[22],ee[23],ee[24],ee[25],ee[26]))
                c+=1

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
    logging.info("{} elementi di SIT aggiunti".format(c))

    


    curr.close()
    curr_p.close()
    curr_p1.close()
    curr_p2.close()




    '''Cerco elementi su SIT Prog e non su sit (forse da eliminare)'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()
    curr_p1 = conn_p.cursor()


    select_elementi_sit_prog='''SELECT 
    id_elemento, tipo_elemento, id_piazzola, difficolta, id_asta, old_idelem,
    id_cliente, posizione, dimensione, privato, peso_reale, peso_stimato,
    numero_civico_old, riferimento, coord_lat, coord_long, id_utenza, nome_attivita,
    modificato_da, data_ultima_modifica, percent_riempimento, x_id_elemento_privato, freq_stimata, numero_civico,
    lettera_civico, colore_civico, note
    FROM elem.elementi WHERE tipo_elemento not in 
            (
            select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
            ) ;'''

    try:
        #logging.info(query_elementi)
        curr_p.execute(select_elementi_sit_prog, ('%'+'bilat'+'%',))
        id_e_bil=curr_p.fetchall()
    except Exception as e:
        logging.error(e)


    c=0
    for ee in id_e_bil:
        '''Cerco se esiste in SIT'''
        select_prog='''select * from elem.elementi where id_elemento = %s'''
        try:
            curr.execute(select_prog,(ee[0],))
            id_e1=curr.fetchall()
        except Exception as e:
            logging.error(e)
        # se c'è l'elemento non lo tocco
        #altrimenti
        if len(id_e1)<1:
            #logging.info('Elemento {} da eliminare'.format(ee[0]))
            delete_prog='''DELETE FROM elem.elementi_aste_percorso
            WHERE id_elemento = %s;
            DELETE FROM elem.elementi
            WHERE id_elemento = %s;'''
            try:
                curr_p1.execute(delete_prog,(ee[0],ee[0]))
            except psycopg2.Error as e:
                logging.error(e)
                logging.error(e.pgerror)
                logging.error(e.diag.message_detail)
            c+=1
    

    logging.info("{} elementi di SIT PROG eliminati".format(c))
    curr.close()
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
    curr_p.close()
    curr_p1.close()




    


    ''' Cerco elementi vetro nelle piazzole bilaterali'''


            



    curr.close()
    conn.close()
    curr_p.close()
    conn_p.close()






if __name__ == "__main__":
    main()   