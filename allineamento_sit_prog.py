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
from mail_log import *
#from credenziali import db, port, user, pwd, host, user_mail, pwd_mail, port_mail, smtp_mail



#libreria per gestione log
import logging


#num_giorno=datetime.datetime.today().weekday()
#giorno=datetime.datetime.today().strftime('%A')

filename = inspect.getframeinfo(inspect.currentframe()).filename
path     = os.path.dirname(os.path.abspath(filename))


giorno_file=datetime.datetime.today().strftime('%Y%m%d')

nomelogfile='{}_allineamento_sit_prog.log'.format(giorno_file)
logfile='{}/log/{}'.format(path, nomelogfile)
errorfile='{}/log/error.log'.format(path, giorno_file)

'''logging.basicConfig(
    handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w'), logging.StreamHandler()],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.INFO)
'''

# Create a custom logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.ERROR)
f_handler.setLevel(logging.DEBUG)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)






def main():




    # carico i mezzi sul DB PostgreSQL
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True

    logger.info('Connessione al db SIT PROG')
    conn_p = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr_p = conn_p.cursor()


    ''' Cerco max data eliminazione su SIT PROG'''


    
    

    data_max='''select max(data_eliminazione) from elem.piazzole '''

    try:
        curr_p.execute(data_max)
        max_data=curr_p.fetchall()
    except Exception as e:
        logger.error(e)
    
    
    
    for d in max_data:
        data_eliminazione_max=d[0]

    curr_p.close()
    curr_p = conn_p.cursor()

    logger.info('''Su SIT PROG l'ultima piazzola eliminata risulta ferma al {}'''.format(data_eliminazione_max))

    ''''Cerco piazzole eliminate da SIT dopo la data_eliminazione_max'''

    p_e_s=[]
    de_s=[]
    query= '''SELECT id_piazzola, data_eliminazione 
    FROM elem.piazzole p 
    WHERE data_eliminazione > %s '''

    try:
        curr.execute(query, (data_eliminazione_max,))
        dettagli_piazzola=curr.fetchall()
    except Exception as e:
        logger.error(e)
    
    
    
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

    logger.info("{} piazzole eliminate".format(len(de_s)))

    curr.close()
    curr_p.close()


    
    

    
    
    '''Cerco piazzole create su SIT'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()

 
 
    '''Prima di tutto devo cercare gli elementi bilaterali dentro SIT PROG e usare numeri negativi come per le piazzole'''

    query_elementi='''select id_elemento from elem.elementi e where tipo_elemento in 
    (
    select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
    )
    and id_elemento > 0'''


    try:
        #logger.info(query_elementi)
        curr_p.execute(query_elementi, ('%'+'bilat'+'%',))
        elementi_bilaterali=curr_p.fetchall()
    except Exception as e:
        logger.error(e)

    curr_p1 = conn_p.cursor()
    k=1
    for e in elementi_bilaterali:
        update_id='''UPDATE elem.elementi 
        SET id_elemento=(select least(0,min(id_elemento))-1 from elem.elementi e )
        where id_elemento=%s'''
        curr_p1.execute(update_id, (e,))
        k+=1

 

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################


    curr_p1.close()
    curr_p.close()

    curr_p = conn_p.cursor()
    curr_p1 = conn_p.cursor()

    logger.info("Terminato check su {} elementi bilaterali già creati".format(len(elementi_bilaterali)))



    ''' Cerco se la campana del vetro di SIT Prog c'era già prima'''

    query_elementi='''select id_elemento, id_piazzola from elem.elementi where id_piazzola in 
        (
        select distinct id_piazzola from elem.elementi e where tipo_elemento in 
        (
        select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
        )
        ) and tipo_elemento = 12 and id_elemento > 0'''



    try:
        #logger.info(query_elementi)
        curr_p.execute(query_elementi, ('%'+'bilat'+'%',))
        elementi_vetro_bilaterali=curr_p.fetchall()
    except Exception as e:
        logger.error(e)

    
    k=1
    #ciclo sulle campane del vetro bilaterali
    for e in elementi_vetro_bilaterali:
        # verifico se c'è la campana su SIT
        select_sit='''select * from elem.elementi 
        WHERE id_elemento=%s and id_piazzola=%s and tipo_elemento=12'''

        
        try:
            #logger.info(query_elementi)
            curr.execute(select_sit, (e[0],e[1]))
            elementi_vetro_sit=curr.fetchall()
        except Exception as e:
            logger.error(e)
        

        select_esiste_sit='''select * from elem.elementi 
        WHERE id_elemento=%s'''

        
        try:
            #logger.info(query_elementi)
            curr.execute(select_esiste_sit, (e[0],))
            elemento_esiste_sit=curr.fetchall()
        except Exception as e:
            logger.error(e)

        #print(len(elementi_vetro_sit))
        # la campana di SIT prog non c'è su SIT
        if len(elementi_vetro_sit) < 1 and len(elemento_esiste_sit)==1:
            logger.info('id_elemento {}'.format(e[0]))
            update_id='''UPDATE elem.elementi 
            SET id_elemento=(select least(0,min(id_elemento))-1 from elem.elementi e )
            where id_elemento=%s'''
            curr_p1.execute(update_id, (int(e[0]),))
            ########################################################################################
            # da testare sempre prima senza fare i commit per verificare che sia tutto OK
            conn_p.commit()
            ########################################################################################
            k+=1
        elif len(elemento_esiste_sit)<1:
            logger.info('id_elemento {} su SIT non corrisponde a quello su SIT PROG - TO CHECK'.format(e[0]))




    

    logger.info("Terminato check su {} elementi del vetro in piazzole bilaterali già creati".format(k))




    curr_p1.close()
    curr_p.close()
    curr.close()








    '''Cerco aste create su SIT'''
    curr = conn.cursor()
    curr_p = conn_p.cursor()


    query_max_id_prog='''select max(id_asta) from elem.aste'''

    try:
        curr_p.execute(query_max_id_prog)
        max_id_q=curr_p.fetchall()
    except Exception as e:
        logger.error(e)

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
        logger.error(e)


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
        
    

        selezione_geom_aste='''SELECT id, idelem, geoloc, osm_id
        FROM geo.grafostradale WHERE id = %s;
        '''
    


        logger.debug('Id_asta = {}'.format(int(aa[0])))
        try:
            curr1.execute(selezione_geom_aste,(int(aa[0]),))
            id_ag=curr1.fetchall()
        except Exception as e:
            logger.error(e)


        insert_geo='''INSERT INTO geo.grafostradale
        (id, idelem, geoloc, osm_id)
        VALUES(%s, %s, %s, %s);
        '''
        for ag in id_ag:
            curr_p1.execute(insert_geo, (ag[0],ag[1],ag[2],ag[3]))

       
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





    logger.info("{} aste aggiunte".format(len(id_a)))



    ''' Devo cercare le piazzole e gli elementi associati creati'''


    query_max_id_prog='''select max(id_piazzola) from elem.piazzole'''

    try:
        curr_p.execute(query_max_id_prog)
        max_id_q=curr_p.fetchall()
    except Exception as e:
        logger.error(e)

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
        logger.error(e)







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
        try:
            curr_p.execute(query_insert, (pp[0], pp[1],pp[2],pp[3],pp[4],pp[5],pp[6], pp[7],pp[8],pp[9],pp[10], pp[11],pp[12],pp[13],pp[14],pp[15],pp[16], pp[17],pp[18],pp[19],pp[20],pp[21],pp[22],pp[23],pp[24],pp[25],pp[26],pp[27],pp[28],pp[29],pp[30]))
        except Exception as e:
            logger.error(e)
            logger.error(query_insert)
            logger.error('Piazzola. {}'.format(pp[0]))
            logger.error('Id asta. {}'.format(pp[6]))
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
    


        selezione_geom_piazzole='''SELECT id, geoloc, coord_lat, coord_long
            FROM geo.piazzola where id = %s;
        '''
    

        # da completare


        try:
            curr1.execute(selezione_geom_piazzole,(pp[0],))
            id_pg=curr1.fetchall()
        except Exception as e:
            logger.error(e)


        for pg in id_pg:
            insert_geo='''INSERT INTO geo.piazzola
            (id, geoloc, coord_lat, coord_long)
            VALUES(%s,%s,%s,%s);
            '''
            curr_p1.execute(insert_geo, (pg[0],pg[1],pg[2],pg[3]))

        curr1.close()
        curr1 = conn.cursor()
        
        curr_p1.close()
        curr_p1 = conn_p.cursor()

        seleziono_elementi='''SELECT id_elemento, tipo_elemento, id_piazzola, /*x_difficolta,*/ id_asta, /*x_old_idelem, 
        x_id_cliente,*/ posizione, dimensione, privato, peso_reale, peso_stimato,
        /*x_numero_civico_old,*/ riferimento, /*x_coord_lat, x_coord_long,*/ id_utenza, nome_attivita,
        modificato_da, data_ultima_modifica, percent_riempimento, /*x_id_elemento_privato,*/ freq_stimata, numero_civico,
        lettera_civico, colore_civico, note
        FROM elem.elementi WHERE id_piazzola =%s;
        '''
        
        

        try:
            curr1.execute(seleziono_elementi,(pp[0],))
            id_e=curr1.fetchall()
        except Exception as e:
            logger.error(e)
            logger.error(seleziono_elementi)
            logger.error('ID Piazzola {}'.format(pp[0]))
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()


        insert_elementi='''INSERT INTO elem.elementi
            (id_elemento,tipo_elemento, id_piazzola, /*difficolta, */ id_asta, /*old_idelem,
            id_cliente,*/ posizione, dimensione, privato, peso_reale, peso_stimato,
            /*numero_civico_old,*/ riferimento, /*coord_lat, coord_long, */id_utenza, nome_attivita,
            modificato_da, data_ultima_modifica, percent_riempimento, /*x_id_elemento_privato,*/ freq_stimata, numero_civico,
            lettera_civico, colore_civico, note)
            VALUES(%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s) ON CONFLICT (id_elemento) DO NOTHING'''

        for ee in id_e:
            curr_p1.execute(insert_elementi,(ee[0],ee[1],ee[2],ee[3],ee[4],ee[5],ee[6],ee[7],ee[8],ee[9],ee[10],ee[11],ee[12],ee[13],ee[14],ee[15],ee[16],ee[17],ee[18],ee[19]))
            c_e+=1
            #27

    logger.info("{} piazzole aggiunte, con {} elementi".format(len(id_p), c_e))

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
   
    curr1.close()
    curr_p1.close()
    curr.close()
    curr_p.close()
    
    
    logger.info(''' Cerco piazzole modificate su SIT''')
    curr = conn.cursor()
    curr_p = conn_p.cursor()
    curr_p1 = conn_p.cursor()
    curr_p2 = conn_p.cursor()
    ''' Parto dagli elementi di SIT '''


    select_elementi_sit='''SELECT id_elemento, tipo_elemento, id_piazzola, /*x_difficolta,*/ id_asta, /*x_old_idelem, 
        x_id_cliente,*/ posizione, dimensione, privato, peso_reale, peso_stimato,
        /*x_numero_civico_old,*/ riferimento, /*x_coord_lat, x_coord_long,*/ id_utenza, nome_attivita,
        modificato_da, data_ultima_modifica, percent_riempimento, /*x_id_elemento_privato,*/ freq_stimata, numero_civico,
        lettera_civico, colore_civico, note
        FROM elem.elementi;'''

    #27

    try:
        curr.execute(select_elementi_sit)
        id_e=curr.fetchall()
    except Exception as e:
        logger.error(e)

    c=0
    for ee in id_e:
        '''Cerco se esiste in SIT PROG'''
        select_prog='''select * from elem.elementi where id_elemento = %s'''
        try:
            curr_p.execute(select_prog,(int(ee[0]),))
            id_e1=curr_p.fetchall()
        except Exception as e:
            logger.error(e)
        # se c'è l'elemento non lo tocco
        #altrimenti
        if len(id_e1)<1:
            select_prog2='''select * from elem.elementi where id_piazzola = %s and tipo_elemento in 
            (
            select tipo_elemento from elem.tipi_elemento te where descrizione ilike %s
            ) '''
            try:
                #logger.info(query_elementi)
                curr_p1.execute(select_prog2, (ee[2], '%'+'bilat'+'%'))
                id_e_bil=curr_p1.fetchall()
            except Exception as e:
                logger.error(e)
            # se non ci sono bilaterali nella stessa piazzola
            insert_elementi='''INSERT INTO elem.elementi
            (id_elemento,tipo_elemento, id_piazzola, /*difficolta, */ id_asta, /*old_idelem,
            id_cliente,*/ posizione, dimensione, privato, peso_reale, peso_stimato,
            /*numero_civico_old,*/ riferimento, /*coord_lat, coord_long, */id_utenza, nome_attivita,
            modificato_da, data_ultima_modifica, percent_riempimento, /*x_id_elemento_privato,*/ freq_stimata, numero_civico,
            lettera_civico, colore_civico, note)
            VALUES(%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,
            %s,%s);'''
            if len(id_e_bil)<1:
                curr_p2.execute(insert_elementi,(ee[0],ee[1],ee[2],ee[3],ee[4],ee[5],ee[6],ee[7],ee[8],ee[9],ee[10],ee[11],ee[12],ee[13],ee[14],ee[15],ee[16],ee[17],ee[18],ee[19]))
                c+=1

    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
    logger.info("{} elementi di SIT aggiunti".format(c))

    


    curr.close()
    curr_p.close()
    curr_p1.close()
    curr_p2.close()
    conn.close()


    logger.info('Ri-connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    logger.info('''Cerco elementi su SIT Prog e non su sit (forse da eliminare)''')
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
        #logger.info(query_elementi)
        curr_p.execute(select_elementi_sit_prog, ('%'+'bilat'+'%',))
        id_e_bil=curr_p.fetchall()
    except Exception as e:
        logger.error(select_elementi_sit_prog)
        logger.error(e)


    c=0
    for ee in id_e_bil:
        #logger.debug('''Cerco se elemento {} esiste in SIT'''.format(ee[0]))
        select_prog='''SELECT * FROM elem.elementi where id_elemento=%s; '''
        try:
            curr.execute(select_prog,(int(ee[0]),))
            id_e1=curr.fetchall()
        except Exception as e:
            logger.error(select_prog)
            logger.error(e)
        # se c'è l'elemento non lo tocco
        #altrimenti
        if len(id_e1)<1:
            #logger.info('Elemento {} da eliminare'.format(ee[0]))
            delete_prog='''DELETE FROM elem.elementi_aste_percorso
            WHERE id_elemento = %s;
            DELETE FROM elem.elementi
            WHERE id_elemento = %s;'''
            try:
                curr_p1.execute(delete_prog,(ee[0],ee[0]))
            except psycopg2.Error as e:
                logger.error(e)
                logger.error(e.pgerror)
                logger.error(e.diag.message_detail)
            c+=1
    

    logger.info("{} elementi di SIT PROG eliminati".format(c))
    curr.close()
    ########################################################################################
    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
    conn_p.commit()
    ########################################################################################
    curr_p.close()
    curr_p1.close()

    conn.close()
    conn_p.close()


    logger.info('Ri-Connessione al db SIT con autocommit')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = True


    logger.info('Ri-Connessione al db SIT PROG con autcommit')
    conn_p = psycopg2.connect(dbname=db_prog,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr_p = conn_p.cursor()
    conn_p.autocommit = True


    vacuum_sql1='''vacuum analyze elem.elementi'''
    vacuum_sql2='''vacuum analyze geo.piazzola'''

    try:
        curr.execute(vacuum_sql1)
    except Exception as e:
        logger.error('curr')
        logger.error(vacuum_sql1)
        logger.error(e)


    curr.close()
    
    
    try:
        curr_p.execute(vacuum_sql1)
    except Exception as e:
        logger.error('currp')
        logger.error(vacuum_sql1)
        logger.error(e)
    conn_p.commit()
    curr_p.close()



    curr = conn.cursor()
    curr_p = conn_p.cursor()


    try:
        curr.execute(vacuum_sql2)
        curr_p.execute(vacuum_sql2)
    except Exception as e:
        logger.error(vacuum_sql2)
        logger.error(e)


    curr.close()
    curr_p.close()
    

    ''' Cerco elementi vetro nelle piazzole bilaterali (già fatto)'''


    logger.info("Fine di tutto. Chiudo le connessioni")      
    conn.close()
    conn_p.close()
    
    # cerco se ci sono stati errori
    count=len(open(errorfile).readlines(  ))
    if  count >0:
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)



if __name__ == "__main__":
    main()   