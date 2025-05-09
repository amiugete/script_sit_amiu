#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2025
# Roberto Marzocchi - Roberta Fagandini

'''
Script per sincronizzare gli utenti del duale con quelli delle mappe tematiche 
Si collega al DB DWH su amiupostgres e sincronizza con il DB di lizmap su amiugis

Poi c'è il dispatcher (programma PHP che reindirizza da un sistema all'altro sulla base del link predisposo da WingSoft)
E' un accrocchio un po' bruttino
'''

import os, sys
import psycopg2
import logging

from credenziali import *

from invio_messaggio import *


path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

# Create a custom logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
    ]
)

logger = logging.getLogger()

# Create handlers
c_handler = logging.FileHandler(filename=errorfile, encoding='utf-8', mode='w')
#f_handler = logging.StreamHandler()
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
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # Mi connetto a DWH (PostgreSQL su amiupostgres)
    nome_db=db_dwh
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    
    
    
    # Mi connetto a lizmap_mappenew3 (PostgreSQL su amiupostgres)
    nome_db=db_lizmap_dwh2
    logger.info('Connessione al db {}'.format(nome_db))
    conn_m = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_webroot,
                        password=pwd_webroot,
                        host=host_amiugis)

    curr_m = conn_m.cursor()
    
    
    
    
    ##########################################################################################
    # 1 - tabella utenti
    
    select_dwh_user='''select distinct
        u.email as usr_login,
        u.email as usr_email,
        %s as usr_password,
        u.firstname as usr_firstname, 
        u.lastname as usr_lastname, 
        1 as status, 
        now() as create_date
        from dwh.users u '''

    
    try:
        curr.execute(select_dwh_user, (pwd_users_duale,))
        users=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(select_dwh_user)
    
    # creo un'array per poi gestire i delete
    nomi_utenti_dwh=[]    
    
    upsert_users='''INSERT INTO public.jlx_user (
                    usr_login, usr_email, usr_password,
                    firstname, lastname, comment, status, create_date) 
                    values 
                    (%s, %s, %s, %s, %s, 'automatic_user_duale', 2, now()) 
                    ON CONFLICT (usr_login) 
                    DO UPDATE  SET 
                    usr_email=%s, firstname=%s, lastname=%s, 
                    comment='automatic_user_duale' '''
    
    
    
    for uu in users:
        # popolo lista utenti dwh
        nomi_utenti_dwh.append(uu[0])
        # faccio insert_update
        try:
            curr_m.execute(upsert_users, (uu[0], uu[1], uu[2], uu[3], uu[4],
                                        uu[1], uu[3], uu[4]))
        except Exception as e:
            logger.error(e)
            logger.error(upsert_users)
            logger.error('''login:{},
                        mail:{},
                        pwd:{},
                        firstname:{},
                        lastname:{}'''.format(uu[0], uu[1], uu[2], uu[3], uu[4]))
    # commit    
    conn_m.commit()
    
    
    curr.close()
    curr_m.close()
    
    ##########################################################################################
    # 2 - creo gruppo __priv_ etc
    curr = conn.cursor()
    curr_m = conn_m.cursor()
    
    select_priv='''select distinct
        concat('__priv_', u.email) as id_aclgrp,
        u.email as "name",
        2 as grouptype,
        u.email as ownerlogin
        from dwh.users u'''
        
    try:
        curr.execute(select_priv)
        g_priv=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(select_priv)
        
    upsert_gpriv='''
    INSERT INTO public.jacl2_group (id_aclgrp, "name", grouptype, ownerlogin) 
    values
    (%s, %s, %s, %s) 
    ON CONFLICT (id_aclgrp) DO NOTHING'''
    
    for gp in g_priv:
        try:
            curr_m.execute(upsert_gpriv, (gp[0], gp[1], gp[2], gp[3]))
        except Exception as e:
            logger.error(e)
            logger.error(upsert_gpriv)
            logger.error('0: {0}, 1:{1}, 2:{2}, 3:{3}'.format(gp[0], gp[1], gp[2], gp[3]))
    
    
    
    # commit    
    conn_m.commit()
    
    
    curr.close()
    curr_m.close()
    
    
    ##########################################################################################
    # 3 - jacl2_user_group
    curr = conn.cursor()
    curr_m = conn_m.cursor()
    
    # qua poi bisogna di nuovo gestire il delete a parte

    select_ug='''select distinct
        u.email as login,
        ac.prefisso_utenti as  id_aclgrp
        from dwh.users u 
        join dwh.users_geo_permissions ugp on ugp.id_user  = u.user_id
        join dwh.anagrafica_comuni ac on ac.id_zona  = ugp.id_zona
        union 
        select distinct
        u.email as login ,
        'users' as id_aclgrp
        from dwh.users u 
        union 
        select distinct
        u.email as login ,
        concat('__priv_', u.email)
        from dwh.users u
        order by 1 '''
    
    #definisco gli array che userò per i delete
    ug_user=[]
    ug_group=[]
    try:
        curr.execute(select_ug)
        user_group=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.error(select_ug)
        
    upsert_ug='''INSERT INTO public.jacl2_user_group (login, id_aclgrp) 
values (%s, %s)
ON CONFLICT (login, id_aclgrp)  DO UPDATE  SET
login=%s, id_aclgrp=%s
'''
    
    
    for ug in user_group:
        ug_user.append(ug[0])
        ug_group.append(ug[1])
        try:
            curr_m.execute(upsert_ug, (ug[0], ug[1], ug[0], ug[1]))
        except Exception as e:
            logger.error(e)
            logger.error(upsert_gpriv)
            logger.error('0: {0}, 1:{1}'.format(ug[0], ug[1]))
    
    
    # commit    
    conn_m.commit()
    
    # da gestire ancora i delete (2 flussi)
    
    curr_m.close()
    conn_m.close()
    curr.close()
    conn.close()
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)


if __name__ == "__main__":
    main()   