#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script della gestione e invio dei dati delle timbrature

DA esipertbo (dblink su UO) minvio i dati sul db dwh per creare un progressivo e gestire le date/ora di aggiornamento

Da dwh spedisco i dati in modo incrementale a Ekovision



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect, os.path
#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta


import xlsxwriter

import psycopg2

import cx_Oracle

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from credenziali import *



# per mandare file a EKOVISION
import pysftp


#import requests

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)






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


# libreria per invio mail
import email, smtplib, ssl
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from invio_messaggio import *

# libreria per scrivere file csv
import csv



# function to return a named tuple
def makeNamedTupleFactory(cursor):
    columnNames = [d[0].lower() for d in cursor.description]
    import collections
    Row = collections.namedtuple('Row', columnNames)
    return Row


# funzionde per restituire un dizionario
def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow    
     

def main():
      


    # preparo gli array 
    id=[]
    cod_dip=[]
    dt_ass_dal=[]
    ore_ass_dal=[]
    dt_ass_al=[]
    ore_ass_al=[]
    cod_cau_ass=[]
    des_cau_ass=[]
    flg_deleted=[]
    flg_ass_contr=[]
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
    
     # Mi connetto al DB oracle UO
    logger.info('Connessione al db {}'.format(service_uo))
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
        
    # Mi connetto a dwh (PostgreSQL) per poi recuperare le mail
    nome_db=db_dwh
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    
    curr = conn.cursor()
    curr1 = conn.cursor()
    cur = con.cursor()
    
    select_data='''select to_char(max(data_ora)::date - interval '1' day, 'YYYYMMDD')::int as data 
from personale_ekovision.invio_assenze_ekovision iae''' 
    
    try:
        curr.execute(select_data)     
        data=curr.fetchall()     
    except Exception as e:
        logger.error(select_x_invio)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
    
    for d in data:
        data_start=int(d[0])
    curr.close()
    curr = conn.cursor()
    
    logger.debug(data_start)
    #exit()        
    
    query_timbrature='''SELECT ID_PERSONA,
        CLTIMBRA AS "DATA", lpad(to_char(MTTIMBRA), 4,'0') AS "ORARIO",
        trim(CDVERSOT) AS VERSO,
        trim(FLMANUAL) AS NOTE 
        FROM esipertbo.v_timbr_eko@sipedb a
        WHERE (cltimbra > :dd) 
        OR (trim(FLMANUAL) IS NOT NULL AND cltimbra > to_char(trunc((sysdate - interval '2' MONTH), 'MONTH'), 'YYYYMMDD'))
        ORDER BY 2,3 '''
    
    
    # cerco le assenze su UO (DB LINK DA ESIPERTBO)
    
    query_assenze='''SELECT ID_PERSONA AS COD_DIP,
        CLINIVAL AS DT_ASS_DAL,
        CASE 
            WHEN  length(MTORAINI)<3 THEN CAST(MTORAINI AS integer)
            WHEN  length(MTORAINI)=3 THEN (CAST(substr(MTORAINI,0,1) as integer)* 60 + cast(substr(MTORAINI,2,3)AS integer))
            WHEN  length(MTORAINI)=4 THEN (CAST(substr(MTORAINI,0,2) as integer)* 60 + cast(substr(MTORAINI,3,4)AS integer))
        END
        ORE_ASS_DAL,
        MTORAINI AS ORA_S,
        CASE
            WHEN CLFINVAL = CLINIVAL AND MTORAINI > MTORAFIN
            THEN CAST(to_char((to_date(CLFINVAL, 'YYYYMMDD') + interval '1' DAY),'YYYYMMDD') AS integer)
            ELSE CLFINVAL
        END DT_ASS_AL,
        CASE 
            WHEN  length(MTORAFIN)<3 THEN CAST(MTORAFIN AS integer)
            WHEN  length(MTORAFIN)=3 THEN CAST(substr(MTORAFIN,0,1) as integer)* 60 + cast(substr(MTORAFIN,2,3)AS integer)
            WHEN  length(MTORAFIN)=4 THEN CAST(substr(MTORAFIN,0,2) as integer)* 60 + cast(substr(MTORAFIN,3,4)AS integer)
        END
        ORE_ASS_AL,
        MTORAFIN AS ORA_E,
        CDCAUSAL AS COD_CAU_ASS, 
        ADCAUSAL AS DES_CAU_ASS
        FROM esipertbo.v_assenze_eko@sipedb
        WHERE 
        clinival > to_char(trunc((sysdate - interval '2' MONTH), 'MONTH'), 'YYYYMMDD')
        OR clfinval > to_char(trunc((sysdate - interval '2' MONTH), 'MONTH'), 'YYYYMMDD') 
        ORDER BY clinival, mtoraini'''
                
    try:
        #cur.execute(query_timbrature, (data_start,))
        cur.execute(query_assenze)
        cur.rowfactory = makeDictFactory(cur)
        assenze=cur.fetchall()
    except Exception as e:
        logger.error(query_assenze)
        logger.error(e)


    # gestione insert / UPDATE
    i=0
    for tt in assenze:
        i+=1
        if i%1000==0:
            logger.debug('''Insert / Update - {0} rows'''.format(i))
        # faccio il controllo di quanto ho su dwh
        
        
        """check_assenza_esiste = '''SELECT id, cod_dip, dt_ass_dal, ore_ass_dal, dt_ass_al, ore_ass_al,
        cod_cau_ass, des_cau_ass, flg_deleted, flg_ass_contr
        FROM personale_ekovision.imp_personale_assenze
        where cod_dip=%s and dt_ass_dal = %s  and ore_ass_dal = %s and dt_ass_al = %s  and ore_ass_al = %s''' """
        
        check_assenza_esiste = '''SELECT id, cod_dip, dt_ass_dal, ore_ass_dal, dt_ass_al, ore_ass_al,
        cod_cau_ass, des_cau_ass, flg_deleted, flg_ass_contr
        FROM personale_ekovision.imp_personale_assenze
        where cod_dip=%s and dt_ass_dal = %s  and ore_ass_dal = %s''' 
                
        try:
            #curr.execute(check_assenza_esiste, (int(tt["COD_DIP"]), int(tt["DT_ASS_DAL"]), int(tt["ORE_ASS_DAL"]), int(tt["DT_ASS_AL"]), int(tt["ORE_ASS_AL"])))     
            curr.execute(check_assenza_esiste, (int(tt["COD_DIP"]), int(tt["DT_ASS_DAL"]), int(tt["ORE_ASS_DAL"])))     
            check_t_e=curr.fetchall()     
        except Exception as e:
            logger.error(check_assenza_esiste)
            logger.error('Codice persona = {}'.format(tt["COD_DIP"]))
            logger.error('Data = {}'.format(tt["DT_ASS_DAL"]))
            logger.error('Ora = {}'.format(tt["ORE_ASS_DAL"]))
            logger.error(e)
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
        
        if len(check_t_e)==0:
            #insert
            query_insert= '''INSERT INTO personale_ekovision.imp_personale_assenze
                (id,
                cod_dip,
                dt_ass_dal, ore_ass_dal,
                dt_ass_al, ore_ass_al,
                cod_cau_ass, des_cau_ass,
                flg_deleted, flg_ass_contr, data_ultima_modifica, ora_s, ora_e)
                VALUES ( (select (coalesce(max(id),0)+1) from personale_ekovision.imp_personale_assenze),
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                0, 1, now(), %s, %s ) '''
            try:
                curr1.execute(query_insert, (int(tt["COD_DIP"]), int(tt["DT_ASS_DAL"]), tt["ORE_ASS_DAL"],tt["DT_ASS_AL"], tt["ORE_ASS_AL"], tt["COD_CAU_ASS"], tt["DES_CAU_ASS"] , int(tt["ORA_S"]), int(tt["ORA_E"])))     
            except Exception as e:
                logger.error(query_insert)
                logger.error('Codice persona = {}'.format(int(tt["COD_DIP"])))
                logger.error('DT_ASS_DAL = {}'.format(int(tt["DT_ASS_DAL"])))
                logger.error('ORE_ASS_DAL = {}'.format(tt["ORE_ASS_DAL"]))
                logger.error('DT_ASS_AL = {}'.format(tt["DT_ASS_AL"]))
                logger.error('ORE_ASS_AL = {}'.format(tt["ORE_ASS_AL"]))
                logger.error(e)
                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
        else:
            for tp in  check_t_e: # tp sta per assenza su PostgreSQL
                if tp[6].strip()!=tt["COD_CAU_ASS"].strip() or tp[7].strip()!= tt["DES_CAU_ASS"].strip() or int(tp[4])!=int(tt["DT_ASS_AL"]) or int(tp[5])!=int(tt["ORE_ASS_AL"]): # allora faccio update
                    query_update='''UPDATE personale_ekovision.imp_personale_assenze
                    SET  cod_cau_ass=%s, des_cau_ass=%s, dt_ass_dal=%s, ore_ass_dal=%s, dt_ass_al=%s, ore_ass_al=%s, data_ultima_modifica=now()
                    WHERE id=%s ; '''
                    try:
                        curr1.execute(query_update, (tt["COD_CAU_ASS"],tt["DES_CAU_ASS"],tt["DT_ASS_DAL"], tt["ORE_ASS_DAL"], tt["DT_ASS_AL"], tt["ORE_ASS_AL"], tp[0]))     
                    except Exception as e:
                        logger.error(query_insert)
                        logger.error('Codice persona = {}'.format(int(tt["COD_DIP"])))
                        logger.error('COD_CAU_ASS = {}'.format(tt["COD_CAU_ASS"]))
                        logger.error('DES_CAU_ASS = {}'.format(tt["DES_CAU_ASS"]))
                        logger.error('DT_ASS_DAL = {}'.format(tt["DT_ASS_DAL"]))
                        logger.error('ORE_ASS_DAL = {}'.format(tt["ORE_ASS_DAL"]))
                        logger.error('DT_ASS_AL = {}'.format(tt["DT_ASS_AL"]))
                        logger.error('ORE_ASS_AL = {}'.format(tt["ORE_ASS_AL"]))
                        logger.error(e)
                        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                        exit()
                #altrimenti non faccio nulla    
    conn.commit()
    logger.info('Fine insert/update DWH (esclusi delete da Esipertbo)')
    curr.close()
    curr1.close()
    cur.close()
    
    cur = con.cursor()
    curr = conn.cursor()
    curr1 = conn.cursor()
    
    
    
    # ora l'inverso.. devo controllare i delete             
    # parto da dwh e verifico se c'è nella vista su UO (DB LINK DA ESIPERTBO)       
    check_assenza_dwh = '''SELECT id, cod_dip, dt_ass_dal, ora_s, dt_ass_al, ora_e,
        cod_cau_ass, des_cau_ass, flg_deleted, flg_ass_contr
        FROM personale_ekovision.imp_personale_assenze
        where dt_ass_dal > to_char((now() - INTERVAL '3' MONTH),'YYYYMMDD')::int
          or  dt_ass_al  > to_char((now() - INTERVAL '3' MONTH),'YYYYMMDD')::int 
          or data_ultima_modifica > (now() - INTERVAL '6' HOUR)'''
    
    try:
        curr.execute(check_assenza_dwh)     
        check_a_d=curr.fetchall()     
    except Exception as e:
        logger.error(check_assenza_dwh)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
    
    
    i=0
    for ad in check_a_d:
        
        
        i+=1
        if i%1000==0:
            logger.debug('''Check for delete - {0} rows'''.format(i))
            
            
        select_esipertbo='''SELECT ID_PERSONA 
        FROM esipertbo.v_assenze_eko@sipedb
        WHERE ID_PERSONA = :c1 
        AND CLINIVAL = :c2 AND MTORAINI = :c3 
        AND CLFINVAL = :c4 AND MTORAFIN = :c5 '''
        
        
        try:
            cur.execute(select_esipertbo, (int(ad[1]),int(ad[2]),int(ad[3]),int(ad[4]),int(ad[5])))
            cur.rowfactory = makeDictFactory(cur)
            ass_esipert=cur.fetchall()
        except Exception as e:
            logger.error(select_esipertbo)
            logger.error(int(ad[1]))
            logger.error(int(ad[2]))
            logger.error(int(ad[3]))
            logger.error(int(ad[4]))
            logger.error(int(ad[5]))
            logger.error(e)
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
    
        if len(ass_esipert)==0:
            #devo aggiungere il file deleted su dwh e modificare la data 
            logger.warning('ASSENZA DIPENDENTE {0} DAL {1} minuti {2} AL {3} minuti {4} CANCELLATA'.format(ad[1],ad[2],ad[3],ad[4],ad[5]))
            query_update='''UPDATE personale_ekovision.imp_personale_assenze
                    SET  flg_deleted=1, data_ultima_modifica=now()
                    WHERE id=%s ; '''
            try:
                curr1.execute(query_update, (ad[0],))
            except Exception as e:
                logger.error(query_update)
                logger.error(e)
            
        # alrimenti nulla
    conn.commit()
    logger.info('Fine update DWH (delete da Esipertbo)') 
    curr.close()
    curr1.close()
    curr = conn.cursor()
    
    
    
    
    #exit()
    
    check_ekovision=200
    select_x_invio='''SELECT id, cod_dip, dt_ass_dal, ore_ass_dal, dt_ass_al, ore_ass_al,
        cod_cau_ass, des_cau_ass, flg_deleted, flg_ass_contr
        FROM personale_ekovision.imp_personale_assenze 
        where (id > (select max(progr) from personale_ekovision.invio_assenze_ekovision iae) 
        or data_ultima_modifica > (select max(data_ora) from personale_ekovision.invio_assenze_ekovision iae))
        and cod_dip in (select id_persona from personale_ekovision.personale p )
        order by id''' 
    
    try:
        curr.execute(select_x_invio)     
        assenze_x_invio=curr.fetchall()     
    except Exception as e:
        logger.error(assenze_x_invio)
        logger.error(e)
        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
        exit()
   
    for t_i in assenze_x_invio:
        
        id.append(int(t_i[0]))
        cod_dip.append(int(t_i[1]))
        dt_ass_dal.append(int(t_i[2]))
        ore_ass_dal.append(int(t_i[3]))
        dt_ass_al.append(int(t_i[4]))
        ore_ass_al.append(int(t_i[5]))
        cod_cau_ass.append(t_i[6])
        des_cau_ass.append(t_i[7])
        flg_deleted.append(t_i[8])
        flg_ass_contr.append(t_i[9])
        #max_progr=t_i[2]
    
    
    
    
    try:    
        nome_csv_ekovision="imp_personale_assenze_{0}.csv".format(giorno_file)
        file_preconsuntivazioni_ekovision="{0}/assenze/{1}".format(path,nome_csv_ekovision)
        fp = open(file_preconsuntivazioni_ekovision, 'w', encoding='utf-8')
                      
        fieldnames = ['id', 'cod_dip', 'dt_ass_dal', 'ore_ass_dal', 'dt_ass_al', 'ore_ass_al',
                      'cod_cau_ass', 'des_cau_ass', 'flg_deleted', 'flg_ass_contr']
      
        '''
        
        myFile = csv.DictWriter(fp, delimiter=';', fieldnames=dizionario[0].keys(), quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        # Write the header defined in the fieldnames argument
        myFile.writeheader()
        # Write one or more rows
        myFile.writerows(dizionario)
        
        # senza usare dizionario
        '''
        #myFile = csv.writer(fp, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        myFile = csv.writer(fp, delimiter=';')
        myFile.writerow(fieldnames)
        
        k=0 
        while k < len(id):
            if k%1000==0:
                logger.debug('''preparazione file csv - {0} rows'''.format(k))
            """logger.debug(int(id[k]))
            logger.debug(int(cod_dip[k]))
            logger.debug(int(dt_ass_dal[k]))
            logger.debug(int(ore_ass_dal[k]))
            logger.debug(int(dt_ass_al[k]))
            logger.debug(int(ore_ass_al[k]))
            logger.debug(cod_cau_ass[k])
            logger.debug(des_cau_ass[k])
            logger.debug(int(flg_deleted[k]))
            logger.debug(int(flg_ass_contr[k]))"""
            row=[int(id[k]), int(cod_dip[k]), 
                 int(dt_ass_dal[k]), int(ore_ass_dal[k]), 
                 int(dt_ass_al[k]), int(ore_ass_al[k]),
                cod_cau_ass[k], des_cau_ass[k],
                int(flg_deleted[k]), int(flg_ass_contr[k])]
            myFile.writerow(row)
            k+=1
            
        fp.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=102 # problema file variazioni


    #exit()
    logger.info('Invio file con la preconsuntivazione via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd('timbrature/in/'): #chdir to public
            srv.put(file_preconsuntivazioni_ekovision) #upload file to nodejs/

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP  
    
    curr.close()
    curr = conn.cursor()
    
    
    if check_ekovision==200 and len(id)>0:
        insert_max_id='''INSERT INTO personale_ekovision.invio_assenze_ekovision
        (progr, data_ora)
        VALUES
        (%s, now())'''
        try:
            curr.execute(insert_max_id, (max(id),))
        except Exception as e:
            logger.error(insert_max_id)
            logger.error(e)
            
        
        conn.commit()   
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    logger.info("Chiusura cursori e connessioni")
    curr.close()
    curr1.close()
    conn.close()
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      