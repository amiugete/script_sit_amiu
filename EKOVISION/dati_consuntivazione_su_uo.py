#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) scarico dati da SFTP Ekovision

2) processo il file json

3) se processo OK lo copio in spazio archiviazione


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


import requests
from requests.exceptions import HTTPError

import logging

#path=os.path.dirname(sys.argv[0]) 

# per scaricare file da EKOVISION
import pysftp

import json



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
f_handler = logging.StreamHandler()
#f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


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


import fnmatch



def main():
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logging.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_ekovision='20231120'
    
    
    
    cartella_sftp_eko='sch_lav_cons/out/'    
    logger.info('Leggo e scarico file SFTP da cartella {}'.format(cartella_sftp_eko))
    


    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    
    # Mi connetto al DB oracle UO
    cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
    #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
    parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
    logger.debug(parametri_con)
    con = cx_Oracle.connect(parametri_con)
    logger.info("Versione ORACLE: {}".format(con.version))
    
    cur = con.cursor()
    
    
    
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd(cartella_sftp_eko): #chdir to public
            #print(srv.listdir('./'))
            for filename in srv.listdir('./'):
                logger.debug(filename)
                if fnmatch.fnmatch(filename, "sch_lav_consuntivi*"):
                    srv.get(filename, path + "/eko_output/" + filename)
                    logger.debug('Scaricato file {}'.format(filename))
                    
                    
                    
                    logger.info ('Inizio processo file'.format(filename))   
                    
                    # imposto a 0 un controllo sulla lettura del file
                    check_lettura=0
                    
                    # Opening JSON file
                    f = open(path + "/eko_output/" + filename)
                    
                    # returns JSON object as 
                    # a dictionary
                    data = json.load(f)
                    
                    i=0
                    while i<len(data):
                        logger.debug('{} - Leggo dati della scheda di lavoro {}'.format(i, data[i]['id_scheda']))
                        if data[i]['data_esecuzione_prevista']>=data_start_ekovision:
                            ''' devo leggere quello che c'è in
                            -   cons_conferimenti 
                                    --> pesi percorsi
                            -   cons_ris_tecniche
                            -   cons_ris_umane
                                    --> hist_servizi
                            -   cons_works
                                    tipo_rec - TRATTI STRADALI   
                                    --> 
                            '''
                            
                            # popolamento hist_servizi
                            
                            # STEP 0 mi prendo id_ser_per_uo
                            query0='''SELECT ID_SER_PER_UO , ID_TURNO, ID_UO 
                            FROM ANAGR_SER_PER_UO aspu WHERE ID_PERCORSO LIKE :c1
                            AND to_date(:c2, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE '''
                            
                            
                            
                            try:
                                cur.execute(query0, (data[i]['codice_serv_pred'], data[i]['data_esecuzione_prevista']))
                                ii_ss=cur.fetchall()
                            except Exception as e:
                                logger.error(query0)
                                logger.error(e)
                                check_lettura+=1                                            

                            id_rimessa=''
                            id_ut=''
                            for ispu in ii_ss:
                                id_ser_per_uo=ispu[0]
                                id_turno=ispu[1]
                                if int(ispu[2])==16 or int(ispu[2])==17:
                                    id_rimessa=ispu[2]
                                else:
                                    id_ut=ispu[2]
                            
                            # STEP 1 cerco lo sportello o gli sportelli
                            
                            sportello=''
                            s=0
                            while s<len(data[i]['cons_ris_tecniche']):
                                # con la funzione strip e usando lo spazio come separatore fra sportelli 
                                # non dovrebbero servire condizioni che distinguano il primo sportello dagli altri
                                sportello='{} {}'.format(sportello, data[i]['cons_ris_tecniche'][s]['cod_matricola_ristec']).strip() 
                                s+=1
                            
                            
                            
                            # ciclo sulle persone 
                            
                            p=0
                            while p<len(data[i]['cons_ris_umane']):
                                
                                # STEP 2 mi ricavo la persona, la durata e il turno (se disponibile)
                                if id_rimessa!='' and data[i]['cons_ris_umane'][p]['id_mansione']==33:
                                    id_ut_ok=id_rimessa
                                elif id_ut != '' and data[i]['cons_ris_umane'][p]['id_mansione']!=33 :
                                    id_ut_ok=id_ut
                                else:
                                    logger.error('Problema con attribuzione UT')
                                    logger.error('Dipendente {}'.format(data[i]['cons_ris_umane'][p]['cod_dipendente']))
                                    logger.error('Mansione (id ekovision) {}'.format(data[i]['cons_ris_umane'][p]['id_mansione']))
                                    logger.error('Id ut {}'.format(id_ut))
                                    logger.error('Id rimessa {}'.format(id_rimessa))
                                    logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                    logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))                                    
                                    logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                    exit()
                                    
                                idpersona=data[i]['cons_ris_umane'][p]['cod_dipendente']
                                durata = 0
                                o=0
                                while o<len(data[i]['cons_ris_umane'][p]['cons_risum_orari']):
                                    
                                    data_ora_start='{} {}'.format(
                                        data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_ini'],
                                        data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_ini']
                                        )
                                    data_ora_fine='{} {}'.format(
                                        data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_fine'],
                                        data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_fine']
                                        )
                                    
                                    fmt='%Y%m%d %H%M%S'
                                    data_ora_start_ok = datetime.strptime(data_ora_start, fmt)
                                    data_ora_fine_ok = datetime.strptime(data_ora_fine, fmt)
                                    # calcolo differenza in minuti ()
                                    durata+=(data_ora_fine_ok - data_ora_start_ok).total_seconds() / 60.0
                                    
                                    o+=1
                            
                            
                                logger.debug('{}, {}, {}, {}, {}'.format(id_ser_per_uo, data[i]['data_esecuzione_prevista'], sportello, idpersona, durata))
                                
                                #################################################
                                
                                # devo fare insert o update se trovo terna di id_ser_per_uo / data / idpersona
                                
                                #################################################
                                
                                query_select='''SELECT * FROM HIST_SERVIZI hs 
                                WHERE DTA_SERVIZIO = to_date(:h1,'YYYYMMDD')
                                AND ID_PERSONA = :h2
                                AND ID_SER_PER_UO = :h3'''
                                
                                try:
                                    cur.execute(query_select, (data[i]['data_esecuzione_prevista'], idpersona, id_ser_per_uo)
                                                )
                                    #cur1.rowfactory = makeDictFactory(cur1)
                                    persone_su_uo=cur.fetchall()
                                except Exception as e:
                                    logger.error(query_select)
                                    logger.error(e)
                                
                                
                                if (len(persone_su_uo)==1):
                                    query_update='''UPDATE UNIOPE.HIST_SERVIZI SET 
                                    ID_UO_LAVORO=:h1, DURATA=:h2, ID_TURNO=:h3,
                                    SPORTELLO=:h4
                                    WHERE DTA_SERVIZIO=to_date(:h5,'YYYYMMDD') AND 
                                    ID_SER_PER_UO=:h6 AND 
                                    ID_PERSONA=:h7'''
                                    
                                    try:
                                        cur.execute(query_update, (id_ut_ok, durata, id_turno, sportello,
                                                                data[i]['data_esecuzione_prevista'], 
                                                                id_ser_per_uo, idpersona)
                                                    )
                                    except Exception as e:
                                        logger.error(query_update)
                                        logger.error(e) 
                                        
                                elif (len(persone_su_uo)==0):
                                    query_insert='''INSERT INTO UNIOPE.HIST_SERVIZI 
                                    (DTA_SERVIZIO, ID_SER_PER_UO, ID_PERSONA,
                                    PROG_SERVIZIO, ID_UO_LAVORO, DURATA,
                                    ID_TURNO, SPORTELLO) 
                                    VALUES(to_date(:h1,'YYYYMMDD'), :h2, :h3,
                                    1 , :h4, :h5,
                                    :h6, :h7)'''
                                    try:
                                        cur.execute(query_insert, (data[i]['data_esecuzione_prevista'], 
                                                                id_ser_per_uo, idpersona,
                                                                id_ut_ok, durata, 
                                                                id_turno, sportello)
                                                    )
                                    except Exception as e:
                                        logger.error(query_insert)
                                        logger.error(e)
                                    
                                else:
                                    logger.error('In HIST_SERVIZI ci sono troppe righe')
                                    logger.error('Dipendente {}'.format(data[i]['cons_ris_umane'][p]['cod_dipendente']))
                                    logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                    logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                    logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                    exit()    
                                
                                
                                
                                
                                p+=1
                           
                            cur.close()
                            cur = con.cursor()
                            
                            # popolamento pesi
                            c=0 # conferimenti
                            while c<len(data[i]['cons_conferimenti']):
                                # con la funzione strip e usando lo spazio come separatore fra sportelli 
                                # non dovrebbero servire condizioni che distinguano il primo sportello dagli altri
                                logger.debug('Ci sono dei conferimenti')
                                data_percorso=data[i]['data_pianif_iniziale']
                                data_conferimento=data[i]['cons_conferimenti'][c]['data_rilevazione']
                                oc= data[i]['cons_conferimenti'][c]['ora_rilevazione']
                                ora_conferimento=oc[:2] + ':'+ oc[2:2]+oc[4:]
                                peso_netto=float(data[i]['cons_conferimenti'][c]['peso_netto'])
                                peso_lordo=float(data[i]['cons_conferimenti'][c]['peso_lordo'])
                                impianto=data[i]['cons_conferimenti'][c]['cod_sede_dest_ext'].split('_')
                                imp_cod_ecos=impianto[0]
                                uni_cod_ecos=impianto[1]
                                logger.debug('Conferimento {} -  {}, {}, {}, {}, {}, {}'.format(c,data_percorso, data_conferimento, ora_conferimento, imp_cod_ecos, uni_cod_ecos, peso_netto))  
                                c+=1
                                #exit()
                                # devo vedere che non ci sia già un conferimento (registrato come PROVENIENZA = 'ECOS' e COD_PROTOCOLLO = 838) in tal caso non faccio niente 
                                
                                
                                
                                #altrimenti
                                
                                # ID_UO_TITOLARE, COD_CER, DESCR_RIFIUTO vanno in qualche modo recuperati
                                
                                # se il peso lordo è 0 vuol dire che il peso proviene da ECOS quindi non serve nemmeno provare a re-inserirlo (solo perdita di tempo)
                                if peso_lordo>0.0:
                                    
                                    select_query='''SELECT * FROM TB_PESI_PERCORSI tpp 
                                    WHERE PROVENIENZA = 'RIMESSA'
                                    AND DATA_PERCORSO = to_date(:c1, 'YYYYMMDD') 
                                    AND ID_SER_PER_UO = :c2
                                    AND NOTE = :c3'''
                                    
                                    
                                    try:
                                        cur.execute(select_query, (data[i]['data_esecuzione_prevista'], id_ser_per_uo,
                                                               data[i]['cons_conferimenti'][c]['id'])
                                                )
                                        #cur1.rowfactory = makeDictFactory(cur1)
                                        conferimenti_su_uo=cur.fetchall()
                                    except Exception as e:
                                        logger.error(select_query)
                                        logger.error(e)
                                    
                                    
                                    if len(conferimenti_su_uo)==0:
                                        # nelle note ci metto l'ID
                                        insert_query='''INSERT INTO UNIOPE.TB_PESI_PERCORSI (
                                        ID_SER_PER_UO, DATA_PERCORSO, 
                                        DATA_CONFERIMENTO, ORA_CONFERIMENTO,
                                        PESO, DESTINAZIONE, PROVENIENZA, INS_DATE, 
                                        ID_UO_TITOLARE, 
                                        COD_CER, DESCR_RIFIUTO, NOTE) 
                                        VALUES
                                        (:c1, to_date(:c2, 'YYYYMMDD'), 
                                        to_date(:c3, 'YYYYMMDD'), 
                                        :c4,
                                        :c5, 
                                        (SELECT ID_DESTINAZIONE 
                                        FROM ANAGR_DESTINAZIONI ad 
                                        WHERE IMP_COD_ECOS =:c6 
                                        AND UNI_COD_ECOS =:c7),'RIMESSA', sysdate,
                                        (SELECT ID_TITOLARE FROM PERCORSI_UT_TITOLARE put
                                        WHERE ID_PERCORSO = :c8 AND 
                                        to_date(:c2, 'YYYYMMDD') BETWEEN DATA_INIZIO AND DATA_FINE)
                                        (SELECT as2.CER  
                                        FROM ANAGR_SERVIZI as2 
                                        JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                        WHERE ID_SERVIZIO =
                                            (SELECT ID_SERVIZIO 
                                            FROM ANAGR_SER_PER_UO 
                                            WHERE ID_SER_PER_UO=:c1
                                            )),
                                        (SELECT ac.DESCR_SEMPL  
                                        FROM ANAGR_SERVIZI as2 
                                        JOIN ANAGR_CER ac ON ac.CODICE_CER = as2.CER  
                                        WHERE ID_SERVIZIO =
                                            (SELECT ID_SERVIZIO 
                                            FROM ANAGR_SER_PER_UO 
                                            WHERE ID_SER_PER_UO=:c1
                                            )),
                                        :c9);'''
                                        
                                        try:
                                            cur.execute(insert_query, (
                                                                id_ser_per_uo,
                                                                data[i]['data_esecuzione_prevista'],
                                                                data_conferimento,
                                                                ora_conferimento,
                                                                peso_netto,
                                                                imp_cod_ecos,
                                                                uni_cod_ecos,
                                                                data[i]['codice_serv_pred'],
                                                                data[i]['cons_conferimenti'][c]['id'])
                                                    )
                                        except Exception as e:
                                            logger.error(insert_query)
                                            logger.error(e)
                                        
                                        
                                    elif len(conferimenti_su_uo)==1:
                                        # da fare UPDATE
                                        update_query='''UPDATE UNIOPE.TB_PESI_PERCORSI 
                                        PESO=0:c1, 
                                        DESTINAZIONE=(SELECT ID_DESTINAZIONE 
                                        FROM ANAGR_DESTINAZIONI ad 
                                        WHERE IMP_COD_ECOS =:c2 
                                        AND UNI_COD_ECOS =:c3),
                                        INS_DATE=sysdate,
                                        WHERE PROVENIENZA = 'RIMESSA'
                                        AND DATA_PERCORSO = to_date(:c4, 'YYYYMMDD') 
                                        AND ID_SER_PER_UO = :c5
                                        AND NOTE = :c6'''
                                        try:
                                            cur.execute(update_query, (
                                                                peso_netto,
                                                                imp_cod_ecos,
                                                                uni_cod_ecos,
                                                                data[i]['data_esecuzione_prevista'],
                                                                id_ser_per_uo,
                                                                data[i]['cons_conferimenti'][c]['id'])
                                                    )
                                        except Exception as e:
                                            logger.error(update_query)
                                            logger.error(e)
                                    else:
                                        logger.error('Ci sono troppi conferimenti con ID {}'.format(data[i]['cons_conferimenti'][c]['id']))
                                        logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                        logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                        logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                        error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                        exit()    
                                
                            
                            
                            
                            
                            cur.close()
                            cur = con.cursor()
                            
                            
                            # consuntivazione 
                            t=0
                            elenco_codici_via=[] # re-inizializzo ogni volta
                            elenco_elementi=[] # re-inizializzo ogni volta
                            elenco_piazzole=[]  # da usare per calcolo elementi non vuotati 
                            elenco_tappe=[] # da usare per calcolo elementi non vuotati
                            logger.debug('Ho inizializzato gli array. La lunghezza è {}'.format(len(elenco_tappe)))
                            ripasso=0
                            while t<len(data[i]['cons_works']):
                                
                                
                                if data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                    logger.debug('Consuntivazione spazzamento')
                                     # SU SIT cerco info sul tratto
                                
                                    elenco_codici_via.append(int(data[i]['cons_works'][t]['cod_tratto'].strip()))
                                    if int(data[i]['cons_works'][t]['pos'])>0 and int(data[i]['cons_works'][t]['flg_non_previsto'])==0:
                                        select_sit_per_tappa='''select codice_modello_servizio, ordine,  a.id_via, at.nota, at.ripasso 
                                    from 
                                        (SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti vpet 
                                        union 
                                        SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs vpeto) at
                                    join elem.aste a on a.id_asta = at.codice
                                    where codice_tipo_servizio = %s and codice_modello_servizio =  %s
                                    and codice = %s 
                                    and (%s between data_inizio and coalesce(data_fine,'20991231'))
                                    and ordine=%s'''
                                    #la query è la stessa i dati sono diversi nei 2 casi
                                        try:
                                            curr.execute(select_sit_per_tappa, (data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_tratto']),
                                                                                data[i]['data_pianif_iniziale'],
                                                                                int(data[i]['cons_works'][t]['pos'])
                                                                                ))
                                            tappe=curr.fetchall()
                                        except Exception as e:
                                            logger.error(select_sit_per_tappa)
                                            logger.error('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_tratto']),
                                                                                data[i]['data_pianif_iniziale'],
                                                                                int(data[i]['cons_works'][t]['pos'])
                                                                                ))
                                            logger.error(e)
                                        
                                        ct=0
                                        for tt in tappe:
                                            ordine=tt[1]
                                            id_via=tt[2]
                                            nota_via=tt[3]
                                            logger.debug('Ordine {} - Id_via {} - Nota {}'.format(tt[1],tt[2],tt[3]))
                                            if ct>=1:
                                                logger.error('Trovata più di una tappa')
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()                                       
                                            ct+=1
                                        
                                        if ct == 0:
                                            logger.error('Tappa non trovata su SIT')
                                            logger.error(select_sit_per_tappa)
                                            logger.error('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_tratto']),
                                                                                data[i]['data_pianif_iniziale'],
                                                                                int(data[i]['cons_works'][t]['pos'])
                                                                                ))
                                            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                            exit()    
                                    
                                        if nota_via is None:
                                            nota_via='ND'
                                        
                                        query_id_tappa='''SELECT ID_TAPPA, DTA_IMPORT, DATA_PREVISTA 
                                        FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                        JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA
                                        WHERE ID_PERCORSO = :t1
                                        AND ID_VIA = :t2
                                        AND ID_ASTA = :t3
                                        AND (NVL(trim(to_char(NOTA_VIA)),'ND') LIKE :t4 OR CRONOLOGIA=:t5) 
                                        and trunc(DTA_IMPORT) = (SELECT max(trunc(DTA_IMPORT)) FROM CONS_PERCORSI_VIE_TAPPE 
                                        WHERE trunc(DTA_IMPORT) < to_date(:t6, 'YYYYMMDD') AND 
                                        ID_PERCORSO = :t1)'''
					

                                        
                                        try:
                                            cur.execute(query_id_tappa, (data[i]['codice_serv_pred'],
                                                                         id_via,
                                                                         int(data[i]['cons_works'][t]['cod_tratto']),
                                                                         nota_via.strip(),
                                                                         ordine, 
                                                                         data[i]['data_pianif_iniziale'])
                                                        )
                                            #cur1.rowfactory = makeDictFactory(cur1)
                                            tappe_uo=cur.fetchall()
                                        except Exception as e:
                                            logger.error(query_id_tappa)
                                            logger.error(e)
                                    
                                        ct=0
                                        for ttu in tappe_uo:
                                            logger.debug(ttu[0])
                                            id_tappa=ttu[0]
                                            if ct>=1:
                                                logger.error('Trovata più di una tappa')
                                                logger.error(query_id_tappa)
                                                logger.error('{} {} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                         id_via,
                                                                         int(data[i]['cons_works'][t]['cod_tratto']),
                                                                         nota_via, ordine,
                                                                         data[i]['data_pianif_iniziale']))
                                                #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                #exit()                                       
                                            ct+=1
                                        if ct == 0:
                                            logger.warning('Tappa non trovata su UO')
                                            logger.warning(query_id_tappa)
                                            logger.warning('{} {} {} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                         id_via,
                                                                         int(data[i]['cons_works'][t]['cod_tratto']),
                                                                         nota_via, ordine,
                                                                         data[i]['data_pianif_iniziale']))
                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                            #exit()
                                        
                                        else:     
                                            
                                            # da fare insert/update
                                            
                                            if int(data[i]['cons_works'][t]['flg_exec'])==1: #and int(data[i]['cons_works'][t]['cod_std_qualita'])==100:
                                                causale=100
                                            else:
                                                causale=int(data[i]['cons_works'][t]['cod_giustificativo_ext'])
                                            
                                            nota_consuntivazione=''
                                            
                                            query_select=''' 
                                            SELECT * 
                                            FROM CONSUNT_SPAZZAMENTO cs 
                                            WHERE DATA_CONS = to_date(:c1, 'YYYYMMDD')
                                            and id_TAPPA= :c2
                                            '''
                                            
                                            
                                            try:
                                                cur.execute(query_select, (data[i]['data_esecuzione_prevista'], id_tappa))
                                                #cur1.rowfactory = makeDictFactory(cur1)
                                                consuntivazioni_uo=cur.fetchall()
                                            except Exception as e:
                                                logger.error(select_query)
                                                logger.error(e)
                                            
                                            
                                            if len(consuntivazioni_uo)==0:
                                                query_insert='''INSERT INTO UNIOPE.CONSUNT_SPAZZAMENTO (
                                                        ID_PERCORSO, ID_VIA, QTA_SPAZZATA, 
                                                        CAUSALE_SPAZZ, NOTA, DATA_CONS,
                                                        ID_TAPPA,
                                                        ID_SERVIZIO, 
                                                        DATA_INS,
                                                        FIRMA, ORIGINE_DATO) VALUES
                                                        (:c1, :c2, :c3,
                                                        :c4, :c5, to_date(:c6, 'YYYYMMDD') ,
                                                        :c7,
                                                        (SELECT DISTINCT ID_SERVIZIO 
                                                        FROM ANAGR_SER_PER_UO aspu 
                                                        WHERE ID_PERCORSO = :c1
                                                        AND to_date(:c6, 'YYYYMMDD') BETWEEN DTA_ATTIVAZIONE AND DTA_DISATTIVAZIONE),
                                                        sysdate,
                                                        NULL, 'EKOVISION')'''
                                                try:
                                                    cur.execute(query_insert, (data[i]['codice_serv_pred'],
                                                                                int(id_via),
                                                                                int(data[i]['cons_works'][t]['cod_std_qualita']),
                                                                                causale,
                                                                                nota_consuntivazione,
                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                int(id_tappa)))
                                                    #cur1.rowfactory = makeDictFactory(cur1)
                                                except Exception as e:
                                                    logger.error(query_insert)
                                                    logger.error('1:{} 2:{} 3:{} 4:{} 5:{} 6:{} 7:{}'.format(data[i]['codice_serv_pred'],
                                                                                id_via,
                                                                                int(data[i]['cons_works'][t]['cod_std_qualita']),
                                                                                causale,
                                                                                nota_consuntivazione,
                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                int(id_tappa)))
                                                    logger.error(e)
                                                    #logger.error('Ci sono troppi conferimenti con ID {}'.format(data[i]['cons_conferimenti'][c]['id']))
                                                    #logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                    #logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                    #logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                    #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                    #exit()        
                                                
                                            elif len(consuntivazioni_uo)==1:
                                                query_update='''
                                                    UPDATE UNIOPE.CONSUNT_SPAZZAMENTO 
                                                    SET QTA_SPAZZATA=:c1, 
                                                    CAUSALE_SPAZZ=:c2, 
                                                    NOTA=:c3, 
                                                    DATA_INS=sysdate
                                                    WHERE DATA_CONS=to_date(:c4, 'YYYYMMDD') AND ID_TAPPA=:c5
                                               '''
                                                try:
                                                    cur.execute(query_update, (int(data[i]['cons_works'][t]['cod_std_qualita']),
                                                                                causale,
                                                                                nota_consuntivazione,
                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                id_tappa))
                                                except Exception as e:
                                                    logger.error(query_insert)
                                                    logger.error('{} {} {} {} {}'.format(int(data[i]['cons_works'][t]['cod_std_qualita']),
                                                                                causale,
                                                                                nota_consuntivazione,
                                                                                data[i]['data_esecuzione_prevista'], 
                                                                                id_tappa))
                                                    logger.error(e) 
                                            else: 
                                                logger.error('Problema consuntivazioni doppie su UO')
                                                logger.error('Id tappa {}'.format(id_tappa))
                                                logger.error('Data percorso progettata {}'.format(data[i]['data_pianif_iniziale']))
                                                logger.error('Data percorso effettiva {}'.format(data[i]['data_esecuzione_prevista']))  
                                                logger.error('Cod percorso {}'.format(data[i]['codice_serv_pred']))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                    
                                    
                                    
                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC':
                                    logger.debug('Consuntivazione raccolta')
                                    elenco_elementi.append(int(data[i]['cons_works'][t]['cod_componente']))
                                    logger.debug(int(data[i]['cons_works'][t]['cod_componente']))
                                    if int(data[i]['cons_works'][t]['pos'])>0 and int(data[i]['cons_works'][t]['flg_non_previsto'])==0:
                                        select_sit_per_tappa='''select codice_modello_servizio, ordine, 
                                        e.id_piazzola , at.nota, at.ripasso, at.codice, at.data_inizio, at.data_fine
                                        from 
                                            (SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti vpet 
                                            union 
                                            SELECT * FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs vpeto) at
                                        left join elem.elementi e on e.id_elemento = at.codice
                                        where codice_tipo_servizio = %s and codice_modello_servizio =  %s
                                        and codice = %s 
                                        and (%s between data_inizio and coalesce(data_fine,'20991231'))
                                        '''
                                        try:
                                            curr.execute(select_sit_per_tappa, (data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_componente']),
                                                                                data[i]['data_pianif_iniziale'])
                                                                                )
                                            tappe=curr.fetchall()
                                        except Exception as e:
                                            logger.error(select_sit_per_tappa)
                                            logger.error('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_componente']),
                                                                                data[i]['data_pianif_iniziale']                                                                                ))
                                            logger.error(e)
                                        
                                        counter=1
                                        ct=0
                                        for tt in tappe:
                                            #logger.debug(elenco_elementi.count(int(data[i]['cons_works'][t]['cod_componente'])))
                                            if counter==elenco_elementi.count(int(data[i]['cons_works'][t]['cod_componente'])):
                                                ordine=tt[1]
                                                id_piazzola=tt[2]
                                                ripasso=tt[4]
                                                logger.debug('Ordine {} - Id_via {} - Ripasso {}'.format(ordine, id_piazzola, ripasso))
                                                if ct>=1 :
                                                    logger.error('Trovata più di una tappa')
                                                    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                    exit()  
                                                ct+=1                                     
                                            
                                            counter+=1
                                        
                                        if ct == 0:
                                            logger.warning('Tappa non trovata su SIT')
                                            logger.warning(select_sit_per_tappa)
                                            logger.warning('{} {} {} {} {}'.format(data[i]['cons_works'][t]['tipo_srv_comp'], 
                                                                                data[i]['codice_serv_pred'],
                                                                                int(data[i]['cons_works'][t]['cod_componente']),
                                                                                data[i]['data_pianif_iniziale'],
                                                                                int(data[i]['cons_works'][t]['pos'])
                                                                                ))
                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                            #exit() 
                                    
                                        query_id_tappa='''SELECT ID_TAPPA, DTA_IMPORT, DATA_PREVISTA, ID_PIAZZOLA 
                                            FROM CONS_PERCORSI_VIE_TAPPE cpvt 
                                            JOIN CONS_MACRO_TAPPA cmt ON cmt.ID_MACRO_TAPPA = cpvt.ID_TAPPA
                                            WHERE ID_PERCORSO = :t1
                                            AND ID_PIAZZOLA = :t2
                                            AND RIPASSO = :t3
                                            and trunc(DTA_IMPORT) = (SELECT max(trunc(DTA_IMPORT)) FROM CONS_PERCORSI_VIE_TAPPE 
                                            WHERE trunc(DTA_IMPORT) < to_date(:t4, 'YYYYMMDD') AND 
                                            ID_PERCORSO = :t1)'''
                                    
                                    
                                        try:
                                            cur.execute(query_id_tappa, (data[i]['codice_serv_pred'],
                                                                        id_piazzola,
                                                                        ripasso, 
                                                                        data[i]['data_pianif_iniziale'])
                                                        )
                                            #cur1.rowfactory = makeDictFactory(cur1)
                                            tappe_uo=cur.fetchall()
                                        except Exception as e:
                                            logger.error(query_id_tappa)
                                            logger.error(e)
                                    
                                        ct=0
                                        for ttu in tappe_uo:
                                            logger.debug(ttu[0])
                                            id_tappa=ttu[0]
                                            #logger.debug('Sono qua')                                           
                                            # verificare se nel caso di tipologie diverse la tappa sia diversa o meno (prendi percorso 0101367901)
                                            if len(elenco_tappe)==0:
                                                count_elementi=1
                                                if int(data[i]['cons_works'][t]['flg_exec'])==1:
                                                    count_fatti=1
                                                else:
                                                    count_fatti=0
                                            elif id_tappa == elenco_tappe[-1]:
                                                count_elementi+=1
                                                if int(data[i]['cons_works'][t]['flg_exec'])==1:
                                                    count_fatti+=1
                                            elif id_tappa != elenco_tappe[-1]:
                                                count_elementi=1
                                                if int(data[i]['cons_works'][t]['flg_exec'])==1:
                                                    count_fatti=1
                                                else:
                                                    count_fatti=0
                                            else:
                                                logger.error('Non capisco perchè finisca qua')
                                                
                                            elenco_piazzole.append(id_piazzola)
                                            elenco_tappe.append(id_tappa)
                                            if ct>=1:
                                                logger.error('Trovata più di una tappa')
                                                logger.error(query_id_tappa)
                                                logger.error('{} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                        id_piazzola,
                                                                        ripasso,
                                                                        data[i]['data_pianif_iniziale']))
                                                #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                #exit()                                       
                                            ct+=1
                                        if ct == 0:
                                            logger.warning('Tappa non trovata su UO')
                                            logger.warning(query_id_tappa)
                                            logger.warning('{} {} {} {}'.format(data[i]['codice_serv_pred'],
                                                                        id_piazzola,
                                                                        ripasso,
                                                                        data[i]['data_pianif_iniziale']))
                                            #error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                            #exit()
                                        
                                        else:
                                            # devo contare in qualche modo gli elementi
                                            
                                            
                                            query_insert='''INSERT INTO UNIOPE.CONSUNT_MACRO_TAPPA (
                                            ID_MACRO_TAPPA, QTA_ELEM_NON_VUOTATI, CAUSALE_ELEM,
                                            NOTA, DATA_CONS, ID_PERCORSO,
                                            ID_VIA, TIPO_ELEMENTO, ID_SERVIZIO,
                                            INS_DATE, MOD_DATE, ORIGINE_DATO) VALUES 
                                            (0, 0, 0, '', '', '', 0, 0, 0, sysdate, NULL, 'EKOVISION'); '''
                                    
                                else:
                                    logger.error('PROBLEMA CONSUNTIVAZIONE')
                                
                                t+=1
                                
                                
                            
                        else:
                            logger.debug('Non processo la scheda perchè antecedente alla data di partenza di Ekovision {}'.format(data_start_ekovision))
                        i+=1
        
                    # Closing file
                    f.close()
                    
                    
                    
                    
                    
                    
                    exit()
                else: 
                    logger.debug('Non scarico nessun file')
        
        #srv.put(file_preconsuntivazioni_ekovision) #upload file to nodejs/
        '''for filename in srv.listdir(cartella_sftp_eko):
            logger.debug(filename)
            if fnmatch.fnmatch(filename, "sch_lav_consuntivi*"):
                srv.get(cartella_sftp_eko + filename, "{0}/eko_output/" + filename)
                logger.debug('Scaricato file {}'.format(filename))
                
                exit()
        '''
        
        
        
        
        

        # Closes the connection
        srv.close()
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema invio SFTP  
    
    
    
    
    
    
    
    
    exit()
    
    
    
    
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    


     # cerco le schede su ekovision
        # PARAMETRI GENERALI WS
    
    
    
    percorsi_da_controllare=['0101033603',
'0101036303',
'0101352703',
'0103003704',
'0104002302',
'0104002402',
'0104002502',
'0104002601',
'0104002701',
'0104002801',
'0104002901',
'0203003501',
'0310000101',
'0507113602',
'0507116202',
'0603000502',
'0612001501',
'0612005702',
'0998001001',
'0999002901',
'0999003001']
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    schede_cancellare=''
    


    
    


    percorso_con_problemi=[]
     
    ii=0
    while ii < len(percorsi_da_controllare):
        check_error=0
       
        
        #exit()
        #gg=(-1)*datetime.today().weekday()
        gg=-30
        
        while gg <= 14-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            #logger.debug(day)
            # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
            
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': day,
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1,
                'cod_modello_srv': percorsi_da_controllare[ii]
                }
            response = requests.post(eko_url, params=params, data=data_json, headers=headers)
            #response.json()
            #logger.debug(response.status_code)
            try:      
                response.raise_for_status()
                check=0
                # access JSOn content
                #jsonResponse = response.json()
                #print("Entire JSON response")
                #print(jsonResponse)
            except HTTPError as http_err:
                logger.error(f'HTTP error occurred: {http_err}')
                check=1
            except Exception as err:
                logger.error(f'Other error occurred: {err}')
                logger.error(response.json())
                check=1
            if check<1:
                letture = response.json()
                #logger.info(letture)
                if len(letture['schede_lavoro']) >1 :
                    logger.debug(letture)
                    s=0
                    while s< len(letture['schede_lavoro']):
                        logger.debug('Percorso {0} giorno {1} ci sono {2} schede'.format(percorsi_da_controllare[ii], day, len(letture['schede_lavoro']))) 
                        id_scheda=letture['schede_lavoro'][s]['id_scheda_lav']
                        logger.info('Id_scheda:{}'.format(id_scheda))
                        percorso_con_problemi.append(percorsi_da_controllare[ii])
                        if letture['schede_lavoro'][s]['flg_eseguito']=='0' and s>0:
                            logger.info('Id_scheda da cancellare:{}'.format(id_scheda))
                            if schede_cancellare=='':
                                schede_cancellare='{}'.format(id_scheda)
                            else:
                                schede_cancellare='{},{}'.format(schede_cancellare,id_scheda)
                            #exit()  
                        s+=1                
            gg+=1 
        ii+=1
     

    
    
    k=0
    percorso_con_problemi_distinct=[]
    while k<len(percorso_con_problemi):
        logger.debug(k)
        if k==0:
            percorso_con_problemi_distinct.append(percorso_con_problemi[k])
            elenco_codici='{0}'.format(percorso_con_problemi[k])
        if k > 0 and percorso_con_problemi[k]!= percorso_con_problemi[k-1]:
            percorso_con_problemi_distinct.append(percorso_con_problemi[k])
            elenco_codici='{0} - {1}'.format(elenco_codici, percorso_con_problemi[k])
        k+=1
    
    
    # provo a mandare la mail
    try:
        if schede_cancellare!='':
            # Create a secure SSL context
            context = ssl.create_default_context()



        # messaggio='Test invio messaggio'


            subject = "ELIMINAZIONE SCHEDE LAVORO - Percorsi doppi per cui va eliminata la scheda di lavoro"
            
            ##sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            debug_email='roberto.marzocchi@amiu.genova.it'

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = debug_email
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Cambio frequenze"


            body='''I seguenti percorsi sono stati disattivati.<br>
            {0}
            <br><br>
            Bisogna eliminare <b>manualmente</b> le schede di lavoro su Ekovision. Usare la voce <i>Eliminazione massiva schede lavoro</i> 
            del menù scorciatoia di Ekovision e caricare la lista di ID schede da cancellare riportata nel seguito.
            Verificare il log e controllare a mano eventuali anomalie.
            <br><br>
            Elenco schede da cancellare: <br>
            {1}
            <br><br>
            AMIU Assistenza Territorio<br>
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(elenco_codici, schede_cancellare)
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))


            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(path1)
            immagine(message,logoname)
            
            

            
            
            text = message.as_string()

            logger.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            logger.info(invio)
    except Exception as e:
        logger.error(e) # se non fossi riuscito a mandare la mail
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      