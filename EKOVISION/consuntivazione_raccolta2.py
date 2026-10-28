#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script si occupa della consuntivazione raccolta 
con il nuovo tracciato con chiavi primarie:

'''

import json
#from msilib import type_short
import os  # ,shutil,glob
import re
import sys
from datetime import date, datetime, timedelta

import cx_Oracle
import psycopg2
import requests
import xlsxwriter
from requests.exceptions import HTTPError

#import getopt  # per gestire gli input

#import pymssql








currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import logging
import uuid

# per mandare file a EKOVISION
import pysftp

from credenziali import *

#import requests


path=os.path.dirname(sys.argv[0]) 
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
f_handler.setLevel(logging.INFO)


# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


cc_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

c_handler.setFormatter(cc_format)
f_handler.setFormatter(cc_format)


# libreria per scrivere file csv
import csv
# libreria per invio mail
import email
import mimetypes
import smtplib
import ssl
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from descrizione_percorso import *

from invio_messaggio import *


def main(SFTP_USER, SFTP_PWD,  datalav_debug=None, cod_percorso_debug=None):

    # controllo se logfile esiste 
    if os.path.exists(errorfile):
        logger.debug(f'{errorfile} esiste')
    else:
        logger.warning(f'{errorfile} NON esiste')

    if debug == 0:
        logger.info("Modalità normale")

    elif debug == 1:
        logger.info("Debug singolo percorso")
        logger.info(f'cod_percorso={cod_percorso_debug}, data={datalav_debug}')

    elif debug == 2:
        logger.info("Debug intervallo date")
        logger.info(datalav_debug)

    # resto del codice
    
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
  


    # preparo gli array 
    
    cod_percorso=[]
    data_percorso=[]
    id_turno=[]
    id_componente=[]
    id_tratto=[]
    flag_esecuzione=[]
    causale=[]
    nota_causale=[]
    sorgente_dati=[]
    data_ora=[]
    lat=[]
    long=[]
    ripasso=[]
    qual=[]
    mail_arr=[]
    id_asta_percorso = []
    id_elemento_asta_percorso = []
    chiave_cons=[]
    
    
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
    
    
        
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    # Mi connetto al DB consuntivazione (PostgreSQL) - HUB
    # commentato il 27/11/2025 --> ora lavoro sul DB totem
    '''
    nome_db=db_consuntivazione
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_consuntivazione,
                        password=pwd_consuntivazione,
                        host=host_hub)
    
    '''
    
    
    # Mi connetto al DB consuntivazione (PostgreSQL) - 
    nome_db=db_totem
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)
    
    
    curr = conn.cursor()
    curr1 = conn.cursor()
    currc = connc.cursor()
    

            
            
            
    # query per controllo causali
    query_causale='''select ct.id, ct.descrizione  
from totem.v_causali ct where id_ekovision = %s '''


    query_verifica_causale='''select ve.*, cpra.desc_percorso from raccolta.v_effettuati ve 
    left join raccolta.cons_percorsi_raccolta_amiu cpra 
on cpra.id_percorso = ve.idpercorso 
and ve.datalav between cpra.data_inizio and cpra.data_fine
where ve.idpercorso =%s  and ve.datalav = to_date(%s, 'YYYYMMDD') 
and ve.id_causale::int <> %s'''        
            
    
    # query (da lanciare su SIT) che verifica eventuali non previsti
    query_racc_np='''select distinct p.cod_percorso, 
    p.id_turno, 
    eap.id_elemento, 
    fo.freq_binaria as freq_elemento, 
    fo2.freq_binaria  as freq_percorso, 
    fo3.freq_binaria  as differenza,
    eap.ripasso, 
    greatest(ap.data_inserimento, eap.data_inserimento)::date+1 as data_inserimento,
    coalesce(eap.data_eliminazione, dpsu.data_fine_validita)::date+1 as data_eliminazione,
    dpsu.data_inizio_validita::date, 
    dpsu.data_fine_validita::date,
    concat(ap.id_asta_percorso,'_',ee.id_piazzola) as id_asta_percorso,
    eap.id_elemento_asta_percorso 
    from (select id_asta_percorso, id_percorso,data_inserimento, frequenza from elem.aste_percorso ap 
    union 
    select id_asta_percorso, id_percorso,data_inserimento, frequenza  from history.aste_percorso ap)
    ap 
    join elem.percorsi p on p.id_percorso = ap.id_percorso 
    join anagrafe_percorsi.date_percorsi_sit_uo dpsu on p.id_percorso = dpsu.id_percorso_sit 
    join (select id_asta_percorso, 
    id_elemento, frequenza, ripasso, data_inserimento, null as data_eliminazione,
    id_elemento_asta_percorso
    from elem.elementi_aste_percorso  
    union 
    select id_asta_percorso, 
    id_elemento, frequenza, ripasso, data_inserimento, data_eliminazione,
    id_elemento_asta_percorso
    from history.elementi_aste_percorso ) eap on ap.id_asta_percorso = eap.id_asta_percorso 
    join 
	(select id_elemento, id_piazzola  from elem.elementi e 
	union 
	select id_elemento, id_piazzola  from history.elementi e
	) ee on ee.id_elemento = eap.id_elemento
    join etl.frequenze_ok fo on fo.cod_frequenza = eap.frequenza::int
    join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
    join elem.servizi s on s.id_servizio = p.id_servizio 
    join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso 
    	and to_date(%s, 'YYYYMMDD') >= ep.data_inizio_validita 
    	and to_date(%s, 'YYYYMMDD') < ep.data_fine_ekovision 
    left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-eap.frequenza::int)
    where p.cod_percorso = %s
    and to_date(%s, 'YYYYMMDD') /*between dpsu.data_inizio_validita and dpsu.data_fine_validita*/
    between greatest(ap.data_inserimento, eap.data_inserimento)::date+1 
    and coalesce(eap.data_eliminazione, dpsu.data_fine_validita)::date+1
    and treg_eko.verify_daily_frequency(eap.frequenza::int, to_date(%s, 'YYYYMMDD'), ep.freq_settimane )=0
    /*and ap.frequenza is not null*/
    /*and eap.frequenza::int <> p.frequenza*/ 
    and s.riempimento > 0 '''
    
    
    
            
    # ciclo su elenco vie / note consuntivate
    
 
    
    # query che parte dal DB totem a partire da 27/11/2025
    if debug == 0:
            query_effettuati_totem='''select distinct 
            e.id, 
            t.id_percorso,
            e.datalav::date as datalav,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            string_agg(vc.descrizione, ', ') as descr_causale,
            e.id_causale as causale,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
            e.datainsert as data_insert, 
            t.id_tappa as tappa, 
            e.nota as note_totem /* è il campo con le note*/,
            string_agg(mu.mail, ', ') as mail, 
            t.tipo_elemento
            from raccolta.cons_percorsi_raccolta_amiu t
            join raccolta.effettuati_amiu e on e.tappa::bigint =  t.id_tappa::bigint
            /* controllo la validità dei percorsi */
            join servizi.servizi_per_ekovision spe 
                on spe.id_percorso = t.id_percorso and e.datalav between spe.data_inizio_validita and spe.data_fine_validita
            join totem.v_causali vc on vc.id = e.id_causale 
            --left join raccolta.effettuati_correzione_date ecd on e.id_percorso=ecd.id_percorso and e.datalav= ecd.datalav_errata 
            left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
            --left join raccolta.causali_testi ct on trim(replace(e.causale, ' - (no in questa giornata)', '')) ilike trim(ct.descrizione)
            left join servizi.mail_ut mu on mu.id_uo::int  = t.id_uo::int
            where 
            /*(trim(replace(e.causale, ' - (no in questa giornata)', '')) != '' /*or e.fatto >0*/) 
            and*/ datalav >= '2024-01-29'
            and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
            and e.id > (select coalesce(max(max_id),0) from raccolta.invio_consuntivazioni_ekovision ice)
            group by 
            e.id, 
            t.id_percorso,
            e.datalav::date ,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            --vc.descrizione as descr_causale,
            e.id_causale ,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) , 
            e.datainsert , 
            t.id_tappa, 
            e.nota,
            t.tipo_elemento
            order by 1'''
    elif debug ==1: 
        query_effettuati_totem='''select distinct 
            e.id, 
            t.id_percorso,
            e.datalav::date as datalav,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            string_agg(vc.descrizione, ', ') as descr_causale,
            e.id_causale as causale,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
            e.datainsert as data_insert, 
            t.id_tappa as tappa, 
            e.nota as note_totem /* è il campo con le note*/,
            string_agg(mu.mail, ', ') as mail,
            t.tipo_elemento
            from raccolta.cons_percorsi_raccolta_amiu t
            join raccolta.effettuati_amiu e on e.tappa::bigint =  t.id_tappa::bigint
            /* controllo la validità dei percorsi */
            join servizi.servizi_per_ekovision spe 
                on spe.id_percorso = t.id_percorso and e.datalav between spe.data_inizio_validita and spe.data_fine_validita
            join totem.v_causali vc on vc.id = e.id_causale 
            --left join raccolta.effettuati_correzione_date ecd on e.id_percorso=ecd.id_percorso and e.datalav= ecd.datalav_errata 
            left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
            --left join raccolta.causali_testi ct on trim(replace(e.causale, ' - (no in questa giornata)', '')) ilike trim(ct.descrizione)
            left join servizi.mail_ut mu on mu.id_uo::int  = t.id_uo::int
            where 
            /*(trim(replace(e.causale, ' - (no in questa giornata)', '')) != '' /*or e.fatto >0*/) 
            and*/ datalav >= to_date('20240129', 'YYYYMMDD')
            and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
            and datalav = to_date(%s, 'YYYYMMDD')
            and t.id_percorso = %s
            group by 
            e.id, 
            t.id_percorso,
            e.datalav::date ,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            --vc.descrizione as descr_causale,
            e.id_causale ,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) , 
            e.datainsert , 
            t.id_tappa, 
            e.nota, 
            t.tipo_elemento
            order by 1'''
    elif debug ==2: 
            query_effettuati_totem='''
            select distinct 
            e.id, 
            t.id_percorso,
            e.datalav::date as datalav,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            string_agg(vc.descrizione, ', ') as descr_causale,
            e.id_causale as causale,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
            e.datainsert as data_insert, 
            t.id_tappa as tappa, 
            e.nota as note_totem /* è il campo con le note*/,
            string_agg(mu.mail, ', ') as mail, 
            t.tipo_elemento
            from raccolta.cons_percorsi_raccolta_amiu t
            join raccolta.effettuati_amiu e on e.tappa::bigint =  t.id_tappa::bigint
            /* controllo la validità dei percorsi */
            join servizi.servizi_per_ekovision spe 
                on spe.id_percorso = t.id_percorso and e.datalav between spe.data_inizio_validita and spe.data_fine_validita
            join totem.v_causali vc on vc.id = e.id_causale 
            --left join raccolta.effettuati_correzione_date ecd on e.id_percorso=ecd.id_percorso and e.datalav= ecd.datalav_errata 
            left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
            --left join raccolta.causali_testi ct on trim(replace(e.causale, ' - (no in questa giornata)', '')) ilike trim(ct.descrizione)
            left join servizi.mail_ut mu on mu.id_uo::int  = t.id_uo::int
            where 
            /*(trim(replace(e.causale, ' - (no in questa giornata)', '')) != '' /*or e.fatto >0*/) 
            and*/ datalav = to_date(%s, 'YYYYMMDD')
            and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
            group by 
            e.id, 
            t.id_percorso,
            e.datalav::date ,
            t.id_piazzola, 
            t.num_elementi, 
            t.cronologia,
            e.fatto,
            --vc.descrizione as descr_causale,
            e.id_causale ,
            concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) , 
            e.datainsert , 
            t.id_tappa, 
            e.nota, 
            t.tipo_elemento
            order by e.datainsert
            '''
            
    else: 
        logger.error('Il valore della variabile debug non è riconosciuto')
    
    
    
    # prima di tutto faccio un controllo che non ci siano causali che non so gestire e nel caso fermo tutto il passaggio dati e lancio allarme
    query_check='''select distinct causale, descr_causale from (
        {}
        ) as foo'''.format(query_effettuati_totem)
    
    
    try:
        if debug ==0:
            currc.execute(query_check)
        elif debug ==1:
            currc.execute(query_check, (datalav_debug, cod_percorso_debug,))
        elif debug ==2:
            currc.execute(query_check, (datalav_debug,))
        lista_causali=currc.fetchall()
    except Exception as e:
        logger.error(query_check)
        logger.error(e)


    for cc in lista_causali:
        if cc[0] == None:
            logger.error('''La causale {} non è riconosciuta. Andare sull'HUB ggiungere un id nella tabella raccolta.causali_testo'''.format(cc[1])) 
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
    
    logger.info('CONTROLLO CAUSALI TERMINATO')
    currc.close()
    currc = connc.cursor()
    currc1 = connc.cursor()
                
    try:
        if debug ==0:
            currc.execute(query_effettuati_totem)
        elif debug ==1:
            currc.execute(query_effettuati_totem, (datalav_debug, cod_percorso_debug,))
        elif debug ==2:
            currc.execute(query_effettuati_totem, (datalav_debug,))
        lista_x_piazzola=currc.fetchall()
    except Exception as e:
        logger.error(query_effettuati_totem)
        logger.error(e)


    logger.info('Trovo {} righe consuntivate'.format(len(lista_x_piazzola)))
    #exit()
    for vv in lista_x_piazzola:
    
    
    
        # controllo che il percorso sia su ekovision
        query_chek_percorso_eko='''select * from anagrafe_percorsi.elenco_percorsi ep
 where ep.ekovision is not true 
 and cod_percorso = %s
 and %s between ep.data_inizio_validita and ep.data_fine_validita 
        '''
        try:
            curr.execute(query_chek_percorso_eko, (vv[1], vv[2]))
            lista_percorsi_non_eko=curr.fetchall()
        except Exception as e:
            logger.error(query_chek_percorso_eko)
            logger.error(e)
        # temporanemente tolgo i percorsi non presenti su SIT lista not in 
    
        
        
        
        if vv[1] not in (  '0101355501',
                            '0101355901',
                            '0101356001',
                            '0508044303',
                            '0508043903',
                            '0507110703',
                            '0507130703',
                            '0500106403',
                            '0500106803',
                            '0501002001',
                            '0501002201',
                            '0501002301',
                            '0502005402', 
                            '0102005501',
                            '0111000301',
                            '0111000402',
                            '0111000501',
                            '0502006201',
                            '0507118001',
                            '0508051001'
                         ) and len(lista_percorsi_non_eko) == 0:
        
        
            # cerco ID_percorso_sit
            query_id_percorso='''select distinct id_percorso_sit, p.id_turno 
            from anagrafe_percorsi.date_percorsi_sit_uo ep
            join elem.percorsi p on p.id_percorso = ep.id_percorso_sit 
            join elem.turni t on t.id_turno =p.id_turno  
                        where ep.id_percorso_sit is not null  
                        and ep.cod_percorso = %s 
                        and ep.data_inizio_validita <= %s 
                        and ep.data_fine_validita > %s'''
            
            try:
                curr.execute(query_id_percorso, (vv[1], vv[2], vv[2]))
                lista_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_id_percorso)
                logger.error(e)
                
            if len(lista_percorso)== 1:
                for lp in lista_percorso:
                    id_percorso_sit=lp[0]
                    turno_percorso=lp[1]
            else: 
                logger.error('Problema individuazione id_percorso_sit per percorso {}'.format(vv[1]))
                logger.error(query_id_percorso)
                logger.error('Codice percorso = {}'.format(vv[1]))
                logger.error('Data rif = {}'.format(vv[2]))
                
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
                
            #logger.debug(id_percorso_sit)
            #logger.debug(vv[3])
            #logger.debug(turno_percorso)
            
            # cerco il turno
            
            
            # per quella id_percorso / via / nota / data cerco le correspondenti aste su SIT
            query_elementi='''select distinct eap1.id_elemento, ee.id_piazzola, 
            eap1.ripasso, eap1.data_inserimento, eap1.data_eliminazione, 
                concat(eap1.id_asta_percorso, '_', ee.id_piazzola) as id_asta_percorso,
                eap1.id_elemento_asta_percorso 
                                from (
                                    select eap.id_elemento, eap.ripasso, eap.data_inserimento, null as data_eliminazione, 
                                    eap.id_asta_percorso,
                                    eap.id_elemento_asta_percorso 
                                    from elem.elementi_aste_percorso eap
                                    where id_asta_percorso in  
                                    (select  id_asta_percorso 
                                    from elem.aste_percorso ap1 
                                    where tipo= 'servizio' and id_percorso =%s
                                    union 
                                    select id_asta_percorso 
                                    from history.aste_percorso ap2
                                    where /*tipo= 'servizio' and*/ id_percorso =%s)
                                    union 
                                    select heap.id_elemento, heap.ripasso, heap.data_inserimento, heap.data_eliminazione, 
                                    heap.id_asta_percorso, heap.id_elemento_asta_percorso 
                                    from history.elementi_aste_percorso heap
                                    where id_asta_percorso in 
                                    (select  id_asta_percorso 
                                    from elem.aste_percorso ap1 
                                    where /*tipo= 'servizio' and*/ id_percorso =%s
                                    union 
                                    select id_asta_percorso 
                                    from history.aste_percorso ap2
                                    where /*tipo= 'servizio' and*/ id_percorso =%s) 
                                ) eap1
                                join 
                                (select id_elemento, id_piazzola, e.tipo_elemento from elem.elementi e 
                                union 
                                select id_elemento, id_piazzola, e.tipo_elemento  from history.elementi e
                                ) ee on ee.id_elemento = eap1.id_elemento     
                                where ee.id_piazzola= %s and ee.tipo_elemento = %s
                                and coalesce(data_eliminazione, '2099-12-31')::date >= %s 
                                and data_inserimento::date < %s
                                '''
            
            try:
                curr.execute(query_elementi, (id_percorso_sit, id_percorso_sit,id_percorso_sit,id_percorso_sit,
                                              vv[3], vv[14], vv[2], vv[2]))
                lista_elementi=curr.fetchall()
            except Exception as e:
                logger.error('NON TROVO GLI ELEMENTI  SUL SIT')
                logger.error(query_elementi)
                logger.error('Codice percorso = {}'.format(vv[1]))
                logger.error('Data rif = {}'.format(vv[2]))
                logger.error('Id percorso SIT = {}'.format(id_percorso_sit))
                logger.error('id_piazzola = {}'.format(vv[3]))
                logger.error('tipo_elemento = {}'.format(vv[14]))
                logger.error(e)
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            # vv[4] num_elementi
            # vv[6] num_elementi_fatti
            
            conteggio=0
            for aa in lista_elementi:
                #logger.debug(aa[0])       
                # controllo sulla consuntivazione pregressa
                
                if conteggio <vv[6]:
                    esec=1;
                    caus=100;
                else: 
                    esec=0;
                    caus=vv[8]
                
                # versione hub
                query_check='''select *  
                    from raccolta.effettuati_amiu e 
                    where id_percorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and id_tappa=%s
                    and id <> %s 
                    and left(codice,2) ilike 'ut'
                    '''
                # versione totem
                query_check='''select *  
                    from raccolta.v_effettuati e 
                    where idpercorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and tappa=%s
                    and id <> %s 
                    and left(codice,2) ilike 'ut'
                    '''
                
                try:
                    currc1.execute(query_check, (vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0])))
                    altre_consuntivazioni=currc1.fetchall()
                except Exception as e:
                    logger.error(query_check)
                    logger.error(vv[11])
                    logger.error('''{0} {1} {2} {3}'''.format(vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0])))
                    logger.error(e)
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                                    
                # se ci fosse un punteggio superiore o una consuntivazione del RUT (serve fino a quando il backoffice è di WingSOFT non servirà più dopo)
                # non passo i dati a ekovision
                if len(altre_consuntivazioni)>0:
                    logger.warning('''Tappa {} del {} già consuntivata con punteggio maggiore. Non passo il dato a Ekovision'''.format(vv[11], vv[2].strftime('%Y-%m-%d')))
                else: 
                    cod_percorso.append(vv[1])
                    data_percorso.append(vv[2].strftime("%Y%m%d"))
                    id_turno.append(turno_percorso)
                    id_componente.append(aa[0])
                    id_tratto.append(None)
                    flag_esecuzione.append(esec)
                    causale.append(caus)
                    nota_causale.append(vv[12])
                    sorgente_dati.append(vv[9])
                    data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                    lat.append(None)
                    long.append(None)
                    ripasso.append(aa[2])
                    qual.append(None) 
                    mail_arr.append(vv[13]) 
                    id_asta_percorso.append(aa[5])
                    id_elemento_asta_percorso.append(aa[6])
                    chiave_cons.append('{}_{}'.format(vv[2].strftime("%Y%m%d"), aa[6]))                               
            
                 

        elif len(lista_percorsi_non_eko)>0: 
            for f in lista_percorsi_non_eko:
                descr_perc_w= f[1]
            messaggio_w1=f'''Il percorso {vv[1]} - {descr_perc_w} del {vv[2]} è stato consuntivato su totem ma non è un percorso di Ekovision. Non invio i dati a Ekovision'''         
            logger.warning(messaggio_w1)
            logger.info('Invio messaggio di Warning anche via mail')
            warning_message_mail(messaggio_w1, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'WARNING consuntivazione totem')


        # mi salvo sempre il max_id    
        max_id=vv[0]

        

        
        
    
    
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Cache-Control': 'no-cache'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    k=0
    cod_percorsi_distinct=[]
    date_distinct=[]
    turno_distinct=[]
    mail_arr_distinct=[]
    sorgente_dati_distinct=[]
    logger.debug(len(cod_percorso))
    while k<len(cod_percorso):
        #logger.debug(k)
        if k==0:
            cod_percorsi_distinct.append(cod_percorso[k])
            date_distinct.append(data_percorso[k])
            turno_distinct.append(id_turno[k])
            mail_arr_distinct.append(mail_arr[k])
            sorgente_dati_distinct.append(sorgente_dati[k])
        if k > 0 and cod_percorso[k]!= cod_percorso[k-1]:
            cod_percorsi_distinct.append(cod_percorso[k])
            date_distinct.append(data_percorso[k])
            turno_distinct.append(id_turno[k])
            mail_arr_distinct.append(mail_arr[k])
            sorgente_dati_distinct.append(sorgente_dati[k])
        k+=1
        
    
    logger.info('Gestisco i non previsti e controllo se le schede di lavoro esistono')
    k=0
    while k< len(cod_percorsi_distinct):
        
        
        # a questo punto cerco i non previsti e li aggiungo
        day_p=datetime.strptime(date_distinct[k], '%Y%m%d').date()           
        try:
            curr.execute(query_racc_np, (date_distinct[k],
                                        date_distinct[k],
                                        cod_percorsi_distinct[k],
                                        date_distinct[k],
                                        date_distinct[k],))
            lista_elementi_np=curr.fetchall() # questi sono solo elemeni non previsti
        except Exception as e:
            check_error=1
            logger.error(e)

        

        for lenp in lista_elementi_np:
            if (
                lenp[7] <= day_p # data attivazione
                and (lenp[8] is None or lenp[8] > day_p)# data dismissione
                and '{}_{}'.format(day_p.strftime("%Y%m%d"),lenp[12]) 
                not in chiave_cons # tappa non consuntivata
            ):
                cod_percorso.append(lenp[0])
                data_percorso.append(day_p.strftime("%Y%m%d"))
                id_turno.append(lenp[1])
                id_componente.append(lenp[2])
                id_tratto.append(None)
                flag_esecuzione.append(2)
                causale.append(999)
                nota_causale.append('Come da progettazione su SIT')
                sorgente_dati.append('Come da progettazione su SIT')
                data_ora.append(day_p.strftime("%Y%m%d%H%M"))
                lat.append(None)
                long.append(None)
                ripasso.append(lenp[6]) 
                qual.append(None) 
                mail_arr.append(None)
                id_asta_percorso.append(lenp[11])
                id_elemento_asta_percorso.append(lenp[12]) 
                chiave_cons.append('{}_{}'.format(day_p.strftime("%Y%m%d"),lenp[12])) 
        
        
        
        # qua provo il WS
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': date_distinct[k],
            'cod_modello_srv': cod_percorsi_distinct[k],
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
            }
        response = requests.post(eko_url, params=params, data=data_json, headers=headers)
        #response.json()
        logger.debug(response.status_code)
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
        if check<1 : 
            letture = response.json()
            #logger.info(letture)
            logger.debug(len(letture['schede_lavoro']))
            if len(letture['schede_lavoro']) == 0 and debug < 2: # se debug = 2 non creo nessuna sched
                #va creata la scheda di lavoro
                logger.info('Va creata la scheda di lavoro')
                """
                curr.close()
                curr = conn.cursor()
                
                query_select_ruid='''select lpad((max(id)+1)::text, 7,'0') 
                from anagrafe_percorsi.creazione_schede_lavoro csl '''
                try:
                    curr.execute(query_select_ruid)
                    lista_ruid=curr.fetchall()
                except Exception as e:
                    logger.error(query_select_ruid)
                    logger.error(e)




                for ri in lista_ruid:
                    ruid=ri[0]


                
                """
                ruid = uuid.uuid4()
                logger.info('ID richiesta Ekovision (ruid):{}'.format(ruid))
                curr.close()
                curr = conn.cursor()
                giason={
                            "crea_schede_lavoro": [
                            {
                                "data_srv": date_distinct[k],
                                "cod_modello_srv": cod_percorsi_distinct[k],
                                "cod_turno_ext": int(turno_distinct[k])
                            }
                            ]
                            } 
                params2={'obj':'crea_schede_lavoro',
                        'act' : 'w',
                        'ruid': ruid,
                        'json': json.dumps(giason)
                        }
                
                try:
                    response2 = requests.post(eko_url, params=params2, data=data_json, headers=headers)
                    letture2 = response2.json()
                    logger.info(letture2)
                    check_creazione_scheda=0
                    id_scheda=letture2['crea_schede_lavoro'][0]['id']
                    check_creazione_scheda=1
                except Exception as e:
                    query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                            (id, cod_percorso, "data", id_scheda_ekovision, "check")
                            VALUES(%s, %s, %s, NULL, %s);'''
                    try:        
                        curr.execute(query_insert, (str(ruid),cod_percorsi_distinct[k], date_distinct[k], check_creazione_scheda))
                    except Exception as e1:
                        logger.error(query_insert)
                        logger.error(e1)        
                    logger.error(e)
                    logger.error(' - id: {}'.format(ruid))
                    logger.error(' - Cod_percorso: {}'.format(cod_percorsi_distinct[k]))
                    logger.error(' - Data: {}'.format(date_distinct[k]))
                    #logger.error('Id Scheda: {}'.format(id_scheda[k]))
                    # check se c_handller contiene almeno una riga 
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.info("chiudo le connessioni in maniera definitiva")
                    currc.close()
                    #currc1.close()
                    connc.close()
                    curr.close()
                    conn.close()
                    exit()
                    
                
                # Cerco la descrizione del percorso
                    
                    
                    
                if check_creazione_scheda==1:
                    query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                            (id, cod_percorso, "data", id_scheda_ekovision, "check")
                            VALUES(%s, %s, %s, %s, %s);'''
                else: 
                    query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                            (id, cod_percorso, "data", id_scheda_ekovision, "check")
                            VALUES(%s, %s, %s, NULL, %s);'''
                try:
                    if check_creazione_scheda ==1:
                        curr.execute(query_insert, (str(ruid),cod_percorsi_distinct[k], date_distinct[k], id_scheda, check_creazione_scheda))
                        body_mail='''E' arrivata una consuntivazione da totem per il percorso {} - {} in data {}.
                        <br>Origine del dato:{}
                        <br>Non esistendo la scheda per il giorno in questione è stata creata in automatico.
                        La nuova scheda ha ID {}'''.format(cod_percorsi_distinct[k],
                                                           descrizione_percorso(cod_percorsi_distinct[k],  date_distinct[k], curr, logger),
                                                           date_distinct[k], sorgente_dati_distinct[k], id_scheda)           
                        creazione_scheda_mail(body_mail, mail_arr_distinct[k], os.path.basename(__file__), logger)
                        conn.commit()
                    else:
                        curr.execute(query_insert, (str(ruid),cod_percorsi_distinct[k], date_distinct[k], check_creazione_scheda))
                        conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
            elif  len(letture['schede_lavoro']) == 0 and debug == 2: 
                logger.error(f'''Non c'è scheda per il percorso {cod_percorsi_distinct[k]}
                                         con data {date_distinct[k]}. Da verificare come mai.''')          
            # in questo caso le schede esistono     
            elif len(letture['schede_lavoro']) > 0 and debug < 2: # se debug == 2 controllo inutile 
                
                
                ss=0
                while ss < len(letture['schede_lavoro']):
                                                          
                    id_scheda=letture['schede_lavoro'][ss]['id_scheda_lav']
                    
                    # CONTROLLO SUI TURNI 
                    try:
                        if letture['schede_lavoro'][0]['cod_turno_ext'] is None or letture['schede_lavoro'][0]['cod_turno_ext'] =='':
                            logger.warning('Anomalia. Nessun turno su Ekovision per il percorso {0}. Scheda di lavoro {1} del {2}. Turno UO ={3}'.format(cod_percorsi_distinct[k], id_scheda, date_distinct[k], turno_distinct[k]))
                        else: 
                            id_turno_ekovision=int(letture['schede_lavoro'][0]['cod_turno_ext'])
                            logger.info(id_scheda)
                            if id_turno_ekovision != int(turno_distinct[k]):
                                logger.warning('Anomalia turni per percorso {0}. Scheda di lavoro {1} del {2}. Turno UO ={3}, Turno Ekovision={4}'.format(cod_percorsi_distinct[k], id_scheda, date_distinct[k], turno_distinct[k], id_turno_ekovision))
                                #warning_log_mail(logfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                    except Exception as e:
                        logger.error(e)
                        logger.error(letture)
                        logger.error('Errore NON BLOCCANTE turni scheda {}'.format(id_scheda))
                        
                    # CONTROLLO SU SCHEDE DICHIARATE COME NON EFFETTUATE E MAIL (aggiunto il 24/10/2025)
                    logger.info(f'Provo a leggere i dettagli della scheda {id_scheda} per capire che non ci sia il "non eseguita" già attivo')
        
                    params2={'obj':'schede_lavoro',
                            'act' : 'r',
                            'id': '{}'.format(id_scheda),
                            }
                    
                    response2 = requests.post(eko_url, params=params2, data=data_json, headers=headers)
                    #letture2 = response2.json()
                    letture2 = response2.json()
                    if int(letture2['schede_lavoro'][0]['servizi'][0]['flg_segn_srv_non_effett'])==1:
                        id_causale_eko=letture2['schede_lavoro'][0]['servizi'][0]['id_caus_srv_non_eseg']
                        logger.debug(id_causale_eko)
                        # cerco descrizione
                        curr2 = connc.cursor()
                        try:
                            curr2.execute(query_causale, (int(id_causale_eko),))
                            row_result=curr2.fetchone()
                            if row_result is not None:
                                id_amiu = row_result[0]
                                descrizione_causale = row_result[1]
                            else: 
                                descrizione_causale='CAUSALE EKOVISION NON TROVATA CONTROLLARE LE TABELLE SU HUB'
                                id_amiu=0
                        except Exception as e:
                            logger.error(e)
                        curr2.close()
                        # verifico se ci sono causali diverse
                        curr2 = connc.cursor()
                        try:
                            curr2.execute(query_verifica_causale, (cod_percorsi_distinct[k],
                                                                   date_distinct[k], 
                                                                   int(id_amiu),))
                            consuntivazioni_diverse=curr2.fetchall()
                        except Exception as e:
                            logger.error(e)
                        curr2.close()
                        testo_causale_eko=letture2['schede_lavoro'][0]['servizi'][0]['txt_segn_srv_non_effett']
                        if len(consuntivazioni_diverse) > 0:
                            for ff in consuntivazioni_diverse:
                                testo_percorso = ff[10]
                            testo_warning=f'''La scheda {id_scheda} di RACCOLTA (cod_percorso = {cod_percorsi_distinct[k]} {testo_percorso} del {date_distinct[k]}) risulta non effettuata 
                            <br>({descrizione_causale} - Note: {testo_causale_eko}). 
                            <br><b>Le consuntivazioni arrivate da totem saranno ignorate. <font color="red">Su totem ci sono delle differenze di consuntivazione</font>.
                            Nel caso in cui il servizio sia stato effettuato correggere i dati su Ekovision</b>'''
                            logger.warning(testo_warning)
                            warning_message_mail(f'MAIL CHE PER ORA ARRIVA SOLO A NOI, a regime al territorio {mail_arr_distinct[k]}<br><br>{testo_warning}', 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'ATTENZIONE - consuntivazioni totem scheda non effettuata')
                        else: 
                            testo_warning=f'''La scheda {id_scheda} di RACCOLTA (cod_percorso = {cod_percorsi_distinct[k]} del {date_distinct[k]}) risulta non effettuata 
                            <br>({descrizione_causale} - Note: {testo_causale_eko}). 
                            <br><b>Le consuntivazioni arrivate da totem saranno ignorate.</b> <font color="green">Tutto è congruente</font>'''
                            logger.warning(testo_warning)
                            warning_message_mail(f'MAIL CHE PER ORA ARRIVA SOLO A NOI, a regime al territorio {mail_arr_distinct[k]}<br><br>{testo_warning}', 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'ATTENZIONE - consuntivazioni totem scheda non effettuata')

                    # VADO ALLA SCHEDA SEGUENTE (stesso codice percorso)
                    ss+=1

                    
                    
                        
                            
        k+=1
        # committo l'inserimento nella tabella di creazione delle schede
        conn.commit() 
    
    
    if datetime.today() >= datetime.strptime('29/01/2024', '%d/%m/%Y'):
        check_ekovision=200
        '''
        #creo un dizionario 
        
        # Creating an empty dictionary
        dizionario = {}
        # Adding list as value
        dizionario["cod_percorso"] = cod_percorso
        dizionario["data"] = data
        
        logger.debug(dizionario)
        # Adding list as value
        exit()
        '''
        try:    
            nome_csv_ekovision="consuntivazioni_raccolta_{0}.csv".format(giorno_file)
            file_preconsuntivazioni_ekovision="{0}/consuntivazioni/{1}".format(path,nome_csv_ekovision)
            fp = open(file_preconsuntivazioni_ekovision, 'w', encoding='utf-8')
                        
            fieldnames = ['cod_percorso', 'data', 'id_turno', 'id_componente','id_tratto',
                            'flag_esecuzione', 'causale', 'nota_causale', 'sorgente_dati', 
                            'data_ora', 'lat', 'long', 'ripasso', 'qual'
                            , 'id_asta_percorso', 'id_elemento_asta_percorso' ]
        
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
            while k < len(cod_percorso):
                row=[cod_percorso[k], data_percorso[k], id_turno[k], id_componente[k],id_tratto[k],
                            flag_esecuzione[k], causale[k], nota_causale[k], sorgente_dati[k],
                            data_ora[k], lat[k], long[k], ripasso[k], qual[k]
                            , id_asta_percorso[k], id_elemento_asta_percorso[k]]
                myFile.writerow(row)
                k+=1
            '''
            matrice=[tuple(cod_percorso), tuple(data), tuple(id_turno), tuple(id_componente),tuple(id_tratto),
                            tuple(flag_esecuzione), tuple(causale), tuple(nota_causale), tuple(sorgente_dati), tuple(data_ora), tuple(lat), tuple(long)]
            myFile.writerows(matrice)
            '''
            fp.close()
        except Exception as e:
            logger.error('Problema creazione file CSV')
            logger.error(e)
            check_ekovision=102 # problema file variazioni


        if debug ==1:
            # invio il file via mail a assterritorio 
            
            # Create a secure SSL context
            context = ssl.create_default_context()
    
            subject = "Invio nuovo tracciato consuntivazione"
            body = f'''
            <p>Invio file CSV per il percorso con cod {cod_percorso_debug} e data {datalav_debug}</p>
            <br><p>
            AMIU Assistenza Territorio
            </p>
    '''
            #sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            debug_email='roberto.marzocchi@amiu.genova.it'
            #cc_mail='pianar@amiu.genova.it'
    
            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = receiver_email #debug_mail
            #message["Cc"] = cc_mail
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = subject
    
            
                            
            # Add body to email
            message.attach(MIMEText(body, "html"))
    
            # aggiunto allegato (usando la funzione importata)
            allegato(message, file_preconsuntivazioni_ekovision, nome_csv_ekovision)
            
            #text = message.as_string()
    
            logging.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            logging.info(invio)
            
            # chiudo tutto
            # check se c_handller contiene almeno una riga 
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
            logger.info("chiudo le connessioni in maniera definitiva")
            
            currc.close()
            #currc1.close()
            connc.close()
            
            curr.close()
            conn.close()
            exit()
        
        ########################################
        ####  Qua sarebbe da migrare a PARAMIKO
        ########################################
        logger.info('Invio file con la consuntivazione raccolta via SFTP')
        try: 
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            srv = pysftp.Connection(host=url_ev_sftp, username=SFTP_USER,
        password=SFTP_PWD, port= port_ev_sftp,  cnopts=cnopts)

            with srv.cd('sch_lav_cons/in/'): #chdir to public
                srv.put(file_preconsuntivazioni_ekovision) #upload file to nodejs/

            # Closes the connection
            srv.close()
        except Exception as e:
            logger.error('problema invio SFTP')
            logger.error(e)
            check_ekovision=103 # problema invio SFTP  
        
        currc.close()
        currc1.close()
        connc.close()
        
        
        nome_db=db_totem
        logger.info('Ri-connessione al db {}'.format(nome_db))
        """connc = psycopg2.connect(dbname=nome_db,
                            port=port,
                            user=user_consuntivazione,
                            password=pwd_consuntivazione,
                            host=host_hub)
        
        """
        connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)
        currc = connc.cursor()
        
        
        if check_ekovision==200 and len(lista_x_piazzola)>0:
            insert_max_id='''INSERT INTO raccolta.invio_consuntivazioni_ekovision
            (max_id, data_ora)
            VALUES
            (%s, now())'''
            try:
                currc.execute(insert_max_id, (max_id,))
                connc.commit()
            except Exception as e: 
                logger.error(insert_max_id)
                logger.error(max_id)
                logger.error(e)
            
        
           

    # check se c_handller contiene almeno una riga 
    #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    
    curr.close()
    conn.close()


#***************************************************

# debug = 0 comportamento standard 
# debug = 1 processo 1 percorso e lo mando via mai
# debug = 2  

debug=2

SFTP_USER = user_ev_sftp_test
SFTP_PWD = pwd_ev_sftp_test
       
#***************************************************    

if __name__ == "__main__":

    if debug == 0:
        main(SFTP_USER,
             SFTP_PWD)
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

    elif debug == 1:
        main(
            SFTP_USER,
            SFTP_PWD,
            cod_percorso_debug='0101004101',
            datalav_debug='20260101'
        )
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)

    elif debug == 2:

        data_start = datetime.strptime("20260101", "%Y%m%d").date()
        data_end = datetime.strptime("20260901", "%Y%m%d").date()

        data = data_start
        logger.info(f'SFTP = {SFTP_USER}')

        while data <= data_end:
            logger.info(f'data = {data}')
            
            main(
                SFTP_USER,
                SFTP_PWD,
                datalav_debug=data.strftime("%Y%m%d")
            )

            data += timedelta(days=1)  
        # alla fine di tutto
        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
