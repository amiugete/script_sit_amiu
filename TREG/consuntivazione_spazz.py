#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Scopo dello script è lavorare giorno per giorno e inviare i dati a TREG a partire da una data che legge dal DB


PUNTI DI PARTENZA: 

1) query che fa union di 3 viste:
    ▪ anagrafe_percorsi.v_percorsi_elementi_tratti
    ▪ anagrafe_percorsi.v_percorsi_elementi_tratti_ovs (OVS = Old Version SIT)
    ▪ anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi 
- join con aste, via per recuperare informazioni sulla via 



2) periodo di attività del percorso, per i percorsi stagionali o dismessi 
l'elemento / elemento_asta_percorso non sono eliminati quindi nella query (tabella anagrafe_percorsi.elenco_percorsi)

3) turno previsto (tabella anagrafe_percorsi.elenco_percorsi)

4) servizio da inviare ad ARERA (tabella anagrafe_percorsi.anagrafe_tipo)


VERIFICHE DA FARE
Devo usare i WS di Ekovision:
    - elenco schede lavoro entrando con cod_percorso, data controllo che ci sia almeno 1 scheda
        --> percorsi_spazz_non_presenti.txt
        --> percorsi_spazz_doppi.txt 
    - entro con id_scheda e devo verificare i tratti 
        --> percorsi_tratti_non_trovati.txt: tutti i tratti di SIT devono essere in Ekovision
        --> percorsi_spazz_spunte_colorate.txt: i tratti di Ekovision non presenti su SIT dovrebbero individuar i percorsi con spunte blue e marroni


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta, timezone, time

import locale

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


from tappa_prevista import *

from crea_dizionario_da_query import *

import uuid




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

from decimal import Decimal

from treg_env import *

def convert_decimal(obj):
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        # Se il valore è "intero", converti in int
        if obj == obj.to_integral_value():
            return int(obj)
        # Altrimenti, converti in float
        else:
            return float(obj)
    else:
        return obj

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0

from psycopg2.extras import execute_values

def bulk_update_consunt(cursor, updates, logger):
    cursor.execute("""
        CREATE TEMP TABLE tmp_consunt_update_spazz (
            codice int8,
            data_ora_inizio TIMESTAMP,
            resumption_date TIMESTAMP
        ) ON COMMIT DROP
    """)


    execute_values(
        cursor,
        """
        INSERT INTO tmp_consunt_update_spazz
        (resumption_date, codice, data_ora_inizio)
        VALUES %s
        """,
        updates
    )

    cursor.execute("""
        UPDATE treg_eko.consunt_ekovision ce
        SET resumption_date = t.resumption_date
        FROM tmp_consunt_update_spazz t
        WHERE ce.codice = t.codice
          AND ce.data_ora_inizio = t.data_ora_inizio
          AND ce.causale NOT IN ('100','110')
          AND ce.resumption_date IS DISTINCT FROM t.resumption_date
    """)



def bulk_update_consunt_tc(cursor, updates, logger):

    '''
    Gestisce update trac_code senza rallentare troppo il processo con update uno a uno,
      creando una tabella temporanea e facendo un unico update con join
    '''


    cursor.execute("""
        CREATE TEMP TABLE tmp_consunt_update_spazz_tc (
            codice int8,
            data_pianif_iniziale varchar,
            cod_percorso varchar,
            trac_code varchar
        ) /*ON COMMIT DROP*/
    """)



    execute_values(
        cursor,
        """
        INSERT INTO tmp_consunt_update_spazz_tc
        (codice, data_pianif_iniziale, cod_percorso, trac_code)
        VALUES %s
        """,
        updates
    )

    cursor.execute("""
        UPDATE treg_eko.consunt_ekovision ce
        SET trac_code = t.trac_code
        FROM tmp_consunt_update_spazz_tc t
        WHERE ce.codice = t.codice
          AND ce.data_pianif_iniziale = t.data_pianif_iniziale
          AND ce.codice_servizio_pred = t.cod_percorso
          AND ce.trac_code IS DISTINCT FROM t.trac_code
    """)


def main():
    
    
    giorno_file=datetime.today().strftime('%Y%m%d_%H%M%S')

    filename = inspect.getframeinfo(inspect.currentframe()).filename
    path=os.path.dirname(sys.argv[0]) 
    path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
    nome=os.path.basename(__file__).replace('.py','')
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    logfile='{0}/log/{2}_{1}.log'.format(path,nome,giorno_file)
    errorfile='{0}/log/{2}_error_{1}.log'.format(path,nome,giorno_file)
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
    
    
    
    
    
      
    ###################### inizio definizione query ######################

    # 1 - cerco il giono da cui partire
    query_first_day='''select min(data_last_update) from treg_eko.consunt_ekovision ce
        where ce.tipo_servizio = 'SPAZZ' and ce.data_last_update >= (
        select coalesce(max(data_last_update), to_date('20250101', 'YYYYMMDD')) from treg_eko.last_import_treg_spazz_cons
        where commit_code=200 and deleted = false
        );'''

    # 2 - estraggo i percorsi dello spazzamento
    query_elenco_percorsi_spazz='''
        with step0 as (
        select ep.cod_percorso, versione_testata, fo.freq_binaria, freq_settimane, 
        id_turno, at2.gestione_arera, ce.data_pianif_iniziale, ce.data_last_update 
        from  treg_eko.consunt_ekovision ce 
        join anagrafe_percorsi.elenco_percorsi ep 
            on ep.cod_percorso = ce.codice_servizio_pred 
            and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') between data_inizio_validita and (data_fine_validita - interval '1' day) 
        join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
        join etl.frequenze_ok fo on fo.cod_frequenza = ep.freq_testata 
        where ce.data_last_update > %s
        and gestione_arera = 't'
        and at2.id_famiglia in (2,3)
        /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
        order by ce.data_last_update asc
        /*limit 100000*/
        ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
        from step0
        group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
        order by 8 asc limit 1150;
    '''
    
    # cerco quelle di SIT
    query_elementi_percorso='''
        with group_scheda as 
        (
        -- PRIMO STEP PER TOGLIERE I RIPASSI (al secondo step dovrò considerare le possibili schede doppie)
            SELECT distinct 
            case
                when flg_riprogrammato = 0 then id_scheda
                else id_scheda_riprogr
            end id_scheda, 
            codice_servizio_pred,
            case
                when flg_riprogrammato = 0 then data_pianif_iniziale
                else (select distinct data_pianif_iniziale from treg_eko.consunt_ekovision ce1 
                where ce1.id_scheda = ce.id_scheda_riprogr)
            end data_pianif_iniziale, 
            data_esecuzione_prevista,
            data_ora_inizio, 
            data_ora_fine,
            ce.codice, 
            case
                when 100 = ANY (array_agg(distinct causale::int)::int[]) then 100
                else max(distinct ce.causale::int)
            end causale, 
            tab.frequenza,
           /*ce.qualita*/
            case
                when 100 = ANY (array_agg(distinct causale::int)::int[]) then max(ce.qualita)
                else min(distinct ce.qualita)
            end qualita
            from treg_eko.consunt_ekovision ce
            join (
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti where codice_modello_servizio = %s and data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where codice_modello_servizio = %s and data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where codice_modello_servizio = %s and data_inizio < coalesce(data_fine, '20991231')
            ) tab 
            on tab.codice_modello_servizio = ce.codice_servizio_pred 
            and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') 
            between to_date(tab.data_inizio,'YYYYMMDD')  and to_date(tab.data_fine, 'YYYYMMDD')
            and tab.codice = ce.codice
            where ce.tipo_servizio = 'SPAZZ'
            and codice_servizio_pred = %s
            and data_pianif_iniziale = %s
            group by 
            case
                when flg_riprogrammato = 0 then id_scheda
                else id_scheda_riprogr
            end , 
            codice_servizio_pred,
            case
                when flg_riprogrammato = 0 then data_pianif_iniziale
                else (select distinct data_pianif_iniziale from treg_eko.consunt_ekovision ce1 
                where ce1.id_scheda = ce.id_scheda_riprogr)
            end ,
            data_esecuzione_prevista,
            data_ora_inizio, 
            data_ora_fine,
            ce.codice,
            tab.frequenza/*,
            ce.qualita*/
        ) 
        -- qua raggruppo per codice e data (dovrei escludere le schede doppie e prendere la causale migliore)
        select  
        codice_servizio_pred,
        data_pianif_iniziale, 
        --data_esecuzione_prevista,
        min(data_ora_inizio) + (ep2.giorno_competenza || ' day')::interval as data_ora_inizio_exec, 
        max(data_ora_fine) + (ep2.giorno_competenza || ' day')::interval as data_ora_fine_exec,
        codice,
        case
            when 100 = ANY (array_agg(distinct gs.causale::int)::int[]) then 100
            else min(distinct gs.causale::int)
        end causale,
        /*(aa.lung_asta * qualita / 100.0) / 1000 as kilometersTravelled,*/
        aa.lung_asta / 1000 as kilometersTravelled,
        ep2.giorno_competenza,
        'PRG' as areaType,
        min(aa.id_via) as streetCode,
        min(v.nome) as streetDescription,
        min(c.cod_istat) as istatCode, 
        /*gs.frequenza*/
        max(treg_eko.verify_daily_frequency(
    	    gs.frequenza,
    	    to_date(gs.data_pianif_iniziale, 'YYYYMMDD'),
    	    ep2.freq_settimane::text
    	)
        ) as in_freq
        from group_scheda gs
        left join anagrafe_percorsi.elenco_percorsi ep2 
        on ep2.cod_percorso = gs.codice_servizio_pred 
        and to_date(gs.data_pianif_iniziale, 'YYYYMMDD') between ep2.data_inizio_validita and ep2.data_fine_validita
        left join (select id_asta, id_via, lung_asta from elem.aste
            union 
            select id_asta, id_via, lung_asta from history.aste) aa 
        on aa.id_asta = gs.codice
        left join topo.vie v on v.id_via = aa.id_via 
        left join topo.comuni c on c.id_comune = v.id_comune 
        left join etl.frequenze_ok fo on fo.cod_frequenza = gs.frequenza
        where codice_servizio_pred = gs.codice_servizio_pred
        and data_pianif_iniziale = gs.data_pianif_iniziale
        and data_pianif_iniziale = %s /* non prendo i soccorsi di giorni precedenti */
        group by codice_servizio_pred,
            data_pianif_iniziale, aa.lung_asta,
            codice, ep2.giorno_competenza 
            /*gs.frequenza*/
    '''
    # ATTENZIONE: 
    # PER TREG non differenzio le aste per id_asta_percorso 
    # che invece è chiave primaria per i report di città metropolitana
     

    select_resumption_date = '''SELECT min(ce.data_ora_inizio) + (ep.giorno_competenza || ' day')::interval , 
min(ce.data_ora_fine ) + (ep.giorno_competenza || ' day')::interval 
        FROM treg_eko.consunt_ekovision ce
        join anagrafe_percorsi.elenco_percorsi ep on
        	ep.cod_percorso = ce.codice_servizio_pred 
        	and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') between ep.data_inizio_validita and ep.data_fine_validita -1
        WHERE codice = %s
        AND causale = '100'
        AND (data_ora_inizio + (ep.giorno_competenza || ' day')::interval  >= %s
        OR /* cerco anche il caso di anticipo*/
        data_pianif_iniziale = %s  and id_turno = %s)
    group by ep.giorno_competenza
    ORDER BY 2,1;
    '''

    query_insert='''INSERT INTO treg_eko.last_import_treg_spazz_cons
        (data_last_update, last_update,
        request_id_amiu, importid_treg, 
        commit_code, commit_message) 
        VALUES(%s, now(), 
        %s, %s, 
        %s, %s);'''
    
    query_insert_error='''INSERT INTO treg_eko.last_import_treg_spazz_cons
            (data_last_update, last_update,
            request_id_amiu, importid_treg) 
            VALUES(%s, now(), 
            %s, %s);'''
    
    
    # verifica se ci sono altri elementi con stesso trac code
    check_del= '''select  
            ce.id_scheda, ce.codice_servizio_pred, ce.codice, ce.causale  
            from treg_eko.consunt_ekovision ce 
            join anagrafe_percorsi.elenco_percorsi ep 
            on ep.cod_percorso = ce.codice_servizio_pred 
            and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') 
            between data_inizio_validita and (data_fine_validita - interval '1' day) 
            where 
            ce.codice = %s
            and ce.data_pianif_iniziale = %s 
            and ep.id_turno = %s
            and ce.causale::int not in (101, 102, 999)  '''
    
    # verifica se ci sono altri elementi con stesso trac_code e causale per colpa del gestore 
    # attenzione che volendo potrebbero essere anche più di uno
    query_check_diss= '''select  
        ce.id_scheda, ce.codice_servizio_pred, ce.codice, ce.causale, cd.id_causale_arera, ep.freq_settimane
        from treg_eko.consunt_ekovision ce 
        join anagrafe_percorsi.elenco_percorsi ep 
        on ep.cod_percorso = ce.codice_servizio_pred 
        and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') 
        between data_inizio_validita and (data_fine_validita - interval '1' day) 
        join etl.cause_disserv cd on cd.codice = ce.causale::int
        where 
        ce.codice = %s
        and ce.data_pianif_iniziale = %s
        and ep.id_turno = %s
        and ce.causale::int not in (101, 102, 999, 100)
        and cd.id_causale_arera = 3
        /*order by id_causale_arera desc  
        limit 1*/'''
        
        
    query_diss_in_freq='''
    with tab as (
SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
                    id_asta_percorso, id_elemento_asta_percorso
    FROM anagrafe_percorsi.v_percorsi_elementi_tratti 
    where codice_modello_servizio = %s 
    and codice = %s and 
    data_inizio < coalesce(data_fine, '20991231')
    union 
    SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
        id_asta_percorso, id_elemento_asta_percorso
    FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs 
    where codice_modello_servizio = %s 
    and codice = %s and data_inizio < coalesce(data_fine, '20991231')
    union 
    SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
        id_asta_percorso, id_elemento_asta_percorso
    FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi 
    where codice_modello_servizio = %s
    and codice = %s and data_inizio < coalesce(data_fine, '20991231')
) select treg_eko.verify_daily_frequency(
    	    tab.frequenza,
    	    to_date(%s, 'YYYYMMDD'),
    	    %s
    	) from tab
    	where %s between tab.data_inizio and tab.data_fine 
    '''
    
    
    
    insert_importid = '''
        INSERT INTO treg_eko.check_status_import
        (pid, id_treg, import_id, tipo_qt, data_insert)
        VALUES(%s, %s, %s, 'sweepings', now());
    '''

    delete_importid = '''
        DELETE FROM treg_eko.check_status_import
        WHERE import_id=%s;
    '''
    
    
    # eliminazione geometria asta 
    check_eliminazione_elem0='''with de as 
        (
        select data_eliminazione from history.grafostradale g where id = %s
        union 
        select data_eliminazione from history.aste a  where id_asta = %s
        )
    select min(data_eliminazione)::date +1 as data_eliminazione,
    min(data_eliminazione)::date +1 + interval '10' minute as data_eliminazione2
    from de'''


    
    
    
    
    # eliminazione asta dal percorso 
    check_eliminazione_elem = '''select ap.data_eliminazione, ap.data_eliminazione + interval '10' minute
        from history.aste_percorso ap 
        where ap.id_asta = %s
        and id_percorso in 
        (select id_percorso from elem.percorsi p where cod_percorso= %s)
        and ap.data_eliminazione > %s 
        order by ap.data_eliminazione desc
        limit 1'''
    
    
    
    # eliminazione / disattivazione percorso
    check_eliminazione_elem_2 ='''select ep.data_fine_validita::timestamp, 
        ep.data_fine_validita + interval '10' minute as data_fine_validita_fine  
        from anagrafe_percorsi.elenco_percorsi ep 
        where ep.cod_percorso = %s
        and %s between ep.data_inizio_validita and ep.data_fine_validita
        and ep.data_fine_validita < now()::date'''
    
    
    ###################### fine definizione query ######################
 
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
  
       
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    conn.autocommit = False  
    

    try:
        curr.execute(query_first_day)
        giorno_mese_anno=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_first_day)
        logger.error(e)
    
    
    for gma in giorno_mese_anno:
        data_last_update=gma[0]

    
    logger.info('Devo trattare i percorsi a partire da data_last_update {}'.format(data_last_update))

    
    #while  data_start <= fine_ciclo:

        
    # inizializzo un check 
    # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
    check_error_upload=0
    lista_update_res_date=[]
    list_trac_code_update=[]
    
    ##################################
    # procedo con il recupero dati
    ##################################
   
    # eseguo query 2 per estrazione percorsi
    try:
        curr.execute(query_elenco_percorsi_spazz, (data_last_update,))
        elenco_percorsi=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_elenco_percorsi_spazz)
        logger.error(e)

    logger.debug(f'Devo trattare {len(elenco_percorsi)} percorsi di igiene')

    if len(elenco_percorsi)>0:

        # qua mi tiro fuori il token TREG 
    
        token=token_treg(logger)
        logger.debug(token)

        ########################
        #recupero import id TREG
        ########################
        guid = uuid.uuid4()
        logger.debug(str(guid))
        #logger.debug(guid.type)
        #json_id={'id': '{}'.format(str(guid))}
        json_id={'id': str(guid)}
        api_url_begin_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/begin-upload/av1'.format(url_ws_treg)          
        response = requests.post(api_url_begin_upload, json=json_id, headers={'accept':'*/*', 
                                                                                'mde': 'PROD',
                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                'Content-Type': 'application/json'})
        importId=response.json()['importId']
        #exit()

        #salvo info su tabella di controllo per successiva verifica dello stato dell'importazione con script check_status_import.py e per eventuale rollback in caso di errori nell'upload a TREG
        try:
            curr.execute(insert_importid, (os.getpid(), str(guid), str(importId),))
        except Exception as e:
            logger.error(insert_importid)
            logger.error(e)
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
            exit()

        conn.commit()
        
        logger.info('ImportId = {}'.format(importId))

        # facciamo un dizionario con chiave cod_percorso e data, e valore una lista contenente turno e data_pianif_iniziale e data_last_update
        dict_percorsi={}
        
        for ep in elenco_percorsi:
            # cod percorso 0
            # versione_testata 1
            # freq_testata 2
            # freq_settimane 3
            # id_turno 4
            # at2.gestione_arera 5
            # ce.data_pianif_iniziale 6
            
            #logger.debug(ep[0])
            # 1 se prevista # - 1 se non prevista
            # check_s è la settimana del giorno (se P o D)
            # freq_settimane può 

            if datetime.strptime(ep[6], '%Y%m%d').date().isocalendar()[1]%2 == 1:
                check_s='D'
            else:
                check_s='P'

            
            if tappa_prevista(datetime.strptime(ep[6], '%Y%m%d').date(),  ep[2])==1 and (ep[3].strip()=='T' or ep[3]==check_s):
                # come chiave metto cod_percorso e data_pianif_iniziale
                dict_percorsi[ep[0], ep[6]]=[ep[4], ep[6], ep[7], ep[3]]
                # verificato se era prevista verifico che ci sia una scheda chiusa

            
        #logger.debug(dict_percorsi)
        
        # estraiamo dal dizionario dei tratti per percorso la massima data_last_update
        max_data = max((v[2] for v in dict_percorsi.values()), default=None)
        
        # c è la chiave (codice turno)
        # t è il turno    
        
        
        if len(dict_percorsi)>0:
            logger.info('Devo trattare {} percorsi di igiene con scheda consuntivata prevista'.format(len(dict_percorsi)))
            for c, t in dict_percorsi.items():
                #logger.debug(c + ' : ' + str(t))

                # ora devo verificare i tratti   
                
                try:
                    curr.execute(query_elementi_percorso, (c[0], c[0], c[0], c[0], t[1],t[1],))
                    elenco_elementi_percorso=curr.fetchall()
                except Exception as e:
                    logger.error(query_elementi_percorso)
                    logger.error(e)
                

                list_sweeping=[]
                list_trac_del=[]
                # popolo tratti_sit
                curr1 = conn.cursor()
                curr2 = conn.cursor()
                
                
                for eep in elenco_elementi_percorso:
                    # verifico se in frequenza con la solita funzione
                    #if tappa_prevista(datetime.strptime(c[1], '%Y%m%d').date(),  eep[12])==1:
                    # questa sarà da passare a TREG, le altre no
                    
                    # lo calcolo una volta poi lo riuso 
                    prog_dates = programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7], logger)
                    
                    if eep[5] is None:
                        interruptionType = None
                        interruptionCause = None
                        interruptionDate = None
                        resumptionDate = None
                        executionStartDate = eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        executionEndingDate = eep[3].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    elif int(eep[5]) in (100,110,102,101,999): # 100 - compleatato 110 - completato con lavaggio
                        interruptionType = None
                        interruptionCause = None
                        interruptionDate = None
                        resumptionDate = None
                        executionStartDate = eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        executionEndingDate = eep[3].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    else:
                        interruptionType = 'LIM'
                        interruptionCause = causale_arera(curr1, eep[5], logger, errorfile)
                        if interruptionCause is None:
                            
                            messaggio_warning= f'''Per il percorso {c[0]} del {datetime.strptime(c[1], "%Y%m%d").date()} 
                                        trovo delle causali {eep[5]} non mappate in ARERA. 
                                        1) verificare la causale in SIT (etl.cause_disserv) e aggiungere mappatura ARERA
                                        2) riprocessare manualmente la scheda TREG aggiornando la data di last update in treg_eko.consunt_ekovision per farla rientrare nel processo di upload automatico dopo la sistemazione della mappatura causali.'''
                            logger.error(messaggio_warning)
                            warning_message_mail(messaggio_warning,
                                                 'assterritorio@amiu.genova.it', 
                                                 os.path.basename(__file__), 
                                                 logger,
                                                 'PROBLEMA CAUSALE NON MAPPATA SPAZZAMENTO')
                            #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                            #exit()
                        # dobbiamo verificare che non ci sia un altro elemento con stesso trac_code e causale colpa del gestore
                        
                        if interruptionCause != 'CSG': 
                            try: 
                                curr1.execute(query_check_diss, (eep[4], t[1], t[0],))
                                check_diss=curr1.fetchall()
                            except Exception as e:
                                check_error=1
                                logger.error(query_check_diss)
                                logger.error(e)
                            
                            if len(check_diss) > 0 :
                                #logger.debug('Verifico se in frequenza')
                                #logger.debug(f''' check_diss[1] = {check_diss[1]} ,check_diss[2] ={check_diss[2]}, t[1] = {t[1]}''')
                                
                                in_freq_diss = 0
                                for cd in check_diss:
                                    try: 
                                        curr1.execute(query_diss_in_freq, (cd[1], cd[2],
                                                                        cd[1], cd[2],
                                                                        cd[1], cd[2], 
                                                                        t[1], cd[5], t[1],))
                                        in_freq_diss+=curr1.fetchone()[0]
                                    except Exception as e:
                                        check_error=1
                                        logger.error(query_diss_in_freq)
                                        logger.error(e)
                            else:
                                in_freq_diss=-1 # se non è disservizio per colpa del gestore non mi interessa se è in frequenza o no, non devo fare escalation alla causale di disservizio più grave tra gli elementi con stesso trac code
                                
                            if in_freq_diss > 0:
                                #interruptionCause = causale_arera(curr1, check_diss[3], logger, errorfile)
                                # in questo caso sappiamo che la causale peggiore è quella colpa del gestore e la scriviamo a mano
                                interruptionCause = 'CSG'
                                
                                # se in_freq fosse 0 o causale non di disservizio per colpa del gestore, 
                                # allora interruption cause rimarrebbe quella della scheda in questione,
                                # senza escalation alla causale di disservizio più grave trovata tra gli elementi con stesso trac code.
                                
                                
                            # se non c'è disservizio per colpa del gestore non faccio niente
                        
                        interruptionDate = prog_dates[0]
                        #executionStartDate = None
                        #executionEndingDate = None
                        # calcolo il resumption date
                        try:
                            #curr1.execute(select_resumption_date, (eep[4], eep[2],))
                            curr1.execute(select_resumption_date, (eep[4],
                                prog_dates[3],
                                t[1], t[0],))
                            tmp_resumptionDate = curr1.fetchone()
                            
                            # dalla query mi aspetto: 
                            # - se non c'è resumption date il fetchone restituisce  tmp_resumptionDate = None mentre fetchall restituirebbe []
                            # - se c'è resumption date: tmp_resumptionDate = (data_ora_inizio_resumption, data_ora_fine_resumption)
                            
                            if tmp_resumptionDate is None:  
                                logger.warning(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()}, codice {eep[4]} con causale {eep[5]}
                                            non trovo resumption date''')
                                
                                
                                # provo a vedere se è stata eliminata l'asta
                                try:
                                    curr.execute(check_eliminazione_elem0, (eep[4], eep[4],))
                                    data_eliminazione0=curr.fetchone()
                                except Exception as e:
                                    check_error=1
                                    logger.error(check_eliminazione_elem0)
                                    logger.error(e)
                                logger.debug(f'Data eliminzione asta {eep[4]}: {data_eliminazione0}')
                                if data_eliminazione0[0] is not None:
                                    logger.info('Trovo data di eliminazione asta, prendo questa come data di esecuzione')
                                    executionStartDate = data_eliminazione0[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione0[0] is not None else None
                                    executionEndingDate = data_eliminazione0[1].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione0[1] is not None else None
                                    resumptionDate = data_eliminazione0[0] if data_eliminazione0[0] is not None else None
                                    #resumptionDate = data_eliminazione_2[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione_2[0] is not None else None
                                    logger.info('''executionStartDate: {}, executionEndingDate: {}, resumptionDate: {}'''.format(executionStartDate, executionEndingDate, resumptionDate))
                                else:
                                    # provo a vedere se è stato eliminata quell'asta dal percorso                                
                                    try:
                                        curr.execute(check_eliminazione_elem, (eep[4], c[0], t[1]))
                                        data_eliminazione=curr.fetchone()
                                    except Exception as e:
                                        check_error=1
                                        logger.error(check_eliminazione_elem)
                                        logger.error(e)
                                    logger.debug(f'Data eliminzione asta percorso {c[0]}: {data_eliminazione}')
                                    if data_eliminazione is not None:
                                        logger.info('Trovo data di eliminazione asta dal percorso, prendo questa come data di esecuzione')
                                        executionStartDate = data_eliminazione[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione[0] is not None else None
                                        executionEndingDate = data_eliminazione[1].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione[1] is not None else None
                                        resumptionDate = data_eliminazione[0] if data_eliminazione[0] is not None else None
                                        #resumptionDate = data_eliminazione_2[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione_2[0] is not None else None
                                        logger.info('''executionStartDate: {}, executionEndingDate: {}, resumptionDate: {}'''.format(executionStartDate, executionEndingDate, resumptionDate))
                                    else:
                                        # provo a vedere se il percorso in questione è stato eliminato o disattivato, 
                                        # in questo caso prendo la data di eliminazione come data di esecuzione e resumption date\
                                        try:
                                            curr.execute(check_eliminazione_elem_2, (c[0], t[1]))
                                            data_eliminazione_2=curr.fetchone()
                                        except Exception as e:
                                            check_error=1
                                            logger.error(check_eliminazione_elem_2)
                                            logger.error(e)
                                        logger.debug(f'Data chiusura percorso {c[0]}: {data_eliminazione_2}')
                                        if data_eliminazione_2 is not None:
                                            logger.info('Trovo data di chiusura percorso, prendo questa come data di esecuzione')
                                            executionStartDate = data_eliminazione_2[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione_2[0] is not None else None
                                            executionEndingDate = data_eliminazione_2[1].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione_2[1] is not None else None
                                            resumptionDate = data_eliminazione_2[0] if data_eliminazione_2[0] is not None else None
                                            #resumptionDate = data_eliminazione_2[0].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if data_eliminazione_2[0] is not None else None
                                            logger.info('''executionStartDate: {}, executionEndingDate: {}, resumptionDate: {}'''.format(executionStartDate, executionEndingDate, resumptionDate))
                                        else:
                                            logger.warning('''Non trovo nemmeno la data di chiusura percorso, 
                                                        non posso calcolare resumption date e data di esecuzione 
                                                        per ora metto + 96 h rispetto alla data di fine programmazione, forse da rivedere per il 2026''')
                                            executionStartDate = prog_dates[4]
                                            executionEndingDate = prog_dates[5]
                                            resumptionDate = prog_dates[6]
                                    
                            else:
                                resumptionDate = tmp_resumptionDate[0]
                                executionStartDate = resumptionDate.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                                executionEndingDate = tmp_resumptionDate[1].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                                # aggiorno la tabella consunt_ekovision
                                # lo faccio fuori dall'else
                                #lista_update_res_date.append( (resumptionDate, eep[4], eep[2],) )
                                
                                # non faccio direttamente l'update ma lo salvo in una lista per fare un unico commit alla fine
                            
                            
                            # per poi fare un update in bulk alla fine del ciclo di tutti i percorsi, 
                            # così da non rallentare troppo il processo con update uno a uno, 
                            # creando una tabella temporanea e facendo un unico update con join
                            lista_update_res_date.append( (resumptionDate, eep[4], eep[2],) )
                        except Exception as e:
                            logger.error(select_resumption_date)
                            logger.debug(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()} trovo il codice {eep[4]} con causale {eep[5]} 
                                        e non c'è resumption date''')
                            logger.error(f'codice: {eep[4]}')
                            logger.error(f'data_ora_inizio: {eep[2]}')
                            logger.error(e)

                
                    ############# DA GESTIRE IL DELETE In caso di non previsto o festivo 
                    if int(eep[5]) in (102,101,999) or eep[12]==0:
                        # prima bisogna verificare che non ci sia una componente consuntivata con causali != 101 / 102 / 999
                        try:
                            curr.execute(check_del, (eep[4], t[1], t[0],))
                            check_del_res=curr.fetchall()
                        except Exception as e:
                            logger.error(check_del)
                            logger.error(e)
                        
                        id_schede_doppie=''
                        for r in check_del_res:
                            id_schede_doppie=id_schede_doppie + ' ' + str(r[0])
                            
                        if len(check_del_res)==0: 
                            # in questo caso posso eliminare da TREG
                            list_trac_del.append('{0}_{1}_{2}'.format(eep[4],t[1],t[0]))
                        """else:
                            logger.info(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()}
                                        trovo il codice {eep[4]} con causale {eep[5]} 
                                        ma anche altre schede ({id_schede_doppie}) con causale 100, non elimino da TREG. 
                                        ''')
                        """    
                    else:
                        sweeping={
                            'traceabilityCode': '{0}_{1}_{2}'.format(eep[4],t[1],t[0]),
                            'kilometersTravelled': eep[6],
                            'areaType':str(eep[8]),
                            'areaCode': str(eep[4]),
                            'streetCode': str(eep[9]),
                            'streetDescription':str(eep[10]),
                            'programmingStartDate':prog_dates[0],
                            'programmingEndingDate':prog_dates[1],
                            'executionStartDate': executionStartDate,
                            'executionEndingDate': executionEndingDate,
                            'interruptionType': interruptionType,
                            'interruptionCause':interruptionCause,
                            'interruptionDate': interruptionDate,
                            'resumptionDate': resumptionDate.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if resumptionDate is not None else None,
                            #'nonComplianceCauseInterruption': interruptionCause,
                            'nonComplianceCauseInterruption': None,
                            'year':int(prog_dates[2]),
                            'istatCode': str(eep[11]) 
                        }
                        list_sweeping.append(sweeping)
                        list_trac_code_update.append((eep[4], t[1], c[0], '{0}_{1}_{2}'.format(eep[4],t[1],t[0]),))
                
                
                # fine percorso         
                curr1.close()
                curr2.close()
                    
                    

                    
                    
                #logger.debug(f'Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()} devo inviare {list_sweeping}')
                #logger.debug(f'list spazzamenti = {convert_decimal(list_sweeping)}')
                #jsonfile='{0}/log/{1}_spazzamento.json'.format(path,c)
                #with open(jsonfile, 'w', encoding='utf-8') as f:
                #    json.dump(convert_decimal(list_sweeping), f, ensure_ascii=False, indent=4)
                ########################################################
                # upload di list_wasteCollection di un singolo percorso
                ########################################################

                #exit()
                logger.info('Inizio upload dati del percorso {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/upload/av1'.format(url_ws_treg)

                # creo una nuova lista per rimuovere eventuali duplicati  list(set(list_wasteCollection)) ## ATTENZIONE CHE NON MANTIENE ORDINE
                list_sweeping_unique = [
                    dict(t) for t in {
                        tuple(sorted(d.items())) for d in list_sweeping
                    }
                ]

                # questa sarà da passare a TREG, le altre no
                
                body_upload={
                    'id': str(guid),
                    'importId': str(importId),
                    'entities': list_sweeping_unique
                }
                
                
                check_error_upload = call_treg_api(token, api_url_upload, body_upload, list_sweeping_unique, logger, errorfile, 'errorCount', importId)
                
                
                # controllo  se per quel percorso ci sono componenti da cancellare in quanto consuntivate con causale non previsto e/o festivo
                
                if len(list_trac_del) > 0:

                    logger.debug(list_trac_del)
                                
                    logger.info('Inizio delete dati del percorso {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                    api_url_delete='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/delete/av1'.format(url_ws_treg)
                    # questa sarà da passare a TREG, le altre no
                    
                    guid_del = uuid.uuid4()
                    body_delete={
                        'id': str(guid_del),
                        'sweepingIds': list_trac_del
                    }         
                    check_error_delete = call_treg_api(token, api_url_delete, body_delete, list_trac_del, logger, errorfile, 'deletedCount', importId)
                    if check_error_delete>0:
                        logger.error('Errore nel delete di TREG per il percorso di igiene {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        api_url_rollback='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/rollback-upload/av1'.format(url_ws_treg)
                        guid_roll = uuid.uuid4()
                        body_rollback={
                            'id': str(guid_roll),
                            'importId': str(importId),
                        }
                        response_roll = requests.post(api_url_rollback, json=body_rollback, headers={'accept':'*/*', 
                            'mde': 'PROD',
                            'Authorization': 'EIP {}'.format(token),
                            'Content-Type': 'application/json'})
                        logger.error('la chiamata di rollback ha dato questo esito: {}'.format(response_roll.text))
                        exit()
                # chiudo ciclo sui percorsi

        
            #if len(dict_percorsi)>0:
            
            ####################################
            # commit upload
            ####################################
            logger.info('Inizio il commit degli upload su TREG')
            
            # estraiamo dal dizionario dei tratti per percorso la massima data_last_update
            max_data = max((v[2] for v in dict_percorsi.values()), default=None)
            logger.info(f'La massima data_last_update tra i percorsi da trattare è {max_data}')
            #exit()
            if check_error_upload==0:
                api_url_commit_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/commit-upload/av1'.format(url_ws_treg)
                # questa sarà da passare a TREG, le altre no
                
                body_commit_upload={
                    'id': str(guid),
                    'importId': str(importId)
                }
                
                
                response_commit_upload = requests.post(api_url_commit_upload, json=body_commit_upload, headers={'accept':'*/*', 
                                                                                'mde': 'PROD',
                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                'Content-Type': 'application/json'})
                logger.info('Fine commit - Risposta TREG: {}'.format(response_commit_upload.text))
                    
                #facciamo insert su tre_eko.last_import_treg_spazz_cons
                try:
                    curr.execute(query_insert, (max_data,
                                                str(guid), str(importId),
                                                response_commit_upload.status_code, response_commit_upload.text,))
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e) 

                # se import andatao a buon fine faccio delete su tabella di controllo importid per evitare che venga intercettato da script check_status_import.py
                try:
                    curr.execute(delete_importid, (str(importId),))
                    conn.commit()
                except Exception as e:
                    logger.error(delete_importid)
                    logger.error(e) 
        
            else: 
                logger.warning('Sono presenti errori, faccio il commit ridotto')

                #non facciamo commit su TREG ma teniamo traccia con insert su tre_eko.last_import_treg_spazz_cons
                try:
                    curr.execute(query_insert_error, (max_data,
                                                str(guid), str(importId),))
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert_error)
                    logger.error(e)

        else:
            logger.info('Non ci sono percorsi di igiene con scheda consuntivata prevista da trattare, faccio rollback dell\'importId su TREG')
            api_url_rollback='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/rollback-upload/av1'.format(url_ws_treg)
            guid_roll1 = uuid.uuid4()
            body_rollback1={
                'id': str(guid_roll1),
                'importId': str(importId),
            }
            response_roll = requests.post(api_url_rollback, json=body_rollback1, headers={'accept':'*/*', 
                'mde': 'PROD',
                'Authorization': 'EIP {}'.format(token),
                'Content-Type': 'application/json'})
            logger.debug('la chiamata di rollback ha dato questo esito: {}'.format(response_roll.text))

            try:
                curr.execute(delete_importid, (str(importId),))
                conn.commit()
            except Exception as e:
                logger.error(delete_importid)
                logger.error(e) 

        if len(list_trac_code_update)>0:
            logger.info('Inizio l\'inserimento dei trac_code calcolati')
            logger.debug(f'list_trac_code_update = {list_trac_code_update}')
            try:
                bulk_update_consunt_tc(curr, list_trac_code_update, logger)
            except Exception as e:
                logger.error(e)
                logger.error(list_trac_code_update)


        
        if len(lista_update_res_date)>0:
            # inserisco le resumption date calcolate
            logger.info('Inizio l\'inserimento delle resumption date calcolate')
            
            try:
                bulk_update_consunt(curr, lista_update_res_date, logger)
            except Exception as e:
                logger.error(e)
                logger.error(lista_update_res_date) 
        
        
        # faccio unico commit sul DB
        conn.commit()
    else:
        logger.info('Nessun percorso da trattare, non faccio upload né commit')
        
        
    #exit()
            
            

    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    



    if len(elenco_percorsi) == 0:
        # cancelllo anche il file di log
        #logfile.close()
        os.remove(logfile)










if __name__ == "__main__":
    main()      