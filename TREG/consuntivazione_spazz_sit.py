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
# per la gestione della mia zona
import pytz
tz_roma = pytz.timezone('Europe/Rome')



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

from consuntivazione_spazz import bulk_update_consunt



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



def main():
    
    ##################################################
    debug = 1
    # se non specifico id_scheda_test lavora con una query specifica dove individuo un elenco di percorsi 
    # da correggere in base a specifiche caratteristiche
    # ATTENZIONE in entambi i casi non scrive null nella tabella di log dei processamenti quindi il rischio e che da crontab vada in loop!!
    
    # volendo si può definire se fare o meno il commit in fondo 
    
    id_scheda_test = 567233 #483903 #None # 748395
    ##################################################



    ###################################################
    # LOGGER
    
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
    f_handler.setLevel(logging.DEBUG)


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
        select coalesce(max(data_last_update), to_date('20250101', 'YYYYMMDD')) 
        from consunt.last_import_sit_spazz_cons
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
        and at2.gestione_duale = 't'
        and at2.id_famiglia in (2,3)
        /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
        order by ce.data_last_update asc
        /*limit 100000*/
        ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
        from step0
        group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
        order by 8 asc limit 1000
    '''
    
    
    
    if debug==1:
        logger.debug('Sono in modalità debug, quindi prendo un singolo id_scheda per testare la query di estrazione dei percorsi di raccolta')
    
        
        if id_scheda_test is None:
            
            # giorno competenza  -1
            """
            query_elenco_percorsi_spazz='''with step0 as (
        select ep.cod_percorso, versione_testata, fo.freq_binaria, freq_settimane, 
        id_turno, at2.gestione_arera, ce.data_pianif_iniziale, ce.data_last_update 
        from  treg_eko.consunt_ekovision ce 
        join anagrafe_percorsi.elenco_percorsi ep 
            on ep.cod_percorso = ce.codice_servizio_pred 
            and to_date(ce.data_pianif_iniziale, 'YYYYMMDD') between data_inizio_validita and (data_fine_validita - interval '1' day) 
        join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
        join etl.frequenze_ok fo on fo.cod_frequenza = ep.freq_testata 
        where ep.giorno_competenza = -1
        and at2.gestione_duale = 't'
        and at2.id_famiglia  in (2,3)
        /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
        order by ce.data_last_update asc
        /*limit 100000*/
        ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
        from step0
        group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
        order by 8'''
            """
            # riprocesso i dati dei percorsi previsti in giornata (c'era un errore nell'update delle tappe non_prev)
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
            where 
            treg_eko.verify_daily_frequency(
                ep.freq_testata ,
                to_date(ce.data_pianif_iniziale,'YYYYMMDD'),
                ep.freq_settimane 
            ) = 1
            and at2.gestione_duale = 't'
            and at2.id_famiglia  in (2,3)
            /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
            --order by ce.data_last_update asc
            /*limit 100000*/
            ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
            from step0
            group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
            order by 8
            '''
            
            
            
        else:
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
        where ce.id_scheda = %s
        and at2.gestione_duale = 't'
        and at2.id_famiglia in (2,3)
        /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
        order by ce.data_last_update asc
        /*limit 100000*/
        ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
        from step0
        group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
        order by 8 limit 1000
        '''
        
        
    
    insert_sql_sit= '''
INSERT INTO consunt.report_spazz (
    trac_code, cod_percorso, id_via,
    id_asta, lung_km, nota_asta,
    data_programmata, orario_progr,
    non_previsto, id_causale, 
    id_causale_totem, qualita, 
    data_ora_ini_esec,  data_ora_fine_esec, tempo_recupero,
    tempo_ripresa, 
    tipo_spazz, id_asta_percorso) 
    VALUES %s
    ON CONFLICT (trac_code, cod_percorso, id_asta_percorso) 
    /* or you may use [DO NOTHING;] */ DO UPDATE 
    SET id_via=EXCLUDED.id_via,
    id_asta=EXCLUDED.id_asta,
    lung_km=EXCLUDED.lung_km,
    nota_asta=EXCLUDED.nota_asta,
    data_programmata=EXCLUDED.data_programmata,
    orario_progr=EXCLUDED.orario_progr,
    non_previsto = EXCLUDED.non_previsto, 
    id_causale= EXCLUDED.id_causale, 
    id_causale_totem= EXCLUDED.id_causale_totem,
    qualita = EXCLUDED.qualita,
    data_ora_ini_esec= EXCLUDED.data_ora_ini_esec,
    data_ora_fine_esec= EXCLUDED.data_ora_fine_esec,
    tempo_recupero= EXCLUDED.tempo_recupero,
    tempo_ripresa = EXCLUDED.tempo_ripresa,
    tipo_spazz=EXCLUDED.tipo_spazz ;
'''
    
    
    update_sql_sit = '''
    update consunt.report_spazz
    set id_causale = %s, id_causale_totem = %s,
    qualita = %s,
    data_ora_ini_esec = %s,  data_ora_fine_esec = %s, 
    tempo_recupero = %s, tempo_ripresa = %s, lung_km = %s,
    non_previsto = %s
    where trac_code = %s and cod_percorso = %s and id_asta_percorso = %s
    '''
    
    
    #########
    # PROBLEMA CHE FACENDO JOIN SOLO CON ID_ASTA TROVA TROPPA ROBA 
    # ma su consunt_ekovision non abbiamo altre informazioni per fare il join corretto
    # ci sarebbe pos ma solo nei casi in cui il percorso non fosse mai stato cambiato
    # 
    
        
    # possibile idea non perfetta:
    # se il percorso è in frequenza prendo solo le tappe in freq
    # se il percorso non è in frequenza (casi piuttosto rari) prendo tutto
    # per il futuro in seguito a evolutive occorre passare a ekovision
    # id_asta_percorso 
    # e id_elemento_asta_percorso 
    # e che le 2 informazioni ci siano restituite nel json
    
    
    
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
            max(ce.causale_totem) as causale_totem,
            tab.frequenza,
            case
                when 100 = ANY (array_agg(distinct causale::int)::int[]) then max(ce.qualita)
                else min(distinct ce.qualita)
            end qualita, 
            tab.nota, 
            tab.id_asta_percorso
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
            /*and tab.ordine = ce.pos*/
            /* codice e posizione dovrebbero essere chiave primaria.. un po' come id_asta_percorso... 
            ma non funziona per casini di Ekovision dovuti alle variazioni dei percorsi */
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
            tab.frequenza, 
            tab.nota,
            tab.id_asta_percorso,
            ce.qualita
            ) 
        -- qua raggruppo per codice e data (dovrei escludere le schede doppie e prendere la causale migliore)
        select  
        codice_servizio_pred,
        data_pianif_iniziale, 
        --data_esecuzione_prevista,
        min(data_ora_inizio) + (ep2.giorno_competenza || ' day')::interval  as data_ora_inizio_exec, 
        max(data_ora_fine) + (ep2.giorno_competenza || ' day')::interval  as data_ora_fine_exec,
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
        ) as in_freq, 
        case
            when 100 = ANY (array_agg(distinct gs.causale::int)::int[]) then NULL
            else min(gs.causale_totem)::int
        end  as causale_totem,
        max(gs.qualita) as qualita, 
        gs.nota, 
        gs.id_asta_percorso
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
            codice, ep2.giorno_competenza, 
            gs.nota, 
            gs.id_asta_percorso
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
        AND causale ='100'
        AND (data_ora_inizio + (ep.giorno_competenza || ' day')::interval  >= %s
        OR /* cerco anche il caso di anticipo*/
        data_pianif_iniziale = %s  and id_turno = %s)
    group by ep.giorno_competenza 
    order by 2,1;
    '''

    query_insert='''INSERT INTO consunt.last_import_sit_spazz_cons
        (data_last_update, last_update) VALUES
        (%s, now());'''
    
    
    # verifica se ci sono altri elementi con stesso trac_code e causale con disservizio
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
        and ce.causale::int not in (101, 102, 999, 100, 110)
        order by id_causale_arera desc limit 1'''
        
        
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
    union all
    SELECT codice_modello_servizio, ordine, objecy_type, 
        codice, quantita, lato_servizio, percent_trattamento,frequenza,
        ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
        codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
        id_asta_percorso, id_elemento_asta_percorso
    FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs 
    where codice_modello_servizio = %s 
    and codice = %s and data_inizio < coalesce(data_fine, '20991231')
    union all
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
    	where %s between tab.data_inizio and tab.data_fine '''
    
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
        if debug ==0:
            curr.execute(query_elenco_percorsi_spazz, (data_last_update,))
        else:
            if id_scheda_test is None:  
                curr.execute(query_elenco_percorsi_spazz)
            else:
                curr.execute(query_elenco_percorsi_spazz, (id_scheda_test,))
        elenco_percorsi=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_elenco_percorsi_spazz)
        logger.error(e)

    logger.debug(f'Devo trattare {len(elenco_percorsi)} percorsi di igiene')

    if len(elenco_percorsi)>0:


        

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
                non_prev = None
            else:
                non_prev = True
            
            
            dict_percorsi[ep[0], ep[6]]=[ep[4], ep[6], ep[7], ep[3], non_prev]
            # verificato se era prevista verifico che ci sia una scheda chiusa

            
        #logger.debug(dict_percorsi)
        
        
        
        # c è la chiave (codice turno)
        # t è il turno    
        
        
        if len(dict_percorsi)>0:
            logger.info('Devo trattare {} percorsi di igiene'.format(len(dict_percorsi)))
            
            # c è la chiave (codice percorso, data)
            # t è il valore (una tupla di turno, data, etc..)
            for c, t in dict_percorsi.items():
                #logger.debug(c + ' : ' + str(t))

                # ora devo verificare i tratti   
                logger.debug('Inizio estrazione elementi per il percorso {} con pianificazione iniziale {}'.format(c[0], t[1]))
                
                
                try:
                    curr.execute(query_elementi_percorso, (c[0], c[0], c[0], c[0], t[1],t[1],))
                    elenco_elementi_percorso=curr.fetchall()
                except Exception as e:
                    logger.error(query_elementi_percorso)
                    logger.error(e)
                

                lista_insert=[]
                lista_update=[]
                # popolo tratti_sit
                curr1 = conn.cursor()
                curr2 = conn.cursor()
                for eep in elenco_elementi_percorso:
                    # verifico se in frequenza con la solita funzione
                    #if tappa_prevista(datetime.strptime(c[1], '%Y%m%d').date(),  eep[12])==1:
                    # questa sarà da passare a TREG, le altre no
                    
                    #logger.debug('Tratto {} del percorso {} con pianificazione iniziale {}'.format(eep[4], c[0], t[1]))
                    prog_dates = programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7], logger)
                    
                    if eep[5] is None:
                        interruptionType = None
                        interruptionCause = None
                        causale_ok = None
                        interruptionDate = None
                        resumptionDate = None
                        executionStartDate = eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        executionEndingDate = eep[3].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    elif int(eep[5]) in (100,110,102,101,999): # 100 - compleatato 110 - completato con lavaggio
                        interruptionType = None
                        interruptionCause = None
                        causale_ok = eep[5]
                        interruptionDate = None
                        resumptionDate = None
                        executionStartDate = eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        executionEndingDate = eep[3].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    else:
                        interruptionType = 'LIM'
                        interruptionCause = causale_arera(curr1, eep[5], logger, errorfile)
                        if interruptionCause is None:
                            logger.error(f'Per il percorso {c[0]} del {datetime.strptime(c[1], "%Y%m%d").date()} trovo delle causali {eep[5]} non mappate in ARERA')
                            #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        
                        
                        # devo verificare che non ci sia lo stesso tratto con causale colpa del gestore
                        
                        try: 
                            curr1.execute(query_check_diss, (eep[4], t[1], t[0],))
                            check_diss=curr1.fetchone()
                        except Exception as e:
                            check_error=1
                            logger.error(query_check_diss)
                            logger.error(e)
                        
                        # se trova qualcosa già consuntivato con altra causale 
                        if check_diss is not None:
                            if check_diss[4] == 3:
                                try: 
                                    curr1.execute(query_diss_in_freq, (check_diss[1], check_diss[2],
                                                                    check_diss[1], check_diss[2],
                                                                    check_diss[1], check_diss[2], 
                                                                    t[1], check_diss[5], t[1],))
                                    in_freq_diss=curr1.fetchone()[0]
                                except Exception as e:
                                    check_error=1
                                    logger.error(query_diss_in_freq)
                                    logger.error(e)
                            else:
                                # che sia in frequenza o no non mi interessa 
                                in_freq_diss = 0
                                 
                            # se colpa del gestore tengo quella
                            if check_diss[4] == 3 and in_freq_diss == 1:
                                #interruptionCause = causale_arera(curr1, check_diss[3], logger, errorfile) 
                                causale_ok=check_diss[3]
                            #altrimenti sovrascrivo
                            else:
                                causale_ok=eep[5]
                        # se non trovo nulla prendo per buona la causale che ho sulla scheda
                        else :
                            causale_ok=eep[5]
                        
                        
                        
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
                            if tmp_resumptionDate is None : # tmp_resumptionDate[0] is None:
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

                
                
                    trac_code= '{0}_{1}_{2}'.format(eep[4],t[1],t[0])
                    # cod_percorso = c[0]
                    # id_via  eep[9]
                    # id_asta eep[4]
                    # lung_km eep[6]
                    # nota_asta eep[15]
                    # data_prpgrammata datetime.strptime(t[1], '%Y%m%d').date()
                    # orario_programmato decode_turno(curr1, t[0], logger) 
                    if t[4] == True:
                        t_non_prev = True
                    else :
                        if eep[12]==0:
                            t_non_prev = True
                        else:
                            t_non_prev = None
                    # id_causael eep[5]
                    # id_causale_tote eep[13]
                    #qualita  eep[14]
                    
                
                     # calcolo tempo recupero
                    if int(eep[5]) in (100,110,102,101,999):
                        t_recupero = None 
                        t_ripresa = None
                        esec_inizio_tz = None 
                        esec_fine_tz = None
                    else:
                        # prendo la data inizio esecuzione UTC e la riconverto in datetime 
                        esec_inizio = datetime.strptime(executionStartDate, 
                                                '%Y-%m-%dT%H:%M:%S.%fZ')
                        esec_fine = datetime.strptime(executionEndingDate, 
                                                '%Y-%m-%dT%H:%M:%S.%fZ')
                        
                        # prendo la data fine programmazione UTC e la riconverto in datetime
                        progr_iniziale = datetime.strptime(prog_dates[0], 
                                                '%Y-%m-%dT%H:%M:%S.%fZ')
                        progr_finale = datetime.strptime(prog_dates[1], 
                                                '%Y-%m-%dT%H:%M:%S.%fZ')
                        
                        # se la data di inizio esecuzione è > di quella di fine programmazione significa che il servizio è stato fatto in maniera non regolare, 
                        # quindi devo calcolare un tempo recupero in h altrimenti no 
                        
                        t_ripresa = round((esec_inizio - progr_iniziale).total_seconds()/3600,3) if esec_inizio >  progr_iniziale else None
                        t_recupero = round((esec_fine - progr_finale).total_seconds()/3600,3) if esec_fine > progr_finale else None
                            
                        
                        # per scrivere su DB SIT 
                        # 1) converto in datetime 
                        # 2) ritorno alle ore non UTC che sono più leggibili
                        esec_inizio_tz = esec_inizio.replace(tzinfo=timezone.utc).astimezone(tz_roma)
                        
                        esec_fine_tz = datetime.strptime(executionEndingDate, 
                                                '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).astimezone(tz_roma)
                    
                    
                        # tipo_spazz eep[8]
                        # id_asta_percorso eep[16]
                    
                    causale_totem = int(eep[13]) if eep[13] is not None else None
                    #print(causale_totem)
                    #exit()
                    
                    if t_non_prev == True: # percorso non previsto faccio insert di tutto
                        lista_insert.append((trac_code, c[0], eep[9], eep[4], 
                                             eep[6], eep[15], datetime.strptime(t[1], '%Y%m%d').date(), 
                                             decode_turno(curr1, t[0], logger), t_non_prev, 
                                             int(causale_ok), causale_totem, eep[14], esec_inizio_tz,  esec_fine_tz,
                                             t_recupero, t_ripresa, eep[8], eep[16]
                                             ))   
                        
                    else:  
                        lista_update.append((int(causale_ok), causale_totem, eep[14], 
                            esec_inizio_tz,  esec_fine_tz,
                            t_recupero, t_ripresa, eep[6], t_non_prev,
                            trac_code, c[0], eep[16]))  

                    
            
                
                
                
                
                
                # fine percorso       
                curr1.close()
                curr2.close()
                # per l'insert non uso cur.execute ma questo metodo execute_values che è più performante
                if len(lista_insert)> 0: 
                    try:        
                        execute_values(curr, insert_sql_sit, lista_insert)
                        #.execute(insert_sql_sit, (lista_insert,))
                    except Exception as e:
                        logger.error(insert_sql_sit)
                        logger.error(e)       
                
                
                # nel caso dell'update faccio un ciclo sulle singole tuple usando il classico curr.execute
                #if len(lista_update)> 0: 
                for tupla in lista_update:
                    try:        
                        curr.execute(update_sql_sit, tupla)
                    except Exception as e:
                        logger.error(tupla)
                        logger.error(update_sql_sit)
                        logger.error(e)      
                
                
            
            
            
                
            # estraiamo dal dizionario dei tratti per percorso la massima data_last_update
            max_data = max((v[2] for v in dict_percorsi.values()), default=None)    

                    
            logger.info(f'La massima data_last_update tra i percorsi da trattare è {max_data}')        

            if debug == 0:     
                try:
                    curr.execute(query_insert, (max_data,))
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
        
        
        else: 
            logger.info('Non ci sono percorsi di spazzamento con scheda consuntivata da trattare ')        
        
       
       
       
        if len(lista_update_res_date)>0:
            logger.info('Inizio l\'inserimento delle resumption date calcolate')
            
            try:
                bulk_update_consunt(curr, lista_update_res_date, logger)
            except Exception as e:
                logger.error(e)
                logger.error(lista_update_res_date)
        
        # faccio unico commit sul DB solo se non sono in modalità (debug ==0)
        if debug == 0:
            conn.commit()
        else:
            logger.debug('Sono in modalità debug') 
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