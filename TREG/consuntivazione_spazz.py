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

def programming_start_ending_date(cursor, data, id_turno, gc):
    
    ''' 
    La funzione in base a giorno, id_turno e giorno_competenza restituisce un array con la programmingStartDate e la programmingEndingDate 
    nel formato voluto da TREG
    '''

    # inizializzo l'array di output
    dates=[]
    
    #logger.debug(id_turno)
    # query per tirare fuori l'intervallo con cui calcolare il giorno di fine
    query='''select 
            case
                when fine_ora < inizio_ora then 
                1
                else 
                0
            end,
            lpad(inizio_ora::text,2,'0')||':'||lpad(inizio_minuti::text,2,'0') as h_inizio, 
            lpad(fine_ora::text,2,'0')||':'||lpad(fine_minuti::text,2,'0') as h_fine 
            from elem.turni t 
            where id_turno = %s'''

    try:
        cursor.execute(query, (id_turno,))
        riga=cursor.fetchone()
        #h_inizio=cursor.fetchone()[1]
        #h_fine=cursor.fetchone()[2]
    except Exception as e:
        logger.error(query)
        logger.error(e)
    
    interval = riga[0]
    h_inizio = riga[1]
    h_fine= riga[2]

    hhi, mmi = map(int, h_inizio.split(':'))
    hhf, mmf = map(int, h_fine.split(':'))
    
    #logger.debug(interval)
    #exit()
    #data = data.astimezone(timezone.utc)
    data = datetime.combine(data, datetime.min.time())
    if gc == 0:
        dt_inizio = data.replace(hour=hhi, minute=mmi, second=0, microsecond=0).astimezone(timezone.utc)
        dt_fine = data.replace(hour=hhf, minute=mmf, second=0, microsecond=0).astimezone(timezone.utc)
    elif gc == -1: 
        data_inizio= data-timedelta(days=1)
        data_fine = data_inizio+timedelta(days=interval)
        dt_inizio = data_inizio.replace(hour=hhi, minute=mmi, second=0, microsecond=0).astimezone(timezone.utc)
        dt_fine = data_fine.replace(hour=hhi, minute=mmi, second=0, microsecond=0).astimezone(timezone.utc)
    else: 
        logger.error('Come mai gc vale {}'.format(gc))
        
    
    dates.append(dt_inizio.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    dates.append(dt_fine.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
    dates.append(dt_inizio.strftime('%Y'))

    return dates

def main():
      
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
        order by 8 asc limit 1600
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
            ce.qualita
            from treg_eko.consunt_ekovision ce
            left join (
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti where data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine,
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.v_percorsi_elementi_tratti_ovs where data_inizio < coalesce(data_fine, '20991231')
                union 
                SELECT codice_modello_servizio, ordine, objecy_type, 
                    codice, quantita, lato_servizio, percent_trattamento,frequenza,
                    ripasso, numero_passaggi, replace(replace(coalesce(nota,''),'DA PIAZZOLA',''),';', ' - ') as nota,
                    codice_qualita, codice_tipo_servizio, data_inizio, coalesce(data_fine, '20991231') as data_fine, 
                    id_asta_percorso, id_elemento_asta_percorso
                FROM anagrafe_percorsi.mv_percorsi_elementi_tratti_dismessi where data_inizio < coalesce(data_fine, '20991231')
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
            tab.frequenza,
            ce.qualita
        ) 
        -- qua raggruppo per codice e data (dovrei escludere le schede doppie e prendere la causale migliore)
        select  
        codice_servizio_pred,
        data_pianif_iniziale, 
        --data_esecuzione_prevista,
        min(data_ora_inizio) as data_ora_inizio_exec, 
        max(data_ora_fine) as data_ora_fine_exec,
        codice,
        case
            when 100 = ANY (array_agg(distinct gs.causale::int)::int[]) then 100
            else min(distinct gs.causale::int)
        end causale,
        (aa.lung_asta * qualita / 100.0) / 1000 as kilometersTravelled,
        ep2.giorno_competenza,
        'PRG' as areaType,
        min(aa.id_via) as streetCode,
        min(v.nome) as streetDescription,
        min(c.cod_istat) as istatCode
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
        group by codice_servizio_pred,
            data_pianif_iniziale, aa.lung_asta,
            gs.qualita,
            codice, ep2.giorno_competenza
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
      
    

    try:
        curr.execute(query_first_day)
        giorno_mese_anno=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_first_day)
        logger.error(e)
    
    
    for gma in giorno_mese_anno:
        data_last_update=gma[0]

    


    # qua mi tiro fuori il token TREG 
    
    token=token_treg(logger)
    logger.debug(token)

    #while  data_start <= fine_ciclo:

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
    
    logger.info('ImportId = {}'.format(importId))
    
    
    # inizializzo un check 
    # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
    check_error_upload=0
    
    
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
            dict_percorsi[ep[0], ep[6]]=[ep[4], ep[6], ep[7]]
            # verificato se era prevista verifico che ci sia una scheda chiusa

        
        
    logger.info(f'Devo trattare {len(dict_percorsi)} percorsi di igiene')
    #logger.debug(dict_percorsi)
    
    # estraiamo dal dizionario dei tratti per percorso la massima data_last_update
    max_data = max((v[2] for v in dict_percorsi.values()), default=None)
    
       
    # c è la chiave (codice turno)
    # t è il turno    
    
    
      
    for c, t in dict_percorsi.items():
        #logger.debug(c + ' : ' + str(t))

        # ora devo verificare i tratti   
        
        try:
            curr.execute(query_elementi_percorso, (c[0], t[1],))
            elenco_elementi_percorso=curr.fetchall()
        except Exception as e:
            logger.error(query_elementi_percorso)
            logger.error(e)
        

        list_sweeping=[]
        list_trac_del=[]
        # popolo tratti_sit
        for eep in elenco_elementi_percorso:
            # verifico se in frequenza con la solita funzione
            #if tappa_prevista(data_start,  eep[1])==1:
                # questa sarà da passare a TREG, le altre no
            curr1 = conn.cursor()

            if eep[5] is None:
                interruptionType = None
                interruptionCause = None
                interruptionDate = None
                resumptionDate = None
            elif int(eep[5]) in (100,110,102,101,999): # 100 - compleatato 110 - completato con lavaggio
                interruptionType = None
                interruptionCause = None
                interruptionDate = None
                resumptionDate = None
            else:
                interruptionType = 'LIM'
                interruptionCause = causale_arera(curr1, eep[5], logger, errorfile)
                if interruptionCause is None:
                    logger.error(f'Per il percorso {c[0]} del {datetime.strptime(c[1], "%Y%m%d").date()} trovo delle causali {eep[5]} non mappate in ARERA')
                    #error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                interruptionDate = programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7])[0]
                resumptionDate= eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            
            ############# DA GESTIRE IL DELETE In caso di non previsto o festivo 
            if int(eep[5]) in (102,101,999):
                list_trac_del.append('{0}_{1}_{2}'.format(eep[4],t[1],t[0]))
            else:
                sweeping={
                    'traceabilityCode': '{0}_{1}_{2}'.format(eep[4],t[1],t[0]),
                    'kilometersTravelled': eep[6],
                    'areaType':str(eep[8]),
                    'areaCode': str(eep[4]),
                    'streetCode': str(eep[9]),
                    'streetDescription':str(eep[10]),
                    'programmingStartDate':programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7])[0],
                    'programmingEndingDate':programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7])[1],
                    'executionStartDate': eep[2].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'executionEndingDate':eep[3].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'interruptionType': interruptionType,
                    'interruptionCause':interruptionCause,
                    'interruptionDate': interruptionDate,
                    'resumptionDate': resumptionDate,
                    'nonComplianceCauseInterruption': interruptionCause,
                    'year':int(programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[7])[2]),
                    'istatCode': str(eep[11]) 
                }
                list_sweeping.append(sweeping)
            
        
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
        # questa sarà da passare a TREG, le altre no
        
        body_upload={
            'id': str(guid),
            'importId': str(importId),
            'entities': list_sweeping
        }
        
        
        check_error_upload = call_treg_api(token, api_url_upload, body_upload, list_sweeping, logger, errorfile, 'errorCount')
        
        
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
            check_error_delete = call_treg_api(token, api_url_delete, body_delete, list_trac_del, logger, errorfile, 'deletedCount')
            if check_error_delete>0:
                logger.error('Errore nel delete di TREG per il percorso di igiene {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
        # chiudo ciclo sui percorsi

    
    if len(dict_percorsi)>0:
        
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
    
        else: 
            logger.warning('Sono presenti errori, non faccio il commit')

            #non facciamo commit su TREG ma teniamo traccia con insert su tre_eko.last_import_treg_spazz_cons
            try:
                curr.execute(query_insert_error, (max_data,
                                            str(guid), str(importId),))
                conn.commit()
            except Exception as e:
                logger.error(query_insert_error)
                logger.error(e)    
    else:
        logger.info('Nessun percorso da trattare, non faccio upload né commit')
        
        
    #exit()
            
            
 
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      