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
- join con elementi, piazzole, aste, via per recuperare informazioni sulla via 
- join con tipo_elemento, tipo_rifiuto per recuperare il CER CODE


2) periodo di attività del percorso, per i percorsi stagionali o dismessi 
l'elemento / elemento_asta_percorso non sono eliminati quindi nella query (tabella anagrafe_percorsi.elenco_percorsi)

3) turno previsto (tabella anagrafe_percorsi.elenco_percorsi)

4) servizio da inviare ad ARERA (tabella anagrafe_percorsi.anagrafe_tipo)

5) consuntivazione (tabella treg_eko.consunt_ekovision ce)


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




#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0

    


from treg_env import *



def bulk_update_consunt(cursor, updates):
    cursor.execute("""
        CREATE TEMP TABLE tmp_consunt_update_racc (
            codice integer,
            data_ora_inizio TIMESTAMP,
            resumption_date TIMESTAMP
        ) ON COMMIT DROP
    """)

    from psycopg2.extras import execute_values

    execute_values(
        cursor,
        """
        INSERT INTO tmp_consunt_update_racc
        (resumption_date, codice, data_ora_inizio)
        VALUES %s
        """,
        updates
    )

    cursor.execute("""
        UPDATE treg_eko.consunt_ekovision ce
        SET resumption_date = t.resumption_date
        FROM tmp_consunt_update_racc t
        WHERE ce.codice = t.codice
          AND ce.data_ora_inizio = t.data_ora_inizio
          AND ce.causale NOT IN ('100','110')
          AND ce.resumption_date IS DISTINCT FROM t.resumption_date
    """)




def main():
      
    
    # definizione query 
    
    
    
    
    # 1 - cerco il giono da cui partire
    query_first_day='''select min(data_last_update) from treg_eko.consunt_ekovision ce
        where ce.tipo_servizio != 'SPAZZ' and ce.data_last_update >= (
        select coalesce(max(data_last_update), to_date('20250101', 'YYYYMMDD')) 
        from treg_eko.last_import_treg_racc_cons
        where commit_code=200 and deleted = false
        )'''
    
    # 2 - percorsi 
    
    query_elenco_percorsi_racccolta='''
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
        and at2.id_famiglia not in (2,3)
        /*ATTENZIONE A QUEST'ORDINAMENTO CHE SERVE PER GESTIRE I GIRI SUCCESSIVI*/
        order by ce.data_last_update asc
        /*limit 100000*/
        ) select cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale, max(data_last_update)
        from step0
        group by cod_percorso, versione_testata, freq_binaria, freq_settimane, id_turno, gestione_arera, data_pianif_iniziale
        order by 8 limit 1000
        '''
    # 3 - cerco dettagli percorso da SIT
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
	/*flg_riprogrammato,
	flg_non_previsto,
	id_scheda_riprogr,*/
	ce.codice, 
	case
		when 100 = ANY (array_agg(distinct causale::int)::int[]) then 100
		else max(distinct ce.causale::int)
	end causale, 
	tab.frequenza
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
	where ce.tipo_servizio in ('RACC', 'RACC-LAV')
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
	tab.frequenza
) 
-- qua raggruppo per codice e data (dovrei escludere le schede doppie e prendere la causale migliore)
select  
	codice_servizio_pred,
	data_pianif_iniziale, 
	--data_esecuzione_prevista,
	min(data_ora_inizio) as data_ora_inizio_exec, 
	max(data_ora_fine) as data_ora_fine_exec,
	/*flg_riprogrammato, DA CAPIRE SE CI SERVISSE*/ 
	codice,
	case
		when 100 = ANY (array_agg(distinct gs.causale::int)::int[]) then 100
		else min(distinct gs.causale::int)
	end causale, 
	ep2.giorno_competenza,
	case
        when ep.id_elemento_privato is null then 'OTH'
        else 'DOM'
        /* in questo momento non c'è perimetrazione delle aree di pregio */
    end as collectionType,
    min(aa.id_via) as streetCode,
    min(v.nome) as streetDescription, 
    min(tr.codice_cer) as cerCode,
    min(tr.nome) as wasteDescription,
    min(c.cod_istat) as istatCode
from group_scheda gs
left join anagrafe_percorsi.elenco_percorsi ep2 
on ep2.cod_percorso = gs.codice_servizio_pred 
and to_date(gs.data_pianif_iniziale, 'YYYYMMDD') between ep2.data_inizio_validita and ep2.data_fine_validita
left join (select id_piazzola, id_elemento, tipo_elemento, id_asta from elem.elementi
union 
select id_piazzola, id_elemento, tipo_elemento, id_asta from history.elementi
) ee 
    on ee.id_elemento = gs.codice
left join elem.piazzole p 
    on p.id_piazzola = ee.id_piazzola
left join (select id_asta, id_via from elem.aste
union 
select id_asta, id_via from history.aste) aa
    on aa.id_asta = coalesce(p.id_asta, ee.id_asta)
left join topo.vie v on v.id_via = aa.id_via 
left join topo.comuni c on c.id_comune = v.id_comune 
left join elem.tipi_elemento te on te.tipo_elemento = ee.tipo_elemento
left join elem.tipi_rifiuto tr on tr.tipo_rifiuto = te.tipo_rifiuto
left join elem.elementi_privati ep on ep.id_elemento = ee.id_elemento
left join etl.frequenze_ok fo on fo.cod_frequenza = gs.frequenza
where codice_servizio_pred = gs.codice_servizio_pred
and data_pianif_iniziale = gs.data_pianif_iniziale
group by codice_servizio_pred,
	data_pianif_iniziale,
	codice, ep2.giorno_competenza,
	ep.id_elemento_privato/*,
	aa.id_via ,
   v.nome , 
   tr.codice_cer,
   tr.nome,
   c.cod_istat*/
            '''
    

    query_update_check='''UPDATE marzocchir.check_upload_treg set upload = 1
    where codice_servizio_pred = %s and data_pianif_iniziale = %s'''

    select_resumption_date = '''SELECT min(ce.data_ora_inizio)
        FROM treg_eko.consunt_ekovision ce
        WHERE codice = %s
        AND causale IN ('100','110')
        AND data_ora_inizio > %s
        /*ORDER BY ce.data_ora_inizio
        LIMIT 1*/;
    '''
    
    update_resumption_date = '''UPDATE treg_eko.consunt_ekovision ce 
    SET resumption_date= %s 
    WHERE ce.codice = %s
    AND ce.causale NOT IN ('100','110')
    AND ce.data_ora_inizio = %s '''
    
    
    query_insert='''INSERT INTO treg_eko.last_import_treg_racc_cons
        (data_last_update, last_update,
        request_id_amiu, importid_treg, 
        commit_code, commit_message) 
        VALUES(%s, now(), 
        %s, %s, 
        %s, %s);'''
    
    query_insert_error='''INSERT INTO treg_eko.last_import_treg_racc_cons
            (data_last_update, last_update,
            request_id_amiu, importid_treg) 
            VALUES(%s, now(), 
            %s, %s);'''
    
    query_create_index = '''
        CREATE INDEX consunt_ekovision_raccolta_tmp ON treg_eko.consunt_ekovision USING btree (codice, data_ora_inizio, causale);'''
    
    ###################### fine definizione query ######################
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # abbiamo notato che ogni tanto si incarta nel fare l'upload delle liste di wastcollection quindi lo gestiamo con più tentativi
          
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    

    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    curr_check = conn.cursor()
    
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

    # inizializzo un check 
    # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
    check_error_upload=0
    lista_update_res_date=[]
    
    ##################################
    # procedo con il recupero dati
    ##################################
     
    # eseguo query 2 per estrazione percorsi
    try:
        curr.execute(query_elenco_percorsi_racccolta, (data_last_update,))
        elenco_percorsi=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_elenco_percorsi_racccolta)
        logger.error(e)

    logger.debug(f'Devo trattare {len(elenco_percorsi)} percorsi di raccolta')

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
        api_url_begin_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/begin-upload/av1'.format(url_ws_treg)          
        response = requests.post(api_url_begin_upload, json=json_id, headers={'accept':'*/*', 
                                                                                'mde': 'PROD',
                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                'Content-Type': 'application/json'})
        importId=response.json()['importId']
        #exit()
        
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
            
            #logger.debug(ep[0])
            # 1 se prevista # - 1 se non prevista
            # check_s è la settimana del giorno (se P o D)
            # freq_settimane può 
            
            if datetime.strptime(ep[6], '%Y%m%d').date().isocalendar()[1]%2 == 1:
                check_s='D'
            else:
                check_s='P'

            #logger.debug('ep[0] = {}'.format(ep[0]))
            #logger.debug('ep[2] = {}'.format(ep[2]))
            #logger.debug('ep[3] = {}'.format(ep[3]))
            #logger.debug('ep[6] = {}'.format(ep[6]))
            #exit()
            

            if tappa_prevista(datetime.strptime(ep[6], '%Y%m%d').date(),  ep[2])==1 and (ep[3].strip()=='T' or ep[3]==check_s): 
                # verificato se era prevista verifico che ci sia una scheda chiusa
            
                dict_percorsi[ep[0], ep[6]]=[ep[4], ep[6], ep[7]]
        
        
        
        logger.info('Devo trattare {} percorsi di raccolta con scheda consuntivata prevista'.format(len(dict_percorsi)))
        #logger.debug(dict_percorsi)
        # c è la chiave (codice percorso)
        # t è il turno      
        
        
        for c, t in dict_percorsi.items():
            #logger.debug(c + ' : ' + str(t))
            
            
            
            # ora devo verificare le componenti
            
            comp_sit=[]
            
            
            
            #logger.debug(data_start.strftime('%Y%m%d'))
            logger.debug('Inizio estrazione elementi per il percorso {} con pianificazione iniziale {}'.format(c[0], t[1]))
            try:
                curr.execute(query_elementi_percorso, (c[0], t[1],))
                elenco_elementi_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_elementi_percorso)
                logger.error(e)
            
            
            
            
            
            
            list_wasteCollection=[]
            list_trac_del=[]
            # popolo comp_sit
            codici_percorso = [] # popolo la lista con i codici componente/tratto del percorsoi pèer evitare ripassi (fittizzi e non) con stesso giorno di frequenza
            for eep in elenco_elementi_percorso:
                #logger.debug(eep[0])  
                # verifico se in frequenza con la solita funzione
                #if tappa_prevista(data_start,  eep[1])==1:
                    
                    
                curr1 = conn.cursor()
                
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
                    interruptionDate = programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[6], logger)[0]
                    executionStartDate = None
                    executionEndingDate = None
                    # calcolo il resumption date
                    try:
                        curr1.execute(select_resumption_date, (eep[4], eep[2],))
                        resumptionDate = curr1.fetchone()[0]
                        
                        # salvo la resumption date in consunt_ekovision
                        if resumptionDate is None:
                            logger.info(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()}, codice {eep[4]} con causale {eep[5]}
                                        non trovo resumption date''')
                        else:
                            #logger.debug(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()}, codice {eep[4]} con causale {eep[5]}
                            #            trovo resumption date {resumptionDate}''')
                            # aggiorno la tabella consunt_ekovision
                            lista_update_res_date.append( (resumptionDate, eep[4], eep[2],) )
                            # non faccio direttamente l'update ma lo salvo in una lista per fare un unico commit alla fine
                            
                    except Exception as e:
                        logger.error(select_resumption_date)
                        logger.debug(f'''Per il percorso {c[0]} del {datetime.strptime(t[1], "%Y%m%d").date()} trovo il codice {eep[4]} con causale {eep[5]} 
                                     e non c'è resumption date''')
                        logger.error(f'codice: {eep[4]}')
                        logger.error(f'data_ora_inizio: {eep[2]}')
                        logger.error(e)
                
                ############# DA GESTIRE IL DELETE In caso di non previsto o festivo 
                if int(eep[5]) in (102,101,999):
                    list_trac_del.append('{0}_{1}_{2}'.format(eep[4],t[1],t[0]))
                else:                
                    wasteCollection={
                        'traceabilityCode': '{0}_{1}_{2}'.format(eep[4],t[1],t[0]),
                        'programmingStartDate':programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[6], logger)[0],
                        'programmingEndingDate':programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[6], logger)[1],
                        'executionStartDate': executionStartDate,
                        'executionEndingDate': executionEndingDate,                   
                        'collectionType':str(eep[7]),
                        'areaCode': str(eep[4]),
                        'streetCode': str(eep[8]),
                        'streetDescription':str(eep[9]),
                        'cerCode':str(eep[10]),
                        'wasteDescription':str(eep[11]),
                        'year':int(programming_start_ending_date(curr1, datetime.strptime(t[1], '%Y%m%d').date(), t[0], eep[6], logger)[2]),
                        'istatCode': str(eep[12]),
                        'interruptionType': interruptionType,
                        'interruptionCause':interruptionCause,
                        'interruptionDate': interruptionDate,
                        'resumptionDate': resumptionDate.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' if resumptionDate is not None else None   
                    }
                    list_wasteCollection.append(wasteCollection)
                #logger.debug(list_wasteCollection)
                #exit()
                curr1.close()
                    
                    
            
            
                
            
            ########################################################
            # upload di list_wasteCollection di un singolo percorso
            ########################################################
            logger.info('Inizio upload dati del percorso {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
            api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/upload/av1'.format(url_ws_treg)
            # questa sarà da passare a TREG, le altre no
            
            body_upload={
                'id': str(guid),
                'importId': str(importId),
                'entities': list_wasteCollection
            }
            
            
            
            
            
            check_error_upload = call_treg_api(token, api_url_upload, body_upload, list_wasteCollection, logger, errorfile, 'errorCount', importId)

            # aggiorno la tabella di controllo upload
            try:
                curr_check.execute(query_update_check, (c[0], t[1],))
            except Exception as e:
                logger.error(query_update_check)
                logger.error(e)  

            # controllo  se per quel percorso ci sono componenti da cancellare in quanto consuntivate con causale non previsto e/o festivo ma già caricate in TREG con il calendario
            
            if len(list_trac_del) > 0:

                logger.debug(list_trac_del)
                            
                logger.info('Inizio delete dati del percorso {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                api_url_delete='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/delete/av1'.format(url_ws_treg)
                # questa sarà da passare a TREG, le altre no
                
                guid_del = uuid.uuid4()
                body_delete={
                    'id': str(guid_del),
                    'wasteCollectionIds': list_trac_del
                }         
                check_error_delete = call_treg_api(token, api_url_delete, body_delete, list_trac_del, logger, errorfile, 'deletedCount', importId)
                if check_error_delete>0:
                    logger.error('Errore nel delete di TREG per il percorso di raccolta {} del {}'.format(c[0], datetime.strptime(t[1], '%Y%m%d').date()))
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    api_url_rollback='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/rollback-upload/av1'.format(url_ws_treg)
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
        
        
        
        
        
        if len(dict_percorsi)>0:
        
            ###########################################
            # commit upload di tutto giorno per giorno
            ###########################################
            logger.info('Inizio il commit degli upload su TREG')
            max_data = max((v[2] for v in dict_percorsi.values()), default=None)
            logger.info(f'La massima data_last_update tra i percorsi da trattare è {max_data}')
            #exit()
            
            
            if check_error_upload==0:
                api_url_commit_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/commit-upload/av1'.format(url_ws_treg)
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
                    
                
                
                try:
                    curr.execute(query_insert, (max_data,
                                                str(guid), str(importId),
                                                response_commit_upload.status_code, response_commit_upload.text,))
                    #conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)  
                
                
                
                        
            else: 
                logger.warning('Sono presenti errori, faccio insert ridotto')                
            
                try:
                    curr.execute(query_insert_error, (max_data,
                                                str(guid), str(importId),
                                                ))
                    #conn.commit()
                except Exception as e:
                    logger.error(query_insert_error)
                    logger.error(e)    
                
    else:
        logger.info('Nessun percorso da trattare, non faccio upload né commit')
            
            
    #exit()
        
   
    if len(lista_update_res_date)>0:
        # inserisco le resumption date calcolate
        logger.info('Inizio l\'inserimento delle resumption date calcolate')
        
        try:
            bulk_update_consunt(curr, lista_update_res_date)
        except Exception as e:
            logger.error(e)
            logger.error(lista_update_res_date) 
    
    
    # faccio unico commit sul DB
    conn.commit()
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    curr_check.close()
    conn.close()
    














if __name__ == "__main__":
    main()      