#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Scopo dello script è lavorare giorno per giorno e inviare i dati a TREG a partire da una data che legge dal DB
Inoltre fa un insert su SIT nella tabella consunt.report_raccolta con i dati che invia a TREG da utilizzare per i report
da inviare a CM tramite il Duale


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


VERIFICHE DA FARE
Devo usare i WS di Ekovision:
    • elenco schede lavoro entrando con cod_percorso, data controllo che ci sia almeno 1 scheda
    • entro con id_scheda e devo verificare le componenti 
        ◦ tutte le componenti di SIT devono esserci in Ekovision
        ◦ le componenti di Ekovision non presenti su SIT dovrebbero individuarci i percorsi con spunte blue e marroni


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



import psycopg2
from psycopg2.extras import execute_values



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


from treg_env import *

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0


    

insert_sql_sit= '''
INSERT INTO consunt.report_raccolta (
    trac_code, cod_percorso, id_piazzola,
    id_elemento, id_via, id_asta,
    civ, riferimento, tipo_elemento,
    data_programmata, orario_progr,
    tipo_raccolta)
    VALUES %s    
    ON CONFLICT (trac_code, cod_percorso) 
    /* or you may use [DO NOTHING;] */ DO UPDATE  
    SET cod_percorso=EXCLUDED.cod_percorso, id_piazzola=EXCLUDED.id_piazzola,
    id_elemento=EXCLUDED.id_elemento, id_via=EXCLUDED.id_via, id_asta=EXCLUDED.id_asta,
    civ=EXCLUDED.civ, riferimento=EXCLUDED.riferimento, tipo_elemento=EXCLUDED.tipo_elemento,
    data_programmata=EXCLUDED.data_programmata, orario_progr=EXCLUDED.orario_progr, 
    tipo_raccolta=EXCLUDED.tipo_raccolta;

'''

# cerco asta, civico e rif


select_from_p = '''SELECT id_asta, numero_civico, riferimento
        FROM elem.piazzole
        WHERE id_piazzola = %s'''
        
select_from_e = '''SELECT id_asta, numero_civico, riferimento
        FROM elem.elementi
        WHERE id_elemento = %s
        '''





def main():
      

    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))


    #definisco una variabile insert_treg 
    # se 1 invia i dati anche a TREG
    # se 0 non invia i dati a TREG
    insert_treg = 1


    # abbiamo notato che ogni tanto si incarta nel fare l'upload delle liste di wastcollection quindi lo gestiamo con più tentativi
    
    MAX_RETRIES = 5  # Numero massimo di tentativi
    DELAY_SECONDS = 10  # Tempo di attesa tra i tentativi
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d')
    
    
    
    # credenziali WS Ekovision
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    

    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    # cerco il giono da cui partire
    query_first_day='''SELECT coalesce(max(data_last_calendar)+1, to_date('20250101', 'YYYYMMDD')) as data_last_calendar
FROM treg_eko.last_import_treg_racc where commit_code=200 and deleted = false'''

    try:
        curr.execute(query_first_day)
        giorno_mese_anno=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_first_day)
        logger.error(e)
    
    
    for gma in giorno_mese_anno:
        data_start=gma[0]
        logger.debug('{} era {}'.format(data_start, data_start.strftime('%A')))

    
    
    #data_start=datetime.strptime('20260321', '%Y%m%d').date() # da utilizzare per debug / lanciare manualmente
    fine_ciclo=oggi
    
    #fine_ciclo = datetime.strptime('20250630', '%Y%m%d')
    #fine_ciclo=date(fine_ciclo.year, fine_ciclo.month, fine_ciclo.day)
    
    
    logger.info(fine_ciclo)

    #exit()
    # qua mi tiro fuori il token TREG 
    
    token=token_treg(logger)
    logger.debug(token)
    
    
    while  data_start <= fine_ciclo:
        logger.info('Processo il giorno {}'.format(data_start))
        
        
        
        
        
        if insert_treg == 1 :
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
            
            
            # inizializzo un check 
            # dovrebbe rimanere 0 per garantirmi di fare il commit solo di roba pulita 
            check_error_upload=0
        
        
        ##################################
        # procedo con il recupero dati
        ##################################
        
        
        if data_start.isocalendar()[1]%2 == 1:
            check_s='D'
        else:
            check_s='P'

        logger.info('La settimana è {}'.format(check_s))
        
        query_elenco_percorsi_racccolta='''
        select ep.cod_percorso, ep.versione_testata, 
        fo.freq_binaria, ep.freq_settimane, 
        ep.id_turno, at2.gestione_arera, 
            ep.descrizione,
            concat(
            lpad(t.inizio_ora::text,2,'0'), ':', lpad(t.inizio_minuti::text,2,'0'),
            ' - ',
            lpad(t.fine_ora::text,2,'0'), ':', lpad(t.fine_minuti::text,2,'0')) as orario, 
            ep.id_tipo 
            from anagrafe_percorsi.elenco_percorsi ep 
            join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
            join elem.turni t on t.id_turno = ep.id_turno 
            join etl.frequenze_ok fo on fo.cod_frequenza = ep.freq_testata 
            where %s between data_inizio_validita and (data_fine_validita - interval '1' day) 
            /*and gestione_arera = 't'*/ 
            and at2.id_famiglia not in (2,3) /* togliamo servizi igiene */ 
            '''

        try:
            curr.execute(query_elenco_percorsi_racccolta, (data_start,))
            elenco_percorsi=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_elenco_percorsi_racccolta)
            logger.error(e)

        # facciamo un dizionario con chiave cod_percorso  e valore turno
        dict_percorsi={}
       
        i=0
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
            
            # !!!!!!!!!!!!!!!!!!!!!!!! DA AGGIUNGERE  CONDIZIONE SUL SERVIZIO ARERA  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            if tappa_prevista(data_start,  ep[2])==1 and (ep[3]=='T' or ep[3]==check_s): 
                # turno ep[4]
                # descr_percorso ep[6]
                # gestione_arera ep[5]
                # orario ep[7]
                dict_percorsi[ep[0]]=[ep[4],ep[6], ep[5], ep[7]]
            
            '''
            i+=1
            if i>10:
                exit()
            '''
            
            
        
        
            
        # c è la chiave (codice percorso)
        # t è il valore che in questo caso è una lista con id_turno, desrizione, gestione_arera, orario       
        for c, t,  in dict_percorsi.items():
            #logger.debug(c + ' : ' + str(t))
            
            
            
            # ora devo verificare le componenti
            
            comp_sit=[]
            
            
            # cerco quelle di SIT
            query_elementi_percorso='''
            select distinct codice_modello_servizio as cod_percorso,
            1 as in_freq, 
            case
                when ep.id_elemento_privato is null then 'OTH'
                else 'DOM'
                /* in questo momento non c'è perimetrazione delle aree di pregio */
            end as collectionType, 
            codice as areaCode, /* non metto il ripasso volutamente*/
            ee.id_piazzola,
            aa.id_via as streetCode,
            v.nome as streetDescription, 
            tr.codice_cer as cerCode,
            tr.nome as wasteDescription,
            c.cod_istat as istatCode, 
            min(tab.data_inizio) as data_inizio,
            max(tab.data_fine) as data_fine, 
            ep2.giorno_competenza, 
            ee.tipo_elemento
            from 
            (
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
            left join (select id_piazzola, id_elemento, tipo_elemento, id_asta from elem.elementi
            union 
            select id_piazzola, id_elemento, tipo_elemento, id_asta from history.elementi
            ) ee 
                on ee.id_elemento = tab.codice
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
            left join etl.frequenze_ok fo on fo.cod_frequenza = tab.frequenza
            left join anagrafe_percorsi.elenco_percorsi ep2 
            on ep2.cod_percorso = tab.codice_modello_servizio 
            and to_date(%s, 'YYYYMMDD') between ep2.data_inizio_validita and ep2.data_fine_validita
            where
            tab.data_fine > '20250101'
            and objecy_type = 'COMP'
            and treg_eko.verify_daily_frequency(tab.frequenza, to_date(%s, 'YYYYMMDD'), ep2.freq_settimane ) = 1
            and tr.tipo_rifiuto not in (
            /* punto di lavaggio */ 99  
            )
            and codice_modello_servizio = %s
            and %s between tab.data_inizio and tab.data_fine
            group by codice_modello_servizio,
            case
                when ep.id_elemento_privato is null then 'OTH'
                else 'DOM'
            end , 
            tab.codice,
            ee.id_piazzola,
            aa.id_via ,
            v.nome , 
            tr.codice_cer ,
            tr.nome,
            ep2.giorno_competenza,
            c.cod_istat, 
            ee.tipo_elemento
            '''
            #logger.debug(data_start.strftime('%Y%m%d'))
            #logger.debug(c)
            try:
                curr.execute(query_elementi_percorso, (data_start.strftime('%Y%m%d'), data_start.strftime('%Y%m%d'), c, data_start.strftime('%Y%m%d'),))
                elenco_elementi_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_elementi_percorso)
                logger.error(e)
            
            
            
            
            
            # lista per TREG
            list_wasteCollection=[]
            
            #lista per importzione su SIT nella tabella consunt.report_raccolta
            list_report_racc_sit=[]
            
            # popolo comp_sit
            codici_percorso = [] # popolo la lista con i codici componente/tratto del percorsoi pèer evitare ripassi (fittizzi e non) con stesso giorno di frequenza
            for eep in elenco_elementi_percorso:
                #logger.debug(eep[0])  
                # verifico se in frequenza con la solita funzione
                #if tappa_prevista(data_start,  eep[1])==1:
                    
                    
                curr1 = conn.cursor()
                
                trac_code= '{0}_{1}_{2}'.format(eep[3],data_start.strftime('%Y%m%d'),t[0])
                
                if insert_treg == 1 and t[2] == True:
                    # mando i dati a 
                    wasteCollection={
                        'traceabilityCode': trac_code,
                        'collectionType':str(eep[2]),
                        'areaCode': str(eep[3]),
                        'streetCode': str(eep[5]),
                        'streetDescription':str(eep[6]),
                        'cerCode':str(eep[7]),
                        'wasteDescription':str(eep[8]),
                        'programmingStartDate':programming_start_ending_date(curr1, data_start, t[0], eep[12], logger)[0],
                        'programmingEndingDate':programming_start_ending_date(curr1, data_start, t[0], eep[12], logger)[1],
                        'year':int(programming_start_ending_date(curr1, data_start, t[0], eep[12], logger)[2]),
                        'istatCode': str(eep[9]) 
                    }
                    list_wasteCollection.append(wasteCollection)
                
                #logger.debug(f'Id piazzola = {eep[4]}')
                if eep[4] is None:
                    try:
                        curr.execute(select_from_e, (eep[3],))
                        row_rif=curr.fetchone()
                    except Exception as e:
                        logger.error(select_from_e)
                        logger.error(e)
                else:
                    try:
                        curr.execute(select_from_p, (eep[4],))
                        row_rif=curr.fetchone()
                    except Exception as e:
                        logger.error(select_from_p)
                        logger.error(e)    
                
                
                # c codice percorso (chiave dizionario)                             
                # eep[4] piazzola
                # eep[3] id_elemento
                # eep[5] id_via
                
                # row_rif[0] id_asta
                # row_rif[1] civico
                # row_rif[2] riferimento
                
                # eep[13] tipo elemento 
                # data_start data_programmata
                # t[3] orario_programmato
                
                #  eep[2] tipo_raccolta 
                list_report_racc_sit.append((trac_code, c, eep[4],
                                             eep[3], eep[5], row_rif[0],
                                             row_rif[1],row_rif[2],eep[13],
                                             data_start, t[3], eep[2] 
                                             ))
                
                #logger.debug(list_wasteCollection)
                #exit()
                
                
                
                
            # faccio insert di tutto il percorso    
            try:
                execute_values(curr, insert_sql_sit, list_report_racc_sit)
            except Exception as e:
                logger.error(insert_sql_sit)
                logger.error(e)        
                    
            
            
             
            if insert_treg == 1 and t[2] == True:
                ########################################################
                # upload di list_wasteCollection di un singolo percorso
                ########################################################
                logger.info('Inizio upload dati del percorso {} del {}'.format(c, data_start))
                api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-wastecollections/upload/av1'.format(url_ws_treg)
                # questa sarà da passare a TREG, le altre no
                
                body_upload={
                    'id': str(guid),
                    'importId': str(importId),
                    'entities': list_wasteCollection
                }
                
                
                
                
                
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        
                        if attempt> 1:
                            logger.warning(f"Tentativo {attempt}")
                        
                        # 🔁 CODICE CHE PUÒ FALLIRE
                        response_upload = requests.post(api_url_upload, json=body_upload, headers={'accept':'*/*', 
                                                                                'mde': 'PROD',
                                                                                'Authorization': 'EIP {}'.format(token),
                                                                                'Content-Type': 'application/json'})
                        
                        logger.debug(response_upload.text)
                        #logger.debug(response_upload.json()['errorCount'])
                        #exit()
                        
                        # controllo che non ci siano errori (nel caso mi stoppo)
                    
                        if response_upload.json()['errorCount']!=0:
                            logger.error(list_wasteCollection)   
                            logger.error(response_upload.text)
                            
                            
                            # butto il dato su check_error_upload          
                            check_error_upload+=response_upload.json()['errorCount']
                        # ✅ Se funziona, esci dal ciclo
                        break

                    except Exception as e:
                        logger.warning(e)

                        if attempt == MAX_RETRIES:
                            logger.error("Tutti i tentativi sono falliti. Operazione interrotta.")
                            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                            exit()  # fermo l'esecuzione
                        else:
                            time.sleep(DELAY_SECONDS)  # Aspetta prima del prossimo tentativo
                        
                        
            
        # commit giornata
        conn.commit()
        
        if insert_treg == 1:    
            ####################################
            # commit upload
            ####################################
            logger.info('Inizio il commit degli upload su TREG')
            
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
                
                
                query_insert='''INSERT INTO treg_eko.last_import_treg_racc 
                    (data_last_calendar, last_update,
                    request_id_amiu, importid_treg, 
                    commit_code, commit_message) 
                    VALUES(to_date(%s, 'YYYYMMDD'), now(), 
                    %s, %s, 
                    %s, %s);'''
                try:
                    curr.execute(query_insert, (data_start.strftime('%Y%m%d'),
                                                str(guid), str(importId),
                                                response_commit_upload.status_code, response_commit_upload.text,))
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)  
                
                
                
                    
            else: 
                logger.warning('Sono presenti errori, non faccio il commit')                
                query_insert='''INSERT INTO treg_eko.last_import_treg_racc 
                    (data_last_calendar, last_update,
                    request_id_amiu, importid_treg) 
                    VALUES(to_date(%s, 'YYYYMMDD'), now(), 
                    %s, %s);'''
                try:
                    curr.execute(query_insert, (data_start.strftime('%Y%m%d'),
                                                str(guid), str(importId),))
                    conn.commit()
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)    

                
            
        #exit()
        data_start = data_start + timedelta(days=1)
        
   
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      