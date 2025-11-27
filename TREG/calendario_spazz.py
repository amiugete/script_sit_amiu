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

from decimal import Decimal

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
    
def token_treg():
    api_url='{}atrif/api/v1/tobin/auth/login'.format(url_ws_treg)
    payload_treg = {"username": user_ws_treg, "password": pwd_ws_treg, }
    logger.debug(payload_treg)
    response = requests.post(api_url, json=payload_treg)
    logger.debug(response)
    #response.json()
    logger.info("Status code: {0}".format(response.status_code))
    try:      
        response.raise_for_status()
        # access JSOn content
        #jsonResponse = response.json()
        #print("Entire JSON response")
        #print(jsonResponse)
    except HTTPError as http_err:
        logger.error(f'HTTP error occurred: {http_err}')
        check=500
    except Exception as err:
        logger.error(f'Other error occurred: {err}')
        logger.error(response.json())
        check=500
    token=response.text
    return token

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
      


    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

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
FROM treg_eko.last_import_treg_spazz where commit_code=200 and deleted = false; '''

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

    #data_start = datetime.strptime('2025-02-01', "%Y-%m-%d").date()
    fine_ciclo=oggi
    
    #fine_ciclo = datetime.strptime('20250131', '%Y%m%d').date()
    #fine_ciclo=date(fine_ciclo.year, fine_ciclo.month, fine_ciclo.day)

    # qua mi tiro fuori il token TREG 
    
    token=token_treg()
    logger.debug(token)

    while  data_start <= fine_ciclo:
        logger.info('Processo il giorno {}'.format(data_start))

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

        if data_start.isocalendar()[1]%2 == 1:
            check_s='D'
        else:
            check_s='P'

        logger.info('La settimana è {}'.format(check_s))
        
        query_elenco_percorsi_spazz='''
        select cod_percorso, versione_testata, fo.freq_binaria, freq_settimane, id_turno, at2.gestione_arera 
            from anagrafe_percorsi.elenco_percorsi ep 
            join anagrafe_percorsi.anagrafe_tipo at2 on at2.id = ep.id_tipo
            join etl.frequenze_ok fo on fo.cod_frequenza = ep.freq_testata 
            where %s between data_inizio_validita and (data_fine_validita - interval '1' day) 
            and gestione_arera = 't'
            and at2.id_famiglia in (2,3) /* consideriamo i soli servizi igiene */ 
            '''

        try:
            curr.execute(query_elenco_percorsi_spazz, (data_start,))
            elenco_percorsi=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_elenco_percorsi_spazz)
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
                dict_percorsi[ep[0]]=ep[4]
            
            '''
            i+=1
            if i>10:
                exit()
            '''
            
            
        
        
        #logger.debug(dict_percorsi)
        #exit()   
        # c è la chiave (codice turno)
        # t è il turno      
        for c, t in dict_percorsi.items():
            #logger.debug(c + ' : ' + str(t))

            # ora devo verificare i tratti
            
            tratti_sit=[]          
            
            # cerco quelle di SIT
            query_elementi_percorso='''
            select distinct codice_modello_servizio as cod_percorso,
            1 as in_freq,
            aa.lung_asta/1000 as kilometersTravelled,
            'PRG' as areaType, 
            codice as areaCode, 
            aa.id_via as streetCode,
            v.nome as streetDescription, 
            c.cod_istat as istatCode, 
            min(tab.data_inizio) as data_inizio,
            max(tab.data_fine) as data_fine,
            ep2.giorno_competenza
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
            left join (select id_asta, id_via, lung_asta from elem.aste
            union 
            select id_asta, id_via, lung_asta from history.aste) aa
                on aa.id_asta = tab.codice
            left join topo.vie v on v.id_via = aa.id_via 
            left join topo.comuni c on c.id_comune = v.id_comune 
            left join etl.frequenze_ok fo on fo.cod_frequenza = tab.frequenza
            left join anagrafe_percorsi.elenco_percorsi ep2 
            on ep2.cod_percorso = tab.codice_modello_servizio 
            and to_date(%s, 'YYYYMMDD') between ep2.data_inizio_validita and ep2.data_fine_validita
            where
            tab.data_fine > '20250101'
            and objecy_type = 'TRATTO'
            and treg_eko.verify_daily_frequency(tab.frequenza, to_date(%s, 'YYYYMMDD'), ep2.freq_settimane ) = 1
            and codice_modello_servizio = %s
            and %s between tab.data_inizio and tab.data_fine
            group by codice_modello_servizio,
            aa.lung_asta,
            tab.codice,
            aa.id_via ,
            v.nome,
            ep2.giorno_competenza,
            c.cod_istat
            '''
            
            try:
                curr.execute(query_elementi_percorso, (data_start.strftime('%Y%m%d'), data_start.strftime('%Y%m%d'), c, data_start.strftime('%Y%m%d'),))
                elenco_elementi_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_elementi_percorso)
                logger.error(e)
            

            list_sweeping=[]
            # popolo tratti_sit
            for eep in elenco_elementi_percorso:
                # verifico se in frequenza con la solita funzione
                #if tappa_prevista(data_start,  eep[1])==1:
                    # questa sarà da passare a TREG, le altre no
                curr1 = conn.cursor()
                
                sweeping={
                    'traceabilityCode': '{0}_{1}_{2}'.format(eep[4],data_start.strftime('%Y%m%d'),t),
                    'kilometersTravelled': eep[2],
                    'areaType':str(eep[3]),
                    'areaCode': str(eep[4]),
                    'streetCode': str(eep[5]),
                    'streetDescription':str(eep[6]),
                    'programmingStartDate':programming_start_ending_date(curr1, data_start, t, eep[10])[0],
                    'programmingEndingDate':programming_start_ending_date(curr1, data_start, t, eep[10])[1],
                    'year':int(programming_start_ending_date(curr1, data_start, t, eep[10])[2]),
                    'istatCode': str(eep[7]) 
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
            logger.info('Inizio upload dati del percorso {} del {}'.format(c, data_start))
            api_url_upload='{}atrif/api/v1/tobin/b2b/process/rifqt-sweepings/upload/av1'.format(url_ws_treg)
            # questa sarà da passare a TREG, le altre no
            
            body_upload={
                'id': str(guid),
                'importId': str(importId),
                'entities': list_sweeping
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
                        logger.error(list_sweeping)   
                        logger.error(response_upload.text)
                        
                        
                        # butto il dato su check_error_upload          
                        check_error_upload+=response_upload.json()['errorCount']
                    # ✅ Se funziona, esci dal ciclo
                    break

                except Exception as e:
                    logger.warning(e)

                    if attempt == MAX_RETRIES:
                        logger.error("Tutti i tentativi sono falliti. Operazione interrotta.")
                        raise ValueError(e)  # fermo l'esecuzione
                    else:
                        time.sleep(DELAY_SECONDS)  # Aspetta prima del prossimo tentativo 
        
        ####################################
        # commit upload
        ####################################
        logger.info('Inizio il commit degli upload su TREG')
        
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
              
            
            query_insert='''INSERT INTO treg_eko.last_import_treg_spazz 
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
            query_insert='''INSERT INTO treg_eko.last_import_treg_spazz 
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