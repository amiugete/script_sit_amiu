#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
1) Processo i file già in archivio a partire dal 2025. 
- solo schede chiuse 
- salvo i dati grezzi su una tabella del DB SIT, schema treg_eko

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
#f_handler = logging.StreamHandler()
f_handler = logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')


c_handler.setLevel(logging.WARNING)
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


from tappa_prevista import tappa_prevista



def fascia_turno(ora_inizio_lav, ora_fine_lav, ora_inizio_lav_2 ,ora_fine_lav_2):
    '''
    Calcolo della fascia turno sulla base degli orari della scheda di lavoro Ekovision
    '''
    fascia_turno=''
    if ora_inizio_lav_2 == '000000' and ora_fine_lav_2 =='000000':
    
        if ora_inizio_lav== '000000' and ora_fine_lav =='000000':
            fascia_turno='D'
        else:
            oi=int(ora_inizio_lav[:2])
            mi=int(ora_inizio_lav[2:4])
            of=int(ora_fine_lav[:2])
            mf=int(ora_fine_lav[2:4])
    else:
        oi=int(ora_inizio_lav[:2])
        mi=int(ora_inizio_lav[2:4])
        of=int(ora_fine_lav_2[:2])
        mf=int(ora_fine_lav_2[2:4])
            
            
    if fascia_turno=='':        
        # calcolo minuti del turno
        if of < oi:
            minuti= 60*(24 - oi) + 60 * of - mi + mf
        else :
            minuti = 60 * (of-oi) - mi + mf 

        
        hh_plus=int(minuti/2/60)
        mm_plus=minuti/2-60*int(minuti/2/60)
        
        # ora media
        if mi+mm_plus >= 60:
            mm=mi+mm_plus-60
            hh=oi+1+hh_plus
        else:
            mm=mi+mm_plus
            hh=oi+hh_plus
        
        #print('{}:{}'.format(hh,mm))
        
        if hh > 5 and hh <= 12:
            fascia_turno = 'M'
        elif hh > 12 and hh <= 20:
            fascia_turno = 'P'
        elif hh > 20 or hh <= 5:
            fascia_turno= 'N'
        
        return fascia_turno




def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    # variabile 
    # se vale 0 fa tutto come di consueto
    # se vale 1 processa il file come di consueto ma non lo cancella nè scrive sulla tabella dei file processati,
    # quindi lo riprocessa fino a che non si è risolto l'errore
    debug = 0

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logger.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_treg='20250101'
    
    
    # per ora vado a leggere in archivio (poi probabilmente è da vedere se abbia senso avere 2 flussi distinti)
    cartella_sftp_eko='sch_lav_cons/out/archive'    
    logger.info('Leggo e scarico file SFTP da cartella {}'.format(cartella_sftp_eko))
    
    # mi creo una lista in cui mettere codPercorso_data dei percorsi dove ci sono delle tappe consuntivate con 999 
    # scopo è per mandare una sola mail e non una per ogni tappa
    percorsi_tappe_anomale=[]
    
    

    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    
    
    select_file='''SELECT coalesce(max(last_json), 'sch_lav_consuntivi_20250101')
                FROM treg_eko.consunt_ekovision
                '''

    try:
        curr.execute(select_file, )
        check_filename=curr.fetchone()
        logger.debug(check_filename[0])
        
    except Exception as e:
        logger.error(select_file)
        logger.error(e)
        
        
    #exit()
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

        with srv.cd(cartella_sftp_eko): #chdir to public
            #print(srv.listdir('./'))
            for filename in srv.listdir('./'):
                #logger.debug(filename)
                
                                    
                # se non ho già letto il file
                #if len(check_filename)==0 and fnmatch.fnmatch(filename, "sch_lav_consuntivi*"):
                if filename > check_filename[0]:
                    logger.debug(f'Devo iniziare a leggere il file {filename}')
                    srv.get(filename, path + "/eko_output3/" + filename)
                    logger.info('Scaricato file {}'.format(filename))
                    
                    
                    
                    logger.info ('Inizio processo file'.format(filename))   
                    
                    # imposto a 0 un controllo sulla lettura del file
                    check_lettura=0
                    
                    
                    # Opening JSON file
                    f = open(path + "/eko_output3/" + filename)
                    
                    # returns JSON object as 
                    # a dictionary
                    try:
                        data = json.load(f)
                     
                        
                        
                        i=0
                        while i<len(data):
                            try:
                                logger.info('{} - Leggo dati della scheda di lavoro {}'.format(i, data[i]['id_scheda']))
                                check=0                    
                                #logger.debug(data[i]['data_esecuzione_prevista'])
                                #exit()
                                if data[i]['data_esecuzione_prevista']>=data_start_treg and data[i]['flg_chiuso'] == '1':
                                    ''' devo leggere quello che c'è in
                                    -   cons_ris_tecniche
                                    -   cons_ris_umane
                                        da cui ricavo gli orari effettivi della scheda
                                    
                                    -   cons_works
                                            da cui ricavo i isultati della consuntivazione su Ekovision 
                                    '''
                                    
                                
                                    # recupero ora scheda 
                                    
                                    # mi creo 2 array con data_ora_inzio e data_ora_fine
                                    data_ora_ini = []
                                    data_ora_fine = []
                                    
                                    t=0 # ristorsa Tecnica
                                    while t<len(data[i]['cons_ris_tecniche']):
                                        o=0
                                        while o < len(data[i]['cons_ris_tecniche'][t]['cons_ristec_orari']):
                                            d_i = data[i]['cons_ris_tecniche'][t]['cons_ristec_orari'][o]['data_ini']
                                            o_i = data[i]['cons_ris_tecniche'][t]['cons_ristec_orari'][o]['ora_ini']
                                            d_f = data[i]['cons_ris_tecniche'][t]['cons_ristec_orari'][o]['data_fine']
                                            o_f = data[i]['cons_ris_tecniche'][t]['cons_ristec_orari'][o]['ora_fine']
                                            data_ora_ini.append(datetime.strptime(f'{d_i} {o_i}', 
                                                              '%Y%m%d %H%M%S'))
                                            data_ora_fine.append(datetime.strptime(f'{d_f} {o_f}', 
                                                              '%Y%m%d %H%M%S'))
                                            o+=1
                                        t+=1
                                    
                                    
                                    p=0 # risorsa Umana
                                    while p<len(data[i]['cons_ris_umane']):
                                        o=0
                                        while o < len(data[i]['cons_ris_umane'][p]['cons_risum_orari']):
                                            d_i = data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_ini']
                                            o_i = data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_ini']
                                            d_f = data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['data_fine']
                                            o_f = data[i]['cons_ris_umane'][p]['cons_risum_orari'][o]['ora_fine']
                                            data_ora_ini.append(datetime.strptime(f'{d_i} {o_i}', 
                                                              '%Y%m%d %H%M%S'))
                                            data_ora_fine.append(datetime.strptime(f'{d_f} {o_f}', 
                                                              '%Y%m%d %H%M%S'))
                                            o+=1
                                        p+=1
                                    
                                    #logger.debug(data_ora_ini)
                                    #logger.debug(data_ora_fine)
                                    
                                    #logger.debug(min(data_ora_ini))
                                    #logger.debug(max(data_ora_fine))    
                                    
                                    if len(data_ora_ini)==0 or len(data_ora_fine)==0:
                                        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

                                        data={'user': eko_user, 
                                            'password': eko_pass,
                                            'o2asp' :  eko_o2asp
                                            }

                                        logger.info('Provo a leggere i dettagli della scheda')
                                        
                                        
                                        params2={'obj':'schede_lavoro',
                                                'act' : 'r',
                                                'id': '{}'.format(data[i]['id_scheda']),
                                                }
                                        
                                        response2 = requests.post(eko_url, params=params2, data=data, headers=headers)
                                        letture2 = response2.json()
                                        data_ora_ini.append(datetime.strptime(f'{letture2["schede_lavoro"][0]["data_inizio_lav"]} {letture2["schede_lavoro"][0]["ora_inizio"]}', 
                                                              '%Y%m%d %H%M%S'))
                                        data_ora_fine.append(datetime.strptime(f'{letture2["schede_lavoro"][0]["data_fine_lav"]} {letture2["schede_lavoro"][0]["ora_fine"]}', 
                                                              '%Y%m%d %H%M%S'))
                                    

                                    
                                    # consuntivazione 
                                    t=0 # contatore tappe
                                    check_cons=0
                                    while t<len(data[i]['cons_works']):
                                        
                                        if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC' or data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                            codice_eko=int(data[i]['cons_works'][t]['cod_componente'].strip())
                                
                                        elif data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                            codice_eko=int(data[i]['cons_works'][t]['cod_tratto'].strip())
                                        # escludo i NON previsti e NON eseguiti
                                        if int(data[i]['cons_works'][t]['flg_exec'].strip())==1 or  int(data[i]['cons_works'][t]['flg_non_previsto'].strip())==0 :
                                            ################################################################
                                            # Preparo i dati da inserire 
                                            
                                            # causale
                                            # il primo if era dopo ma l'ho sposato sopra (28/11/2024 sarebbero da riprocessare un po di dati)
                                            if int(data[i]['flg_segn_srv_non_effett'].strip())==1:
                                                causale=int(data[i]['cod_caus_srv_non_eseg_ext'].strip())
                                                qualita=0
                                            elif int(data[i]['cons_works'][t]['flg_exec'].strip())==1:
                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='RACC' or data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                    causale=100
                                                    qualita=100
                                                elif data[i]['cons_works'][t]['tipo_srv_comp']=='RACC-LAV':
                                                    causale=110
                                                    qualita=100
                                                if data[i]['cons_works'][t]['tipo_srv_comp']=='SPAZZ':
                                                    qualita=int(data[i]['cons_works'][t]['cod_std_qualita'].strip())
                                            #lo sposto prima perchè ci sono alcuni casi in cui int(data[i]['cons_works'][t]['flg_exec'].strip())==1 
                                            # anche se il servizio non è stato effettuato
                                            # elif int(data[i]['flg_segn_srv_non_effett'].strip())==1:
                                            #    causale=int(data[i]['cod_caus_srv_non_eseg_ext'].strip())
                                            #    qualita=0
                                            # se il servizio non fosse stato completato
                                            else :
                                                try:
                                                    causale=int(data[i]['cons_works'][t]['cod_giustificativo_ext'].strip())
                                                    qualita=0
                                                except Exception as e:
                                                    check_cons=1
                                                    logger.error(f'{filename}')
                                                    logger.error('ID SCHEDA:{}'.format(data[i]['id_scheda']))
                                                    logger.error('Causale servizio non effettuato:{}'.format(data[i]['cod_caus_srv_non_eseg_ext']))
                                                    logger.error('FLG Eseguito:{}'.format(data[i]['cons_works'][t]['flg_exec']))
                                                    logger.error('PROBLEMA CAUSALE')
                                                    logger.error(e)
                                                    causale=-1
                                                    #error_log_mail(logfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                                                    #exit()
                                            # la causale 999, creata per le preconsuntivazione, in realtà non dovrebbe essere usata.. 
                                            # se fosse arrivato qualcosa lo assimilo alla 102 (percorso non previsto)
                                            previsto = 0
                                            if causale == 999:
                                                causale = 102
                                                if '{}_{}'.format(data[i]['codice_serv_pred'],data[i]['data_esecuzione_prevista']) not in percorsi_tappe_anomale:
                                                    
                                                    
                                                    # qua dovrei verificare se la componente o il tratto stradale è previsto o meno in quel giorno 
                                                    
                                                    
                                                    query_verifica='''select distinct codice_modello_servizio as cod_percorso,
                                                        fo.freq_binaria, 
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
                                                        and tab.codice = %s
                                                        and codice_modello_servizio = %s
                                                        and %s between tab.data_inizio and tab.data_fine
                                                        group by codice_modello_servizio,
                                                        fo.freq_binaria, 
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
                                                        c.cod_istat'''
                                                        
                                                        
                                                    try:
                                                        curr.execute(query_verifica, (data[i]['data_esecuzione_prevista'],
                                                                                        codice_eko,
                                                                                        data[i]['codice_serv_pred'], 
                                                                                    data[i]['data_esecuzione_prevista']
                                                                                    )
                                                                                )
                                                        check_componente_previsto=curr.fetchone()
                                                        
                                                    except Exception as e:
                                                        logger.error(query_verifica)
                                                        logger.error(e)
                                                    
                                                    
                                                            
                                                    
                                                            
                                                        
                                                    
                                                    if tappa_prevista(datetime.strptime(data[i]['data_esecuzione_prevista'], '%Y%m%d'),
                                                                                        check_componente_previsto[1]) ==1 :
                                                        # faccio append
                                                        percorsi_tappe_anomale.append('{}_{}'.format(data[i]['codice_serv_pred'],data[i]['data_esecuzione_prevista']))
                                                        # invio mail warning 
                                                        messaggio = '''STO RIPROCESSANDO I DATI DELLE SCHEDE CHIUSE PER TREG <br><br> 
                                                        Per il percorso {0} del {1} (id_scheda = {2})
                                                        sono state consuntivate alcune tappe previste 
                                                        con la causale "<i>Frequenza non prevista</i>" (999) 
                                                        che non andrebbe usata se non per le tappe effettivamente non previste da SIT.<br>
                                                        Si prega di controllare e correggere il dato su Ekovision inserendo una causale corretta.
                                                        '''.format(data[i]['codice_serv_pred'], 
                                                                data[i]['data_esecuzione_prevista'],
                                                                data[i]['id_scheda'])
                                                        
                                                        
                                                        
                                                        warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                                                            
                                                #else:
                                                    # non faccio nulla 
                                                    
                                                        
                                            # vedo se consuntivazione arriva da totem o meno (per ora non lo salvo su SIT)
                                            if int(data[i]['cons_works'][t]['ts_exec']) == 0:
                                                totem=0
                                            else :
                                                totem=1
                                            
                                            
                                            # riprogrammato
                                            try:
                                                if int(data[i]['cons_works'][t]['flg_riprogrammato']) == 0:
                                                    riprogrammato=0
                                                elif int(data[i]['cons_works'][t]['flg_riprogrammato']) == 1 :
                                                    riprogrammato=1
                                            except Exception as e:
                                                riprogrammato=None
                                                    
                                            
                                            # note
                                            if data[i]['cons_works'][t]['note'] =='':
                                                note=None
                                            else :
                                                note=data[i]['cons_works'][t]['note']
                                                    
                                                    
                                            if data[i]['cons_works'][t]['tipo_srv_comp'] in ['SPAZZ', 'RACC', 'RACC-LAV']:
                                                
                                                                
                                                upsert_query='''INSERT INTO treg_eko.consunt_ekovision 
                                                (id_scheda, codice_servizio_pred,
                                                data_pianif_iniziale, data_esecuzione_prevista,
                                                data_ora_inizio, data_ora_fine,
                                                flg_riprogrammato, flg_non_previsto,
                                                qualita, id_scheda_riprogr,
                                                tipo_servizio, codice,
                                                pos, causale,
                                                last_json, data_last_update) 
                                                VALUES
                                                (%s, %s, 
                                                %s, %s,
                                                %s, %s,
                                                %s, %s,
                                                %s, %s,
                                                %s, %s,
                                                %s, %s,
                                                %s, now() 
                                                )
                                                ON CONFLICT (id_scheda, codice, pos, causale ) DO UPDATE 
                                                SET id_scheda=%s, codice_servizio_pred=%s,
                                                data_pianif_iniziale=%s,  data_esecuzione_prevista=%s,
                                                data_ora_inizio=%s, data_ora_fine=%s,
                                                flg_riprogrammato=%s,  flg_non_previsto=%s,
                                                qualita=%s, id_scheda_riprogr=%s,
                                                tipo_servizio=%s,  codice=%s,
                                                pos=%s, causale=%s,
                                                last_json=%s, data_last_update=now()
                                                '''
                                                            
                                                try:
                                                    curr.execute(upsert_query, 
                                                            (data[i]['id_scheda'], data[i]['codice_serv_pred']
                                                            ,data[i]['data_pianif_iniziale'], data[i]['data_esecuzione_prevista']
                                                            ,min(data_ora_ini),max(data_ora_fine)
                                                            ,data[i]['cons_works'][t]['flg_riprogrammato'],data[i]['cons_works'][t]['flg_non_previsto']
                                                            ,qualita,data[i]['cons_works'][t]['id_sch_riprogrammata']
                                                            ,data[i]['cons_works'][t]['tipo_srv_comp'], codice_eko
                                                            ,data[i]['cons_works'][t]['pos'], causale
                                                            ,filename
                                                            ,data[i]['id_scheda'], data[i]['codice_serv_pred']
                                                            ,data[i]['data_pianif_iniziale'], data[i]['data_esecuzione_prevista']
                                                            ,min(data_ora_ini),max(data_ora_fine)
                                                            ,data[i]['cons_works'][t]['flg_riprogrammato'],data[i]['cons_works'][t]['flg_non_previsto']
                                                            ,qualita,data[i]['cons_works'][t]['id_sch_riprogrammata']
                                                            ,data[i]['cons_works'][t]['tipo_srv_comp'], codice_eko
                                                            ,data[i]['cons_works'][t]['pos'], causale
                                                            ,filename
                                                            )
                                                        )
                                                except Exception as e:
                                                    logger.error(e)
                                                    logger.error(upsert_query)
                                                    logger.error('''id_scheda:{0}, cod_percorso:{1}, 
                                                                 data_pian:{2}, data_eff:{3},
                                                                 data_ora_ini:{4}, data_ora_fine:{5},
                                                                 flg_riprogrammato:{6}, flg_non_previsto:{7},
                                                                 qualita:{8}, id_scheda_ripr:{9},
                                                                 tipo_servizio:{10}, codice_eko:{11},
                                                                 posizione:{12}, causale:{13}, 
                                                                 filename:{14}
                                                                 '''
                                                            .format(data[i]['id_scheda'], data[i]['codice_serv_pred']
                                                            ,data[i]['data_pianif_iniziale'], data[i]['data_esecuzione_prevista']
                                                            ,min(data_ora_ini),max(data_ora_fine)
                                                            ,data[i]['cons_works'][t]['flg_riprogrammato'],data[i]['cons_works'][t]['flg_non_previsto']
                                                            ,qualita,data[i]['cons_works'][t]['id_sch_riprogrammata']
                                                            ,data[i]['cons_works'][t]['tipo_srv_comp'], codice_eko
                                                            ,data[i]['cons_works'][t]['pos'], causale
                                                            ,filename))
                                                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger) 
                                                    exit()           
                                                #logger.debug('Sono arrivato qua senza errori')
                                                #exit()            
                                                conn.commit()
                                            else:
                                                check=1
                                                logger.error('PROBLEMA CONSUNTIVAZIONE')
                                                logger.error('File:{}'.format(filename))
                                                logger.error('Mi sono fermato alla riga {}'.format(i))
                                                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                                                exit()
                                        #else:
                                        #   logger.debug('Tappa non prevista e non effettuata')
                                        t+=1
                                        conn.commit()
                                    
                                    
                                    
                                    
                                else:
                                    if data[i]['data_esecuzione_prevista']>=data_start_treg:
                                        logger.debug('Non processo la scheda perchè antecedente alla data di partenza di TREG {}'.format(data_start_treg))
                                    elif data[i]['flg_chiuso'] == '1':
                                        logger.debug('Non processo la scheda {} perchè non è chiusa'.format(data[i]['id_scheda']))
                            except Exception as e:
                                check=1
                                logger.error('File:{}'.format(filename))
                                logger.error('Non processo la riga {}'.format(i))
                            i+=1
                        #con.commit()
                        
                        
                        # Closing file
                        f.close()
                        logger.info('Chiudo il file {}'.format(filename))
                        logger.info('-----------------------------------------------------------------------------------------------------------------------')
                        #exit()
                        #srv.rename("./"+ filename, "./archive/" + filename)
                    except Exception as e:
                        logger.error(e)
                        logger.error('Problema processamemto file {}'.format(filename))
                        #logger.error('File spostato nella cartella json_error')
                        f.close()
                        #srv.rename("./"+ filename, "./json_error/" + filename)
                        #error_log_mail(errorfile, 'assterritorio@amiu.genova.it; andrea.volpi@ekovision.it; francesco.venturi@ekovision.it', os.path.basename(__file__), logger)
                    
                       
                    
                    # in modalità debug non scrivo nella tabella UNIOPE.EKOVISION_LETTURA_CONSUNT 
                    # in modo da pote processare più volte lo stesso file fino a che non trovo errore
                    if debug == 1:
                        logger.warning('Sono in modalità DEBUG. Mi fermo qua senza scrivere in UNIOPE.EKOVISION_LETTURA_CONSUNT')
                        exit() 
                        
                    os.remove(path + "/eko_output3/" + filename)
                    
                    
                    
                    
                    
                #else: 
                #    logger.info('Non scarico il file {} perchè già letto e processato'.format(filename))
        
        
        
        
        
        
        
        
        # Closes the connection
        srv.close()
        logger.info('Connessione chiusa')
    except Exception as e:
        logger.error(e)
        check_ekovision=103 # problema scarico SFTP  
    
    
    logger.debug('Fine ciclo')
    
    
    
    
    
    #exit()
    
 
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()





if __name__ == "__main__":
    main()      
    