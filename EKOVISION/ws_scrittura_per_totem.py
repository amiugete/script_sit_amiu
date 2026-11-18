#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi, Roberta Fagandini

'''
Lo script gestisce  la scrittura dei dati registrati sul totem su Ekovision. 
'''

import json
#from msilib import type_short
import os  # ,shutil,glob
#import re
import sys
from datetime import date, datetime, timedelta

import psycopg2
import requests
from requests.exceptions import HTTPError

#import getopt  # per gestire gli input
#import pymssql


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
# libreria per scrivere file csv
#import csv
# libreria per invio mail
import email
import logging
import mimetypes
#import smtplib
#import ssl
import uuid
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Funzione per recuperare la descrizione del percorso (usata per l'invio mail in caso di creazione scheda di lavoro)
from descrizione_percorso import *

from credenziali import *
from invio_messaggio import *


def cerco_mail(curr_c, cp: str, dd: str, logger_name) -> str: 
    
    """ 
    Funzione per cercare le mail in caso di errori
    La richiamo in alcuni punti del main 
    
    """


    query_mails='''select concat_ws(', ', mu.mail, mu1.mail) as mails
    from servizi.servizi_per_ekovision spe 
    left join servizi.mail_ut mu on mu.id_ut_sit = spe.id_uo_sit 
    left join servizi.mail_ut mu1 on mu1.id_ut_sit = spe.id_rimessa_sit
    where spe.id_percorso = %s 
    and to_date(%s, 'YYYYMMDD') between spe.data_inizio_validita and spe.data_fine_validita -1'''
    
    
    logger_name.info(f'cerco la mail per percorso {cp} del {dd}')
    try:
        curr_c.execute(query_mails, (cp, dd))
        mails_percorso=curr_c.fetchone()[0]
    except Exception as e:
        logger_name.error(e) 
    
    return mails_percorso     
     

def totem_message_mail(test: int, messaggio: str, receiver_email, script_name, logger_name, subject=None, CC=None) -> int:
    
    """
    Funzione che usa lo script invio_messaggio.py per inviare l'eventuale LOG con WARNING via mail
    Input:
        - test
        - messaggio
        - receiver_email
        - script_name
        - logger_name
        [- soggetto mail] OPZIONALE
    """


    
    if subject is None:
        subject = 'WARNING MESSAGE - {}'.format(script_name)

        body = '''Questo è un messaggio automatico:<br>
        {0}<br><br>
        Automatically sent from AMIU Genova by {1} script<br><br>
        '''.format(messaggio,script_name)
    else:
        body='''Questo è un messaggio automatico:<br>
        {0}<br><br>
        Messaggio automatico. Si prega di non rispondere. In caso di problemi contattare {1} <br><br>
        '''.format(messaggio,'assterritorio@amiu.genova.it')
    #sender_email = user_mail


    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = "totem_noreply@amiu.genova.it"
    if test == 1:
        message["To"] = mail_cc_assterritorio
        body=f'''<font color="red">Ambiente di test.</font>. I destinatari in prod sarebbero {receiver_email}
        <br><hr><br>
        {body}
        '''
    elif test == 0:
        message["To"] = receiver_email
        message["Bcc"] = mail_cc_assterritorio  # Recommended for mass emails
    
    
    
    # qualora non ci fosse già, aggiungo in CC assterritorio
    #if 'assterritorio' not in receiver_email:    
    #    message["CC"] = 'assterritorio@amiu.genova.it'
    message["Subject"] = subject
    if CC is not None:
        message["CC"] = CC
    
    message.preamble = subject
                    
    # Add body to email
    message.attach(MIMEText(body, "html"))


    try:
        invio_messaggio(message)
        logger_name.info('Messaggio inviato')
        return 200
    except Exception as e:
        logger_name.error('Errore invio mail: {}'.format(e))
        return 500




def main():
      
    try:
        if sys.argv[1]== 'prod':
            test=0
        elif sys.argv[1]== 'test':
            test=1
        else: 
            print('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
            exit()
    except Exception as e:
        test=1
    
    
    
    path=os.path.dirname(sys.argv[0]) 
    nome=os.path.basename(__file__).replace('.py','')
    #tmpfolder=tempfile.gettempdir() # get the current temporary directory
    if test==0:
        logfile='{0}/log/{1}.log'.format(path,nome)
        errorfile='{0}/log/error_{1}.log'.format(path,nome)
    else: 
        logfile='{0}/log/{1}_test.log'.format(path,nome)
        errorfile='{0}/log/error_{1}_test.log'.format(path,nome)
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
    
    
    
    if test==1:
        logger.info('Ambiente di TEST')
        incipit_mail_test='<font color="red"><b>AMBIENTE DI TEST</b></font><hr>'
    else:
        incipit_mail_test=''
      
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    
    #num_giorno=datetime.today().weekday()
    #giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d%H%M')
    #oggi1=datetime.today().strftime('%d/%m/%Y')
    logger.debug(giorno_file)
    
    
    id_progressivo_tmp = None
    
    
    # Mi connetto al nuovo DB consuntivazione  
    if test == 1:
        nome_db= db_totem_test
    elif test== 0:
        nome_db=db_totem
    else:
        logger.error(f'La variabilie test vale {test}. Si tratta di un valore anomalo. Mi fermo qua')
        exit()
    
        
    logger.info('Connessione al db {} su {}'.format(nome_db, host_totem))
    conn_c = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)

    
    
    
    # in questo caso mi connetto a SIT (PostgreSQL) per recuperare le mail e il turno e la descrizione percorso
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)
                        
    curr = conn.cursor()
    
    query_registrazioni= '''select r.codice,
pe.id_ekovision,
pe.cognome, pe.nome,
r.id_percorso, 
to_char(r.datalav, 'YYYYMMDD') as data_percorso, 
r.id_qualifica, 
mmq.id_mansione, 
nullif(trim(r.sportello),'') as sportello, 
trim(replace(mi.targa, ' ', '')) as targa,
me.id_ekovision as id_rt_eko,
r.datainsert,
concat('TOTEM Badge ', r.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati
from servizi.registrazioni r 
left join totem.mapping_mansioni_qualifiche mmq on mmq.id_qualifica=r.id_qualifica 
left join v_personale_ekovision_step1 vpes on r.codice::numeric = vpes.matricola::numeric
left join totem.mezzi_infopm mi on trim(mi.sportello) = trim(lpad(r.sportello,5,'0')) 
left join totem.mezzi_ekovision me on trim(replace(mi.targa, ' ',''))=trim(replace(me.targa, ' ','')) 
left join (SELECT 
	nome, cognome, dt_nascita, max(pe.id_ekovision) as id_ekovision
	FROM totem.personale_ekovision AS pe
	group by nome, cognome, dt_nascita) pe 
	on trim(pe.cognome)  = trim(vpes.cognome)
	and trim(pe.nome) = trim(vpes.nome)
	and pe.dt_nascita = vpes.data_nascita 
where r.send_ekovision is true
and r.codice not like 'badge_%%'
and errori is null'''


    query_percorsi = f''' with registrazioni_agg as ({query_registrazioni})
    select id_percorso, data_percorso, min(datainsert) as data_insert, 
    min(sorgente_dati) as sorgente_dati
from registrazioni_agg 
group by id_percorso, data_percorso
order by 3
'''
     
    query_registrazioni_percorso = f'''{query_registrazioni} and id_percorso = %s and datalav = %s order by datainsert'''
    
    
    
    
     
     
     
    query_update_error='''update servizi.registrazioni set errori = true
        where codice=%s
        and id_percorso = %s
        and datalav=%s
        and datainsert=%s'''
    query_update_success='''update servizi.registrazioni set send_ekovision = null
        where codice=%s
        and id_percorso = %s
        and datalav=%s
        and datainsert=%s'''
    
    
    curr_c = conn_c.cursor()
  
    check=0   
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    #headers = {'Content-type': 'application/json;'}

    auth_data_eko={'user': eko_user, 'password': eko_pass, 'o2asp' :  eko_o2asp}
    
    
    
    
    
    
    query_badge_temporanei='''
    select r.codice,
    r.id_percorso, 
    to_char(r.datalav, 'YYYYMMDD') as data_percorso, 
    r.id_qualifica, 
    nullif(trim(r.sportello),'') as sportello, 
    trim(replace(mi.targa, ' ', '')) as targa,
    me.id_ekovision as id_rt_eko,
    r.datainsert
    from servizi.registrazioni r 
    left join totem.mezzi_infopm mi on trim(mi.sportello) = trim(lpad(r.sportello,5,'0')) 
    left join totem.mezzi_ekovision me on trim(replace(mi.targa, ' ',''))=trim(replace(me.targa, ' ','')) 
    where r.send_ekovision is true
    and r.codice like 'badge_%'
    and errori is null
        '''
    
    
    

    
    if test == 1 :
        eko_URL=eko_url_test
        logger.debug('Uso ambiente di TEST')
    else:
        eko_URL=eko_url
    
    #exit()




    # gestione badge temporanei
    
    
    try:
        curr_c.execute(query_badge_temporanei)
        registrazini_badge_temp=curr_c.fetchall()
    except Exception as e:
        logger.error(query_badge_temporanei)
        logger.error(e)
    
    
    if len(registrazini_badge_temp) ==0 :
        logger.info('Non ci sono percorsi registrati con badge temporanei')
    else:
        logger.info('Ci sono {0} percorsi registrati con badge temporanei'.format(len(registrazini_badge_temp)))    
    
    
    for rbt in registrazini_badge_temp:
        
        msg = f'''Ci sono problemi a salvare le informazioni registrate su totem per il percorso 
        codice percorso {rbt[1]} - {descrizione_percorso(rbt[1],  rbt[2], curr, logger)} del {datetime.strptime(rbt[2], '%Y%m%d').strftime('%d/%m/%Y')}
        ) per un utente che si è registrato con un badge non riconosciuto 
        <br><br>
        Dati registrati su Totem: 
        <ul> 
        <li>cod percorso: {rbt[1]}</li>
        <li>data: {datetime.strptime(rbt[2], '%Y%m%d').strftime('%d/%m/%Y')}</li>
        <li><font color="red">badge: {rbt[0].replace("badge_","")} </font></li>'''
        # se c'è un mezzo
        if rbt[4]:
            msg =f'''{msg}<li>sportello: {rbt[4]} (Targa: {rbt[5]})</li>
            <li>id risorsa tecnica ekovision: {rbt[6]}</li>'''
        else: 
            msg=f'''{msg}<li>Nessun mezzo registrato</li>'''
        msg=f'''{msg}<li>data inserimento su totem: {rbt[7].strftime('%d/%m/%Y %H:%M')} </li>
        </ul>
        <br><br> 
        <b>I dati, se corretti, vanno inseriti manualmente su Ekovision. </b>
        <hr>
        '''
    
        mails_percorso = cerco_mail(curr_c, rbt[1], rbt[2], logger)
        
        logger.info (f'Invio mail a {mails_percorso}')
    
        res_mail_rbt = totem_message_mail(test, msg, mails_percorso,  os.path.basename(__file__), logger,  'Problema trasferimento dati totem ekovision')
            
        if res_mail_rbt != 200:
            logger.error(f'Non riesco ad inviare la mail per segnalare la registrazione con badge tempopraneo {rbt}')

        try: 
            curr_c.execute(query_update_error, (rbt[0], rbt[1], rbt[2], rbt[7]))
            conn_c.commit()
        except Exception as e:
            logger.error(query_update_error)
            logger.error(e)


    # gestione percorsi regolari
    try:
        curr_c.execute(query_percorsi)
        percorsi=curr_c.fetchall()
    except Exception as e:
        logger.error(query_percorsi)
        logger.error(e)
    
    
    '''datalav= '20260525'  #oggi.strftime('%Y%m%d')
    cod_percorso= '0101362801'
    
    rt_write = None
    ru_write =  2425
    
    ru_mans = 165
    '''
    
    if len(percorsi) == 0:
        logger.info('Non ci sono percorsi da inviare ad Ekovision')
    else:
        logger.info('Ci sono {0} percorsi da inviare ad Ekovision'.format(len(percorsi)))
        
        #exit()
        for percorso in percorsi:
            cod_percorso=percorso[0]
            datalav=percorso[1]
            sorgente_dati = percorso[3]
            logger.info('Elaboro percorso {0} del {1}'.format(cod_percorso, datalav))
            # cerco id della scheda da modificare
    
            params={'obj':'schede_lavoro',
                'act' : 'r',
                'sch_lav_data': datalav,
                'cod_modello_srv': cod_percorso,
                'flg_includi_eseguite': 1,
                'flg_includi_chiuse': 1
                }
            
            
            response = requests.post(eko_URL, params=params, data=auth_data_eko, headers=headers)
            #response.json()
            logger.debug(response.status_code)
            try:      
                response.raise_for_status()
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
                logger.info(len(letture['schede_lavoro']))
                logger.debug(letture['schede_lavoro'])
                #exit()
                
                if len(letture['schede_lavoro']) == 0:
                    ##########################################
                    #  TODO
                    ##########################################
                    #va creata la scheda di lavoro
                    logger.info('Andrebbe creata la scheda di lavoro')
                    
                    
                    
                    
                    # cerco turno e mail
                    
                    query_turno_mail ='''  select pu.id_turno, t.cod_turno as descr_turno,
                        string_agg(u.mail, ', ') 
                        from anagrafe_percorsi.elenco_percorsi ep 
                        left join anagrafe_percorsi.percorsi_ut pu 
                            on pu.cod_percorso = ep.cod_percorso and pu.data_disattivazione = ep.data_fine_validita
                        left join anagrafe_percorsi.cons_mapping_uo cmu on cmu.id_uo = pu.id_ut
                        left join topo.ut u on u.id_ut = cmu.id_uo_sit
                        left join elem.turni t on t.id_turno = pu.id_turno
                        where ep.cod_percorso = %s
                        and to_date(%s, 'YYYYMMDD') between ep.data_inizio_validita and ep.data_fine_validita 
                        group by pu.id_turno, t.cod_turno'''
                    
                    try:
                        curr.execute(query_turno_mail, (cod_percorso, datalav, ))
                        riga1=curr.fetchone()
                    except Exception as e:
                        logger.error(query_percorsi)
                        logger.error(e) 
                    
                    if riga1 is not None:
                        id_turno_sit = riga1[0]
                        mails_sent = riga1[2]
                        cod_turno_sit = riga1[1]
                    else:
                        logger.error(f'Non trovo turno e mail associate al percorso {cod_percorso} del {datalav}')
                        
                    
                    ruid = uuid.uuid4()
                    logger.info('ID richiesta Ekovision (ruid):{}'.format(ruid))



                    giason={
                                "crea_schede_lavoro": [
                                {
                                    "data_srv": datalav,
                                    "cod_modello_srv": cod_percorso,
                                    "cod_turno_ext": int(id_turno_sit)
                                }
                                ]
                                } 
                    params2={'obj':'crea_schede_lavoro',
                            'act' : 'w',
                            'ruid': ruid,
                            'json': json.dumps(giason)
                            }
                    
                    try:
                        response2 = requests.post(eko_URL, params=params2, data=auth_data_eko, headers=headers)
                        letture2 = response2.json()
                        logger.info(letture2)
                        check_creazione_scheda=0
                        id_scheda=letture2['crea_schede_lavoro'][0]['id']
                        check_creazione_scheda=1
                    except Exception as e:
                        query_insert='''INSERT INTO anagrafe_percorsi.creazione_schede_lavoro
                                (id, cod_percorso, "data", id_scheda_ekovision, "check")
                                VALUES(%s, %s, %s, NULL, %s);'''
                        if test==0:
                            try:        
                                curr.execute(query_insert, (str(ruid),cod_percorso, datalav, check_creazione_scheda))
                            except Exception as e1:
                                logger.error(query_insert)
                                logger.error(e1)        
                        logger.error(e)
                        logger.error(' - id: {}'.format(ruid))
                        logger.error(' - Cod_percorso: {}'.format(cod_percorso))
                        logger.error(' - Data: {}'.format(datalav))
                        #logger.error('Id Scheda: {}'.format(id_scheda[k]))
                        # check se c_handller contiene almeno una riga 
                        error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                        logger.info("chiudo le connessioni in maniera definitiva")
                        curr_c.close()
                        #currc1.close()
                        conn_c.close()
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
                            
                            body_mail='''E' arrivata una consuntivazione da totem per il percorso {} - {} in data {}.
                            <br>Origine del dato:{}
                            <br>Non esistendo la scheda per il giorno in questione è stata creata in automatico.
                            La nuova scheda ha ID {}'''.format(cod_percorso,
                                                                descrizione_percorso(cod_percorso,  datalav, curr, logger),
                                                                datalav, sorgente_dati, id_scheda)           
                            if test ==1: 
                                body_mail = f'''<font color=red>Ambiente di TEST</font><hr> Destinatari produzione : 
                                {mails_sent}<br>{body_mail}'''
                                creazione_scheda_mail(body_mail, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                            else:
                                # insert solo in PRODUZIONE
                                curr.execute(query_insert, (str(ruid),cod_percorso, datalav, id_scheda, check_creazione_scheda))
                                creazione_scheda_mail(body_mail, mails_sent, os.path.basename(__file__), logger)
                                conn.commit()
                        else:
                            curr.execute(query_insert, (str(ruid),cod_percorso, datalav, check_creazione_scheda))
                            conn.commit()
                    except Exception as e:
                        logger.error(query_insert)
                        logger.error(e)
                    
                    
                    
                    
                    
                    
                    esiste = 0 
                    eseguita=0
                    chiusa = 0
                    continue # non elaboro questa scheda ma proseguo con le righe successive
                    # dovrebbe essere codice morto da rimuovere
                    # response2 = requests.post(eko_URL, params=params2, data=auth_data_eko, headers=headers)
                    # letture2 = response2.json()
                    # logger.info(letture2)
                    # try: 
                    #     id_scheda=letture['crea_schede_lavoro'][0]['id']
                    # except Exception as e:
                    #     logger.error(e)
                    
                elif len(letture['schede_lavoro']) > 0 : 
                    esiste = 1 
                    id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                    turno=letture['schede_lavoro'][0]['cod_turno_ext']
                    in_lavorazione= letture['schede_lavoro'][0]['flg_in_lavorazione']
                    eseguita=int(letture['schede_lavoro'][0]['flg_eseguito'])
                    chiusa= int(letture['schede_lavoro'][0]['flg_chiuso'])
                    logger.info(f'id scheda: {id_scheda}')
                    logger.info(f'Cod Turno: {turno}')
            

            
            if eseguita == 1 or chiusa == 1:
                
                # cerco comunque le registrazioni per mandare mail di "warning"
                try:
                    curr_c.execute(query_registrazioni_percorso, (cod_percorso, datalav))
                    registrazioni_percorso=curr_c.fetchall()
                except Exception as e:
                    logger.error(query_registrazioni_percorso)
                    logger.error(e)
                
                
                msg = f'''La scheda {id_scheda} di Ekovision
                (codice percorso {cod_percorso} - {descrizione_percorso(cod_percorso,  datalav, curr, logger)} del {datetime.strptime(datalav, '%Y%m%d').strftime('%d/%m/%Y')}
                ) <b>è già eseguita</b>. Non posso caricare personale e mezzi
                <br><br>
                Dati registrati su Totem: 
                <ul> 
                <li>cod percorso: {cod_percorso}</li>
                <li>data: {datetime.strptime(datalav, '%Y%m%d').strftime('%d/%m/%Y')}</li>'''
                
                for rr in registrazioni_percorso:
                    codice_badge=rr[0]
                    id_percorso=rr[4]
                    data_percorso=rr[5]
                    datainsert_registrazione=rr[11]
                    curr_c.execute(query_update_error, (codice_badge, id_percorso, data_percorso, datainsert_registrazione))

                    msg =f'''{msg} 
                    <ul>                      
                    <li>matricola: {rr[0]} ({rr[2]} {rr[3]})</li>
                    <li>id risorsa umana ekovision: {rr[1]} </li>'''
                    # se c'è un mezzo
                    if rr[8]:
                        msg =f'''{msg}<li>sportello: {rr[8]} (Targa: {rr[9]})</li>
                            <li>id risorsa tecnica ekovision: {rr[10]} </li>'''
                    else: 
                        msg=f'''{msg}<li>Nessun mezzo registrato</li>'''
                    msg=f'''{msg}
                    <li>data inserimento su totem: {rr[11].strftime('%d/%m/%Y %H:%M')} </li>
                    </ul>'''
                msg = f'''{msg}     
                </ul>
                <br><br> 
                <b>Solo in caso di dubbi<b> controllare i dati inseriti su Ekovision.
                <hr>
                '''
                logger.info(msg)
                
                mails_percorso = cerco_mail(curr_c, cod_percorso, datalav, logger)
                logger.info (f'Invio mail a {mails_percorso} e non faccio nulla')
                
                totem_message_mail(test, msg, mails_percorso,  os.path.basename(__file__), logger,  'Trasferimento dati totem ekovision. Scheda già eseguita')
                # registro l'informazione sul DB 
                conn_c.commit()
                
                
                
            elif esiste == 1 :     
                
                logger.info('Provo a leggere i dettagli della scheda')
                
                
                params2={'obj':'schede_lavoro',
                        'act' : 'r',
                        'id': '{}'.format(id_scheda),
                        'flg_esponi_consunt': 1
                        }

                response2 = requests.post(eko_URL, params=params2, data=auth_data_eko, headers=headers)
                #letture2 = response2.json()
                letture2 = response2.json()
                #logger.info(letture2)
                #exit()
                # key to remove
                #key_to_remove = "status"
                del letture2["status"]  
                del letture2['schede_lavoro'][0]['trips']  
                # del letture2['schede_lavoro'][0]['risorse_tecniche']
                # del letture2['schede_lavoro'][0]['risorse_umane']   
                del letture2['schede_lavoro'][0]['filtri_rfid']        
                #logger.info(letture2)
                
                
                
                id_rt=[]
                progr_rt=[]
                targa_rt=[]
                
                
                id_ru=[]
                progr_ru=[]
                flg_auti=[]
                ru_progr_rt=[]
                mansioni= []
                # 0 se non cìè niente
                # 1 se c'è risorsa predefinita Ekovision
                # 2 se già inserito il dato
                tipo_inserimento_ru=[]
                tipo_inserimento_rt=[]
                
                rt=0
                while rt < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                    logger.debug('Dettagli risorsa tecnica {0} : {1}'.format(
                        rt, 
                        letture2['schede_lavoro'][0]['risorse_tecniche'][rt])
                    )
                    id_rt.append(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id'])
                    targa_rt.append(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['targa'])
                    progr_rt.append(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id_progressivo'])
                    if int(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id']) == 0: 
                        tipo_inserimento_rt.append(0)
                        
                    elif int(letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['id']) > 0 \
                        and letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['ora_inizio'] == '000000' \
                        and letture2['schede_lavoro'][0]['risorse_tecniche'][rt]['ora_fine'] == '000000':
                        tipo_inserimento_rt.append(1)
                    else:
                        tipo_inserimento_rt.append(2)
                    
                    rt=rt+1
                
                    
                ru=0
                while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                    logger.debug('Dettagli risorsa umana {0} : {1}'.format(
                        ru, 
                        letture2['schede_lavoro'][0]['risorse_umane'][ru])
                    )
                    id_ru.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id'])
                    progr_ru.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_progressivo'])
                    flg_auti.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista'])
                    ru_progr_rt.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_progr_ristec'])
                    mansioni.append(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_mansione'])
                    # controlllo il tipo inserimento presente
                    if int(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']) == 0: 
                        tipo_inserimento_ru.append(0)
                        # di default se non c'è nessuna risorsa umana, metto flg_autista a 0
                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista'] = 0
                    elif int(letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']) > 0 and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_inizio'] == '000000' and letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_fine'] == '000000':
                        tipo_inserimento_ru.append(1)
                        # di default se non c'è nessuna risorsa umana, metto flg_autista a 0
                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista'] = 0
                    else:
                        tipo_inserimento_ru.append(2)
                        # in questo caso non cambio il flg_autista, perché è già stato impostato da Ekovision
                    
                    ru=ru+1
                
                logger.info('La scheda ha {0} risorse tecniche e {1} risorse umane'.format(
                        len(letture2['schede_lavoro'][0]['risorse_tecniche']),
                        len(letture2['schede_lavoro'][0]['risorse_umane'])))
                
                
                # risorse tecniche e umane vanno modificate in questo modo:
                # se c'è una sola risorsa tecnica, va modificata 
                logger.info('La scheda ha le seguenti risorse tecniche: {0}'.format(id_rt))
                logger.info('La scheda ha le seguenti risorse umane: {0}'.format(id_ru))
                logger.info('FLG autista: {0}'.format(flg_auti))
                logger.info('Mansioni: {0}'.format(mansioni))
                logger.info('Tipi inserimento: {0}'.format(tipo_inserimento_ru))
                
                
                
                # ora faccio un ciclo sulle registrazioni per quel percorso
                
                try:
                    curr_c.execute(query_registrazioni_percorso, (cod_percorso, datalav))
                    registrazioni_percorso=curr_c.fetchall()
                except Exception as e:
                    logger.error(query_registrazioni_percorso)
                    logger.error(e)
                
                
                for registrazione in registrazioni_percorso:
                    codice_badge=registrazione[0]
                    id_ekovision=registrazione[1]
                    cognome_ru=registrazione[2]
                    nome_ru=registrazione[3]
                    id_percorso=registrazione[4]
                    data_percorso=registrazione[5]
                    id_qualifica=registrazione[6]
                    id_mansione=registrazione[7]
                    sportello=registrazione[8]
                    targa=registrazione[9]
                    id_rt_eko=registrazione[10]
                    datainsert_registrazione=registrazione[11]
                    
                    
                    
                    logger.info('Elaboro registrazione di {0} {1} con badge = {2}, cod_eko = {6} e qualifica {3} per il percorso {4} del {5}'.format(
                        nome_ru, cognome_ru, codice_badge, id_qualifica, id_percorso, data_percorso, id_ekovision
                    ))
            
                    if sportello is not None:
                        logger.info('Dovrei inserire la risorsa tecnica con sportello {0}, targa = "{1}"'.format(sportello, targa))
                        logger.debug(id_rt)
                        # verifico se targa già presente
                        if targa in targa_rt:
                            logger.info('La scheda ha già la risorsa tecnica con sportello {0} - targa {1} con indice {2}'.format(sportello, targa, targa_rt.index(targa)))
                            # prendo il progressivo della risorsa tecnica da usare per inserire la risorsa umana
                            progressivo_tmp = progr_rt[targa_rt.index(targa)]
                            logger.info('Il progressivo da usare per inserire la risorsa umana è {0}'.format(progressivo_tmp))
                        else:
                            # non c'è la risorsa tecnica da inserire, devo inserirla io
                            # caso 1 solo mezzo
                            if len(letture2['schede_lavoro'][0]['risorse_tecniche'])==1:
                                id_progressivo_tmp=letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id_progressivo']
                                logger.info('La scheda ha una sola risorsa tecnica, va modificata')
                                # se id = 0 signigica che è una risorsa tecnica non è statadefinita  va modificata la prima risorsa tecnica
                                insert_update_rt = 0
                                if int(letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']) == 0 :
                                    logger.warning('La scheda non ha nessuna risorsa tecnica, la aggiorno'.format(id_rt_eko))
                                    insert_update_rt = 1    
                                elif int(letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']) > 0 and tipo_inserimento_rt[0] == 1:
                                    logger.warning('La scheda ha una risorsa tecnica predefinita, la aggiorno con id {0} da totem'.format(id_rt_eko))
                                    #letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=id_rt_eko 
                                    insert_update_rt = 1     
                                else:
                                    testo_mail= f'''{incipit_mail_test}
                                    La scheda di lavoro con id {id_scheda} (codice percorso {cod_percorso} - {letture2['schede_lavoro'][0]['descr_scheda_lav']} 
                                    del {datalav}) ha una risorsa tecnica con targa {letture2['schede_lavoro'][0]['risorse_tecniche'][0]['targa']} 
                                    impostata manualmente su Ekovision, 
                                    ma dalle info del totem dovrebbe essere modificata con sportello {sportello}.
                                    <br>Verificare le informazioni inserite dall'operatore con badge {codice_badge} 
                                    ed eventualmente correggere Ekovision''' 
                                    
                                    #warning_message_mail(testo_mail, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')
                                    
                                    mails_percorso = cerco_mail(curr_c, cod_percorso, datalav, logger)
                                    totem_message_mail(test, testo_mail, mails_percorso,  os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')
                                                       
                                                       
                                if insert_update_rt == 1 :
                                    # cambio tipo inserimento rt
                                    tipo_inserimento_rt[0] = 2   
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=id_rt_eko
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['targa']=targa
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['data_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['ora_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['data_fine']=letture2['schede_lavoro'][0]['servizi'][0]['data_fine']
                                    letture2['schede_lavoro'][0]['risorse_tecniche'][0]['ora_fine']=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']     
                            elif len(letture2['schede_lavoro'][0]['risorse_tecniche'])==0:
                                logger.info('La scheda non ha risorse tecniche, sarebbe da aggiungere ma forse meglio inviare mail')
                                #letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id']=sportello 
                                # TO DO MAIL
                                
                            elif len(letture2['schede_lavoro'][0]['risorse_tecniche'])>1:
                                # TODO ancora tutto da gestire
                                
                                # controllo se c'è id = 0 e se c'è modifico il primo 
                                logger.info('La scheda ha più di una risorsa tecnica, va modificata la prima con id=0')
                                # imposto questa variabile a 0
                                update_rt=0
                                rtt=0
                                while rtt < len(letture2['schede_lavoro'][0]['risorse_tecniche']):
                                    # se arrivo qua ho già verificato che la targa non è tra quelle già presenti, quindi devo solo capire 
                                    # se ho spazio o meno per inserire questo sportello
                                    if letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id'] == 0:
                                        update_rt=1
                                        id_progresssivo_tmp=letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id_progressivo']
                                        
                                    elif letture2['schede_lavoro'][0]['risorse_tecniche'][0]['id'] > 0 and tipo_inserimento_rt[rtt] == 1:
                                        logger.warning('La scheda ha una risorsa tecnica predefinita, la aggiorno con id {0} da totem'.format(id_rt_eko))
                                        update_rt=1
                                        id_progresssivo_tmp=letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id_progressivo']
                                        
                                    else:    
                                        testo_mail= f'''{incipit_mail_test}
                                        La scheda di lavoro con id {id_scheda} (codice percorso {cod_percorso} - {letture2['schede_lavoro'][0]['descr_scheda_lav']} 
                                        del {datalav}) ha più risorse tecniche già impostate manualmente su Ekovision, 
                                        ma dalle info del totem dovrebbe esserci anche lo sportello {sportello}.
                                        <br>Verificare le informazioni inserite dall'operatore con badge {codice_badge} 
                                        ed eventualmente correggere Ekovision''' 
                                        mails_percorso = cerco_mail(curr_c, cod_percorso, datalav, logger)
                                        totem_message_mail(test, testo_mail, mails_percorso,  os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')
                                        #warning_message_mail(testo_mail, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')
                                        break # inutile andare al passo successivo
                                    if update_rt==1:
                                        # cambio a 2 per non toccarla più
                                        tipo_inserimento_rt[rtt]=2
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['id']=id_rt_eko
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['data_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['ora_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['data_fine']=letture2['schede_lavoro'][0]['servizi'][0]['data_fine']
                                        letture2['schede_lavoro'][0]['risorse_tecniche'][rtt]['ora_fine']=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']
                                    # vado al passo successivo
                                    rtt=rtt+1
                    if id_progressivo_tmp is None:
                        if len(letture2['schede_lavoro'][0]['risorse_tecniche'])>=1:
                            id_progressivo_tmp=1
                        else :
                            id_progressivo_tmp=0
                    
                        
                    if id_ekovision in id_ru:
                        logger.info('La scheda ha già la risorsa umana con id {0} con indice {1}'.format(id_ekovision, id_ru.index(id_ekovision)))
                        # se su Ekovision fosse definito come autista, ma su totem no tolgo il flag autista
                        if int(flg_auti[id_ru.index(id_ekovision)]) == 1 and sportello is None:
                            logger.info('La risorsa umana id {0} è autista su Ekovision ma va tolto'.format(id_ekovision))
                            letture2['schede_lavoro'][0]['risorse_umane'][id_ru.index(id_ekovision)]['flg_autista']=0
                        elif int(flg_auti[id_ru.index(id_ekovision)]) == 0 and sportello is not None:
                            logger.info('La risorsa umana con id {0} non è autista su Ekovision, ma lo è dalle info del totem --> correggo'.format(id_ekovision))
                            letture2['schede_lavoro'][0]['risorse_umane'][id_ru.index(id_ekovision)]['flg_autista']=1
                    
                    else:
                        logger.info('La scheda non ha la risorsa umana con id {0}, va aggiunta'.format(id_ekovision))
                        if len(letture2['schede_lavoro'][0]['risorse_umane'])==0:
                            logger.info('La scheda non ha risorse umane, devo capire se posso aggiungerle')
                            # INVIO MAIL
                            
                            
                            
                        else:
                            logger.info('La scheda ha già risorse umane - OK ')
                            ru=0
                            # primo giro 
                            check_inserimento=0
                            while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                                if id_mansione == mansioni[ru] and tipo_inserimento_ru[ru] == 0:
                                    # cambio tipo inserimento (mettendo 2 non dovrei toccarla più)
                                    tipo_inserimento_ru[ru]=2
                                    check_inserimento=1
                                    logger.info('Ho trovato una risorsa umana con mansione {0} e tipo inserimento 0, quindi posso inserirla'.format(id_mansione))
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']=id_ekovision 
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['id_progr_ristec']=id_progressivo_tmp 
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['cognome']=cognome_ru # non funziona ma forse non serve
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['nome']=nome_ru # non funziona ma forse non serve
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['data_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['data_inizio']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_inizio']=letture2['schede_lavoro'][0]['servizi'][0]['ora_inizio']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['data_fine']=letture2['schede_lavoro'][0]['servizi'][0]['data_fine']
                                    letture2['schede_lavoro'][0]['risorse_umane'][ru]['ora_fine']=letture2['schede_lavoro'][0]['servizi'][0]['ora_fine']
                                    
                                    # se c'è una risorsa tecnica con id da totem e questa risorsa umana è autista, metto il flag autista
                                    if sportello is not None:
                                        logger.info(''' C'è una risorsa tecnica sportello {0} da totem, 
                                                    quindi la risorsa umana con id {1} è autista --> metto flag autista'''.format(sportello, id_ekovision))
                                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=1
                                    else: 
                                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=0
                                    logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][ru])
                                    break
                                ru=ru+1
                            # secondo giro 
                            if check_inserimento == 0:
                                logger.info(f'''Non ho trovato una risorsa umana con mansione {id_mansione} e tipo inserimento 0, 
                                            quindi cerco una risorsa umana con tipo inserimento 1 per sostituirla''')
                                ru=0
                                while ru < len(letture2['schede_lavoro'][0]['risorse_umane']):
                                    if id_mansione == mansioni[ru] and tipo_inserimento_ru[ru] == 1:
                                        logger.info('Ho trovato una risorsa umana con mansione {0} e tipo inserimento 1, quindi posso sostituirla con la risorsa da inserire'.format(id_mansione))
                                        check_inserimento=1
                                        # cambio tipo inserimento (mettendo 2 non dovrei toccarla più)
                                        tipo_inserimento_ru[ru]=2
                                        
                                        letture2['schede_lavoro'][0]['risorse_umane'][ru]['id']=id_ekovision  
                                        # se c'è una risorsa tecnica con id da totem e questa risorsa umana è autista, metto il flag autista
                                        if sportello is not None:
                                            logger.info('C\'è una risorsa tecnica con id {0} da totem, quindi la risorsa umana con id {1} è autista --> metto flag autista'.format(sportello, id_ekovision))
                                            letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=1
                                        else: 
                                            letture2['schede_lavoro'][0]['risorse_umane'][ru]['flg_autista']=0
                                        logger.debug(letture2['schede_lavoro'][0]['risorse_umane'][ru])
                                        break
                                    ru=ru+1

                            if check_inserimento == 0:
                                # devo renderlo più comprensibile questo messaggio di errore
                                
                                warning_msg = f'''{incipit_mail_test}
                                Non sono riuscito ad inserire la risorsa umana con badge {codice_badge}, 
                                id ekovision {id_ekovision}, mansione {id_mansione} nella scheda {id_scheda}
                                (codice percorso {cod_percorso} - {letture2['schede_lavoro'][0]['descr_scheda_lav']} 
                                del {datalav}) '''
                                warning_message_mail(warning_msg, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa umana non inseribile su Ekovision')
                                #mails_percorso = cerco_mail(curr_c, cod_percorso, datalav, logger)
                                #totem_message_mail(test, testo_mail, mails_percorso,  os.path.basename(__file__), logger, 'Scheda di lavoro con risorsa tecnica da modificare')

                
                #exit()
                logger.info('Provo a salvare la scheda')
                #logger.info(letture2)
                
                guid = uuid.uuid4()
                params2={'obj':'schede_lavoro',
                        'act' : 'w',
                        'ruid': '{}'.format(str(guid)),
                        'json': json.dumps(letture2, ensure_ascii=False).encode('utf-8')
                        }
                #exit()
                response2 = requests.post(eko_URL, params=params2, data=auth_data_eko, headers=headers)
                result2 = response2.json()
                if result2['status']=='error':
                    # devo mandare una mail 
                    # codice_badge=registrazione[0]
                    # id_ekovision=registrazione[1]
                    # cognome_ru=registrazione[2]
                    # nome_ru=registrazione[3]
                    # id_percorso=registrazione[4]
                    # data_percorso=registrazione[5]
                    # id_qualifica=registrazione[6]
                    # id_mansione=registrazione[7]
                    # sportello=registrazione[8]
                    # targa=registrazione[9]
                    # id_rt_eko=registrazione[10]
                    # datainsert_registrazione=registrazione[11]
                    msg = f'''Ci sono problemi a salvare le informazioni registrate su totem sulla scheda {id_scheda} di Ekovision 
                    (codice percorso {cod_percorso} - {descrizione_percorso(cod_percorso,  datalav, curr, logger)} del {datetime.strptime(datalav, '%Y%m%d').strftime('%d/%m/%Y')}
                    )
                    <br><br>
                    Dati registrati su Totem: 
                    <ul> 
                    <li>cod percorso: {cod_percorso}</li>
                    <li>data: {datetime.strptime(datalav, '%Y%m%d').strftime('%d/%m/%Y')}</li>'''
                    
                    for rr in registrazioni_percorso:
                        msg =f'''{msg} 
                        <ul>                      
                        <li>matricola: {rr[0]} ({rr[2]} {rr[3]})</li>
                        <li>id risorsa umana ekovision: {rr[1]} </li>'''
                        # se c'è un mezzo
                        if rr[8]:
                            msg =f'''{msg}<li>sportello: {rr[8]} (Targa: {rr[9]})</li>
                                <li>id risorsa tecnica ekovision: {rr[10]} </li>'''
                        else: 
                            msg=f'''{msg}<li>Nessun mezzo registrato</li>'''
                        msg=f'''{msg}
                        <li>data inserimento su totem: {rr[11].strftime('%d/%m/%Y %H:%M')} </li>
                        </ul>'''
                    msg = f'''{msg}     
                    </ul>
                    <br><br>
                    Messaggio di errore Ekovision:<br>
                    <i>
                    {result2['error']['error_message']}
                    </i>
                    <br><br> 
                    <b>I dati, se corretti, vanno inseriti manualmente su Ekovision. </b>
                    <hr>
                    '''
                    
                    mails_percorso = cerco_mail(curr_c, cod_percorso, datalav, logger)
                    logger.info (f'Invio mail a {mails_percorso}')
                    
                    totem_message_mail(test, msg, mails_percorso,  os.path.basename(__file__), logger,  'Problema trasferimento dati totem ekovision')
                    
                    logger.warning('Id_scheda = {}'.format(id_scheda))
                    logger.warning(result2)
                    logger.warning('**************************************************')
                    logger.warning('              JSON                                ')
                    logger.warning('**************************************************')
                    logger.warning(letture2)
                    curr_c.execute(query_update_error, (codice_badge, id_percorso, data_percorso, datainsert_registrazione))
                else :
                    logger.info(result2['status'])
                    # a questo punto dovrei fare 
                    curr_c.execute(query_update_success, (codice_badge, id_percorso, data_percorso, datainsert_registrazione))
                conn_c.commit()

            else:
                # non esiste la scheda non faccio niente, la processerò al prossimo giro
                continue



    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    curr_c.close()
    #currc1.close()
    conn_c.close()
    
    curr.close()
    conn.close()
    
    

if __name__ == "__main__":
    main()      