#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberto Marzocchi Roberta Fagandini

'''
Lo script gestisce i percorsi stagionali


1) la partenza è la tabella elem.percorsi del SIT che per gli stagionali ha senso resti il punto di partenza 
    in quanto contiene i campi stwitch on e switch off 

NOTA c'è un job che controlla che le date di attivazione e disattivazione degli stagionali siano posticipate nel tempo


MOLTO PRIMA DELL'ATTIVAZIONE DEVO FARE QUESTE COSE:
1) aggiorno le 4 tabelle dello schema anagrafe_percorsi del SIT per creare già il record. Stesso codice, 
    ma nuova versione per mantenere tutti gli storici su Ekovision e su tutti i sistemi

2) faccio la stessa cosa su ANAGR_SER_PER_UO della UO per creare già il record


In questo modo il percorso stagionale è già presente sul DB e posso fare eventuali modifiche a descrizione, 
mezzi, turni e frequenze prima che parta


12 GIORNI PRIMA MANDA NOTIFICA AL TERRITORIO: 



IL GIORNO PRIMA DOVREI CAMBIARE ID_CATEGORIA_USO sul SIT 
(TODO ora lo fa il job)

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

import csv



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
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
        
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logger.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    giorno_file=datetime.today().strftime('%Y%m%d')

    logger.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logger.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    data_start_ekovision='20231120'
    
    
    

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
    
    
    select_stagionali = '''select p.id_percorso, p.cod_percorso, p.descrizione, p.stagionalita,
    p.ddmm_switch_on, p.ddmm_switch_off, u.descrizione as ut, 
    s.descrizione as servizio, to_char(data_attivazione, 'DD/MM/YYYY') as data_attivazione, 
    case 
    when data_dismissione is null then '01/12/2099'
    else to_char(data_dismissione, 'DD/MM/YYYY')
    end data_dismissione, 
    3 as attivo, 
    id_categoria_uso, 
    ep.cod_percorso
    from elem.percorsi p
    left join elem.percorsi_ut pu on pu.cod_percorso = p.cod_percorso 
    left join topo.ut u on u.id_ut = pu.id_ut and pu.responsabile = 'S'
    left join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso and ep.data_inizio_validita = p.data_attivazione
    join elem.servizi s on s.id_servizio= p.id_servizio
    where p.stagionalita is not null and data_attivazione > now() 
    and ep.cod_percorso is null
    and id_categoria_uso = 6
    order by p.data_attivazione '''
    
    
    select_stagionali_mail = '''select 
    p.cod_percorso, 
    p.descrizione, 
    p.stagionalita,
    p.ddmm_switch_on,
    p.ddmm_switch_off,
    u.descrizione as ut, 
    s.descrizione as servizio, 
    to_char(data_attivazione, 'DD/MM/YYYY') as data_attivazione, 
    case 
    when data_dismissione is null then '01/12/2099'
    else to_char(data_dismissione, 'DD/MM/YYYY')
    end data_dismissione, 
    ep.cod_percorso, 
    u.mail as mail_ut, 
    za.mail as mail_zona, 
    case 
    	when data_attivazione  = current_date + interval '{0}' day 
    	then 'attivazione'
    	when data_dismissione  = current_date + interval '{0}' day 
    	then 'disattivazione'
    end tipo_notifica
    from elem.percorsi p
    left join elem.percorsi_ut pu on pu.cod_percorso = p.cod_percorso 
    left join topo.ut u on u.id_ut = pu.id_ut and pu.responsabile  = 'S'
    left join topo.zone_amiu za on za.id_zona = u.id_zona 
    left join anagrafe_percorsi.elenco_percorsi ep on ep.cod_percorso = p.cod_percorso and ep.data_inizio_validita = p.data_attivazione
    join elem.servizi s on s.id_servizio= p.id_servizio
    where p.stagionalita is not null and 
    (data_attivazione  = current_date + interval '{0}' day 
    or 
    data_dismissione  = current_date + interval '{0}' day)
    and id_categoria_uso in (3,6)
    order by p.data_attivazione'''.format(12)
    
    
 
    try:
        curr.execute(select_stagionali)
        lista_stagionali=curr.fetchall()
    except Exception as e:
        logger.error(select_stagionali)
        logger.error(e)

    
    
    
    insert_percorso1 = '''INSERT INTO anagrafe_percorsi.elenco_percorsi (cod_percorso, descrizione, id_tipo, freq_testata,
        id_turno, durata, codice_cer, versione_testata,
        data_inizio_validita, data_fine_validita, freq_settimane, ekovision, stagionalita, ddmm_switch_on, ddmm_switch_off)
        (
            select cod_percorso, descrizione, id_tipo, freq_testata, id_turno, durata, codice_cer,
            versione_testata+1, 
            to_date(%s,'DD/MM/YYYY') , to_date(%s,'DD/MM/YYYY'), freq_settimane, ekovision, stagionalita, ddmm_switch_on, ddmm_switch_off
            from anagrafe_percorsi.elenco_percorsi ep 
            where cod_percorso = %s
            and versione_testata = (select max(ep1.versione_testata) from anagrafe_percorsi.elenco_percorsi ep1 where ep1.cod_percorso = ep.cod_percorso)
        )''' 



    insert_percorso2 = '''INSERT INTO anagrafe_percorsi.elenco_percorsi_old (
    id_percorso_sit, cod_percorso, descrizione, id_tipo,
    freq_testata, versione_uo, data_inizio_validita, data_fine_validita) 
    (
        select %s, cod_percorso, descrizione, id_tipo,
        freq_testata, versione_uo+1, to_date(%s,'DD/MM/YYYY'), to_date(%s,'DD/MM/YYYY')
        from anagrafe_percorsi.elenco_percorsi_old ep where cod_percorso = %s
        and versione_uo = (select max(ep1.versione_uo) 
        from anagrafe_percorsi.elenco_percorsi_old ep1 where ep1.cod_percorso = ep.cod_percorso)
    ) '''
    
    
    
    insert_percorso3 = '''INSERT INTO anagrafe_percorsi.percorsi_ut 
    (cod_percorso, id_ut, id_squadra, responsabile, solo_visualizzazione,
    rimessa, id_turno, durata,
    data_attivazione, data_disattivazione, cdaog3) 
    (
        select cod_percorso, id_ut, id_squadra, responsabile, solo_visualizzazione,
        rimessa, id_turno, durata,
        to_date(%s,'DD/MM/YYYY'), to_date(%s,'DD/MM/YYYY'), cdaog3 
        from anagrafe_percorsi.percorsi_ut pu where cod_percorso = %s
        and data_disattivazione = (select max(data_disattivazione) from anagrafe_percorsi.percorsi_ut pu1
        where pu1.cod_percorso = pu.cod_percorso)
    )  '''
    
    
    
    insert_percorso4 = '''INSERT INTO anagrafe_percorsi.date_percorsi_sit_uo 
        (id_percorso_sit, cod_percorso, data_inizio_validita, data_fine_validita)
        VALUES(%s, %s, to_date(%s,'DD/MM/YYYY'), to_date(%s,'DD/MM/YYYY'))'''
        
        
    # modifiche 2026 insert anche in anagrafe_percorsi.percorsi_mezzi, angrafe_percorsi.percorsi_comuni, anagrafe_percorsi.percorsi_destinazione
    
    insert_percorso5 = '''INSERT INTO anagrafe_percorsi.percorsi_mezzi
        (cod_percorso, versione, id_mezzo) 
        values 
        (%s, (select max(versione)+1 from anagrafe_percorsi.percorsi_mezzi pm where pm.cod_percorso = %s),
        (select id_mezzo from anagrafe_percorsi.percorsi_mezzi pm 
            where pm.cod_percorso = %s 
                and pm.versione = (select max(versione) from anagrafe_percorsi.percorsi_mezzi pm1 where pm1.cod_percorso = pm.cod_percorso)
        )'''
    
    insert_percorso6 = '''INSERT INTO anagrafe_percorsi.percorsi_comuni
            (cod_percorso, versione, id_comune, competenza) 
            values 
            (%s, (select max(versione)+1 from anagrafe_percorsi.percorsi_mezzi pm where pm.cod_percorso = %s),
            (select id_comune from anagrafe_percorsi.percorsi_mezzi pm 
                where pm.cod_percorso = %s 
                    and pm.versione = (select max(versione) from anagrafe_percorsi.percorsi_mezzi pm1 where pm1.cod_percorso = pm.cod_percorso)
            )%s, (select max(versione)+1 from anagrafe_percorsi.percorsi_mezzi pm where pm.cod_percorso = %s),
            (select competenza from anagrafe_percorsi.percorsi_mezzi pm 
                where pm.cod_percorso = %s 
                    and pm.versione = (select max(versione) from anagrafe_percorsi.percorsi_mezzi pm1 where pm1.cod_percorso = pm.cod_percorso)
            ), 
            '''
    insert_percorso7 = '''INSERT INTO anagrafe_percorsi.percorsi_destinazione
            (cod_percorso, versione, id_destinazione) 
            values 
            (%s, (select max(versione)+1 from anagrafe_percorsi.percorsi_mezzi pm where pm.cod_percorso = %s),
            (select id_destinazione from anagrafe_percorsi.percorsi_mezzi pm 
                where pm.cod_percorso = %s 
                    and pm.versione = (select max(versione) from anagrafe_percorsi.percorsi_mezzi pm1 where pm1.cod_percorso = pm.cod_percorso)
            )'''
    
    
    
    for ls in lista_stagionali:
        # id_percorso  ls[0]
        # cod_percorso ls[1]
        # data attivazione ls[8]
        # data disattivazione ls[9]
        
        
        
        # INSERT INTO anagrafe_percorsi.elenco_percorsi
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso1, (ls[8], ls[9], ls[1]))
        except Exception as e:
            logger.error(insert_percorso1)
            logger.error(e)
        
        curr1.close()
        
        
        
        # INSERT INTO anagrafe_percorsi.elenco_percorsi_old 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso2, (ls[0], ls[8], ls[9], ls[1]))
        except Exception as e:
            logger.error(insert_percorso2)
            logger.error(e)
        
        curr1.close()
        
        
    
        
        # INSERT INTO anagrafe_percorsi.percorsi_ut 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso3, (ls[8], ls[9], ls[1]))
        except Exception as e:
            logger.error(insert_percorso3)
            logger.error(e)
        
        curr1.close()
    
    
        # anagrafe_percorsi.date_percorsi_sit_uo
        # INSERT INTO anagrafe_percorsi.elenco_percorsi_old 
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso4, (ls[0], ls[1], ls[8], ls[9]))
        except Exception as e:
            logger.error(insert_percorso4)
            logger.error(e)
        
        curr1.close()
        
        curr1 = conn.cursor()
        try:
            curr1.execute(insert_percorso5, (ls[1], ls[1], ls[1]))
        except Exception as e:
            logger.error(insert_percorso5)
            logger.error(e)
        
        try:
            curr1.execute(insert_percorso6, (ls[1], ls[1], ls[1], ls[1], ls[1], ls[1]))
        except Exception as e:
            logger.error(insert_percorso6)
            logger.error(e)
        
        try:
            curr1.execute(insert_percorso7, (ls[1], ls[1], ls[1]))
        except Exception as e:
            logger.error(insert_percorso7)
            logger.error(e)
        
        
        curr1.close() 
        
        conn.commit()
        
        # lanciare procedura o funzione della UO 
    
        try:
            logger.debug(ls[8])
            #exit()
            #strptime
            ret=cur.callproc('UNIOPE.ATTIVA_PERCORSI_STAGIONALI',
                    [ls[1],datetime.strptime(ls[8], '%d/%m/%Y'), datetime.strptime(ls[9], '%d/%m/%Y')])
            logger.debug(ret)
        except Exception as e:
            logger.error(e) 
    
    
    
        con.commit()
        #exit()
        
        
    # invio mail al territorio per avvisare che il percorso stagionale sarà attivo tra 12 giorni
    logger.info('Invio mail al territorio per avvisare che il percorso stagionale sarà attivo tra 12 giorni')
    try:
        curr.execute(select_stagionali_mail)
        lista_stagionali_mail=curr.fetchall()
    except Exception as e:
        logger.error(select_stagionali_mail)
        logger.error(e)     
        
    for lsm in lista_stagionali_mail:
        logger.debug(lsm)
        if lsm[2] == 'I':
            stag='<font color=blue> INVERNALE </font>'
        elif lsm[2] == 'E':
            stag='<font color=orange> ESTIVO </font>'
        else:
            stag='<font color=red>STAGIONALE</font>'
        if lsm[12] == 'attivazione':
            testo='<font color="green">attivazione</font>'
            testo2=f'<br><br>Il percorso sarà nuovamente attivo dal <strong>{lsm[7]}</strong> al <strong>{lsm[8]}</strong>.<br><br>'
        elif lsm[12] == 'disattivazione':
            testo='<font color="red">disattivazione</font>'
            testo2=f'''<br><br>Il percorso sarà disattivato dal <strong>{lsm[8]}</strong>. 
            <br>Le date configurate su SIT per attivazione disattivazione sono:
            <ul>
            <li>attivazione: <strong>{lsm[3][:2]}/{lsm[3][2:4]}</strong></li>
            <li>disattivazione: <strong>{lsm[4][:2]}/{lsm[4][2:4]}</strong></li>
            </ul>'''
        else:
            testo='attivazione/disattivazione'
        mail_ut=lsm[10]
        mail_zona=lsm[11]
       
        
        
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'
        oggetto='ATTENZIONE - Notifica {} percorso stagionale {} - {} dal {} '.format(lsm[12], lsm[0], lsm[1],  lsm[7], lsm[8])
        
        testo_mail=f'''Gentile {lsm[5]}, <br>
        questa mail è una notifica di {testo} del il percorso {stag} <b>{lsm[0]} - {lsm[1]} </b> 
        {testo2}
        E' possibile visualizzare/modificare sul SIT i dettagli di tutti i <a href="{new_sit_url}/percorsi.php">percorsi</a>.<br><br>
        In caso di problemi con le date di attivazione / disattivazione  o altro contattare i propri referenti 
        e, per supporto informatico assistenza Territorio <href="mailto:{receiver_email}">{receiver_email}</a>'''
        
        
        
        
    
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'noreply@amiu.genova.it'
        message["To"] =  mail_ut
        message["CC"] = '{}'.format(mail_zona)
        message["Bcc"] = '{}'.format(receiver_email)
        #message["CCn"] = debug_email
        message["Subject"] = oggetto
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Chiusura schede di lavoro"
    
    
        body='''{0}
        <br><br>
        <hr>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico su vostra richiesta, non rispondere a questa mail ma non ignorarla.'''.format(testo_mail, )
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))
    
    
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        
    
        
        
        text = message.as_string()
    
        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        #invio_mail(mail_to, oggetto, testo, logger)
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    cur.close()
    con.close()




if __name__ == "__main__":
    main()      
    