#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione spazzamento:



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob


import requests
from requests.exceptions import HTTPError

import json

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



# per mandare file a EKOVISION
import pysftp


#import requests

import logging
import uuid


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


from descrizione_percorso import *  
    
     

def main():
      
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
    
    
    # Mi connetto al DB consuntivazione (PostgreSQL) - HUB
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


    query_verifica_causale='''select ve.*, cpra.desc_percorso from spazzamento.v_effettuati ve 
left join  spazzamento.cons_percorsi_spazz_x_app cpra 
on cpra.id_percorso = ve.idpercorso 
and ve.datalav between cpra.data_inizio and cpra.data_fine    
where ve.idpercorso =%s  and ve.datalav = to_date(%s, 'YYYYMMDD') 
and ve.id_causale <> %s'''
            
    # ciclo su elenco vie / note consuntivate
    """query_effettuati_totem='''select 
	e.id, 
	e.idpercorso,
	e.datalav::date ,
	t.id_via,
	trim(t.nota_via) as nota_via,
	case 
		when e.punteggio::int = 100 or ct.id = 100 then 1
		when e.punteggio::int = 0 and ct.id <> 100 then 0 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then 1
	end flag_esecuzione, 
	e.causale as descr_causale,
	ct.id as causale,
	case 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then concat('Svolto al ', e.punteggio,'% CAUSALE: ', ct.id ,' - ',  e.causale)
	end note_causale, 
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
	e.datainsert, 
    e.tappa, 
    e.punteggio,
    mu.mail
	from spazzamento.cons_percorsi_spazz_x_app t
	join spazzamento.effettuati e on e.tappa::int =  t.id_tappa_raggr::int
 	left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
	left join spazzamento.causali_testi ct on trim(e.causale) ilike trim(ct.descrizione)
 	left join servizi.mail_ut mu on mu.id_uo::int  =t.id_uo::int 
	where e.id > (select coalesce(max(max_id),0) from spazzamento.invio_consuntivazioni_ekovision ice) 
    /*and
	e.datalav <= (select max(datalav) + interval '3' day from  spazzamento.invio_consuntivazioni_ekovision ice)*/
	order by 1 limit 5000'''
    """
    
    
    # modifica del 08/05 per integrare nuovo backoffice AMIU
    query_effettuati_totem='''select 
	distinct 
	substr(e.id,3)::int as id,
	e.idpercorso,
	e.datalav::date ,
	t.id_via,
	trim(t.nota_via) as nota_via,
	case 
		when e.punteggio::int = 100 or ct.id = 100 then 1
		when e.punteggio::int = 0 and ct.id <> 100 then 0 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then 1
	end flag_esecuzione, 
	e.causale as descr_causale,
	ct.id as causale,
	case 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then concat('Svolto al ', e.punteggio,'% CAUSALE: ', ct.id ,' - ',  e.causale)
	end note_causale, 
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
	e.datainsert, 
    e.tappa, 
    e.punteggio,
    string_agg(distinct mu.mail, ',')
	from spazzamento.cons_percorsi_spazz_x_app t
	join spazzamento.v_effettuati e on e.tappa::int =  t.id_tappa_raggr::int
 	left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
	left join spazzamento.causali_testi ct on trim(e.causale) ilike trim(ct.descrizione)
 	left join servizi.mail_ut mu on mu.id_uo::int  =t.id_uo::int 
	where
	/*tabella wingsoft*/
	(substr(e.id,1,1) = 'w'
	and substr(e.id,3)::int > (select coalesce(max(max_id),0) from spazzamento.invio_consuntivazioni_ekovision ice) 
	)
	or 
	(substr(e.id,1,1) = 'a'
	and substr(e.id,3)::int > (select coalesce(max(max_id),0) from spazzamento.invio_consuntivazioni_a_ekovision ice)
	)
    /*and
	e.datalav <= (select max(datalav) + interval '3' day from  spazzamento.invio_consuntivazioni_ekovision ice)*/
	group by 
	substr(e.id,3),
	e.idpercorso,
	e.datalav::date ,
	t.id_via,
	trim(t.nota_via) ,
	case 
		when e.punteggio::int = 100 or ct.id = 100 then 1
		when e.punteggio::int = 0 and ct.id <> 100 then 0 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then 1
	end , 
	e.causale ,
	ct.id ,
	case 
		when e.punteggio::int > 0 and e.punteggio::int < 100 then concat('Svolto al ', e.punteggio,'% CAUSALE: ', ct.id ,' - ',  e.causale)
	end , 
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome), 
	e.datainsert, 
    e.tappa, 
    e.punteggio
	order by 1 limit 5000'''
    
    
    # prima di tutto faccio un controllo che non ci siano causali che non so gestire e nel caso fermo tutto il passaggio dati e lancio allarme
    query_check='''select distinct causale, descr_causale from (
        {}
        ) as foo'''.format(query_effettuati_totem)
    
    
    try:
        currc.execute(query_check)
        lista_causali=currc.fetchall()
    except Exception as e:
        logger.error(query_check)
        logger.error(e)


    for cc in lista_causali:
        if cc[0] == None:
            logger.error('''La causale {} non è riconosciuta. Andare sull'HUB aggiungere un id nella tabella spazzamento.causali_testo'''.format(cc[1])) 
            error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
    
    logger.info('CONTROLLO CAUSALI TERMINATO')
    currc.close()
    
    # recupero la max_datalav NON SERVE era un tentativo
    """currc = connc.cursor()
    query_dl='''select max(datalav) from (
        {}
        ) as foo'''.format(query_effettuati_totem)
    
    
    try:
        currc.execute(query_dl)
        lista_datalav=currc.fetchall()
    except Exception as e:
        logger.error(query_dl)
        logger.error(e)


    for dl in lista_datalav:
        max_datalav=dl[0].strftime("%Y%m%d")
    
    logger.info('max_datalav={}'.format(max_datalav))
    currc.close()
    """
    
    
    currc = connc.cursor()
    currc1 = connc.cursor()
                
    try:
        currc.execute(query_effettuati_totem)
        lista_x_via=currc.fetchall()
    except Exception as e:
        logger.error(query_effettuati_totem)
        logger.error(e)

    for vv in lista_x_via:
        
        # temporanemente tolgo i percorsi non presenti su SIT
        
        if vv[1] not in (#'0209000401', '0209000301', #raccolte siringhe
                         #'0207000201', '0207003301', '0207003401', '0207002801', #Aree verdi
                         '0207003701' # percorso doppio che non dovrebbe più esserci, ma per sicurezza lo mettiamo
                         ):
        
            # per quella id_percorso / via / nota / data cerco le correspondenti aste su SIT
            query_aste='''select ap.id_asta, p.id_turno, p.id_servizio, ap.id_asta_percorso, coalesce(ap.ripasso_fittizio,0) as ripasso_fittizio
            from 
            (select id_asta, id_asta_percorso, id_percorso, nota, ap1.ripasso_fittizio, data_inserimento, now()::date + interval '100 years' as data_eliminazione 
            from elem.aste_percorso ap1 
            where tipo= 'servizio'
            union 
            select id_asta, id_asta_percorso, id_percorso, nota, 0 as ripasso_fittizio, data_inserimento, data_eliminazione 
            from history.aste_percorso ap2
            where tipo= 'servizio' and data_eliminazione > %s) as ap
            join elem.percorsi p on p.id_percorso = ap.id_percorso 
            where ap.id_percorso = 
            (
                select id_percorso_sit  from anagrafe_percorsi.date_percorsi_sit_uo ep 
                where id_percorso_sit is not null  
                and cod_percorso = %s 
                and data_inizio_validita < %s 
                and data_fine_validita >= %s
            ) and id_asta in (
                select id_asta from elem.aste where id_via= %s
            )'''
            # se nota asta fosse nulla
            if vv[4]==None:
                query_aste='''{} and (ap.nota is null or trim(ap.nota) = '') '''.format(query_aste)
            else:
                #query_aste='''{} and trim(ap.nota) like %s'''.format(query_aste) 
                query_aste='''{} and similarity(trim(ap.nota), trim(%s))>=1'''.format(query_aste) 
                
            # prima di lanciare la query faccio questo check
            query_check='''select *  
                    from spazzamento.v_effettuati e 
                    where idpercorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and tappa=%s
                    and substr(e.id,3)::int <> %s 
                    and ((punteggio::int > %s) or left(codice,2) ilike 'ut')
                    '''
                
            try:
                currc1.execute(query_check, (vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0]), int(vv[12])))
                altre_consuntivazioni=currc1.fetchall()
            except Exception as e:
                logger.error(vv[11])
                logger.error('''{0} {1} {2} {3} {4}'''.format(vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0]), int(vv[12])))
                logger.error(query_check)
                logger.error(e)
                exit()
                                
            # se ci fosse un punteggio superiore o una consuntivazione del RUT (serve fino a quando il backoffice è di WingSOFT non servirà più dopo)
            # non passo i dati a ekovision
            if len(altre_consuntivazioni)>0:
                logger.warning('''Tappa {} del {} già consuntivata con punteggio maggiore. Non passo il dato a Ekovision'''.format(vv[11], vv[2].strftime('%Y-%m-%d')))
            else:
                # devo passare i dati a ekovision quindi procedo con il resto dello script
                try:
                    # se nota asta fosse nulla
                    if vv[4]==None:
                        curr.execute(query_aste, (vv[2], vv[1], vv[2], vv[2], vv[3]))
                    else:
                        curr.execute(query_aste, (vv[2], vv[1], vv[2], vv[2], vv[3], vv[4]))
                    lista_aste=curr.fetchall()
                except Exception as e:
                    logger.error('NON TROVO LE ASTE SUL SIT')
                    logger.error(query_aste)
                    logger.error('Codice percorso = {}'.format(vv[1]))
                    logger.error('Data rif = {}'.format(vv[2]))
                    logger.error('Id Via = {}'.format(vv[3]))
                    logger.error('Nota = {}'.format(vv[4]))
                    logger.error(e)
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                for aa in lista_aste:
                    #logger.debug(aa[0])       
                    # controllo sulla consuntivazione pregressa
                
                 
                    if aa[2]==33:
                        logger.debug('CONSUNTIVAZIONI BOTTICELLA')
                        # lavaggio con botticella devo cercare le componenti per quell'asta percorso
                        select_componenti='''select id_elemento from (   
                                        select id_asta_percorso, id_elemento from elem.elementi_aste_percorso eap 
                                        union 
                                        select id_asta_percorso, id_elemento from history.elementi_aste_percorso eap 
                                    ) as eap where id_asta_percorso::int = %s'''
                        try:
                            curr1.execute(select_componenti, (int(aa[3]),))
                            componenti=curr1.fetchall()
                        except Exception as e:
                            logger.error(int(aa[3]))
                            logger.error(select_componenti)
                            logger.error(e)
                            error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                            exit()
                        for cc in componenti:
                            cod_percorso.append(vv[1])
                            data_percorso.append(vv[2].strftime("%Y%m%d"))
                            id_turno.append(aa[1])
                            id_componente.append(cc[0])
                            id_tratto.append(None)
                            flag_esecuzione.append(vv[5])
                            causale.append(vv[7])
                            nota_causale.append(vv[8])
                            sorgente_dati.append(vv[9])
                            data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                            lat.append(None)
                            long.append(None)
                            ripasso.append(None)
                            qual.append(vv[12])
                            mail_arr.append(vv[13])                                 
                    else:
                        cod_percorso.append(vv[1])
                        data_percorso.append(vv[2].strftime("%Y%m%d"))
                        id_turno.append(aa[1])
                        id_componente.append(None)
                        id_tratto.append(aa[0])
                        if vv[7]==100:
                            flag_esecuzione.append(1)
                        else:
                            flag_esecuzione.append(vv[5])
                        causale.append(vv[7])
                        nota_causale.append(vv[8])
                        sorgente_dati.append(vv[9])
                        data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                        lat.append(None)
                        long.append(None)
                        ripasso.append(aa[4])
                        qual.append(vv[12])
                        mail_arr.append(vv[13])  
                    
        # mi salvo sempre il max_id    
        max_id=vv[0]
      
    
    
    # PARAMETRI GENERALI WS
    
    
    
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

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
    logger.debug('Lunghezza array cod_percorso {}'.format(len(cod_percorso)))
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
        
    
    
    k=0
    while k< len(cod_percorsi_distinct):
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
        if check<1:
            letture = response.json()
            logger.debug(letture)
            logger.debug(len(letture['schede_lavoro']))
            if len(letture['schede_lavoro']) == 0:
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
                    logger.error(e)
                    logger.error(' - id: {}'.format(ruid))
                    logger.error(' - Cod_percorso: {}'.format(cod_percorsi_distinct[k]))
                    logger.error(' - Data: {}'.format(date_distinct[k]))
                    #logger.error('Id Scheda: {}'.format(id_scheda[k]))
                    # check se c_handller contiene almeno una riga 
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.info("chiudo le connessioni in maniera definitiva")
                    currc.close()
                    #currc1.close()
                    connc.close()
                    curr.close()
                    conn.close()
                    exit()
                    
                    
                    
                    
                if check_creazione_scheda ==1:
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
                        conn.commit() 
                        body_mail='''E' arrivata una consuntivazione da totem per il percorso {} - {} in data {}.
                        <br>Origine del dato:{}
                        <br>Non esistendo la scheda per il giorno in questione è stata creata in automatico.
                        La nuova scheda ha ID {}'''.format(cod_percorsi_distinct[k],
                                                           descrizione_percorso(cod_percorsi_distinct[k],  date_distinct[k], curr, logger),
                                                           date_distinct[k], sorgente_dati_distinct[k], id_scheda)           
                        creazione_scheda_mail(body_mail, mail_arr_distinct[k], os.path.basename(__file__), logger)
                    else:
                        curr.execute(query_insert, (str(ruid),cod_percorsi_distinct[k], date_distinct[k], check_creazione_scheda))
                        conn.commit() 
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
                
                
                
                   
                if check_creazione_scheda ==1: 
                    logger.info('Forzo la pre-consuntivizione delle tappe non fatte perchè di fatto questo percorso non era previsto, quindi tutto ciò che non ho consuntivato è non previsto')   
                    # nei casi di creazione schede devo anche forzare la pre-consuntivazione delle tappe non fatte che altrimenti su Ekovision risulterebbero tutte da fare 
                    query_hub= '''select 
                        null as id, 
                        t.id_percorso as idpercorso,
                        to_date(%s, 'YYYYMMDD')::date as datalav ,
                        t.id_via,
                        trim(t.nota_via) as nota_via,
                        2 as flag_esecuzione, 
                        null as descr_causale,
                        999 as causale,
                        null as note_causale, 
                        'Preconsuntivazione percorso straordinario' as sorgente_dati, 
                        now() as datainsert, 
                        t.id_tappa_raggr as tappa, 
                        0 as punteggio,
                        null as mail
                        from spazzamento.cons_percorsi_spazz_x_app t
                        where t.id_percorso = %s
                        and t.data_inizio <= to_date(%s, 'YYYYMMDD')
                        and t.data_fine > to_date(%s, 'YYYYMMDD')
                        and t.id_tappa_raggr not in (
                            select tappa from spazzamento.v_effettuati e1 
                            where e1.idpercorso = %s 
                            and e1.datalav=to_date(%s, 'YYYYMMDD')
                        ) '''
                    try:
                        currc.execute(query_hub, (date_distinct[k], 
                                                cod_percorsi_distinct[k],
                                                date_distinct[k],
                                                date_distinct[k], 
                                                cod_percorsi_distinct[k],
                                                date_distinct[k]))
                        lista_x_via2=currc.fetchall()
                    except Exception as e:
                        logger.error(query_hub)
                        logger.error(e)
                        lista_x_via2=[]

                    

                    for vv2 in lista_x_via2:
                        query_aste='''select ap.id_asta, p.id_turno, p.id_servizio, ap.id_asta_percorso, coalesce(ap.ripasso_fittizio,0) as ripasso_fittizio
                        from 
                        (select id_asta, id_asta_percorso, id_percorso, nota, ap1.ripasso_fittizio, data_inserimento, now()::date + interval '100 years' as data_eliminazione 
                        from elem.aste_percorso ap1 
                        where tipo= 'servizio'
                        union 
                        select id_asta, id_asta_percorso, id_percorso, nota, 0 as ripasso_fittizio, data_inserimento, data_eliminazione 
                        from history.aste_percorso ap2
                        where tipo= 'servizio' and data_eliminazione > %s) as ap
                        join elem.percorsi p on p.id_percorso = ap.id_percorso 
                        where ap.id_percorso = 
                        (
                            select id_percorso_sit  from anagrafe_percorsi.date_percorsi_sit_uo ep 
                            where id_percorso_sit is not null  
                            and cod_percorso = %s 
                            and data_inizio_validita < %s 
                            and data_fine_validita >= %s
                        ) and id_asta in (
                            select id_asta from elem.aste where id_via= %s
                        )'''
                        
                        
                        # se nota asta fosse nulla
                        if vv[4]==None:
                            query_aste='''{} and (ap.nota is null or trim(ap.nota) = '') '''.format(query_aste)
                        else:
                            #query_aste='''{} and trim(ap.nota) like %s'''.format(query_aste) 
                            query_aste='''{} and similarity(trim(ap.nota), trim(%s))>=1'''.format(query_aste) 
                            
                            
                        try:
                            # se nota asta fosse nulla
                            if vv[4]==None:
                                curr.execute(query_aste, (vv[2], vv[1], vv[2], vv[2], vv[3]))
                            else:
                                curr.execute(query_aste, (vv[2], vv[1], vv[2], vv[2], vv[3], vv[4]))
                            lista_aste=curr.fetchall()
                        except Exception as e:
                            logger.error('NON TROVO LE ASTE SUL SIT')
                            logger.error(query_aste)
                            logger.error('Codice percorso = {}'.format(vv[1]))
                            logger.error('Data rif = {}'.format(vv[2]))
                            logger.error('Id Via = {}'.format(vv[3]))
                            logger.error('Nota = {}'.format(vv[4]))
                            logger.error(e)
                            error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                            exit()
                        for aa in lista_aste:
                            #logger.debug(aa[0])       
                            
                            if aa[2]==33:
                                logger.debug('CONSUNTIVAZIONI BOTTICELLA')
                                # lavaggio con botticella devo cercare le componenti per quell'asta percorso
                                select_componenti='''select id_elemento from (   
                                                select id_asta_percorso, id_elemento from elem.elementi_aste_percorso eap 
                                                union 
                                                select id_asta_percorso, id_elemento from history.elementi_aste_percorso eap 
                                            ) as eap where id_asta_percorso::int = %s'''
                                try:
                                    curr1.execute(select_componenti, (int(aa[3]),))
                                    componenti=curr1.fetchall()
                                except Exception as e:
                                    logger.error(int(aa[3]))
                                    logger.error(select_componenti)
                                    logger.error(e)
                                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                                    exit()
                                for cc in componenti:
                                    cod_percorso.append(vv[1])
                                    data_percorso.append(vv[2].strftime("%Y%m%d"))
                                    id_turno.append(aa[1])
                                    id_componente.append(cc[0])
                                    id_tratto.append(None)
                                    flag_esecuzione.append(vv[5])
                                    causale.append(vv[7])
                                    nota_causale.append(vv[8])
                                    sorgente_dati.append(vv[9])
                                    data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                                    lat.append(None)
                                    long.append(None)
                                    ripasso.append(None)
                                    qual.append(vv[12])
                                    mail_arr.append(vv[13])                                 
                            else:
                                cod_percorso.append(vv[1])
                                data_percorso.append(vv[2].strftime("%Y%m%d"))
                                id_turno.append(aa[1])
                                id_componente.append(None)
                                id_tratto.append(aa[0])
                                flag_esecuzione.append(vv[5])
                                causale.append(vv[7])
                                nota_causale.append(vv[8])
                                sorgente_dati.append(vv[9])
                                data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                                lat.append(None)
                                long.append(None)
                                ripasso.append(aa[4])
                                qual.append(vv[12])
                                mail_arr.append(vv[13])   
                    
            elif len(letture['schede_lavoro']) > 0 : 
                
                
                id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                
                
                ss=0
                while ss < len(letture['schede_lavoro']):
                                                          
                    id_scheda=letture['schede_lavoro'][ss]['id_scheda_lav']
                    
                    # CONTROLLO SUI TURNI 
                    try:
                        if letture['schede_lavoro'][0]['cod_turno_ext'] is None or letture['schede_lavoro'][0]['cod_turno_ext'] =='':
                            logger.warning('Anomalia. Nessun turno su Ekovision per il percorso {0}. Scheda di lavoro {1} del {2}. Turno UO ={3}'.format(cod_percorsi_distinct[k], id_scheda, date_distinct[k], turno_distinct[k]))
                        else: 
                            id_turno_ekovision=int(letture['schede_lavoro'][0]['cod_turno_ext'])
                            #logger.info(id_scheda)
                            if id_turno_ekovision != int(turno_distinct[k]):
                                logger.warning('Anomalia turni per percorso {0}. Scheda di lavoro {1} del {2}. Turno UO ={3}, Turno Ekovision={4}'.format(cod_percorsi_distinct[k], id_scheda, date_distinct[k], turno_distinct[k], id_turno_ekovision))
                                warning_log_mail(logfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
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
                        #logger.info(f'id causale eko: {id_causale_eko}')
                        #recupero id_amiu e descrizione
                        curr2 = connc.cursor()
                        try:
                            curr2.execute(query_causale, (int(id_causale_eko),))
                            row_result=curr2.fetchone()
                            id_amiu= row_result[0]
                            descrizione_causale=row_result[1]
                        except Exception as e:
                            logger.error(e)
                            descrizione_causale='CAUSALE EKOVISION NON TROVATA CONTROLLARE LE TABELLE SU HUB'
                            id_amiu=0
                        #logger.info(f'id amiu: {id_amiu}')
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
                            testo_warning=f'''La scheda {id_scheda} di SPAZZAMENTO/LAVAGGIO (cod_percorso = {cod_percorsi_distinct[k]} {testo_percorso} del {date_distinct[k]}) risulta non effettuata 
                            <br>({id_causale_eko} - {descrizione_causale} - Note: {testo_causale_eko}). 
                            <br><b>Le consuntivazioni arrivate da totem saranno ignorate. <font color="red">Su totem ci sono delle differenze di consuntivazione</font>.
                            Nel caso in cui il servizio sia stato effettuato correggere i dati su Ekovision</b>'''
                            logger.warning(testo_warning)
                            warning_message_mail(f'MAIL CHE PER ORA ARRIVA SOLO A NOI, a regime al territorio {mail_arr_distinct[k]}<br><br>{testo_warning}', 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'ATTENZIONE - consuntivazioni totem scheda non effettuata')
                        else: 
                            testo_warning=f'''La scheda {id_scheda} di SPAZZAMENTO/LAVAGGIO (cod_percorso = {cod_percorsi_distinct[k]} del {date_distinct[k]}) risulta non effettuata 
                            <br>({id_causale_eko} - {descrizione_causale} - Note: {testo_causale_eko}). 
                            <br><b>Le consuntivazioni arrivate da totem saranno ignorate.</b> <font color="green">Tutto è congruente</font>'''
                            logger.warning(testo_warning)
                            warning_message_mail(f'MAIL CHE PER ORA ARRIVA SOLO A NOI, a regime al territorio {mail_arr_distinct[k]}<br><br>{testo_warning}', 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, 'ATTENZIONE - consuntivazioni totem scheda non effettuata')

                    # VADO ALLA SCHEDA SEGUENTE (stesso codice percorso)
                    ss+=1
    
        k+=1
        conn.commit() 

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
        nome_csv_ekovision="consuntivazioni_spazzamento_{0}.csv".format(giorno_file)
        file_preconsuntivazioni_ekovision="{0}/consuntivazioni/{1}".format(path,nome_csv_ekovision)
        fp = open(file_preconsuntivazioni_ekovision, 'w', encoding='utf-8')
                      
        fieldnames = ['cod_percorso', 'data', 'id_turno', 'id_componente','id_tratto',
                        'flag_esecuzione', 'causale', 'nota_causale', 'sorgente_dati', 'data_ora', 'lat', 'long', 'ripasso', 'qual' ]
      
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
                        flag_esecuzione[k], causale[k], nota_causale[k], sorgente_dati[k], data_ora[k], lat[k], long[k], ripasso[k], qual[k]]
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



    logger.info('Invio file con la consuntivazione spazzamento via SFTP')
    try: 
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(host=url_ev_sftp, username=user_ev_sftp,
    password=pwd_ev_sftp, port= port_ev_sftp,  cnopts=cnopts,
    log="/tmp/pysftp.log")

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
    
    
    if check_ekovision==200 and len(lista_x_via)>0:
        """insert_max_id='''INSERT INTO spazzamento.invio_consuntivazioni_ekovision
        (max_id, data_ora, datalav)
        VALUES
        (%s, now(),to_date(%s,'YYYYMMDD'))'''
        """
        """
        insert_max_id='''INSERT INTO spazzamento.invio_consuntivazioni_ekovision
        (max_id, data_ora)
        VALUES
        (%s, now())'''
        try:
            #currc.execute(insert_max_id, (max_id,max_datalav,))
            currc.execute(insert_max_id, (max_id,))
            connc.commit()
        except Exception as e: 
            logger.error(insert_max_id)
            logger.error(max_id)
            #logger.error(max_datalav)
            logger.error(e)
        
        """
        
         # prima di tutto inserisco i dati recuperati con Totem Wingsoft
        insert_max_id='''INSERT INTO spazzamento.invio_consuntivazioni_ekovision
        (max_id, data_ora)
        VALUES
        ((select max(substr(e.id,3)::int ) from spazzamento.v_effettuati e 
	where substr(e.id,1,1) = 'w'), now())'''
        try:
            #currc.execute(insert_max_id, (max_id,max_datalav,))
            currc.execute(insert_max_id)
            connc.commit()
        except Exception as e: 
            logger.error(insert_max_id)
            logger.error(e)
            
            
        # quindi inserisco i dati recuperati con Totem Amiu
        insert_max_id='''INSERT INTO spazzamento.invio_consuntivazioni_a_ekovision
        (max_id, data_ora)
        VALUES
        ((select max(substr(e.id,3)::int ) from spazzamento.v_effettuati e 
	where substr(e.id,1,1) = 'a'), now())'''
        try:
            #currc.execute(insert_max_id, (max_id,max_datalav,))
            currc.execute(insert_max_id)
            connc.commit()
        except Exception as e: 
            logger.error(insert_max_id)
            logger.error(e)  
        
        
         
           
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      