#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione raccolta di dati persi su EKOVISION



'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob

#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import requests
from requests.exceptions import HTTPError

import json


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

    
from tappa_prevista import tappa_prevista

     

def main(id_scheda_input, codice_percorso_input, data_percorso_input):
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))
  

    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()

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
    
    
    
    # prima faccio un giro di pre-consuntivazione per le giornate mancant
    query_racc_np='''select p.cod_percorso, 
    p.id_turno, 
eap.id_elemento, 
fo.freq_binaria as freq_elemento, 
fo2.freq_binaria  as freq_percorso, 
fo3.freq_binaria  as differenza,
eap.ripasso, 
dpsu.data_inizio_validita::date, 
dpsu.data_fine_validita::date 
--p.data_attivazione::date, 
--p.data_dismissione::date 
from (select id_asta_percorso, id_percorso,data_inserimento, frequenza from elem.aste_percorso ap 
union 
select id_asta_percorso, id_percorso,data_inserimento, frequenza  from history.aste_percorso ap)
ap 
join elem.percorsi p on p.id_percorso = ap.id_percorso 
join anagrafe_percorsi.date_percorsi_sit_uo dpsu on p.id_percorso = dpsu.id_percorso_sit 
join (select id_asta_percorso, 
id_elemento, frequenza, ripasso, data_inserimento, 
id_elemento_asta_percorso
from elem.elementi_aste_percorso  
union 
select id_asta_percorso, 
id_elemento, frequenza, ripasso, data_inserimento, 
id_elemento_asta_percorso
from history.elementi_aste_percorso ) eap on ap.id_asta_percorso = eap.id_asta_percorso 
join etl.frequenze_ok fo on fo.cod_frequenza = eap.frequenza::int
join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
join elem.servizi s on s.id_servizio = p.id_servizio 
left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-eap.frequenza::int)
where p.cod_percorso = %s
and to_date(%s, 'YYYYMMDD') between dpsu.data_inizio_validita and dpsu.data_fine_validita 
and ap.frequenza is not null 
and eap.frequenza::int <> p.frequenza and s.riempimento > 0  '''
    day=datetime.strptime(data_percorso_input, '%Y%m%d').date()            
    try:
        curr.execute(query_racc_np, (codice_percorso_input, data_percorso_input,))
        lista_elementi=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

    for aa in lista_elementi:
        
        #aa[3] frequenza asta 
        #aa[4] frequenza percorso
        #logger.debug(aa[3])
        #logger.debug(tappa_prevista(day, aa[3]))
        #logger.debug(aa[4])
        #logger.debug(tappa_prevista(day, aa[4]))
        if (tappa_prevista(day, aa[4])==1 
            and tappa_prevista(day, aa[3])==-1
            and aa[7] <= day # data attivazione
            and (aa[8] is None or aa[8] > day) # data dismissione
            ):
            cod_percorso.append(aa[0])
            data_percorso.append(day.strftime("%Y%m%d"))
            id_turno.append(aa[1])
            id_componente.append(aa[2])
            id_tratto.append(None)
            flag_esecuzione.append(2)
            causale.append(999)
            nota_causale.append('Pre-consuntivazione tappe non previste in giornata')
            sorgente_dati.append('SIT')
            data_ora.append(day.strftime("%Y%m%d%H%M"))
            lat.append(None)
            long.append(None)
            ripasso.append(aa[6]) 
            qual.append(None) 
            mail_arr.append(None)     

    
    
    
        
    
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
    
    curr.close()
    
    
    
    # Mi connetto anche al DB consuntivazione (PostgreSQL) - 
    nome_db=db_totem
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_totem,
                        password=pwd_totem,
                        host=host_totem)
    
    
    curr = conn.cursor()
    #curr1 = conn.cursor()
    currc = connc.cursor()
    currc1 = connc.cursor()
    
    
            
            
    # query per controllo causali
    query_causale='''select ct.id, ct.descrizione  
from totem.v_causali ct where id_ekovision = %s '''


    query_verifica_causale='''select ve.*, cpra.desc_percorso from raccolta.v_effettuati ve 
    left join raccolta.cons_percorsi_raccolta_amiu cpra 
on cpra.id_percorso = ve.idpercorso 
and ve.datalav between cpra.data_inizio and cpra.data_fine
where ve.idpercorso =%s  and ve.datalav = to_date(%s, 'YYYYMMDD') 
and ve.id_causale::int <> %s'''        
            
            

 
    
    # query che parte dal DB totem a partire da 27/11/2025
    
    query_effettuati_totem='''select  
	e.id, 
	t.id_percorso,
	e.datalav::date as datalav,
	t.id_piazzola, 
	t.num_elementi, 
    t.cronologia,
    e.fatto,
	string_agg(vc.descrizione, ', ') as descr_causale,
	e.id_causale as causale,
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) as sorgente_dati, 
	e.datainsert as data_insert, 
    t.id_tappa as tappa, 
    e.nota as note_totem /* è il campo con le note*/,
    string_agg(mu.mail, ', ') as mail
	from raccolta.cons_percorsi_raccolta_amiu t
	join raccolta.effettuati_amiu e on e.tappa::bigint =  t.id_tappa::bigint
	join totem.v_causali vc on vc.id = e.id_causale 
 	--left join raccolta.effettuati_correzione_date ecd on e.id_percorso=ecd.id_percorso and e.datalav= ecd.datalav_errata 
  	left join totem.v_personale_ekovision_step1 vpes on vpes.codice_badge::text = e.codice 
	--left join raccolta.causali_testi ct on trim(replace(e.causale, ' - (no in questa giornata)', '')) ilike trim(ct.descrizione)
    left join servizi.mail_ut mu on mu.id_uo::int  = t.id_uo::int
	where 
    t.id_percorso = %s
	and datalav = to_date(%s, 'YYYYMMDD')
    and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
	group by 
    e.id, 
	t.id_percorso,
	e.datalav::date ,
	t.id_piazzola, 
	t.num_elementi, 
    t.cronologia,
    e.fatto,
	--vc.descrizione as descr_causale,
	e.id_causale ,
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome) , 
	e.datainsert , 
    t.id_tappa, 
    e.nota
    order by 1'''
    
    
    
    
                
    try:
        currc.execute(query_effettuati_totem, (codice_percorso_input, data_percorso_input,))
        lista_x_piazzola=currc.fetchall()
    except Exception as e:
        logger.error(query_effettuati_totem)
        logger.error(e)


    logger.info('Trovo {} righe consuntivate'.format(len(lista_x_piazzola)))
    #exit()
    for vv in lista_x_piazzola:
    
    
    
        # controllo che il percorso sia su ekovision
        query_chek_percorso_eko='''select * from anagrafe_percorsi.elenco_percorsi ep
 where ep.ekovision is not true 
 and cod_percorso = %s
 and %s between ep.data_inizio_validita and ep.data_fine_validita 
        '''
        try:
            curr.execute(query_chek_percorso_eko, (vv[1], vv[2]))
            lista_percorsi_non_eko=curr.fetchall()
        except Exception as e:
            logger.error(query_chek_percorso_eko)
            logger.error(e)
        # temporanemente tolgo i percorsi non presenti su SIT lista not in 
    
        
        
        
        if vv[1] not in (  '0101355501',
                            '0101355901',
                            '0101356001',
                            '0508044303',
                            '0508043903',
                            '0507110703',
                            '0507130703',
                            '0500106403',
                            '0500106803',
                            '0501002001',
                            '0501002201',
                            '0501002301',
                            '0502005402', 
                            '0102005501',
                            '0111000301',
                            '0111000402',
                            '0111000501',
                            '0502006201',
                            '0507118001',
                            '0508051001'
                         ) and len(lista_percorsi_non_eko) == 0:
        
        
            # cerco ID_percorso_sit
            query_id_percorso='''select id_percorso_sit, p.id_turno 
            from anagrafe_percorsi.date_percorsi_sit_uo ep
            join elem.percorsi p on p.id_percorso = ep.id_percorso_sit 
            join elem.turni t on t.id_turno =p.id_turno  
                        where ep.id_percorso_sit is not null  
                        and ep.cod_percorso = %s 
                        and ep.data_inizio_validita <= %s 
                        and ep.data_fine_validita > %s'''
            
            try:
                curr.execute(query_id_percorso, (vv[1], vv[2], vv[2]))
                lista_percorso=curr.fetchall()
            except Exception as e:
                logger.error(query_id_percorso)
                logger.error(e)
                
            if len(lista_percorso)== 1:
                for lp in lista_percorso:
                    id_percorso_sit=lp[0]
                    turno_percorso=lp[1]
            else: 
                logger.error('Problema individuazione id_percorso_sit per percorso {}'.format(vv[1]))
                logger.error(query_id_percorso)
                logger.error('Codice percorso = {}'.format(vv[1]))
                logger.error('Data rif = {}'.format(vv[2]))
                
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
                
            #logger.debug(id_percorso_sit)
            #logger.debug(vv[3])
            #logger.debug(turno_percorso)
            
            # cerco il turno
            
            
            # per quella id_percorso / id_piazzola cerco le correspondenti aste su SIT
            query_elementi='''select  distinct eap1.id_elemento, ee.id_piazzola, eap1.ripasso, eap1.data_inserimento, eap1.data_eliminazione
                from (
                    select eap.id_elemento, eap.ripasso, eap.data_inserimento, null as data_eliminazione
                    from elem.elementi_aste_percorso eap 
                    where id_asta_percorso in  
                    (select  id_asta_percorso 
                    from elem.aste_percorso ap1 
                    where tipo= 'servizio' and id_percorso =%s
                    union 
                    select id_asta_percorso 
                    from history.aste_percorso ap2
                    where /*tipo= 'servizio' and*/ id_percorso =%s)
                    union 
                    select heap.id_elemento, heap.ripasso, heap.data_inserimento, heap.data_eliminazione
                    from history.elementi_aste_percorso heap
                    where id_asta_percorso in 
                    (select  id_asta_percorso 
                    from elem.aste_percorso ap1 
                    where /*tipo= 'servizio' and*/ id_percorso =%s
                    union 
                    select id_asta_percorso 
                    from history.aste_percorso ap2
                    where /*tipo= 'servizio' and*/ id_percorso =%s) 
                ) eap1
                join 
                (select id_elemento, id_piazzola  from elem.elementi e 
                union 
                select id_elemento, id_piazzola  from history.elementi e
                ) ee on ee.id_elemento = eap1.id_elemento     
                where id_piazzola= %s 
                and coalesce(data_eliminazione, '2099-12-31')::date >= %s and data_inserimento::date < %s
                '''
            
            try:
                curr.execute(query_elementi, (id_percorso_sit, id_percorso_sit,id_percorso_sit,id_percorso_sit,vv[3], vv[2], vv[2]))
                lista_elementi=curr.fetchall()
            except Exception as e:
                logger.error('NON TROVO GLI ELEMENTI  SUL SIT')
                logger.error(query_elementi)
                logger.error('Codice percorso = {}'.format(vv[1]))
                logger.error('Data rif = {}'.format(vv[2]))
                logger.error('Id percorso SIT = {}'.format(id_percorso_sit))
                logger.error('id_piazzola = {}'.format(vv[3]))
                logger.error(e)
                error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                exit()
            
            # vv[4] num_elementi
            # vv[6] num_elementi_fatti
            
            conteggio=0
            for aa in lista_elementi:
                #logger.debug(aa[0])       
                # controllo sulla consuntivazione pregressa
                
                if conteggio <vv[6]:
                    esec=1;
                    caus=100;
                else: 
                    esec=0;
                    caus=vv[8]
                
                # versione hub
                query_check='''select *  
                    from raccolta.effettuati_amiu e 
                    where id_percorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and id_tappa=%s
                    and id <> %s 
                    and left(codice,2) ilike 'ut'
                    '''
                # versione totem
                query_check='''select distinct id, tappa, idpercorso, 
                    string_agg(zona, ', ') as zona,
                    e.descr_tappa, 
                    id_causale,
                    causale,
                    e.datainsert, 
                    datalav, 
                    codice, 
                    fatto
                    from raccolta.v_effettuati e 
                    where idpercorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and tappa=%s
                    and id <> %s 
                    and left(codice,2) ilike 'ut'
                    group by id, tappa, idpercorso, 
                    descr_tappa, 
                    id_causale,
                    causale,
                    datainsert, 
                    datalav, 
                    codice, 
                    fatto 
                    '''
                
                try:
                    currc1.execute(query_check, (vv[1], vv[2].strftime('%Y-%m-%d'), int(vv[11]), int(vv[0])))
                    altre_consuntivazioni=currc1.fetchall()
                except Exception as e:
                    logger.error(query_check)
                    logger.error(vv[11])
                    logger.error('''{0} {1} {2} {3}'''.format(vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0])))
                    logger.error(e)
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it, pianar@amiu.genova.it', os.path.basename(__file__), logger)
                    exit()
                                    
                # se ci fosse un punteggio superiore o una consuntivazione del RUT (serve fino a quando il backoffice è di WingSOFT non servirà più dopo)
                # non passo i dati a ekovision
                if len(altre_consuntivazioni)>0:
                    logger.warning('''Tappa {} del {} già consuntivata con punteggio maggiore. Non passo il dato a Ekovision'''.format(vv[11], vv[2].strftime('%Y-%m-%d')))
                else: 
                    cod_percorso.append(vv[1])
                    data_percorso.append(vv[2].strftime("%Y%m%d"))
                    id_turno.append(turno_percorso)
                    id_componente.append(aa[0])
                    id_tratto.append(None)
                    flag_esecuzione.append(esec)
                    causale.append(caus)
                    nota_causale.append(vv[12])
                    sorgente_dati.append(vv[9])
                    data_ora.append(vv[10].strftime("%Y%m%d%H%M"))
                    lat.append(None)
                    long.append(None)
                    ripasso.append(aa[2])
                    qual.append(None) 
                    mail_arr.append(vv[13])                                

        
        
    
    
    
    
    
    
    
    if datetime.today() >= datetime.strptime('29/01/2024', '%d/%m/%Y'):
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
            nome_csv_ekovision="consuntivazioni_raccolta_scheda_{0}.csv".format(id_scheda_input)
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

        logger.info('File con la consuntivazione raccolta creato correttamente: {}'.format(file_preconsuntivazioni_ekovision))
        exit()
        logger.info('Invio file con la consuntivazione raccolta via SFTP')
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
        
        
            
        
           

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    #main('481241', '0507130502', '20250102')      
    main ('667780',	'0101363201',	'20250806')