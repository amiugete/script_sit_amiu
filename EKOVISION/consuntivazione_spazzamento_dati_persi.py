#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione spazzamento persi su EKOVISION per baco (ticket 6465)
bisogna passare come parametri:
- id_scheda_input
- codice_percorso_input
- data_percorso_input (YYYYMMDD)



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

from tappa_prevista import tappa_prevista

from descrizione_percorso import *  
    
     

def main(id_scheda_input, codice_percorso_input, data_percorso_input, folder_output):
      
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



    curr = conn.cursor()
    
    
    # prima faccio un giro di pre-consuntivazione per le giornate mancant
    query_spazz_np= '''
    select p.cod_percorso, 
p.id_turno, 
ap.id_asta, 
fo.freq_binaria as freq_asta, 
fo2.freq_binaria  as freq_percorso, 
fo3.freq_binaria  as differenza, 
p.data_attivazione::date, 
p.data_dismissione::date, 
coalesce(ap.ripasso_fittizio,0) as ripasso_fittizio               
from (select num_seq, id_asta_percorso, id_percorso, id_asta, data_inserimento, frequenza, ripasso_fittizio from elem.aste_percorso ap 
union 
select num_seq, id_asta_percorso, id_percorso, id_asta, data_inserimento, frequenza, 0 as ripasso_fittizio  from history.aste_percorso ap)
ap  
join elem.percorsi p on p.id_percorso = ap.id_percorso 
join anagrafe_percorsi.date_percorsi_sit_uo dpsu on dpsu.id_percorso_sit = p.id_percorso 
join etl.frequenze_ok fo on fo.cod_frequenza = ap.frequenza
join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-ap.frequenza)
join elem.servizi s on s.id_servizio = p.id_servizio
where p.cod_percorso = %s
and to_date(%s, 'YYYYMMDD') between dpsu.data_inizio_validita and dpsu.data_fine_validita 
and ap.frequenza is not null 
and ap.frequenza <> p.frequenza
and s.riempimento = 0
order by p.cod_percorso, ap.num_seq'''
    
    
    query_spazz= '''
    select p.cod_percorso, 
p.id_turno, 
ap.id_asta, 
fo.freq_binaria as freq_asta, 
fo2.freq_binaria  as freq_percorso, 
fo3.freq_binaria  as differenza, 
p.data_attivazione::date, 
p.data_dismissione::date, 
coalesce(ap.ripasso_fittizio,0) as ripasso_fittizio               
from (select num_seq, id_asta_percorso, id_percorso, id_asta, data_inserimento, frequenza, ripasso_fittizio from elem.aste_percorso ap 
union 
select num_seq, id_asta_percorso, id_percorso, id_asta, data_inserimento, frequenza, 0 as ripasso_fittizio  from history.aste_percorso ap)
ap  
join elem.percorsi p on p.id_percorso = ap.id_percorso 
join anagrafe_percorsi.date_percorsi_sit_uo dpsu on dpsu.id_percorso_sit = p.id_percorso 
join etl.frequenze_ok fo on fo.cod_frequenza = ap.frequenza
join etl.frequenze_ok fo2 on fo2.cod_frequenza = p.frequenza
left join etl.frequenze_ok fo3 on fo3.cod_frequenza = (p.frequenza-ap.frequenza)
join elem.servizi s on s.id_servizio = p.id_servizio
where p.cod_percorso = %s
and to_date(%s, 'YYYYMMDD') between dpsu.data_inizio_validita and dpsu.data_fine_validita 
and s.riempimento = 0
order by p.cod_percorso, ap.num_seq'''
    
    
    day=datetime.strptime(data_percorso_input, '%Y%m%d').date()            
    try:
        curr.execute(query_spazz_np, (codice_percorso_input, data_percorso_input,))
        lista_aste=curr.fetchall() # sono solo le aste per calcolare non previste
    except Exception as e:
        check_error=1
        logger.error(e)
    
    
    try:
        curr.execute(query_spazz, (codice_percorso_input, data_percorso_input,))
        lista_aste_tot=curr.fetchall() # tutte le aste di quel percorso
    except Exception as e:
        check_error=1
        logger.error(e)
    
    for aa in lista_aste:
        if (tappa_prevista(day, aa[4])==1 # frequenza percorso
                and tappa_prevista(day, aa[3])==-1 # frequenza asta
                and aa[6] <= day # data attivazione
                and (aa[7] is None or aa[7] > day) # data dismissione
                ):
                cod_percorso.append(aa[0])
                data_percorso.append(day.strftime("%Y%m%d"))
                id_turno.append(aa[1])
                id_componente.append(None)
                id_tratto.append(aa[2])
                flag_esecuzione.append(2)
                causale.append(999)
                nota_causale.append(None)
                sorgente_dati.append('Come da progettazione su SIT')
                data_ora.append(day.strftime("%Y%m%d%H%M"))
                lat.append(None)
                long.append(None)
                ripasso.append(aa[8])
                qual.append(0)
                mail_arr.append(None)
    

    curr.close()
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
		when e.punteggio::int > 0 and e.punteggio::int < 100 then concat('Svolto al ', e.punteggio, %s, ct.id ,' - ',  e.causale)
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
	t.id_percorso = %s
	and datalav = to_date(%s, 'YYYYMMDD')
    and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
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
		when e.punteggio::int > 0 and e.punteggio::int < 100 then concat('Svolto al ', e.punteggio, %s , ct.id ,' - ',  e.causale)
	end , 
	concat('TOTEM Badge ', e.codice, ' - Matr. ', vpes.matricola::text, ' - ', vpes.cognome, ' ', vpes.nome), 
	e.datainsert, 
    e.tappa, 
    e.punteggio
	order by 1 limit 5000'''
    
    
    curr = conn.cursor()
    curr1 = conn.cursor()
    currc = connc.cursor()
    currc1 = connc.cursor()
                
    try:
        currc.execute(query_effettuati_totem, ('% CAUSALE: ', codice_percorso_input, data_percorso_input, '% CAUSALE: ',))
        lista_x_via=currc.fetchall()
    except Exception as e:
        logger.error(query_effettuati_totem)
        logger.error('Codice percorso = {}'.format(codice_percorso_input))
        logger.error('Data rif = {}'.format(data_percorso_input))
        logger.error(e)


    logger.info('Trovate {} tappe da consuntivare su Ekovision'.format(len(lista_x_via)))
    if len(lista_x_via) == 0:
        logger.info('Nessuna tappa consuntivata su Totem, posso solo dare le tappe previste come fatte')
        for aa in lista_aste_tot:
            if (tappa_prevista(day, aa[4])==1 # frequenza percorso
                and tappa_prevista(day, aa[3])==1 # frequenza asta
                and aa[6] <= day # data attivazione
                and (aa[7] is None or aa[7] > day) # data dismissione
                ):
                cod_percorso.append(aa[0])
                data_percorso.append(day.strftime("%Y%m%d"))
                id_turno.append(aa[1])
                id_componente.append(None)
                id_tratto.append(aa[2])
                flag_esecuzione.append(1) # fatto
                causale.append(100)
                nota_causale.append(None)
                sorgente_dati.append('Non consuntivato su Totem, prendiamo per buona progettazione su SIT')
                data_ora.append(None)
                lat.append(None)
                long.append(None)
                ripasso.append(aa[8])
                qual.append(100) # fatto con qualità 100
                mail_arr.append(None)
    
    
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
    
    
    
    
    
    
    try:    
        nome_csv_ekovision="consuntivazioni_spazzamento_scheda_{0}.csv".format(id_scheda_input)
        file_preconsuntivazioni_ekovision="{0}/consuntivazioni/{2}/{1}".format(path,nome_csv_ekovision, folder_output)
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


    logger.info('File con la consuntivazione spazzamento creato correttamente: {}'.format(file_preconsuntivazioni_ekovision))
    
    

    
        
         
           
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    #main('544611',	'0201245001',	'20250314') # consuntivazione su totem presente
    #main('478967',	'0201011001',	'20250103') # consuntivazione su totem assente
    
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]
    # Call main function
    main(arg1, arg2, arg3, arg4)