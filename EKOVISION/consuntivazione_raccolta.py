#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa della consuntivazione spazzamento:



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
    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    
   
        
    
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


    nome_db=db_consuntivazione
    logger.info('Connessione al db {}'.format(nome_db))
    connc = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user_consuntivazione,
                        password=pwd_consuntivazione,
                        host=host_hub)
    
    curr = conn.cursor()
    curr1 = conn.cursor()
    currc = connc.cursor()
    
    
            
    # ciclo su elenco vie / note consuntivate
    query_effettuati_totem='''select distinct 
	e.id, 
	e.id_percorso,
	e.datalav::date ,
	t.id_piazzola, 
	t.num_elementi, 
    t.cronologia,
	e.fatto,
	trim(replace(e.causale, ' - (no in questa giornata)', '')) as descr_causale,
	ct.id as causale,
	concat('TOTEM Matricola ', e.codice) as sorgente_dati, 
	e.inser as data_insert, 
    e.id_tappa as tappa, 
    e.nota_via as note_totem /* è il campo con le note*/
	from raccolta.cons_percorsi_raccolta_amiu t
	join raccolta.effettuati_amiu e on e.id_tappa::bigint =  t.id_tappa::bigint
	left join raccolta.causali_testi ct on trim(replace(e.causale, ' - (no in questa giornata)', '')) ilike trim(ct.descrizione)
	where 
	trim(replace(e.causale, ' - (no in questa giornata)', '')) != '' 
	and datalav >= '2024-01-29'
    and codice not in ('1111', '2222', '3333', '4444', '8888', '9998', '9999')
    and e.id > (select coalesce(max(max_id),0) from raccolta.invio_consuntivazioni_ekovision ice)
	order by 1'''
    
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
            logger.error('''La causale {} non è riconosciuta. Andare sull'HUB ggiungere un id nella tabella raccolta.causali_testo'''.format(aa[1])) 
            error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
            exit()
    
    logger.info('CONTROLLO CAUSALI TERMINATO')
    currc.close()
    currc = connc.cursor()
    currc1 = connc.cursor()
                
    try:
        currc.execute(query_effettuati_totem)
        lista_x_piazzola=currc.fetchall()
    except Exception as e:
        logger.error(query_effettuati_totem)
        logger.error(e)


    
    for vv in lista_x_piazzola:
        
        # temporanemente tolgo i percorsi non presenti su SIT
        
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
                            '0501002301'
                         ):
        
        
            # cerco ID_percorso_sit
            query_id_percorso='''select id_percorso_sit, p.id_turno 
            from anagrafe_percorsi.date_percorsi_sit_uo ep
            join elem.percorsi p on p.id_percorso = ep.id_percorso_sit 
            join elem.turni t on t.id_turno =p.id_turno  
                        where ep.id_percorso_sit is not null  
                        and ep.cod_percorso = %s 
                        and ep.data_inizio_validita < %s 
                        and ep.data_fine_validita >= %s'''
            
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
                exit()
                
            #logger.debug(id_percorso_sit)
            #logger.debug(vv[3])
            #logger.debug(turno_percorso)
            
            # cerco il turno
            
            
            # per quella id_percorso / via / nota / data cerco le correspondenti aste su SIT
            query_elementi='''select  eap1.id_elemento, ee.id_piazzola, eap1.ripasso, eap1.data_inserimento, eap1.data_eliminazione
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
                    where tipo= 'servizio' and id_percorso =%s)
                    union 
                    select heap.id_elemento, heap.ripasso, heap.data_inserimento, heap.data_eliminazione
                    from history.elementi_aste_percorso heap
                    where id_asta_percorso in 
                    (select  id_asta_percorso 
                    from elem.aste_percorso ap1 
                    where tipo= 'servizio' and id_percorso =%s
                    union 
                    select id_asta_percorso 
                    from history.aste_percorso ap2
                    where tipo= 'servizio' and id_percorso =%s) 
                ) eap1
                join 
                (select id_elemento, id_piazzola  from elem.elementi e 
                union 
                select id_elemento, id_piazzola  from history.elementi e
                ) ee on ee.id_elemento = eap1.id_elemento     
                where id_piazzola= %s and coalesce(data_eliminazione, '2099-12-31') > %s
                '''
            
            try:
                curr.execute(query_elementi, (id_percorso_sit, id_percorso_sit,id_percorso_sit,id_percorso_sit,vv[3], vv[2]))
                lista_elementi=curr.fetchall()
            except Exception as e:
                logger.error('NON TROVO GLI ELEMENTI  SUL SIT')
                logger.error(query_elementi)
                logger.error('Codice percorso = {}'.format(vv[1]))
                logger.error('Data rif = {}'.format(vv[2]))
                logger.error('Id percorso SIT = {}'.format(id_percorso_sit))
                logger.error('id_piazzola = {}'.format(vv[3]))
                logger.error(e)
                error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
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
                
                
                query_check='''select *  
                    from raccolta.effettuati_amiu e 
                    where id_percorso = %s
                    and to_char(datalav, 'YYYY-MM-DD') = %s
                    and id_tappa=%s
                    and id <> %s 
                    and left(codice,2) ilike 'ut'
                    '''
                
                try:
                    currc1.execute(query_check, (vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0])))
                    altre_consuntivazioni=currc1.fetchall()
                except Exception as e:
                    logger.error(query_check)
                    logger.error(vv[11])
                    logger.error('''{0} {1} {2} {3}'''.format(vv[1], vv[2].strftime('%Y-%m-%d'), vv[11], int(vv[0])))
                    logger.error(e)
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

                    
        # mi salvo sempre il max_id    
        max_id=vv[0]
      
    
    
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    k=0
    cod_percorsi_distinct=[]
    date_distinct=[]
    turno_distinct=[]
    logger.debug(len(cod_percorso))
    while k<len(cod_percorso):
        logger.debug(k)
        if k==0:
            cod_percorsi_distinct.append(cod_percorso[k])
            date_distinct.append(data_percorso[k])
            turno_distinct.append(id_turno[k])
        if k > 0 and cod_percorso[k]!= cod_percorso[k-1]:
            cod_percorsi_distinct.append(cod_percorso[k])
            date_distinct.append(data_percorso[k])
            turno_distinct.append(id_turno[k])
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
            #logger.info(letture)
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
                    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
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
                        curr.execute(query_insert, (int(ruid),cod_percorsi_distinct[k], date_distinct[k], id_scheda, check_creazione_scheda))
                    else:
                        curr.execute(query_insert, (int(ruid),cod_percorsi_distinct[k], date_distinct[k], check_creazione_scheda))
                except Exception as e:
                    logger.error(query_insert)
                    logger.error(e)
                    
            """       
            elif len(letture['schede_lavoro']) > 0 : 
                id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                try:
                    id_turno_ekovision=int(letture['schede_lavoro'][0]['cod_turno_ext'])
                    logger.info(id_scheda)
                    if id_turno_ekovision != int(turno_distinct[k]):
                        logger.warning('Anomalia turni per percorso {0}. Scheda di lavoro {1} del {2}. Turno UO ={3}, Turno Ekovision={4}'.format(cod_percorsi_distinct[k], id_scheda, date_distinct[k], turno_distinct[k], id_turno_ekovision))
                        warning_log_mail(logfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
                except Exception as e:
                    logger.error(e)
                    logger.error(letture)
                    logger.error('Errore NON BLOCCANTE turni scheda {}'.format(id_scheda))
    
        k+=1
        #conn.commit() 
    
    
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
            nome_csv_ekovision="consuntivazioni_raccolta_{0}.csv".format(giorno_file)
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
        
        logger.info('Ri-connessione al db {}'.format(nome_db))
        connc = psycopg2.connect(dbname=nome_db,
                            port=port,
                            user=user_consuntivazione,
                            password=pwd_consuntivazione,
                            host=host_hub)
        currc = connc.cursor()
        
        
        if check_ekovision==200 and len(lista_x_piazzola)>0:
            insert_max_id='''INSERT INTO raccolta.invio_consuntivazioni_ekovision
            (max_id, data_ora)
            VALUES
            (%s, now())'''
            try:
                currc.execute(insert_max_id, (max_id,))
                connc.commit()
            except Exception as e: 
                logger.error(insert_max_id)
                logger.error(max_id)
                logger.error(e)
            
        
           

    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    
    currc.close()
    #currc1.close()
    connc.close()
    
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      