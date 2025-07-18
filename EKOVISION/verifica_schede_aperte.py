#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Lo script si occupa di verificare se ci sono ancora delle schede non eseguite e/o non chiuse per un determinato mese

'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

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


filename = inspect.getframeinfo(inspect.currentframe()).filename
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

# libreria per scrivere file csv
import csv



    
     

def main():
      


    

    
    
    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    #logging.debug('Oggi {}'.format(oggi))
    
    mese_anno_oggi=oggi.strftime('%Y%m')
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    
    check=0
    
    chiusura_ok = 0 # se rimane 0 vuole dire che è tutto chiuso
    
    # cerco il primo mese da controllare
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    # cerco mese e anno da analizzare
    query_mese_anno='''select *,  
    anno::text||lpad(mese::text,2,'0')::text
    from etl.ekovision_chiusura_schede ecs 
where anno::text||lpad(mese::text,2,'0')::text = 
(select max(anno::text||lpad(mese::text,2,'0')::text) from  etl.ekovision_chiusura_schede)'''

    try:
        curr.execute(query_mese_anno)
        mese_anno=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(query_mese_anno)
        logger.error(e)
    
    
    for m_a in mese_anno:
        anno=int(m_a[1])
        mese=int(m_a[2])
        mese_anno_eko =m_a[3]
    
    if mese_anno_eko == mese_anno_oggi: 
        logger.info('Non devo fare nessuna verifica. Tutti i mesi precedenti sono chiusi')
        exit()
    elif oggi.day<=5:
        logger.info('Inizio le verifiche dal 6 del mese successivo')
        exit()
    else: 
        logger.info('Procedo con le verifiche')
     
    #logger.debug(mese)
    #logger.debug(anno)
    #logger.debug(oggi.day)
    #exit()
    curr.close()
    
    
    # consider the start date as 2021-february 1 st
    start_date = date(anno, mese, 1)

    locale.setlocale(locale.LC_ALL, "") # prendo la lingua del server

    mese_mail=start_date.strftime('%B')
    
    logger.debug(mese_mail)
    #exit()
    
    if mese ==12:
        # consider the end date as 2021-march 1 st
        end_date = date(anno+1, 1, 1)
    else:
        end_date = date(anno, mese+1, 1)

    # delta time
    delta = timedelta(days=1)

    # iterate over range of dates
    data_mese=start_date
    
    data_ne=[]
    data_nc=[]
    
    id_scheda_ne=[]
    id_scheda_nc=[]
    
    servizio_ne=[]
    servizio_nc=[]
    
    cod_servizio_ne=[]
    cod_servizio_nc=[]
    
    while data_mese < end_date:
        data_ws=data_mese.strftime('%Y%m%d')
        logger.info(data_ws)
        data_mese += delta
    
    
    

        # provo il WS solo con la data 
        params={'obj':'schede_lavoro',
            'act' : 'r',
            'sch_lav_data': data_ws,
            'flg_includi_eseguite': 1,
            'flg_includi_chiuse': 1
            }
        response = requests.post(eko_url, params=params, data=data, headers=headers)
        #response.json()
        #logger.debug(response.status_code)
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
            #logger.info(len(letture['schede_lavoro']))
            
            # leggo tutte le schede di quel giorno
            ss=0
            while ss < len(letture['schede_lavoro']):
             
                if int(letture['schede_lavoro'][ss]['flg_eseguito'])==0:
                    data_ne.append(data_ws)
                    id_scheda_ne.append(letture['schede_lavoro'][ss]['id_scheda_lav'])                  
                    servizio_ne.append(letture['schede_lavoro'][ss]['descr_scheda_lav'])                  
                    cod_servizio_ne.append(letture['schede_lavoro'][ss]['cod_serv_pred'])

                if int(letture['schede_lavoro'][ss]['flg_chiuso'])==0:
                    data_nc.append(data_ws)
                    id_scheda_nc.append(letture['schede_lavoro'][ss]['id_scheda_lav'])                  
                    servizio_nc.append(letture['schede_lavoro'][ss]['descr_scheda_lav'])                  
                    cod_servizio_nc.append(letture['schede_lavoro'][ss]['cod_serv_pred'])
                
                
                ss+=1


    
    # ora devo processare gli array
    curr = conn.cursor()
    
    # seleziono tutti i percorsi con frequenze quindicinali

    
    query_ut="""SELECT pu.cod_percorso,
        cmu1.id_uo_sit, 
        u1.id_zona
        /*u1.descrizione as ut,
        u1.mail as mail_uts,
        za1.cod_zona as zona,
        za1.mail as mail_zona*/
        FROM anagrafe_percorsi.percorsi_ut pu 
        left join anagrafe_percorsi.cons_mapping_uo cmu1 on cmu1.id_uo = pu.id_ut 
        left join topo.ut u1 on u1.id_ut = cmu1.id_uo_sit
        /*left join topo.zone_amiu za1 on u1.id_zona = za1.id_zona*/
        where pu.cod_percorso = %s
        and to_date(%s, 'YYYYMMDD') between pu.data_attivazione and pu.data_disattivazione and id_squadra != 15"""
    
    
    #testo_mail=''
    
    
    
    # inzio dalle Schede Non Eseuguite
    
    
    ut_ne=[]
    zone_ne=[]   
    sne=0
    while sne<len(id_scheda_ne):
        check_rimessa=0 # controllo percorsi su UT e rimessa
        try:
            curr.execute(query_ut, (cod_servizio_ne[sne], data_ne[sne]))
            lista_ut_ne=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(e)

        if len(lista_ut_ne) == 0:
            logger.error(query_ut)
            logger.error(cod_servizio_ne[sne])
            logger.error(data_ne[sne])   
            exit() 
        if len(lista_ut_ne) > 1:
            check_rimessa=1
            #exit()
        for une in lista_ut_ne:
            if check_rimessa == 1: # in questo caso salvo solo la rimessa e non l'UT
                if une[2] == 5:
                    ut_ne.append((une[1]))
                    zone_ne.append((une[2]))
            else: 
                ut_ne.append((une[1]))
                zone_ne.append((une[2]))
        sne+=1
    
   
    curr.close()
   
    #zones = list(set(zone_ne))
    #logger.debug('zone con schede non eseguite: {}'.format(zones))
   
    uts = list(set(ut_ne))
    logger.info('UT con schede non eseguite: {}'.format(uts))
    
    
    
    #Invio le mail alle UT
    
    query_mail='''select id_ut, 
u.descrizione, 
coalesce(u.mail, 'assterritorio@amiu.genova.it') as mail_ut,
za.cod_zona as zona, 
coalesce(za.mail, 'assterritorio@amiu.genova.it') as mail_zona
from topo.ut u
join topo.zone_amiu za on za.id_zona = u.id_zona
where id_ut = %s'''
    
    
    curr = conn.cursor()
    
    logger.debug('ut_ne len: {}'.format(len(ut_ne)))
    logger.debug('id_scheda_ne len: {}'.format(len(id_scheda_ne)))
    uu = 0
    while uu < len(uts):
        
        logger.debug(uts[uu])
        messaggio_start = '''ALERT AUTOMATICO EKOVISION
    <br><br><font color="red">A {0} {1} ci sono {2} schede ancora da eseguire</font>. 
    <br><br> <b>Si ricorda che, ai fini della chiusura delle schede da parte dei capi zona, è necessario che tutte le schede siano <i>salvate come eseguite</i>.
Pertanto si richiede gentilmente di controllare le schede ancora aperte sotto elencate e salvarle come eseguite, indicando eventuali causali nel caso di servizio non effettuato.
</b>
<br><br>Di seguito l'elenco <ul>'''.format(mese_mail, anno, ut_ne.count(uts[uu]))
        messaggio_end = '</ul>'
        try:
            curr.execute(query_mail, (int(uts[uu]),))
            uts_ne=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_mail)
            logger.error(e)
            exit()
        # predispongo l'intestazione del messaggio
        for mune in uts_ne:
            #mune[]
            messaggio='UT: {0} (Zona: {1})<br><br>{2}'.format(mune[1], mune[3], messaggio_start)     
            subject = "{} Schede da eseguire di {} {}".format(mune[1], mese_mail, anno)
            mail_to=mune[2]
            mail_cc=mune[4] 
        sne=0
        while sne<len(id_scheda_ne):
            #logger.debug('sne= {}'.format(sne))
            #logger.debug('uu= {}'.format(uu))
            #logger.debug(ut_ne[sne])
            #logger.debug(uts[uu])
            if ut_ne[sne] == uts[uu]:
                messaggio='{0}<li>Data: {1} - {2} - id scheda: {3}</li>'.format(messaggio, data_ne[sne], servizio_ne[sne], id_scheda_ne[sne])
            sne+=1
        
        messaggio='{}'.format(messaggio,messaggio_end)
        
        
            
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'noreply@amiu.genova.it'
        message["To"] = mail_to #debug_email #mail_to
        message["CC"] = mail_cc #debug_email #mail_cc
        message["Bcc"] = receiver_email #debug_email #receiver_email
        #message["CCn"] = debug_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Schede di lavoro non eseguite "


        body='''{0}
        <br><br><hr>
        AMIU<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico. 
        In caso di dubbi contattare i vostri referenti'''.format(messaggio)
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        

        
        
        text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        uu+=1    
    
    
    
    curr.close()

    
    # Ora analizzo le schede non chiuse  
    # primo ciclo cerco ut e zone di ogni scheda non chiusa 
    curr = conn.cursor()
    ut_nc=[]
    zone_nc=[]   
    snc=0
    while snc<len(id_scheda_nc):
        try:
            curr.execute(query_ut, (cod_servizio_nc[snc], data_nc[snc]))
            lista_ut_nc=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(e)

            
        for unc in lista_ut_nc:
            ut_nc.append((unc[1]))
            zone_nc.append((unc[2]))
        snc+=1
    
   
    curr.close()
   
   
    #cerco i valori unici di zona
    zones = list(set(zone_nc))
    logger.debug('Zone con schede non chiuse: {}'.format(zones))
   

    
    # su queste faccio ciclo per inviare le mail alle Zone
    
    query_mail2='''select id_zona, cod_zona, mail 
from topo.zone_amiu za where id_zona = %s'''
    
    messaggio_start = '''ALERT AUTOMATICO EKOVISION<br><br> 
    <font color="red">A {0} {1} ci sono schede ancora da chiudere</font>.
    <br><br><b>Si ricorda che le schede eseguite ma non chiuse sono modificabili e ciò genera diverse problematiche ai fini dell'invio dati a Città Metropolitana ed ARERA.</b>
    <br><br>Le schede possono essere chiuse massivamente selezionando il periodo interessato e utilizzando il tasto <i>Azioni</i>.
    <br><br>Nel seguito l'elenco delle UT con schede aperte <ul>'''.format(mese_mail, anno)
    messaggio_end = '</ul>'
    curr = conn.cursor()
    
    # ciclo sulle zone
    zz = 0
    while zz < len(zones):
        
        logger.debug(zones[zz])
        # cerco la mail
        try:
            curr.execute(query_mail2, (int(zones[zz]),))
            zones_nc=curr.fetchall()
        except Exception as e:
            check_error=1
            logger.error(query_mail2)
            logger.error(e)
            exit()
        
        # predispongo l'intestazione del messaggio
        for munc in zones_nc:
            #mune[]
            messaggio='(Zona: {0})<br><br>{1}'.format(munc[1], messaggio_start)     
            subject = "{} Schede non chiuse di {} {}".format(munc[1], mese_mail, anno)
            mail_to=munc[2]
            if zones[zz]==5:
                mail_cc='rapettia@amiu.genova.it'
            else: 
                mail_cc='magni@amiu.genova.it, longo@amiu.genova.it'
        # su quella specifica zona cerco le UT
        snc=0
        ut_zona=[]
        while snc<len(id_scheda_nc):
            if zone_nc[snc] == zones[zz]:
                ut_zona.append(ut_nc[snc]) #
            snc+=1

        # filtro le UT di quella zona 
        
        curr1= conn.cursor()  
      
        ut_zona_distinct = list(set(ut_zona))

        # faccio un ciclo su quelle UT per scrivere il messaggio
        unc=0
        while unc <len(ut_zona_distinct): 
            try:
                curr1.execute(query_mail, (int(ut_zona_distinct[unc]),))
                uts=curr1.fetchall()
            except Exception as e:
                check_error=1
                logger.error(query_mail)
                logger.error(e)
                exit()
        
            for u in uts:
                ut_mail=u[1]
            
            
            messaggio='{0} <li>{2} schede non correttamente chiuse di {1}</li>'.format(messaggio, ut_mail, ut_nc.count(ut_zona_distinct[unc]))
        
        
            unc+=1



        curr1.close()    
            
        
        messaggio='{}'.format(messaggio,messaggio_end)
        
        
            
        ##sender_email = user_mail
        receiver_email='assterritorio@amiu.genova.it'
        debug_email='roberto.marzocchi@amiu.genova.it'

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'noreply@amiu.genova.it'
        message["To"] = mail_to #debug_email #mail_to
        message["CC"] = mail_cc #debug_email #mail_cc
        message["Bcc"] = receiver_email #debug_email
        #message["CCn"] = debug_email
        message["Subject"] = subject
        #message["Bcc"] = debug_email  # Recommended for mass emails
        message.preamble = "Schede di lavoro non eseguite "


        body='''{0}
        <br><br><hr>
        AMIU<br>
        <img src="cid:image1" alt="Logo" width=197>
        <br>Questa mail è stata creata in automatico. 
        In caso di dubbi contattare i vostri referenti'''.format(messaggio)
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))


        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(path1)
        immagine(message,logoname)
        
        

            
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        zz+=1 


    
    # controllo array per capire se aggiornare o meno il DB
    
    if len(data_ne) ==0 and len (data_nc)==0:
        if mese<=12:
            anno_new = anno
            mese_new=mese + 1
        elif mese==12: 
            anno_new = anno + 1
            mese_new = 1
            
        logger.info('Devo fare update mese anno')
        query_insert='''INSERT INTO 
        etl.ekovision_chiusura_schede 
        (data_last_update, anno, mese) 
        values 
        (now(), %s, %s) ON CONFLICT (anno, mese) DO NOTHING
        '''
        
        try:
            curr.execute(query_insert, (anno_new, mese_new))
        except Exception as e:
            logger.error(query_insert)
            logger.error(e)
        
        conn.commit()
        
        
     #logger.debug(versioni)
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()      