#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2023
# Roberto Marzocchi

'''
Controlla i servizi disattivati nell'ultima settimana e crea un elenco delle schede da cancellare a mano 


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

from preconsuntivazione import tappa_prevista

import requests
from requests.exceptions import HTTPError

import json

import logging

#path=os.path.dirname(sys.argv[0]) 



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





def main():
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    oggi=oggi.replace(hour=0, minute=0, second=0, microsecond=0)
    oggi=date(oggi.year, oggi.month, oggi.day)
    logging.debug('Oggi {}'.format(oggi))
    
    num_giorno=datetime.today().weekday()
    giorno=datetime.today().strftime('%A')
    logging.debug('Il giorno della settimana è {} o meglio {}'.format(num_giorno, giorno))

    start_week = date.today() - timedelta(days=datetime.today().weekday())
    logging.debug('Il primo giorno della settimana è {} '.format(start_week))
    
    
    # Mi connetto a SIT (PostgreSQL) per poi recuperare le mail
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)


    curr = conn.cursor()
    
    
    


     # cerco le schede su ekovision
        # PARAMETRI GENERALI WS
    
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Cache-Control': 'no-cache'}

    data_json={'user': eko_user, 
        'password': eko_pass,
        'o2asp' :  eko_o2asp
        }
    
    percorsi_giorni_creare=''
    

    data_inizio = num_giorno+8    
    data_fine = 13 - num_giorno # 6 di questa settimana e 7 per la prossima
    
    logger.debug ('Data inizio: {}, Data fine:{}'.format(data_inizio, data_fine))
    #exit()
    
    query="""select a.cod_percorso, vspe.descrizione, vspe.data_inizio_validita::date, vspe.data_fine_validita::date, 
    fo.freq_binaria, vspe.id_turno 
from (select cod_percorso, max(versione) as mv 
	from anagrafe_percorsi.v_servizi_per_ekovision vspe 
	group by cod_percorso) a
join anagrafe_percorsi.v_servizi_per_ekovision vspe on vspe.cod_percorso= a.cod_percorso and a.mv= vspe.versione
join etl.frequenze_ok fo on fo.cod_frequenza = vspe.freq_testata 
where data_fine_validita > now()::date 
and data_inizio_validita >= (now()::date - interval '%s' day)
and data_inizio_validita <  (now()::date + interval '%s' day)
and (select distinct cod_percorso from anagrafe_percorsi.v_servizi_per_ekovision vspe2 
		where vspe2.cod_percorso = a.cod_percorso 
		and vspe2.data_fine_validita >= (now()::date - interval '%s' day )
		and vspe2.data_fine_validita <  (now()::date + interval '%s' day) ) is null
order by data_inizio_validita"""
    testo_mail=''
    
    try:
        #cur.execute(query, (new_freq, id_servizio, new_freq))
        curr.execute(query, (data_inizio,data_fine,data_inizio,data_fine))
        lista_variazioni=curr.fetchall()
    except Exception as e:
        check_error=1
        logger.error(e)

    percorso_con_problemi=[]
           
    for vv in lista_variazioni:
        check_error=0
        #logger.debug(vv[0])
        
        #logger.debug(oggi)
        #logger.debug(vv[1])
        
        gg_indietro=oggi-vv[2]
        
        #logger.debug(gg_indietro.days)
        #exit()
        gg=-gg_indietro.days
        
        while gg <= data_fine: #6-datetime.today().weekday():
            day_check=oggi + timedelta(gg)
            day= day_check.strftime('%Y%m%d')
            
            if tappa_prevista(day_check, vv[4])==1:
                #logger.debug(day)
                # se il percorso è previsto in quel giorno controllo che ci sia la scheda di lavoro corrispondente
                
                #####################################
                # METTERE QUA IL CONTROLLO
                
                
                params={'obj':'schede_lavoro',
                    'act' : 'r',
                    'sch_lav_data': day,
                    'cod_modello_srv': vv[0],
                    'flg_includi_eseguite': 1,
                    'flg_includi_chiuse': 1
                    }
                try:
                    #requests.Cache.remove(eko_url)
                    response = requests.post(eko_url, headers=headers, params=params, data=data_json)
                except Exception as err:
                    logger.error(f'Errore in connessione: {err}')
                    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                    logger.info("chiudo le connessioni in maniera definitiva")
                    curr.close()
                    conn.close()
                    exit()
                #response.json()
                #logger.debug(response.status_code)
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
                    if len(letture['schede_lavoro']) > 0 : 
                        id_scheda=letture['schede_lavoro'][0]['id_scheda_lav']
                        #logger.info('Id_scheda:{}'.format(id_scheda))
                    else:
                        percorso_con_problemi.append(vv[0])
                        if percorsi_giorni_creare=='':
                            percorsi_giorni_creare='{} - {}'.format(vv[0], day)
                        else:
                            percorsi_giorni_creare='{}, {} - {}'.format(percorsi_giorni_creare,vv[0], day)
                            
                            
                            
                            
                        curr.close()
                        logger.info('Chiusura e Ri-Connessione al db {}'.format(nome_db))
                        conn.close()
                        
                        conn = psycopg2.connect(dbname=nome_db,
                                            port=port,
                                            user=user,
                                            password=pwd,
                                            host=host)
                        curr = conn.cursor()
                        
                        query_select_ruid='''select lpad((max(id)+1)::text, 7,'0') 
                        from anagrafe_percorsi.creazione_schede_lavoro csl'''
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
                                        "data_srv": day,
                                        "cod_modello_srv": vv[0],
                                        "cod_turno_ext": int(vv[5])
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
                            #logger.info(letture2)
                            check_creazione_scheda=0
                            id_scheda=letture2['crea_schede_lavoro'][0]['id']
                            logger.info('ID scheda creata {}'.format(id_scheda))
                            percorsi_giorni_creare='{} - Id scheda: {}'.format(percorsi_giorni_creare, id_scheda)
                            check_creazione_scheda=1
                        except Exception as e:
                            logger.error(e)
                            logger.error(' - id: {}'.format(ruid))
                            logger.error(' - Cod_percorso: {}'.format(vv[0]))
                            logger.error(' - Data: {}'.format(day))
                            #logger.error('Id Scheda: {}'.format(id_scheda[k]))
                            # check se c_handller contiene almeno una riga 
                            error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
                            logger.info("chiudo le connessioni in maniera definitiva")
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
                                curr.execute(query_insert, (int(ruid),vv[0], day, id_scheda, check_creazione_scheda))
                            else:
                                curr.execute(query_insert, (int(ruid),vv[0], day, check_creazione_scheda))
                        except Exception as e:
                            logger.error(query_insert)
                            logger.error(e)
                        conn.commit()
            else:
                logger.debug('Percorso {} non previsto il giorno {}'.format(vv[0], day))
            gg+=1 
     

    
    
    k=0
    percorso_con_problemi_distinct=[]
    while k<len(percorso_con_problemi):
        #logger.debug(k)
        if k==0:
            percorso_con_problemi_distinct.append(percorso_con_problemi[k])
            elenco_codici='{0}'.format(percorso_con_problemi[k])
        if k > 0 and percorso_con_problemi[k]!= percorso_con_problemi[k-1]:
            percorso_con_problemi_distinct.append(percorso_con_problemi[k])
            elenco_codici='{0} - {1}'.format(elenco_codici, percorso_con_problemi[k])
        k+=1
    
    
    # provo a mandare la mail
    try:
        if percorsi_giorni_creare!='':
            # Create a secure SSL context
            context = ssl.create_default_context()



        # messaggio='Test invio messaggio'


            subject = "CREAZIONE SCHEDE LAVORO - Percorsi creati sulla UO per cui va creata la scheda di lavoro"
            
            ##sender_email = user_mail
            receiver_email='assterritorio@amiu.genova.it'
            debug_email='roberto.marzocchi@amiu.genova.it'

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = sender_email
            message["To"] = receiver_email
            message["Subject"] = subject
            #message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Creazione schede di lavoro"


            body='''I seguenti percorsi sono stati attivati recentemente e sono privi di schede di lavoro in queste settimane.<br>
            {0}
            <br><br>
            Sono state create <b>automaticamente</b> le schede di lavoro su Ekovision. 
            Verificare il log e controllare a mano eventuali anomalie.
            <br><br>
            Elenco giorni creati: <br>
            {1}
            <br><br>
            AMIU Assistenza Territorio<br>
            <img src="cid:image1" alt="Logo" width=197>
            <br>'''.format(elenco_codici, percorsi_giorni_creare)
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))


            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(path1)
            immagine(message,logoname)
            
            

            
            
            text = message.as_string()

            logger.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            logger.info(invio)
    except Exception as e:
        logger.error(e) # se non fossi riuscito a mandare la mail
    
    
    
    
    
    
    
    
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()




if __name__ == "__main__":
    main()      