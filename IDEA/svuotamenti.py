#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script interroga il WS di IDEA che registra i conferimenti
'''


import os, sys, getopt, re  # ,shutil,glob
import requests
from requests.exceptions import HTTPError




import json


import inspect, os.path

import datetime


import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

sys.path.append('../')
from credenziali import *
from recupera_token import *

from footer_mail_idea import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
#path = os.path.dirname(os.path.abspath(filename))

path=os.path.dirname(sys.argv[0]) 
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)

'''logging.basicConfig(
    #handlers=[logging.FileHandler(filename=logfile, encoding='utf-8', mode='w')],
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    #filemode='w', # overwrite or append
    #fileencoding='utf-8',
    #filename=logfile,
    level=logging.DEBUG)
'''


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


# MAIL - libreria per invio mail
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
    #################################################################
    logger.info("Recupero il token")
    token1=token()
    logger.debug(token1)
    #################################################################
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
    #conn.autocommit = True
    #################################################################
    api_url='{}/svuotamenti'.format(url_idea)
    headers1 = {'''Authorization: Token {0}'''.format(token1)}
    

    #print(headers1)
    #exit()
    p=1
    check=0
    
    #giorno='{}000000'.format((datetime.datetime.today()-datetime.timedelta(days = 1)).strftime('%Y%m%d'))
    
    """
    giorno = '20221024000000'
    logger.debug("From date:{}".format(giorno))
    
    
    
    query_select='''select max(id_idea) from idea.svuotamenti ch'''
    
    try:
        curr.execute(query_select)
        max_id0=curr.fetchall()
    except Exception as e:
        logging.error(e)


    
    
    k=0       
    for ii in max_id0:
        max_id=ii[0] 

    if max_id is None:
        max_id=169376 #  parto dal 24 ottobre 2022

    #giorno='{}000000'.format((datetime.datetime.today()-datetime.timedelta(days = 11)).strftime('%Y%m%d'))
    #logger.debug("From date:{}".format(giorno))
    logger.info('from_id >= {}'.format(max_id))
    
    
    
    
    
    """
    
    query_select='''
        with a as (
            select 
            coalesce(max(modificato), to_date('20230101', 'YYYYMMDD')) as max_date
            from idea.svuotamenti s 
            union 
            select 
            coalesce(max(modificato), to_date('20230101', 'YYYYMMDD')) as max_date
            from idea.svuotamenti_altro s
        ) select max(max_date) from a    
        '''
    
    
    # temporaneamente 
    """query_select='''select 
        to_timestamp('20251203', 'YYYYMMDD')::timestamp,
        coalesce(max(modificato), to_date('20230101', 'YYYYMMDD'))
        from idea.svuotamenti s'''
    """
    
    try:
        curr.execute(query_select)
        max_date0=curr.fetchall()
    except Exception as e:
        logging.error(e)


    
    
    k=0       
    for ii in max_date0:
        max_date=ii[0]  
    
    logger.info(f'Max date ={max_date}')    
    #exit()
    
    while check<1:
        logger.info('Page index {}'.format(p))
        #response = requests.get(api_url, params={'date_from': giorno, 'page_size': 1000, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
        #response = requests.get(api_url, params={'id_from': max_id, 'page_size': 1000, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
        response = requests.get(api_url, params={'modified_from': max_date, 'page_size': 1000, 'page_index': p}, headers={'Authorization': 'Token {}'.format(token1)})
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
            if response.status_code == 500:
                logger.error('Errore 500. Avvisare tempestivamente Zamboni') 
            elif response.status_code == 502:
                logger.error('''Errore 502. E' andato in timeot il server. Il DB sembra messo male, se il problema persiste con le prossime chiamate avvisare Zamboni''')
            else:
                logger.error('''Non so che tipo di errore sia, se il problema persiste con le prossime chiamate avvisare Zamboni''')
            check=500
        except Exception as err:
            logger.error(f'Other error occurred: {err}')
            logger.error(response.json())
            check=500
        if check<1:
            letture = response.json()
            
            #print(letture)
            #exit()
            
            colonne=letture['meta']['columns']
            
            logger.debug(len(colonne))
            logger.debug(colonne)
            
            
            logger.debug('Lette {} righe dalle API'.format(len(letture['data'])))
            if len(letture['data'])>=3:
                logger.info('Id IDEA = {}'.format(letture['data'][2][0]))
            
            #exit()
            if len(letture['data'])==0:
                check=100
            i=0
            while i < len(letture['data']):
                # 0 id_idea
                #logger.debug(letture['data'][i][0])
                if int(letture['data'][i][0])>0:
                    #id_isola
                    id_idea=int(letture['data'][i][0])
                    id_pdr=letture['data'][i][1]
                    if letture['data'][i][4] is None:
                        lat = 0.0
                        lon = 0.0
                    else:
                        lat=float(letture['data'][i][4])
                        lon=float(letture['data'][i][5])
                    cod_cont=letture['data'][i][6]
                    riempimento=letture['data'][i][11]
                    data_ora_svuotamento=datetime.datetime.strptime(letture['data'][i][10], "%Y%m%d%H%M%S").strftime("%Y/%m/%d %H:%M:%S")
                    p_netto=letture['data'][i][12]
                    p_lordo=letture['data'][i][13]
                    p_tara=letture['data'][i][14]
                    id_percorso=letture['data'][i][15]
                    cod_percorso=letture['data'][i][16]
                    desc_percorso=letture['data'][i][17]
                    sportello=letture['data'][i][18]
                    modified=letture['data'][i][19]
                    # da usare in qualche modo
                    tipo_record=letture['data'][i][20]
                    dettaglio_record=letture['data'][i][21]
                    #logger.debug(tipo_record)
                    #logger.debug(dettaglio_record)
                    #exit()
                    if tipo_record != 0:
                        messaggio = f'''tipo_record={tipo_record}, 
                        id_idea = {id_idea}, 
                        id_pdr = {id_pdr}, 
                        dettaglio_record = {dettaglio_record}'''
                        #warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, '''API SVUOTAMENTI ID&A: E' arrivato un evento con tipo != 0 ''')
                        logger.info(messaggio)
                    if tipo_record in [0,1]:
                        if tipo_record==0 and cod_cont is None:
                            messaggio = f'''ATTENZIONE - svuotamento senza targa contenitore. 
                            tipo_record={tipo_record}, 
                            id_idea = {id_idea}, 
                            id_pdr = {id_pdr}, 
                            dettaglio_record = {dettaglio_record}'''
                            warning_message_mail(messaggio, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger, '''API SVUOTAMENTI ID&A: manca targa contenitore in svuotamento''')
                        # faccio inserimento
                        query_upsert='''INSERT INTO idea.svuotamenti 
                        (id_idea, id_piazzola, targa_contenitore, 
                        riempimento, data_ora_svuotamento, peso_netto,
                        peso_lordo, peso_tara, id_percorso_selezionato,
                        codice_percorso_selezionato, descrizione_percorso_selezionato, sportello,
                        modificato, dettaglio_record, 
                        tipo_record, geoloc)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, ST_SetSRID(ST_MakePoint(%s, %s),4326))
                        ON CONFLICT (id_idea)
                        DO UPDATE  
                        SET id_piazzola=EXCLUDED.id_piazzola, targa_contenitore=EXCLUDED.targa_contenitore, riempimento=EXCLUDED.riempimento,
                        data_ora_svuotamento=EXCLUDED.data_ora_svuotamento, peso_netto=EXCLUDED.peso_netto,  peso_lordo=EXCLUDED.peso_lordo,
                        peso_tara=EXCLUDED.peso_tara, id_percorso_selezionato=EXCLUDED.id_percorso_selezionato,
                        codice_percorso_selezionato=EXCLUDED.codice_percorso_selezionato,
                        descrizione_percorso_selezionato=EXCLUDED.descrizione_percorso_selezionato,
                        sportello=EXCLUDED.sportello, modificato=EXCLUDED.modificato,
                        dettaglio_record=EXCLUDED.dettaglio_record, tipo_record = EXCLUDED.tipo_record, 
                        geoloc = EXCLUDED.geoloc
                        '''
                        try:
                            #curr.execute(query_insert, (id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento, p_netto, p_lordo, p_tara, lon, lat))
                            curr.execute(query_upsert, (
                                id_idea, id_pdr, cod_cont,
                                riempimento, data_ora_svuotamento, p_netto,
                                p_lordo, p_tara, id_percorso,
                                cod_percorso, desc_percorso, sportello,
                                modified, dettaglio_record,
                                tipo_record, lon, lat
                                ))
                        except Exception as e:
                            logger.error(query_upsert)
                            logger.error(id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento,
                                         p_netto, p_lordo, p_tara, id_percorso, cod_percorso, desc_percorso, 
                                         sportello, modified, dettaglio_record,
                                tipo_record, lon, lat)
                            logger.error(e)
                    
                    else:
                        query_upsert='''INSERT INTO idea.svuotamenti_altro (
                            id_idea, data_ora, modificato, tipo_record, dettaglio_record, 
                            geoloc) 
                            VALUES (%s, %s, %s, %s, %s,
                            ST_SetSRID(ST_MakePoint(%s, %s),4326)
                            )
                            ON CONFLICT (id_idea) DO UPDATE  
                            SET data_ora =  EXCLUDED.data_ora, 
                            modificato = EXCLUDED.modificato, 
                            tipo_record =EXCLUDED.tipo_record,
                            dettaglio_record=EXCLUDED.dettaglio_record, 
                            geoloc=EXCLUDED.geoloc'''
                        try:
                            #curr.execute(query_insert, (id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento, p_netto, p_lordo, p_tara, lon, lat))
                            curr.execute(query_upsert, (
                                id_idea, data_ora_svuotamento, modified, tipo_record, dettaglio_record, 
                                lon, lat
                                ))
                        except Exception as e:
                            logger.error(query_upsert)
                            logger.error(id_idea, data_ora_svuotamento, modified, tipo_record, dettaglio_record, lon, lat)
                            logger.error(e)
                            

                    """
                    query_select='''SELECT * FROM idea.svuotamenti WHERE id_idea = %s;'''
                    try:
                        curr.execute(query_select, (id_idea,))
                        conferimento=curr.fetchall()
                    except Exception as e:
                        logger.error(query_select, id_idea)
                        logger.error(e)
                    curr.close()
                    curr = conn.cursor()
                    # se c'è già la entry faccio 
                    #logger.debug('Sono qua')
                    if len(conferimento)>0: 
                        query_update='''UPDATE idea.svuotamenti
                        SET id_piazzola=%s, riempimento=%s, peso_netto=%s, peso_lordo=%s, peso_tara=%s,
                        targa_contenitore=%s, data_ora_svuotamento=%s, id_percorso_selezionato=%s, codice_percorso_selezionato= %s ,
                        descrizione_percorso_selezionato = %s, sportello=%s, modificato=%s
                        WHERE id_idea=%s;'''
                        try:
                            #curr.execute(query_update, (id_pdr, riempimento, p_netto, p_lordo, p_tara, lon, lat, cod_cont, data_ora_svuotamento, id_idea))
                            curr.execute(query_update, (id_pdr, riempimento, p_netto, p_lordo, p_tara, cod_cont, data_ora_svuotamento, id_percorso, cod_percorso, desc_percorso, sportello, modified, id_idea))
                        except Exception as e:
                            logger.error(query_update)
                            logger.error(e)
                    else:
                        query_insert='''INSERT INTO idea.svuotamenti
                        (id_idea, id_piazzola, targa_contenitore, riempimento, data_ora_svuotamento, peso_netto, peso_lordo, peso_tara, 
                        id_percorso_selezionato, codice_percorso_selezionato, descrizione_percorso_selezionato, 
                        sportello, modificato)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
                        #logger.debug(query_insert)
                        try:
                            #curr.execute(query_insert, (id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento, p_netto, p_lordo, p_tara, lon, lat))
                            curr.execute(query_insert, (id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento, p_netto, p_lordo, p_tara, id_percorso, cod_percorso, desc_percorso, sportello, modified))
                        except Exception as e:
                            logger.error(query_insert, id_idea, id_pdr, cod_cont, riempimento, data_ora_svuotamento, p_netto, p_lordo, p_tara, id_percorso, cod_percorso, desc_percorso, sportello, modified)
                            logger.error(e)
                            
                    """
                    ########################################################################################
                    # da testare sempre prima senza fare i commit per verificare che sia tutto OK
                    conn.commit()
                    ########################################################################################
                #print(i,letture['data'][i][9], letture['data'][i][10], letture['data'][i][14], letture['data'][i][16],letture['data'][i][17])
                i+=1
            p+=1
   


    # faccio un check sulle date 
    curr.close()
    curr = conn.cursor()
    query_select='''select max(data_ora_svuotamento)
    from idea.svuotamenti ch'''
    try:
        curr.execute(query_select)
        max_date=curr.fetchall()
    except Exception as e:
        logging.error(e)
    
    for dd in max_date:
        max_data=dd[0] 
        
    
    if (datetime.datetime.now() - max_data) > datetime.timedelta(hours=24):
        logger.warning("interval = {0}".format(datetime.datetime.now() - max_data))
        receiver_email='roberto.marzocchi@amiu.genova.it'
        mail_cc='assterritorio@amiu.genova.it'
        
        
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = 'no_reply@amiu.genova.it'
        message["To"] = receiver_email
        message["To"] = mail_cc
        ####################################################
        message["Subject"] = 'WARNING - Ultimo svuotamento registrato > 24 ore'
        message["Bcc"] = mail_cc  # Recommended for mass emails
        message.preamble = "Ultimo svuotamento > 24 ore"

        body='''L'ultimo svuotamento scaricato tramite le API Id&A
        risale al <b>{0}</b>.
        <br><br>Verificare la correttezza dei dati
        {1}
        <img src="cid:image1" alt="Logo" width=197>
        <br>'''.format(max_data, footer_mail_idea)
            
                            
        # Add body to email
        message.attach(MIMEText(body, "html"))

        
        #aggiungo logo 
        logoname='{}/img/logo_amiu.jpg'.format(parentdir)
        immagine(message,logoname)
        
        #text = message.as_string()

        logger.info("Richiamo la funzione per inviare mail")
        invio=invio_messaggio(message)
        logger.info(invio)
        if invio==200:
            logger.info('Messaggio inviato')

        else:
            logger.error('Problema invio mail. Error:{}'.format(invio))


    logger.info("Chiudo definitivamente la connesione al DB")
    curr.close()
    conn.close()
    
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    #while i
    
    
    
    
if __name__ == "__main__":
    main()   