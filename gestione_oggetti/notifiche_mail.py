#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script gestisce le notifiche via MAIL dell'applicativo Gestione Oggetti (sviluppato da GRUPPO SIGLA)
Abbiamo realizzato uno script esterno per essere indipendenti nella gestione di eventuali modifiche

Lo script si divide in 2 parti e deve girare ogni 5':

- PARTE 1: invio mail di apertura
--> le mail di apertura debbono essere inviate tutte a un'unica mail ()




- PARTE 2: invio mail di chiusura / abort
--> le mail di chiusura debbono essere inviate a chi ha aperto l'intervento o a chi ha le notifiche attive


- PARTE 3: invio mail di aggiornamento
--> le mail di apertura debbono essere inviate tutte a un'unica mail ()


'''


from doctest import ELLIPSIS_MARKER
import os, sys, getopt, re
from dbus import DBusException  # ,shutil,glob
import requests
from requests.exceptions import HTTPError







import json


import inspect, os.path




import psycopg2
import sqlite3


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)

sys.path.append(parentdir)

#print(parentdir)
#exit()
#sys.path.append('../')

from credenziali import *
from invio_messaggio import *

#import requests
import datetime

import logging

filename = inspect.getframeinfo(inspect.currentframe()).filename
path = os.path.dirname(os.path.abspath(filename))

giorno_file=datetime.datetime.today().strftime('%Y%m%d%H%M')



#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{}/notifiche_mail.log'.format(path)
errorfile='{}/error_notifiche_mail.log'.format(path)
#if os.path.exists(logfile):
#    os.remove(logfile)

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
f_handler.setLevel(logging.INFO)


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


#################################################
try:
    logger.debug(len(sys.argv))
    if sys.argv[1]== 'prod':
        test=0
    else: 
        logger.error('Il parametro {} passato non è riconosciuto'.format(sys.argv[1]))
        exit()
except Exception as e:
    logger.info('Non ci sono parametri, sono in test')
    test=1


debug_email= 'roberto.marzocchi@amiu.genova.it'
if test==1:
    hh=host
    dd=db_test
    mail_notifiche_apertura='magmaco@amiu.genova.it'
    mail_notifiche_apertura=debug_email
    und_test='_TEST'
    oggetto= ' (TEST)'
    incipit_mail='''<p style="color:red"><b>Questa mail proviene dagli applicativi di TEST (SIT e Gestione oggetti).
     NON si tratta di un reale intervento</b></p>'''
else:
    hh=host
    dd=db
    mail_notifiche_apertura='magmaco@amiu.genova.it'
    und_test=''
    oggetto =''
    incipit_mail=''
#################################################


def connect():
    logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=dd,
                        port=port,
                        user=user_manut,
                        password=pwd_manut,
                        host=hh)
    return conn



def main():

    #################################################################
    """logger.info('Connessione al db SIT')
    conn = psycopg2.connect(dbname=dd,
                        port=port,
                        user=user,
                        password=pwd,
                        host=hh)
    """
    conn=connect()
    curr = conn.cursor()
    curr1 = conn.cursor()
    
    

    #conn.autocommit = True
    #################################################################

    query_select = '''SELECT e.intervento_id, to_char(i.data_creazione,'DD/MM/YYYY ore HH24:MI') as data_creazione , 
i.utente,su.email,
i.piazzola_id, i.elemento_id, e2.matricola, i.descrizione as descrizione_intervento, 
string_agg(ti.descrizione, ',') as tipo_intervento,
te.descrizione as tipo_elemento, m.descrizione as municipio,
q.nome as quartiere_comune,
u.descrizione as UT,
concat (v.nome, ', ', p.numero_civico) as indirizzo,
p.riferimento, p.note, e.id,
string_agg(distinct su2.email, ',') as cc, i.note_chiusura,
tp.id as id_priorita, tp.descrizione as priorita
FROM gestione_oggetti.email e 
JOIN gestione_oggetti.intervento i on e.intervento_id = i.id 
JOIN gestione_oggetti.intervento_tipo_intervento iti on iti.intervento_id = i.id 
JOIN gestione_oggetti.tipo_intervento ti on ti.id = iti.tipo_intervento_id
JOIN gestione_oggetti.tipo_priorita tp on tp.id = i.tipo_priorita_id
JOIN elem_temporanei.elementi e2 on e2.id_elemento = i.elemento_id
JOIN elem.tipi_elemento te on te.tipo_elemento = e2.tipo_elemento 
JOIN elem.piazzole p on p.id_piazzola =i.piazzola_id 
JOIN elem.aste a on a.id_asta = p.id_asta
JOIN topo.vie v on a.id_via = v.id_via
JOIN topo.comuni c on c.id_comune = v.id_comune
JOIN topo.ut u on a.id_ut = u.id_ut
JOIN topo.quartieri q on q.id_quartiere = a.id_quartiere
LEFT JOIN gestione_oggetti.notifica n on n.intervento_id = i.id 
LEFT JOIN util.sys_users su2 on n.utente = su2."name"
LEFT JOIN topo.municipi m on m.id_municipio = a.id_circoscrizione
LEFT JOIN util.sys_users su on i.utente = su."name"
WHERE e.data_invio is null and e.tipo_mail ilike %s
GROUP BY e.intervento_id, i.data_creazione, 
i.piazzola_id, i.elemento_id, e2.matricola, i.descrizione, 
te.descrizione, m.descrizione,
q.nome, u.descrizione, v.nome, p.numero_civico,
p.riferimento, p.note, i.utente, su.email, e.id, i.note_chiusura, tp.id , tp.descrizione
UNION 
SELECT e.intervento_id, to_char(i.data_creazione,'DD/MM/YYYY ore HH24:MI') as data_creazione ,
i.utente,su.email,
i.piazzola_id, i.elemento_id, e2.matricola, i.descrizione as descrizione_intervento, 
string_agg(ti.descrizione, ',') as tipo_intervento,
te.descrizione as tipo_elemento, m.descrizione as municipio,
q.nome as quartiere_comune,
u.descrizione as UT,
concat (v.nome, ', ', p.numero_civico) as indirizzo,
p.riferimento, p.note, e.id,
string_agg(distinct su2.email, ',') as cc, i.note_chiusura,
tp.id as id_priorita, tp.descrizione as priorita
FROM gestione_oggetti.email e 
JOIN gestione_oggetti.intervento i on e.intervento_id = i.id 
JOIN gestione_oggetti.intervento_tipo_intervento iti on iti.intervento_id = i.id 
JOIN gestione_oggetti.tipo_intervento ti on ti.id = iti.tipo_intervento_id
JOIN gestione_oggetti.tipo_priorita tp on tp.id = i.tipo_priorita_id 
JOIN elem.elementi e2 on e2.id_elemento = i.elemento_id
JOIN elem.tipi_elemento te on te.tipo_elemento = e2.tipo_elemento 
JOIN elem.piazzole p on p.id_piazzola =i.piazzola_id
JOIN elem.aste a on a.id_asta = p.id_asta
JOIN topo.vie v on a.id_via = v.id_via
JOIN topo.comuni c on c.id_comune = v.id_comune
JOIN topo.ut u on a.id_ut = u.id_ut
JOIN topo.quartieri q on q.id_quartiere = a.id_quartiere
LEFT JOIN gestione_oggetti.notifica n on n.intervento_id = i.id 
LEFT JOIN util.sys_users su2 on n.utente = su2."name"
LEFT JOIN topo.municipi m on m.id_municipio = a.id_circoscrizione
LEFT JOIN util.sys_users su on i.utente = su."name"
WHERE e.data_invio is null and e.tipo_mail ilike %s
GROUP BY e.intervento_id, i.data_creazione, 
i.piazzola_id, i.elemento_id, e2.matricola, i.descrizione, 
te.descrizione, m.descrizione,
q.nome, u.descrizione, v.nome, p.numero_civico,
p.riferimento, p.note, i.utente, su.email, e.id, i.note_chiusura, tp.id , tp.descrizione 
UNION
SELECT e.intervento_id, to_char(i.data_creazione,'DD/MM/YYYY ore HH24:MI') as data_creazione ,
i.utente,su.email,
i.piazzola_id, i.elemento_id, e2.matricola, i.descrizione as descrizione_intervento, 
string_agg(ti.descrizione, ',') as tipo_intervento,
te.descrizione as tipo_elemento, m.descrizione as municipio,
q.nome as quartiere_comune,
u.descrizione as UT,
concat (v.nome, ', ', p.numero_civico) as indirizzo,
p.riferimento, p.note,
e.id as id,
string_agg(distinct su2.email, ',') as cc, i.note_chiusura,
tp.id as id_priorita, tp.descrizione as priorita
FROM gestione_oggetti.email e 
JOIN gestione_oggetti.intervento i on e.intervento_id = i.id 
JOIN gestione_oggetti.intervento_tipo_intervento iti on iti.intervento_id = i.id 
JOIN gestione_oggetti.tipo_intervento ti on ti.id = iti.tipo_intervento_id
JOIN gestione_oggetti.tipo_priorita tp on tp.id = i.tipo_priorita_id  
left JOIN elem.elementi e2 on  i.elemento_id = e2.id_elemento
left JOIN elem.tipi_elemento te on te.tipo_elemento = e2.tipo_elemento 
left JOIN elem.piazzole p on p.id_piazzola =i.piazzola_id
left JOIN elem.aste a on a.id_asta = p.id_asta
left JOIN topo.vie v on a.id_via = v.id_via
left JOIN topo.comuni c on c.id_comune = v.id_comune
left JOIN topo.ut u on a.id_ut = u.id_ut
left JOIN topo.quartieri q on q.id_quartiere = a.id_quartiere
LEFT JOIN topo.municipi m on m.id_municipio = a.id_circoscrizione
LEFT JOIN gestione_oggetti.notifica n on n.intervento_id = i.id 
LEFT JOIN util.sys_users su2 on n.utente = su2."name"
LEFT JOIN util.sys_users su on i.utente = su."name"
WHERE e.data_invio is null and (e.tipo_mail ilike %s or e.tipo_mail ilike 'ABORTITO')  
GROUP BY e.intervento_id, i.data_creazione, 
i.piazzola_id, i.elemento_id, i.descrizione, 
i.utente, su.email, i.note_chiusura, e.id, tp.id , tp.descrizione,
te.descrizione, m.descrizione, q.nome, u.descrizione, v.nome, p.numero_civico, p.riferimento, p.note, e2.matricola 
ORDER BY data_creazione'''



    footer='''<hr>
<p>Questa è una mail automatica inviata dall'applicativo Gestione Oggetti.
L'applicativo è raggiungibile al seguente <a href="http://{0}/GestioneOggetti{1}/#/interventi">indirizzo</a>.<br>
Si prega di NON RISPONDERE alla presente mail. In caso di problemi con l'applicativo scrivere a assterritorio@amiu.genova.it
</p>'''.format(host, und_test)


    #####################################################################
    
    try:
        curr.execute(query_select, ('APERTO', 'APERTO', 'APERTO'))
        lista_interventi_apertura=curr.fetchall()
    except Exception as e:
        logger.error(e)

    c=0
    try:
        if len(lista_interventi_apertura) > 0:
            logger.info('Invio mail APERTURA')
            c=1
    except Exception as e:
        logger.info('Non ci sono nuovi interventi aperti')

    if c==1:
        for ii in lista_interventi_apertura:
            logger.debug(ii[19])
            if ii[19]==3:
                logger.info('Invio mail  apertura per intervento {}'.format(ii[0]))
                if ii[6] is None:
                    matr='nd'
                else: 
                    matr=ii[6]
                # compongo la mail
                body='''{16}
        L'utente {0} ({1}) in data {2} ha creato il seguente intervento (Priorità <b>{17}</b>):<br>
        <ul>
        <li> Tipo intervento:   {3}</li>
        <li> Descr Intervento:  {4}</li>
        <li> Piazzola:          <a href="http://{13}/SIT{14}/#!/home/edit-piazzola/{5}/">{5}</a> (Rif:{6} Note:{7})</li>
        <li> Indirizzo:         {8}</li>
        <li> UT:                {9}</li>
        <li> Quartiere/comune:  {10}</li>
        <li> Elemento:           Tipo: {11} (matr. {12})</li>
        </ul>
        Le informazioni di cui sopra si intendono sostitutive del modulo 816 rev. 3 e sono memorizzate sull'applicativo
        {15}
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''.format(ii[2], ii[3], ii[1], ii[8], ii[7], ii[4], ii[14], ii[15], ii[13], ii[12], ii[11], ii[9], matr, host, und_test, footer, incipit_mail, ii[20])

                logger.debug(body)  


                # messaggio='Test invio messaggio'


                subject = "NUOVO INTERVENTO RICHIESTO{}".format(oggetto)
                #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
                sender_email = user_mail
                #receiver_email='assterritorio@amiu.genova.it'
                #debug_email='roberto.marzocchi@amiu.genova.it'

                receiver_email=mail_notifiche_apertura

                # Create a multipart message and set headers
                message = MIMEMultipart()
                message["From"] = 'no_reply@amiu.genova.it'
                message["To"] = receiver_email
                message["Subject"] = subject
                message["Bcc"] = debug_email  # Recommended for mass emails
                message.preamble = "Nuovo intervento"


                    
                                    
                # Add body to email
                message.attach(MIMEText(body, "html"))



                # aggiunto allegato (usando la funzione importata)
                #allegato(message, file_variazioni, nome_file)
                # Add body to email
                #message.attach(MIMEText(body, "plain"))
                

                logoname='{}/img/logo_amiu.jpg'.format(parentdir)
                immagine(message,logoname)

                #text = message.as_string()

                logging.info("Richiamo la funzione per inviare mail")
                invio=invio_messaggio(message)
                

                if invio==200:
                    query_update='''UPDATE gestione_oggetti.email
                    SET data_invio=now(), "destinatario_A"=%s 
                    WHERE id=%s
                    '''
                    try:
                        curr1.execute(query_update, (receiver_email,ii[16]))
                    except Exception as e:
                        logger.error(e)
                    #logging.info(invio)
                    # COMMIT
                    logger.info('Faccio il commit')
                    conn.commit()
                else:
                    logging.error('Problema invio mail. Error:{}'.format(invio))
            # se non era da inviare la mail imposto comunque una data ma non metto l'indirizzo
            else:
                logger.debug('Sono qua') 
                query_update='''UPDATE gestione_oggetti.email
                    SET data_invio=now() 
                    WHERE id=%s
                '''
                try:
                    curr1.execute(query_update, (ii[16],))
                except Exception as e:
                    logger.error(e)
                #logging.info(invio)
                # COMMIT
                logger.info('Faccio il commit')
                conn.commit()


    curr.close()
    curr1.close()
    conn.close()
    conn=connect()
    curr = conn.cursor()
    curr1 = conn.cursor()

    #####################################################################
    # 2 CHIUSURA
    try:
        curr.execute(query_select, ('CHIUSO', 'CHIUSO', 'CHIUSO'))
        lista_interventi_chiusura=curr.fetchall()
    except Exception as e:
        logger.error(e)
    
    c=0
    try: 
        logger.debug(len(lista_interventi_chiusura))
        
        if len(lista_interventi_chiusura) > 0:
            logger.info('Invio mail CHIUSURA')
            c=1
    except Exception as e:
        logger.info('Non ci sono nuovi interventi CHIUSI')


    if c==1:
        for ii in lista_interventi_chiusura:
            logger.info('Invio mail chiusura per intervento {}'.format(ii[0]))
            if ii[6] is None:
                matr='nd'
            else: 
                matr=ii[6]
            # compongo la mail
            body='''{16}
    Gent. {0} <br>

    L'intervento con id {1} è stato chiuso con le seguenti note {2}
    
    <br><br>Dettagli intervento:<br>
    <ul>
    <li> Tipo intervento:    {3}</li>
    <li> Descr Intervento:   {4}</li>
    <li> Piazzola:          <a href="http://{13}/SIT{14}/#!/home/edit-piazzola/{5}/">{5}</a> (Rif:{6} Note:{7})</li>
    <li> Indirizzo:         {8}</li>
    <li> UT:                {9}</li>
    <li> Quartiere/comune:  {10}</li>
    <li> Elemento:           Tipo: {11} (matr. {12})</li>
    <li> Data apertura:     {17}</li>
    </ul>
    {15}
    <img src="cid:image1" alt="Logo" width=197>
    <br>
    '''.format(ii[2], ii[0], ii[18], ii[8], ii[7], ii[4], ii[14], ii[15], ii[13], ii[12], ii[11], ii[9], matr, host, und_test, footer, incipit_mail, ii[1])

            logger.debug(body)  


            # messaggio='Test invio messaggio'


            subject = "INTERVENTO CHIUSO{}".format(oggetto)
            #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
            sender_email = user_mail

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = 'no_reply@amiu.genova.it'
            message["To"] =ii[3]
            if ii[17] != None:
                message["CC"] = ii[17]
            message["Subject"] = subject
            message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Intervento chiuso"


                
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))

            # aggiunto allegato (usando la funzione importata)
            #allegato(message, file_variazioni, nome_file)
            # Add body to email
            #message.attach(MIMEText(body, "plain"))
            
            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(parentdir)
            immagine(message,logoname)
            
            #text = message.as_string()

            logging.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            
            if invio==200:
                query_update='''UPDATE gestione_oggetti.email
                SET data_invio=now(), "destinatario_A"=%s 
                WHERE id=%s
                '''
                try:
                    curr1.execute(query_update, (ii[3],ii[16]))
                except Exception as e:
                    logger.error(e)

                if ii[17] != None:
                    query_update='''UPDATE gestione_oggetti.email
                    SET "destinatario_CC"=%s 
                    WHERE id=%s
                    '''
                    try:
                        curr1.execute(query_update, (ii[17],ii[16]))
                    except Exception as e:
                        logger.error(e)   
                #logging.info(invio)
                # COMMIT
                logger.info('Faccio il commit')
                conn.commit()
            else:
                logging.error('Problema invio mail. Error:{}'.format(invio))
    

    curr.close()
    curr1.close()
    conn.close()
    conn=connect()
    curr = conn.cursor()
    curr1 = conn.cursor()


    #####################################################################
    # 4 ABORT
    try:
        curr.execute(query_select, ('ABORTITO', 'ABORTITO' , 'ABORTITO'))
        lista_interventi_chiusura=curr.fetchall()
    except Exception as e:
        logger.error(e)
    
    c=0
    try: 
        logger.debug(len(lista_interventi_chiusura))
        
        if len(lista_interventi_chiusura) > 0:
            logger.info('Invio mail ABORTED')
            c=1
    except Exception as e:
        logger.info('Non ci sono nuovi interventi ABORTITI')


    if c==1:
        for ii in lista_interventi_chiusura:
            logger.info('Invio mail per intervento {} abortito'.format(ii[0]))
            if ii[6] is None:
                matr='nd'
            else: 
                matr=ii[6]
            # compongo la mail in casi diversi dal "Nuovo Posizionamento"
            if ii[9] is None:
                body='''{6}
        Gent. {0} <br>

        L'intervento con id {1} conteneva delle informazioni inesatte ed è stato rigettato da <b><i>Manutenzioni Contenitori
        </i></b> con le seguenti note <br><br><b>{2}</b>
        
        <br><br>Dettagli intervento:<br>
        <ul>
        <li> Tipo intervento:    {3}</li>
        <li> Descr Intervento:   {4}</li>
        <li> Data apertura:     {5}</li>
        </ul>
        {7}
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''.format(ii[2], ii[0], ii[18], ii[8], ii[7], ii[1], incipit_mail, footer)

            else:
                body='''{16}
        Gent. {0} <br>

        L'intervento con id {1} conteneva delle informazioni inesatte ed è stato rigettato da <b><i>Manutenzioni Contenitori
        </i></b> con le seguenti note <br><br><b>{2}</b>
        
        <br><br>Dettagli intervento:<br>
        <ul>
        <li> Tipo intervento:    {3}</li>
        <li> Descr Intervento:   {4}</li>
        <li> Piazzola:          <a href="http://{13}/SIT{14}/#!/home/edit-piazzola/{5}/">{5}</a> (Rif:{6} Note:{7})</li>
        <li> Indirizzo:         {8}</li>
        <li> UT:                {9}</li>
        <li> Quartiere/comune:  {10}</li>
        <li> Elemento:           Tipo: {11} (matr. {12})</li>
        <li> Data apertura:     {2}</li>
        </ul>
        {15}
        <img src="cid:image1" alt="Logo" width=197>
        <br>
        '''.format(ii[2], ii[0], ii[18], ii[8], ii[7], ii[4], ii[14], ii[15], ii[13], ii[12], ii[11], ii[9], matr, host, und_test, footer, incipit_mail)

            logger.debug(body)  


            # messaggio='Test invio messaggio'


            subject = "INTERVENTO CON INFORMAZIONI INESATTE NON ESEGUITO {}".format(oggetto)
            #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
            sender_email = user_mail

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = 'no_reply@amiu.genova.it'
            message["To"] =ii[3]
            if ii[17] != None:
                message["CC"] = ii[17]
            message["Subject"] = subject
            message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Intervento con informazioni inesatte"


                
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))

            # aggiunto allegato (usando la funzione importata)
            #allegato(message, file_variazioni, nome_file)
            # Add body to email
            #message.attach(MIMEText(body, "plain"))
            
            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(parentdir)
            immagine(message,logoname)
            
            #text = message.as_string()

            logging.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            
            if invio==200:
                query_update='''UPDATE gestione_oggetti.email
                SET data_invio=now(), "destinatario_A"=%s 
                WHERE id=%s
                '''
                try:
                    curr1.execute(query_update, (ii[3],ii[16]))
                except Exception as e:
                    logger.error(e)
                
                if ii[17] != None:
                    query_update='''UPDATE gestione_oggetti.email
                    SET "destinatario_CC"=%s 
                    WHERE id=%s
                    '''
                    try:
                        curr1.execute(query_update, (ii[17],ii[16]))
                    except Exception as e:
                        logger.error('16:{}, 17:{}'.format(ii[16], ii[17]))
                        logger.error(e)   
                #logging.info(invio)
                # COMMIT
                logger.info('Faccio il commit')
                conn.commit()
            else:
                logging.error('Problema invio mail. Error:{}'.format(invio))

    

    curr.close()
    curr1.close()
    conn.close()
    conn=connect()
    curr = conn.cursor()
    curr1 = conn.cursor()

    #####################################################################
    # 3 AGGIORNATO

   

    try:
        curr.execute(query_select, ('AGGIORNATO', 'AGGIORNATO', 'AGGIORNATO'))
        lista_interventi_agg=curr.fetchall()
    except Exception as e:
        logger.error(e)
        logger.debug('Va in errore')
    c=0
    try:  
        if len(lista_interventi_agg) > 0:
            logger.info('Invio mail AGGIORNAMENTO')
            c=1
    except Exception as e:
        logger.info('Non ci sono nuovi interventi AGGIORNATI')


    if c==1:
        for ii in lista_interventi_agg:
            logger.info('Invio mail  aggiornamento intervento {}'.format(ii[0]))
            if ii[6] is None:
                matr='nd'
            else: 
                matr=ii[6]
            # compongo la mail
            body='''{16}
    L'utente {0} ({1}) in data {2} ha aggiornato il seguente intervento:<br>
    <ul>
    <li> Tipo intervento:    {3}</li>
    <li> Descr Intervento:   {4}</li>
        <li> Piazzola:          <a href="http://{13}/SIT{14}/#!/home/edit-piazzola/{5}/">{5}</a> (Rif:{6} Note:{7})</li>
    <li> Indirizzo:         {8}</li>
    <li> UT:                {9}</li>
    <li> Quartiere/comune:  {10}</li>
    <li> Elemento:           Tipo: {11} (matr. {12})</li>
    </ul>
    Le informazioni di cui sopra si intendono sostitutive del modulo 816 rev. 3 e sono memorizzate sull'applicativo
    {15}
    <img src="cid:image1" alt="Logo" width=197>
    <br>
    '''.format(ii[2], ii[3], ii[1], ii[8], ii[7], ii[4], ii[14], ii[15], ii[13], ii[12], ii[11], ii[9], matr, host, und_test, footer, incipit_mail)

            logger.debug(body)  


            # messaggio='Test invio messaggio'


            subject = "NUOVO AGGIORNAMENTO SU INTERVENTO RICHIESTO{}".format(oggetto)
            #body = "Report giornaliero delle variazioni.\n Giorno {}\n\n".format(giorno_file)
            sender_email = user_mail
            #receiver_email='assterritorio@amiu.genova.it'
            #debug_email='roberto.marzocchi@amiu.genova.it'

            receiver_email=mail_notifiche_apertura

            # Create a multipart message and set headers
            message = MIMEMultipart()
            message["From"] = 'no_reply@amiu.genova.it'
            message["To"] = receiver_email
            message["Subject"] = subject
            message["Bcc"] = debug_email  # Recommended for mass emails
            message.preamble = "Nuovo aggiornamento intervento"


                
                                
            # Add body to email
            message.attach(MIMEText(body, "html"))

            # aggiunto allegato (usando la funzione importata)
            #allegato(message, file_variazioni, nome_file)
            # Add body to email
            #message.attach(MIMEText(body, "plain"))
            
            #aggiungo logo 
            logoname='{}/img/logo_amiu.jpg'.format(parentdir)
            immagine(message,logoname)
            
            #text = message.as_string()

            logging.info("Richiamo la funzione per inviare mail")
            invio=invio_messaggio(message)
            
            if invio==200:
                query_update='''UPDATE gestione_oggetti.email
                SET data_invio=now(), "destinatario_A"=%s 
                WHERE id=%s
                '''
                try:
                    curr1.execute(query_update, (receiver_email,ii[16]))
                except Exception as e:
                    logger.error(e)
                #logging.info(invio)
                # COMMIT
                logger.info('Faccio il commit')
                conn.commit()
            else:
                logging.error('Problema invio mail. Error:{}'.format(invio))





    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'roberto.marzocchi@amiu.genova.it', os.path.basename(__file__), logger)
    logger.info("chiudo le connessioni in maniera definitiva")

    curr.close()
    curr1.close()
    conn.close()




if __name__ == "__main__":
    main()  