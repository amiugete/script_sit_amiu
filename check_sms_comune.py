#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2021
# Roberto Marzocchi

'''
Lo script verifica verifica che ci siano stati caricati i dati  relativi allo spazzamento meccanizzato da inviare al Comune tramite il portale:
  - se trova i dati li salva in un csv, lo invia ad assterritorio per il caricamento su portale del Comune e scrive sulla tabella 
  - se non li trova invia mail di remind ai responsabili delle UT


   - lo script deve girare solo dal 15 del mese, e per prima cosa deve verificare se è già stato creato il csv per il mese successivo a quello corrente,
     se si esce altrimenti inizia la creazione facendo le verifiche al punto sotto
   - verificare quali ut hanno caricato i dati rispetto a quelle memorizzate nella tabella etl.csv_sms_comune_ut
        - se nessuna --> invio mail di warning
        - se entrambe --> creo csv e salvo l'info nella tabella etl.csv_sms_comune
        - se una delle due --> mando warning a ut che non ha ancora caricato. Creo il csv solo se ci sono i dati di tutte le ut salvate in etl.csv_sms_comune_ut
        - se trovo dati caricati da altre ut oltre a quelle salvate in etl.csv_sms_comune_ut aggiorno la tabella con id nuova ut
'''

import os, sys, re  # ,shutil,glob
import inspect, os.path

import csv

import psycopg2

import cx_Oracle

from datetime import date, datetime, timedelta

from credenziali import *


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


def main():
    logger.info('Il PID corrente è {0}'.format(os.getpid()))


    oggi = datetime.now()
    mese = oggi.month
    anno = oggi.year
    giorno = oggi.day

    # Se dicembre, passa a gennaio dell'anno successivo
    if mese == 12:
        next_mese = 1
        anno += 1
    else:
        next_mese = mese + 1

    #giorno = 18

    if 15 <= giorno <= 31:
        logging.info('Connessione al db SIT')
        conn = psycopg2.connect(dbname=db,
                port=port,
                user=user,
                password=pwd,
                host=host)

        curr = conn.cursor()
        #conn.autocommit = True

        logging.info('Connessione al db UO')
        cx_Oracle.init_oracle_client(percorso_oracle) # necessario configurare il client oracle correttamente
        #cx_Oracle.init_oracle_client() # necessario configurare il client oracle correttamente
        parametri_con='{}/{}@//{}:{}/{}'.format(user_uo,pwd_uo, host_uo,port_uo,service_uo)
        logger.debug(parametri_con)
        con = cx_Oracle.connect(parametri_con)
        logger.info("Versione ORACLE: {}".format(con.version))
        cur = con.cursor()

        # verifico se il csv è già stato creato per il mese successivo
        query_csv = "select * from etl.csv_sms_comune where mese=%s and anno=%s ;"
        try:
            curr.execute(query_csv, (str(next_mese), str(anno),))
            lista_csv=curr.fetchall()
            logger.debug(lista_csv)
        except Exception as e:
            logger.error(query_csv)
            logger.error(e)
        
        if len(lista_csv)==0:
            # seleziono le ut che sappiamo inviano i dati
            query_ut = "select id_ut from etl.csv_sms_comune_ut"
            try:
                curr.execute(query_ut)
                ut=curr.fetchall()
                ut = [u[0] for u in ut]
                logger.debug(ut)
            except Exception as e:
                logger.error(query_ut)
                logger.error(e)

            # seleziono le ut che hanno inviato i dati per vedere se ce n'è qualcuna in più
            query_ut_csv = '''
            SELECT DISTINCT u.DESCR_UT , u.ID_UT, u.EMAIL, to_char(td.DTA_PROSS_INTERV, 'YYYYMM') AS prossimo_intervento
            FROM UNIOPE.SM_ANAGR_TRATTIVIA_DIN td
            INNER JOIN UNIOPE.SM_ANAGR_TRATTIVIA_ST  ts  ON td.ID_TRATTO =ts.ID_TRATTO
            INNER JOIN STRADE.STRADE s ON s.CODICE_VIA = ts.ID_VIA
            INNER JOIN STRADE.UNITATERRITORIALI u ON u.ID_UT =s.ID_UO
            WHERE to_char(td.DTA_PROSS_INTERV, 'YYYYMM')  = to_char(sysdate, 'YYYYMM') +1
            '''
            try:
                cur.execute(query_ut_csv)
                lista_ut=cur.fetchall()
                logger.debug(lista_ut)
            except Exception as e:
                logger.error(query_ut_csv)
                logger.error(e)

            #verifico se hanno caricato i dati altre UT oltre alla 8 e la 9
            check_new_ut = 0
            
            for lut in lista_ut:
                if lut[1] in ut:
                    logger.debug("{} è in lista tabella ut".format(lut[1]))
                    check_new_ut += 1
                else:
                    logger.debug("{} NON è in etl.csv_sms_comune_ut e la aggiungo".format(lut[1]))
                    query_in_ut= "INSERT INTO etl.csv_sms_comune_ut (id_ut, data_inserimento) VALUES(%s, now());"
                    try:
                        curr.execute(query_in_ut, (lut[1],))
                        conn.commit()
                    except Exception as e:
                        logger.error(query_in_ut)
                        logger.error(e)
            
            lista_ut_upload = [lup[1] for lup in lista_ut]
            check_data = 0
            send_mail_to = []
            for u in ut:
                if u in lista_ut_upload:
                    check_data += 1
                else:
                    send_mail_to.append(u)

            if check_data >= len(lista_ut):
                # verifico se tutte le UT che ci aspettiamo e/o qualcuna in più hanno caricato i dati, se si creo il CSV
                query_dati = '''
                    SELECT 
                    'SPAZZAMENTO MECCANIZZATO' AS "Tipo Servizio/Intervento",
                    TO_CHAR(
                        FROM_TZ(
                        TO_TIMESTAMP(
                            TO_CHAR(td.DTA_PROSS_INTERV, 'YYYY-MM-DD') || ' ' || 
                            LPAD(td.ORA_INIZIO, 5, '0'),
                            'YYYY-MM-DD HH24:MI'
                        ),
                        'Europe/Rome'
                        ) AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    ) AS "Data Inizio Intervento",
                    TO_CHAR(
                        FROM_TZ(
                        TO_TIMESTAMP(
                            TO_CHAR(td.DTA_PROSS_INTERV, 'YYYY-MM-DD') || ' ' || 
                            LPAD(td.ORA_FINE, 5, '0'),
                            'YYYY-MM-DD HH24:MI'
                        ),
                        'Europe/Rome'
                        ) AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    ) AS "Data fine Intervento", 
                    s.NOME2 AS "Strada", 
                    s.CODICE_VIA AS "Codice Strada",
                    c.DESCR_CIRC AS "Divisione",
                    c.id_circ AS "Codice Divisione",
                    q.DESCR_QUART AS "Circoscrizione",
                    q.ID_QUART AS "Codice Circoscrizione", 
                    ts.ESTREMI_TRATTO AS "Note",
                    u.ID_UT
                    FROM UNIOPE.SM_ANAGR_TRATTIVIA_DIN td
                    INNER JOIN UNIOPE.SM_ANAGR_TRATTIVIA_ST  ts  ON td.ID_TRATTO =ts.ID_TRATTO
                    INNER JOIN STRADE.STRADE s ON s.CODICE_VIA = ts.ID_VIA
                    INNER JOIN STRADE.CIRCOSCRIZIONI c  ON s.CIRCOSCRIZIONE = c.ID_CIRC 
                    INNER JOIN strade.QUARTIERI q ON s.QUARTIERE = q.ID_QUART 
                    INNER JOIN STRADE.UNITATERRITORIALI u ON u.ID_UT =s.ID_UO
                    WHERE to_char(td.DTA_PROSS_INTERV, 'YYYYMM') = to_char(sysdate, 'YYYYMM') +1
                    ORDER BY c.id_circ, td.DTA_PROSS_INTERV ASC
                '''
                try:
                    cur.execute(query_dati)
                    lista_interventi=cur.fetchall()
                    #logger.debug(lista_interventi)
                except Exception as e:
                    logger.error(query_dati)
                    logger.error(e)
                
                if len(lista_interventi) > 0:
                    logger.info("Trovati {} interventi per lo spazzamento meccanizzato per il mese {}/{}".format(len(lista_interventi), next_mese, anno))
                    logging.info('Scrivo il file spazzamento_sms_comune_{}_{}.csv'.format(next_mese, anno))
                    nomi_colonne = [row[0] for row in cur.description]
                    nomi_colonne = nomi_colonne[:-1] #tolgo ultima colonna

                    #logging.info(nomi_colonne)

                    lista_interventi_csv = [li[:-1] for li in lista_interventi] #tolgo ultima colonna

                    nome_file = "spazzamento_sms_comune_{}_{}.csv".format(next_mese, anno)
                    file_csv="{0}/csv_spazz_comune/{1}".format(path,nome_file)
                    # Salva su CSV
                    with open(file_csv, mode="w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f, delimiter=",")
                        writer.writerow(nomi_colonne) 
                        writer.writerows(lista_interventi_csv)

                    # faccio insert in etl.csv_sms_comune
                    query_in_csv= '''
                    INSERT INTO etl.csv_sms_comune
                    (id, file, mese, anno, data_inserimento)
                    VALUES(nextval('etl.csv_sms_comune_id_seq'::regclass), %s, %s, %s, now());
                    '''
                    try:
                        curr.execute(query_in_csv, (nome_file, next_mese, anno,))
                        conn.commit()
                    except Exception as e:
                        logger.error(query_in_csv)
                        logger.error(e)

                    receiver_email='assterritorio@amiu.genova.it'
                    debug_email='roberta.fagandini@amiu.genova.it'

                    # Create a multipart message and set headers
                    message = MIMEMultipart()
                    message["From"] = sender_email
                    message["To"] = receiver_email
                    message["Subject"] = "csv per portale Comune"
                    #message["Bcc"] = debug_email  # Recommended for mass emails
                    message.preamble = "File csv da caricare su portale Comune"

                    body = "In allegato il file csv con gli spazzamenti da caricare sul portale del Comune. <br>"
                    
                                    
                    # Add body to email
                    message.attach(MIMEText(body, "html"))


                    #aggiungo logo 
                    logoname='{}/img/logo_amiu.jpg'.format(path)
                    immagine(message,logoname)
                    
                    
                    # aggiunto allegato (usando la funzione importata)
                    allegato(message, file_csv, nome_file)
                    #text = message.as_string()

                    logger.info("Richiamo la funzione per inviare mail")
                    invio=invio_messaggio(message)
                    logger.info(invio)
                else:
                    logger.info("Non sono stati trovati interventi per lo spazzamento meccanizzato per il mese {}/{}".format(next_mese, anno))
            else:
                # mando warning a tutte le ut da cui mi aspetto i dati ma che non sono ancora stati caricati
                for smt in send_mail_to:
                    for lu in lista_ut:
                        if smt == lu[1]:
                            logging.info('devo mandare warning a {}'.format(lu[2]))

                            receiver_email=lu[2]
                            debug_email='roberta.fagandini@amiu.genova.it'

                            # Create a multipart message and set headers
                            message = MIMEMultipart()
                            message["From"] = sender_email
                            message["To"] = receiver_email
                            message["CC"] = 'assterritorio@amiu.genova.it'
                            message["Subject"] = "Dati spazzamento meccanizzato"
                            #message["Bcc"] = debug_email  # Recommended for mass emails
                            message.preamble = "Non sono ancora stati caricati i dati per lo spazzamento meccanizzato"

                            body = "Non sono ancora stati caricati i dati per lo spazzamento meccanizzato. Si prega di procedere quanto prima così da poter trasmettere i dati al Comune."
                            
                                            
                            # Add body to email
                            message.attach(MIMEText(body, "html"))
            
                            logger.info("Richiamo la funzione per inviare mail")
                            invio=invio_messaggio(message)
                            logger.info(invio)

        else:
            logger.info("il file csv è già stato creato per il {}/{}".format(next_mese, anno))
    else:
        logger.info("Oggi è il {} quindi lo script non viene eseguito. Deve essere eseguito dal 15 in poi.".format(giorno))
    

if __name__ == "__main__":
    main()