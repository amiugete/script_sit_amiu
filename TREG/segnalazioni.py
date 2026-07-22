#!/usr/bin/env python
# -*- coding: utf-8 -*-

# AMIU copyleft 2026
# Roberta Fagandini, Roberto Marzocchi

'''
Scopo dello script è 

1) importare i dati delle segnalazioni da xls scaricato da SITO (per ora, in futuro da WS) e caricarli su DB SIT

2)....


'''

#from msilib import type_short
import os, sys, re  # ,shutil,glob
import inspect
from pathlib import Path

import requests
from requests.exceptions import HTTPError

import json


#import getopt  # per gestire gli input

#import pymssql

from datetime import date, datetime, timedelta

import locale

import xlsxwriter

# per leggere file excel
#from python_calamine import CalamineWorkbook
import openpyxl


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


from tappa_prevista import *

from crea_dizionario_da_query import *

import uuid


from datetime import date
import holidays



filename = inspect.getframeinfo(inspect.currentframe()).filename
path=os.path.dirname(sys.argv[0]) 
path1 = os.path.dirname(os.path.dirname(os.path.abspath(filename)))
nome=os.path.basename(__file__).replace('.py','')
#tmpfolder=tempfile.gettempdir() # get the current temporary directory
logfile='{0}/log/{1}.log'.format(path,nome)
errorfile='{0}/log/error_{1}.log'.format(path,nome)
#if os.path.exists(logfile):
#    os.remove(logfile)








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

import time

#variabile che specifica se devo fare test ekovision oppure no
test_ekovision=0
    

# da capire come gestisce datetime e date
    
def normalize(v):
    if v is None:
        return None

    # date / datetime -> lascia così (psycopg2 li gestisce)
    if isinstance(v, (datetime, date)):
        return v
    
    # stringhe
    if isinstance(v, str):
        v = v.strip()

        if v == "":
            return None

        if v.lower() in ("sì", "si"):
            return True

        if v.lower() == "no":
            return False

        # prova a convertire una data/ora
        try:
            dt = datetime.strptime(v, "%d/%m/%Y %H:%M")
            if dt.hour == 0 and dt.minute == 0:
                return dt.date()
            else:
                return dt
        except ValueError:
            pass
        
        return v

    # float tipo 10.0 -> 10
    if isinstance(v, float):
        if v.is_integer():
            return int(v)
        return False
        
    

    
    return v



def main():


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
    
    
    
    logger.info('Il PID corrente è {0}'.format(os.getpid()))

    

    # Get today's date
    #presentday = datetime.now() # or presentday = datetime.today()
    oggi=datetime.today()
    #logging.debug('Oggi {}'.format(oggi))
    
    oggi_char=oggi.strftime('%Y%m%d%H%M%S')
    
    
    
    
    ################################################################
    # QUERY
    ################################################################
    select_istat = '''select c.cod_istat, descr_comune
    from topo.comuni c
    where similarity(upper(descr_comune),upper(%s))>=0.71'''
    
   
    # per passare la tupla devo usare execute_values, altrimenti mi da errore di sintassi
    from psycopg2.extras import execute_values
    
    upsert='''INSERT INTO treg_sito.richieste (
            numero, tipo, stato,
            arera, categorie, 
            comune, cod_istat, codice_municipio, nominativo,
            cf_piva, email, telefono, 
            cod_tari, ind_residenza, ind_evento,
            data_evento, descrizione, richiesta,
            richiesta_aggiuntiva, info_aggiuntive, proprietario,
            gestito_da, risposta, data_risposta,
            dataora_annullamento, motivo_annullamento,
            dataora_inoltro_a_gest, dataora_inoltro_da_gest,
            dataora_risp_cittadino, dataora_sopralluogo,
            dataora_creazione, dataora_aggiornamento,
            dataora_gestione, dataora_chiusura) 
            VALUES %s
            ON CONFLICT (numero) DO UPDATE 
            SET tipo=EXCLUDED.tipo, stato=EXCLUDED.stato, 
            arera=EXCLUDED.arera, categorie=EXCLUDED.categorie, cod_istat=EXCLUDED.cod_istat, 
            comune=EXCLUDED.comune, codice_municipio=EXCLUDED.codice_municipio, nominativo=EXCLUDED.nominativo, 
            cf_piva=EXCLUDED.cf_piva, email=EXCLUDED.email, telefono=EXCLUDED.telefono, 
            cod_tari=EXCLUDED.cod_tari, ind_residenza=EXCLUDED.ind_residenza, ind_evento=EXCLUDED.ind_evento, 
            data_evento=EXCLUDED.data_evento, descrizione=EXCLUDED.descrizione, richiesta=EXCLUDED.richiesta, 
            richiesta_aggiuntiva=EXCLUDED.richiesta_aggiuntiva, info_aggiuntive=EXCLUDED.info_aggiuntive, proprietario=EXCLUDED.proprietario, 
            gestito_da=EXCLUDED.gestito_da, risposta=EXCLUDED.risposta, data_risposta=EXCLUDED.data_risposta, 
            dataora_annullamento=EXCLUDED.dataora_annullamento, motivo_annullamento=EXCLUDED.motivo_annullamento, 
            dataora_inoltro_a_gest=EXCLUDED.dataora_inoltro_a_gest, dataora_inoltro_da_gest=EXCLUDED.dataora_inoltro_da_gest, 
            dataora_risp_cittadino=EXCLUDED.dataora_risp_cittadino, dataora_sopralluogo=EXCLUDED.dataora_sopralluogo, 
            dataora_creazione=EXCLUDED.dataora_creazione, dataora_aggiornamento=EXCLUDED.dataora_aggiornamento, 
            dataora_gestione=EXCLUDED.dataora_gestione, dataora_chiusura=EXCLUDED.dataora_chiusura'''
    
    
    # alla fine controllo ultimo aggiornamento e se è più vecchio di 7 giorni mando mail di warning, 
    query_controllo_aggiornamento= '''SELECT
        case 
            when 
            (now() - max(r.dataora_aggiornamento)) > interval '7 day'
            then 1
            else 0
        end as check_aggiornamento
        from treg_sito.richieste r   ''' 
    
    
    # connessione a SIT
    nome_db=db
    logger.info('Connessione al db {}'.format(nome_db))
    conn = psycopg2.connect(dbname=nome_db,
                        port=port,
                        user=user,
                        password=pwd,
                        host=host)

    curr = conn.cursor()
      
    
    # da qualche parte salveremo i file excel processati in modo da non processarli più di una volta
    
    # cerca se c'è un file excel nella cartella, se c'è lo processa, altrimenti esce
    # richiede Path pathlib --> from pathlib import Path
    
      
    cartella=  Path(f'{path}/segnalazioni_nuovo_sito')
    # cerca file Excel (.xlsx e .xls)
    excel_files = list(cartella.glob("*.xlsx")) + list(cartella.glob("*.xls"))
    logger.debug('Len: {}'.format(len(excel_files)))
    logger.debug('Lista file: {}'.format(excel_files))
    #exit()
    if len(excel_files) == 1:
        logger.info("Trovato un file Excel:")
        for f in excel_files:
            logger.info(f" - {f.name}")
        
        
            df = openpyxl.load_workbook(f"{path}/segnalazioni_nuovo_sito/{f.name}")
            df1 = df.active

            #
            for row in df1.iter_rows(min_row=2, values_only=True):    
                row = list(row)
                
                """for i, v in enumerate(row):
                    logger.debug(
                    f"data_inizio={row[13]!r} tipo={type(row[15])}"
                    )
                """
                row = [normalize(v) for v in row]
                #logger.debug('comune_grezzo {}'.format(row[5]))
                raw_comune = row[5]

                if raw_comune and str(raw_comune).startswith("Genova"):
                    parts = str(raw_comune).split()
                    comune = parts[0]
                    cod_municipio = parts[1] if len(parts) > 1 else None
                else:
                    comune = raw_comune
                    cod_municipio = None

                try:
                    curr.execute(select_istat, (comune,))
                    res = curr.fetchone()
                    if res :
                        cod_istat = res[0] 
                    else:
                        logger.error(f"Errore nel recupero del codice ISTAT per il comune '{comune}'") 
                        continue
                except Exception:
                    cod_istat = None
                    logger.error(f"Errore nel recupero del codice ISTAT per il comune '{comune}'")
                    #salta l’iterazione corrente del ciclo e passa alla successiva
                    continue

                row.insert(6, cod_istat)
                row.insert(7, int(cod_municipio) if cod_municipio else None)
                #logger.debug('row{}'.format(row))
                #logger.debug(len(row))
                try: 
                    tupla=tuple(row)
                except Exception as e:
                    logger.error(e)
                    logger.error('Problema con riga {}'.format(row))
                    exit()
                #logger.debug('tupla {}'.format(tupla))
                try:
                    # per passare la tupla non posso usare curr.execute, altrimenti mi da errore di sintassi, devo usare
                    execute_values(curr, upsert, [tuple(row)])
                    #curr.execute(upsert, tuple(row))
                    #conn.commit()
                except Exception as e:
                    logger.error(e)
                    logger.error('Problema con riga {}'.format(row))
                    exit()
                    
                #exit()
                
                
            conn.commit()
            sorgente = Path(f"{path}/segnalazioni_nuovo_sito/{f.name}")
            destinazione = Path(f"{path}/segnalazioni_nuovo_sito/archive/{oggi_char}_{f.name}")

            sorgente.replace(destinazione)
    
    elif len(excel_files) > 1:
        logger.error("Trovati più file Excel. Per favore, processa un file alla volta. Uscita.")        
    else:
        logger.warning("Nessun nuovo file Excel trovato. Uscita.")
    
    
    # alla fine controllo ultimo aggiornamento e se è più vecchio di 7 giorni mando mail di warning,
    try:
        curr.execute(query_controllo_aggiornamento)
        res = curr.fetchone()
        if res and res[0] == 1:
            # Calendario delle festività italiane
            it_holidays = holidays.IT()
            data_oggi = date.today()
            logger.info(f"Oggi è {data_oggi} (giorno = {data_oggi.day}, mese = {data_oggi.month})")
            if data_oggi.weekday() >= 5:  # 5 = sabato, 6 = domenica
                logger.warning("""Attenzione: l'ultimo aggiornamento delle segnalazioni è più vecchio di 7 giorni, 
                        ma essendo sabato o domenica inutile avvisare.""")
            # aggiungo le festività italiane e il 24 giugno (festa di san Giovanni, patrono di Genova) 
            # che è un giorno in cui molti uffici sono chiusi
            elif data_oggi in it_holidays or (data_oggi.month == 6 and data_oggi.day ==24): 
                logger.warning("""Attenzione: l'ultimo aggiornamento delle segnalazioni è più vecchio di 7 giorni, 
                        ma essendo giorno festivo inutile avvisare.""")
            else:
                logger.info("""Oggi è un giorno lavorativo.""")
                messaggio_warning ="""Attenzione: l'ultimo aggiornamento delle segnalazioni è più vecchio di 7 giorni.
                <ul> 
                <li>Andare sul <a href="https://admin.amiu.genova.it">backoffice</a> del nuovo sito</li>
                <li>Rimuovere filtro su sole segnalazioni cliccando su <i>'Tutti'</i> nella prima card</li>
                <li>Scaricare excel</li>
                <li>Aprire file xls e convertirlo in xlsx (lode a Wordpress e ai suoi *** di plugin)</li>
                <li>Salvare file xlsx nella cartella <a href="file://///amiupostgres\SegnalazioniNuovoSitosegnalazioni_nuovo_sito">
                    amiupostgres\SegnalazioniNuovoSitosegnalazioni_nuovo_sito
                </a> dove verrà automaticamente processato fra mezz'ora al massimo</li>
                """
                
                warning_message_mail(messaggio_warning, 
                                'assterritorio@amiu.genova.it, pianar@amiu.genova.it, davide.berninzone@amiu.genova.it', os.path.basename(__file__), logger, 'Attenzione: aggiornamento segnalazioni')
        else:
            logger.info("L'ultimo aggiornamento delle segnalazioni è recente.")
    except Exception as e:
        logger.error("Errore durante il controllo dell'ultimo aggiornamento: {}".format(e))
        
        
    # check se c_handller contiene almeno una riga 
    error_log_mail(errorfile, 'assterritorio@amiu.genova.it', os.path.basename(__file__), logger)
    
    
    logger.info("chiudo le connessioni in maniera definitiva")
    curr.close()
    conn.close()
    














if __name__ == "__main__":
    main()      